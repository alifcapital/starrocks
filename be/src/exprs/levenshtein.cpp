// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Levenshtein edit distance using StringZilla SIMD implementation
// Supports UTF-8 codepoint-level distance (not byte-level)

#include <array>
#include <cstddef>
#include <limits>
#include <vector>

#include "column/column_builder.h"
#include "exprs/string_functions.h"
#include "thirdparty/stringzillas/similarities.hpp"
#include "util/utf8.h"
#include "util/utf8_check.h"

namespace starrocks {
namespace {
namespace sz = ashvardanian::stringzilla;
namespace szs = ashvardanian::stringzillas;

// Keep the original SQL contract: replacement costs two edits (Indel distance).
#if SZ_USE_ICELAKE
constexpr sz_capability_t kSimilarityCaps = sz_caps_sil_k;
#elif SZ_USE_HASWELL
constexpr sz_capability_t kSimilarityCaps = sz_caps_sh_k;
#else
constexpr sz_capability_t kSimilarityCaps = sz_cap_serial_k;
#endif
using LevenshteinScorer = szs::levenshtein_distance_utf8<szs::linear_gap_costs_t, kSimilarityCaps>;

struct alignas(64) ScratchBlock {
    std::array<std::byte, 64> bytes;
};

class DistanceScratch {
public:
    StatusOr<size_t> score(const Slice& first, const Slice& second) {
        // The StringZilla scorer uses an unchecked decoder. Validate each slice first,
        // including empty/nonempty pairs, so it cannot read into the following row.
        if (!validate_utf8(first.data, first.size) || !validate_utf8(second.data, second.size)) {
            return Status::InvalidArgument("Levenshtein functions require valid UTF-8 input");
        }
        if (first.size == 0) return utf8_len(second.data, second.data + second.size);
        if (second.size == 0) return utf8_len(first.data, first.data + first.size);
        sz::span<const char> a(first.data, first.size);
        sz::span<const char> b(second.data, second.size);
        size_t bytes = _scorer.scratch_space_needed(a, b, _specs);
        size_t blocks = bytes / sizeof(ScratchBlock) + (bytes % sizeof(ScratchBlock) != 0);
        if (blocks > _scratch.size()) _scratch.resize(blocks);
        size_t distance = 0;
        szs::scratch_space_t scratch(reinterpret_cast<std::byte*>(_scratch.data()),
                                     _scratch.size() * sizeof(ScratchBlock));
        auto status = _scorer(a, b, distance, scratch, _executor, _specs);
        if (status != szs::status_t::success_k) {
            return Status::InternalError("StringZilla could not compute Levenshtein distance");
        }
        return distance;
    }

private:
    LevenshteinScorer _scorer{szs::uniform_substitution_costs_t{0, 2}, szs::linear_gap_costs_t{1}};
    sz::cpu_specs_t _specs;
    szs::dummy_executor_t _executor;
    // Reuse across rows of this chunk; release at the end of the call, not at thread exit.
    std::vector<ScratchBlock> _scratch;
};
} // namespace

StatusOr<ColumnPtr> StringFunctions::levenshtein_distance(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    ColumnViewer<TYPE_VARCHAR> first(columns[0]);
    ColumnViewer<TYPE_VARCHAR> second(columns[1]);
    ColumnBuilder<TYPE_INT> result(columns[0]->size());
    DistanceScratch scratch;
    for (size_t row = 0; row < columns[0]->size(); ++row) {
        if (first.is_null(row) || second.is_null(row)) {
            result.append_null();
            continue;
        }
        ASSIGN_OR_RETURN(size_t distance, scratch.score(first.value(row), second.value(row)));
        if (distance > std::numeric_limits<int32_t>::max()) {
            return Status::InvalidArgument("Levenshtein distance exceeds INT range");
        }
        result.append(static_cast<int32_t>(distance));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

StatusOr<ColumnPtr> StringFunctions::levenshtein_ratio(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);
    ColumnViewer<TYPE_VARCHAR> first(columns[0]);
    ColumnViewer<TYPE_VARCHAR> second(columns[1]);
    ColumnBuilder<TYPE_DOUBLE> result(columns[0]->size());
    DistanceScratch scratch;
    for (size_t row = 0; row < columns[0]->size(); ++row) {
        if (first.is_null(row) || second.is_null(row)) {
            result.append_null();
            continue;
        }
        Slice a = first.value(row);
        Slice b = second.value(row);
        ASSIGN_OR_RETURN(size_t distance, scratch.score(a, b));
        size_t total = utf8_len(a.data, a.data + a.size) + utf8_len(b.data, b.data + b.size);
        result.append(total == 0 ? 1.0 : 1.0 - static_cast<double>(distance) / static_cast<double>(total));
    }
    return result.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks
