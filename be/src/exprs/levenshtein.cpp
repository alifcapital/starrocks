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

#include "exprs/string_functions.h"

#include "thirdparty/stringzilla/types.hpp"
#include "thirdparty/stringzillas/similarities.hpp"
#include "util/utf8.h"

namespace starrocks {

namespace sz = ashvardanian::stringzilla;
namespace szs = ashvardanian::stringzillas;

// fuzzywuzzy-style costs: match=0, mismatch=2, gap=1 (Indel distance)
static const szs::uniform_substitution_costs_t kLevenshteinSubs{0, 2};
static const szs::linear_gap_costs_t kLevenshteinGaps{1};

// UTF-8 scorer with SIMD when available
#if defined(__AVX512BW__) && defined(__VAES__)
using LevenshteinScorer = szs::levenshtein_distance_utf8<char, szs::linear_gap_costs_t,
                                                          std::allocator<char>, sz_caps_si_k>;
#else
using LevenshteinScorer = szs::levenshtein_distance_utf8<char, szs::linear_gap_costs_t,
                                                          std::allocator<char>, sz_cap_serial_k>;
#endif

// Thread-local scorer instance to avoid repeated allocations
static thread_local LevenshteinScorer tls_scorer(kLevenshteinSubs, kLevenshteinGaps);

StatusOr<ColumnPtr> StringFunctions::levenshtein_distance(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto& col1 = columns[0];
    const auto& col2 = columns[1];

    size_t num_rows = col1->size();
    auto result = Int32Column::create();
    result->reserve(num_rows);

    ColumnViewer<TYPE_VARCHAR> viewer1(col1);
    ColumnViewer<TYPE_VARCHAR> viewer2(col2);

    for (size_t i = 0; i < num_rows; i++) {
        if (viewer1.is_null(i) || viewer2.is_null(i)) {
            result->append(0);
            continue;
        }

        auto s1 = viewer1.value(i);
        auto s2 = viewer2.value(i);

        sz::span<const char> span1(s1.data, s1.size);
        sz::span<const char> span2(s2.data, s2.size);

        size_t dist = 0;
        auto status = tls_scorer(span1, span2, dist);

        if (status != szs::status_t::success_k) {
            return Status::InvalidArgument("levenshtein_distance requires valid UTF-8 input");
        }

        result->append(static_cast<int32_t>(dist));
    }

    return result;
}

StatusOr<ColumnPtr> StringFunctions::levenshtein_ratio(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto& col1 = columns[0];
    const auto& col2 = columns[1];

    size_t num_rows = col1->size();
    auto result = DoubleColumn::create();
    result->reserve(num_rows);

    ColumnViewer<TYPE_VARCHAR> viewer1(col1);
    ColumnViewer<TYPE_VARCHAR> viewer2(col2);

    for (size_t i = 0; i < num_rows; i++) {
        if (viewer1.is_null(i) || viewer2.is_null(i)) {
            result->append(0.0);
            continue;
        }

        auto s1 = viewer1.value(i);
        auto s2 = viewer2.value(i);

        // Empty strings: ratio is 1.0 if both empty, 0.0 if only one empty
        if (s1.size == 0 && s2.size == 0) {
            result->append(1.0);
            continue;
        }
        if (s1.size == 0 || s2.size == 0) {
            result->append(0.0);
            continue;
        }

        sz::span<const char> span1(s1.data, s1.size);
        sz::span<const char> span2(s2.data, s2.size);

        size_t dist = 0;
        auto status = tls_scorer(span1, span2, dist);

        if (status != szs::status_t::success_k) {
            return Status::InvalidArgument("levenshtein_ratio requires valid UTF-8 input");
        }

        // Count UTF-8 codepoints for ratio calculation (fuzzywuzzy-style).
        // Use SIMD-accelerated counter.
        size_t len1 = utf8_len(s1.data, s1.data + s1.size);
        size_t len2 = utf8_len(s2.data, s2.data + s2.size);
        size_t total_len = len1 + len2;

        // fuzzywuzzy formula: ratio = (len1 + len2 - dist) / (len1 + len2)
        double ratio = 1.0 - static_cast<double>(dist) / static_cast<double>(total_len);
        result->append(ratio);
    }

    return result;
}

} // namespace starrocks
