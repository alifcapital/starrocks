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

// Unicode 17 lowercase followed by the existing Tajik name substitutions.

#include "common/compiler_util.h"
#include "exprs/string_functions.h"
#include "util/utf8_case.h"

namespace starrocks {

// Canonicalize ж/Ж variants by collapsing дж → ж on emit. Both Ҷ→дж and
// "Дж/дж" written natively converge on a single ж byte pair, so e.g.
// Раҷабов / Раджабов / Ражабов all normalize to one stem.
static inline void emit_zh(uint8_t* dst, size_t& di) {
    // Consume the whole preceding д run: deduplication would otherwise leave
    // a fresh дж pair, making a second norm_tj call change the result again.
    while (di >= 2 && dst[di - 2] == 0xD0 && dst[di - 1] == 0xB4) {
        di -= 2;
    }
    dst[di++] = 0xD0;
    dst[di++] = 0xB6;
}

// Lowercase Cyrillic consonants after Unicode casing and Tajik substitutions.
static inline bool is_cyr_consonant_lower(uint8_t b0, uint8_t b1) {
    if (b0 == 0xD0) {
        // б B1, в B2, г B3, д B4, ж B6, з B7, к BA, л BB, м BC, н BD, п BF
        return b1 == 0xB1 || b1 == 0xB2 || b1 == 0xB3 || b1 == 0xB4 || b1 == 0xB6 || b1 == 0xB7 || b1 == 0xBA ||
               b1 == 0xBB || b1 == 0xBC || b1 == 0xBD || b1 == 0xBF;
    }
    if (b0 == 0xD1) {
        // р 80, с 81, т 82, ф 84, х 85, ц 86, ч 87, ш 88, щ 89
        return b1 == 0x80 || b1 == 0x81 || b1 == 0x82 || b1 == 0x84 || b1 == 0x85 || b1 == 0x86 || b1 == 0x87 ||
               b1 == 0x88 || b1 == 0x89;
    }
    return false;
}

// Collapse consecutive identical Cyrillic consonants (Хассан→Хасан,
// Мухаммад→Мухамад). In-place since w <= r always.
static inline size_t dedup_doubled_consonants(uint8_t* dst, size_t len) {
    size_t r = 0;
    size_t w = 0;
    while (r < len) {
        uint8_t b0 = dst[r];
        if ((b0 == 0xD0 || b0 == 0xD1) && r + 1 < len) {
            uint8_t b1 = dst[r + 1];
            if (w >= 2 && dst[w - 2] == b0 && dst[w - 1] == b1 && is_cyr_consonant_lower(b0, b1)) {
                r += 2;
                continue;
            }
            dst[w++] = b0;
            dst[w++] = b1;
            r += 2;
            continue;
        }
        if (b0 < 0x80) {
            dst[w++] = b0;
            r++;
            continue;
        }
        if ((b0 & 0xF0) == 0xE0 && r + 2 < len) {
            dst[w++] = dst[r++];
            dst[w++] = dst[r++];
            dst[w++] = dst[r++];
            continue;
        }
        if ((b0 & 0xF8) == 0xF0 && r + 3 < len) {
            dst[w++] = dst[r++];
            dst[w++] = dst[r++];
            dst[w++] = dst[r++];
            dst[w++] = dst[r++];
            continue;
        }
        dst[w++] = dst[r++];
    }
    return w;
}

// Input is already lowercased by the shared Unicode 17 converter. Only the
// application-specific substitutions belong here; unrelated bytes are preserved.
static size_t normalize_tj_fast(const uint8_t* src, size_t src_len, uint8_t* dst) {
    size_t si = 0;
    size_t di = 0;
    while (si < src_len) {
        uint8_t b0 = src[si];
        if (b0 >= 0xD0 && b0 <= 0xD3 && si + 1 < src_len && (src[si + 1] & 0xC0) == 0x80) {
            uint32_t cp = ((b0 & 0x1F) << 6) | (src[si + 1] & 0x3F);
            switch (cp) {
            case 0x0436: // ж, including a preceding дж run
            case 0x04B7: // ҷ
                emit_zh(dst, di);
                si += 2;
                continue;
            case 0x0451:
                cp = 0x0435;
                break;   // ё -> е
            case 0x045E: // ў
            case 0x04EF:
                cp = 0x0443;
                break; // ӯ -> у
            case 0x049B:
                cp = 0x043A;
                break; // қ -> к
            case 0x04B3:
                cp = 0x0445;
                break; // ҳ -> х
            case 0x04E3:
                cp = 0x0438;
                break; // ӣ -> и
            case 0x0493:
                cp = 0x0433;
                break; // ғ -> г
            default:
                break;
            }
            dst[di++] = 0xC0 | (cp >> 6);
            dst[di++] = 0x80 | (cp & 0x3F);
            si += 2;
        } else {
            dst[di++] = src[si++];
        }
    }
    return di;
}

StatusOr<ColumnPtr> StringFunctions::norm_tj(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto& col = columns[0];
    const size_t num_rows = col->is_constant() ? 1 : col->size();

    ColumnViewer<TYPE_VARCHAR> viewer(col);

    auto result = BinaryColumn::create();
    auto& result_offsets = result->get_offset();
    auto& result_bytes = result->get_bytes();

    size_t total_src_bytes = 0;
    for (size_t i = 0; i < num_rows; i++) {
        if (!viewer.is_null(i)) {
            total_src_bytes += viewer.value(i).size;
        }
    }
    result_bytes.resize(total_src_bytes);
    result_offsets.resize(num_rows + 1);
    result_offsets[0] = 0;

    size_t dst_offset = 0;
    std::string lowered;

    for (size_t row = 0; row < num_rows; row++) {
        if (viewer.is_null(row)) {
            result_offsets[row + 1] = dst_offset;
            continue;
        }

        auto src = viewer.value(row);
        utf8_tolower(src.data, src.size, lowered);
        const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(lowered.data());
        size_t src_len = lowered.size();

        if (result_bytes.size() < dst_offset + src_len) {
            result_bytes.resize(dst_offset + src_len);
        }

        uint8_t* dst_ptr = result_bytes.empty() ? nullptr : result_bytes.data() + dst_offset;
        size_t written = normalize_tj_fast(src_ptr, src_len, dst_ptr);
        written = dedup_doubled_consonants(dst_ptr, written);

        if (written >= Column::MAX_CAPACITY_LIMIT - dst_offset) {
            return Status::InvalidArgument("Normalized name column exceeds binary column capacity");
        }
        dst_offset += written;
        result_offsets[row + 1] = dst_offset;
    }

    result_bytes.resize(dst_offset);

    if (col->has_null()) {
        auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result), NullColumn::create(*nullable->null_column()));
    }

    if (col->is_constant()) {
        result->resize(1);
        return ConstColumn::create(std::move(result), col->size());
    }
    return result;
}

} // namespace starrocks
