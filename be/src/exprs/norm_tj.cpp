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

// Tajik Cyrillic normalization: lowercase + transliteration in single pass
// Uses arithmetic (no lookup tables) following StringZilla approach

#include "exprs/string_functions.h"
#include "common/compiler_util.h"

namespace starrocks {

static inline size_t normalize_tj_fast(const uint8_t* __restrict src, size_t src_len,
                                        uint8_t* __restrict dst) {
    size_t si = 0;
    size_t di = 0;

    while (si < src_len) {
        uint8_t b0 = src[si];

        // ASCII fast path - branchless lowercase
        if (LIKELY(b0 < 0x80)) {
            // 'A'-'Z' → 'a'-'z': add 0x20 if in range
            uint8_t is_upper = (b0 >= 'A') & (b0 <= 'Z');
            dst[di++] = b0 + (is_upper << 5);  // +32 if upper
            si++;
            continue;
        }

        // 2-byte UTF-8
        if ((b0 & 0xE0) == 0xC0 && si + 1 < src_len) {
            uint8_t b1 = src[si + 1];
            if (UNLIKELY((b1 & 0xC0) != 0x80)) {
                dst[di++] = b0;
                si++;
                continue;
            }

            // D0: Basic Cyrillic uppercase + Ѐ-Џ
            if (b0 == 0xD0) {
                // Ё (D0 81) → е (D0 B5) - Tajik transliteration
                if (UNLIKELY(b1 == 0x81)) {
                    dst[di++] = 0xD0;
                    dst[di++] = 0xB5;
                    si += 2;
                    continue;
                }
                // Ў (D0 8E) → у (D1 83) - Uzbek/Belarusian short U
                if (UNLIKELY(b1 == 0x8E)) {
                    dst[di++] = 0xD1;
                    dst[di++] = 0x83;
                    si += 2;
                    continue;
                }
                // Ѐ-Џ (D0 80-8F) → ѐ-џ (D1 90-9F): lead D0→D1, +0x10
                if (b1 <= 0x8F) {
                    dst[di++] = 0xD1;
                    dst[di++] = b1 + 0x10;
                    si += 2;
                    continue;
                }
                // А-П (D0 90-9F) → а-п (D0 B0-BF): +0x20
                if (b1 <= 0x9F) {
                    dst[di++] = 0xD0;
                    dst[di++] = b1 + 0x20;
                    si += 2;
                    continue;
                }
                // Р-Я (D0 A0-AF) → р-я (D1 80-8F): lead D0→D1, -0x20
                if (b1 <= 0xAF) {
                    dst[di++] = 0xD1;
                    dst[di++] = b1 - 0x20;
                    si += 2;
                    continue;
                }
                // а-п (D0 B0-BF): already lowercase
                dst[di++] = b0;
                dst[di++] = b1;
                si += 2;
                continue;
            }

            // D1: Basic Cyrillic lowercase р-я, ѐ-џ
            if (b0 == 0xD1) {
                // ё (D1 91) → е (D0 B5) - Tajik transliteration
                if (UNLIKELY(b1 == 0x91)) {
                    dst[di++] = 0xD0;
                    dst[di++] = 0xB5;
                    si += 2;
                    continue;
                }
                // ў (D1 9E) → у (D1 83) - Uzbek/Belarusian short U
                if (UNLIKELY(b1 == 0x9E)) {
                    dst[di++] = 0xD1;
                    dst[di++] = 0x83;
                    si += 2;
                    continue;
                }
                // р-џ (D1 80-9F): already lowercase
                dst[di++] = b0;
                dst[di++] = b1;
                si += 2;
                continue;
            }

            // D2-D3: Extended Cyrillic (Tajik-specific)
            if (b0 == 0xD2 || b0 == 0xD3) {
                uint32_t cp = ((b0 & 0x1F) << 6) | (b1 & 0x3F);
                switch (cp) {
                    case 0x049A: case 0x049B:  // Қ/қ → к
                        dst[di++] = 0xD0; dst[di++] = 0xBA;
                        si += 2; continue;
                    case 0x04B2: case 0x04B3:  // Ҳ/ҳ → х
                        dst[di++] = 0xD1; dst[di++] = 0x85;
                        si += 2; continue;
                    case 0x04EE: case 0x04EF:  // Ӯ/ӯ → у
                        dst[di++] = 0xD1; dst[di++] = 0x83;
                        si += 2; continue;
                    case 0x04E2: case 0x04E3:  // Ӣ/ӣ → и
                        dst[di++] = 0xD0; dst[di++] = 0xB8;
                        si += 2; continue;
                    case 0x0492: case 0x0493:  // Ғ/ғ → г
                        dst[di++] = 0xD0; dst[di++] = 0xB3;
                        si += 2; continue;
                    case 0x04B6: case 0x04B7:  // Ҷ/ҷ → дж
                        dst[di++] = 0xD0; dst[di++] = 0xB4;
                        dst[di++] = 0xD0; dst[di++] = 0xB6;
                        si += 2; continue;
                }
                // Other extended: copy as-is
                dst[di++] = b0;
                dst[di++] = b1;
                si += 2;
                continue;
            }

            // Other 2-byte: copy as-is
            dst[di++] = b0;
            dst[di++] = b1;
            si += 2;
            continue;
        }

        // 3-byte UTF-8
        if ((b0 & 0xF0) == 0xE0 && si + 2 < src_len) {
            dst[di++] = src[si++];
            dst[di++] = src[si++];
            dst[di++] = src[si++];
            continue;
        }

        // 4-byte UTF-8
        if ((b0 & 0xF8) == 0xF0 && si + 3 < src_len) {
            dst[di++] = src[si++];
            dst[di++] = src[si++];
            dst[di++] = src[si++];
            dst[di++] = src[si++];
            continue;
        }

        // Invalid/incomplete
        dst[di++] = src[si++];
    }

    return di;
}

StatusOr<ColumnPtr> StringFunctions::norm_tj(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto& col = columns[0];
    const size_t num_rows = col->size();

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
    result_bytes.resize(total_src_bytes * 2);
    result_offsets.resize(num_rows + 1);
    result_offsets[0] = 0;

    size_t dst_offset = 0;

    for (size_t row = 0; row < num_rows; row++) {
        if (viewer.is_null(row)) {
            result_offsets[row + 1] = dst_offset;
            continue;
        }

        auto src = viewer.value(row);
        const uint8_t* src_ptr = reinterpret_cast<const uint8_t*>(src.data);
        size_t src_len = src.size;

        if (result_bytes.size() < dst_offset + src_len * 2) {
            result_bytes.resize(dst_offset + src_len * 2);
        }

        uint8_t* dst_ptr = result_bytes.data() + dst_offset;
        size_t written = normalize_tj_fast(src_ptr, src_len, dst_ptr);

        dst_offset += written;
        result_offsets[row + 1] = dst_offset;
    }

    result_bytes.resize(dst_offset);

    if (col->has_null()) {
        auto* nullable = down_cast<const NullableColumn*>(col.get());
        return NullableColumn::create(std::move(result), nullable->null_column());
    }

    return result;
}

} // namespace starrocks
