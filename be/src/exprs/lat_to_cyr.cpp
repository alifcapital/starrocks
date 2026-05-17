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

// Latin-to-Cyrillic transliteration for Uzbek/Tajik/English-spelled names.
// Greedy left-to-right: digraphs (kh, gh, zh, ch, sh, oo, ee, o', g') match
// before single letters. Non-Latin bytes (Cyrillic, punctuation, digits)
// are copied through unchanged. Output is Basic Cyrillic; chain into norm_tj
// to fold ҷ/қ/ҳ etc., lowercase, and canonicalize дж→ж.

#include "common/compiler_util.h"
#include "exprs/string_functions.h"

namespace starrocks {

// Detect any apostrophe-like character at position si; return its byte length
// (0 if not an apostrophe). Covers ASCII ', U+2018/2019 (curly quotes),
// U+02BB/02BC (modifier letter turned comma / apostrophe — Uzbek o', g' convention).
static inline int apostrophe_bytes_at(const uint8_t* src, size_t si, size_t src_len) {
    if (si >= src_len) return 0;
    uint8_t b = src[si];
    if (b == '\'') return 1;
    if (si + 1 < src_len && b == 0xCA && (src[si + 1] == 0xBB || src[si + 1] == 0xBC)) {
        return 2;
    }
    if (si + 2 < src_len && b == 0xE2 && src[si + 1] == 0x80 && (src[si + 2] == 0x98 || src[si + 2] == 0x99)) {
        return 3;
    }
    return 0;
}

// Emit a single Basic Cyrillic letter (b0,b1 form the lowercase UTF-8 pair).
// If upper, convert to uppercase using arithmetic on the Basic Cyrillic range.
static inline void emit_cyr(uint8_t* dst, size_t& di, uint8_t b0, uint8_t b1, bool upper) {
    if (!upper) {
        dst[di++] = b0;
        dst[di++] = b1;
        return;
    }
    if (b0 == 0xD0 && b1 >= 0xB0) {
        // а-п (D0 B0-BF) → А-П (D0 90-9F)
        dst[di++] = 0xD0;
        dst[di++] = b1 - 0x20;
    } else if (b0 == 0xD1 && b1 <= 0x8F) {
        // р-я (D1 80-8F) → Р-Я (D0 A0-AF)
        dst[di++] = 0xD0;
        dst[di++] = b1 + 0x20;
    } else {
        dst[di++] = b0;
        dst[di++] = b1;
    }
}

static inline size_t lat_to_cyr_fast(const uint8_t* __restrict src, size_t src_len, uint8_t* __restrict dst) {
    size_t si = 0;
    size_t di = 0;

    while (si < src_len) {
        uint8_t b = src[si];

        // Latin letter detection
        bool upper = (b >= 'A' && b <= 'Z');
        bool lower = (b >= 'a' && b <= 'z');
        if (!upper && !lower) {
            // Standalone apostrophe (not o'/g' — those are caught below): drop.
            int ap = apostrophe_bytes_at(src, si, src_len);
            if (ap > 0) {
                si += ap;
                continue;
            }
            // Non-Latin byte: copy through (Cyrillic, punctuation, digits, multi-byte UTF-8).
            dst[di++] = b;
            si++;
            continue;
        }

        uint8_t lc = upper ? (b + 0x20) : b;

        // Two-letter digraphs (case based on first letter)
        if (si + 1 < src_len) {
            uint8_t nb = src[si + 1];
            uint8_t nlc = (nb >= 'A' && nb <= 'Z') ? (nb + 0x20) : nb;

            // kh → х
            if (lc == 'k' && nlc == 'h') {
                emit_cyr(dst, di, 0xD1, 0x85, upper);
                si += 2;
                continue;
            }
            // gh → г
            if (lc == 'g' && nlc == 'h') {
                emit_cyr(dst, di, 0xD0, 0xB3, upper);
                si += 2;
                continue;
            }
            // zh → ж
            if (lc == 'z' && nlc == 'h') {
                emit_cyr(dst, di, 0xD0, 0xB6, upper);
                si += 2;
                continue;
            }
            // ch → ч
            if (lc == 'c' && nlc == 'h') {
                emit_cyr(dst, di, 0xD1, 0x87, upper);
                si += 2;
                continue;
            }
            // sh → ш
            if (lc == 's' && nlc == 'h') {
                emit_cyr(dst, di, 0xD1, 0x88, upper);
                si += 2;
                continue;
            }
            // oo → у
            if (lc == 'o' && nlc == 'o') {
                emit_cyr(dst, di, 0xD1, 0x83, upper);
                si += 2;
                continue;
            }
            // ee → и
            if (lc == 'e' && nlc == 'e') {
                emit_cyr(dst, di, 0xD0, 0xB8, upper);
                si += 2;
                continue;
            }
        }

        // Apostrophe digraphs: o' → у, g' → г (Uzbek convention)
        if (lc == 'o') {
            int ap = apostrophe_bytes_at(src, si + 1, src_len);
            if (ap > 0) {
                emit_cyr(dst, di, 0xD1, 0x83, upper);
                si += 1 + ap;
                continue;
            }
        }
        if (lc == 'g') {
            int ap = apostrophe_bytes_at(src, si + 1, src_len);
            if (ap > 0) {
                emit_cyr(dst, di, 0xD0, 0xB3, upper);
                si += 1 + ap;
                continue;
            }
        }

        // Single-letter mappings
        switch (lc) {
        case 'a':
            emit_cyr(dst, di, 0xD0, 0xB0, upper);
            break;
        case 'b':
            emit_cyr(dst, di, 0xD0, 0xB1, upper);
            break;
        case 'c':
            emit_cyr(dst, di, 0xD0, 0xBA, upper);
            break;
        case 'd':
            emit_cyr(dst, di, 0xD0, 0xB4, upper);
            break;
        case 'e':
            emit_cyr(dst, di, 0xD0, 0xB5, upper);
            break;
        case 'f':
            emit_cyr(dst, di, 0xD1, 0x84, upper);
            break;
        case 'g':
            emit_cyr(dst, di, 0xD0, 0xB3, upper);
            break;
        case 'h':
            emit_cyr(dst, di, 0xD1, 0x85, upper);
            break;
        case 'i':
            emit_cyr(dst, di, 0xD0, 0xB8, upper);
            break;
        case 'j': {
            // j → дж; for upper J propagate case to both letters based on next char
            bool second_upper = upper && si + 1 < src_len && src[si + 1] >= 'A' && src[si + 1] <= 'Z';
            emit_cyr(dst, di, 0xD0, 0xB4, upper);
            emit_cyr(dst, di, 0xD0, 0xB6, second_upper);
            break;
        }
        case 'k':
            emit_cyr(dst, di, 0xD0, 0xBA, upper);
            break;
        case 'l':
            emit_cyr(dst, di, 0xD0, 0xBB, upper);
            break;
        case 'm':
            emit_cyr(dst, di, 0xD0, 0xBC, upper);
            break;
        case 'n':
            emit_cyr(dst, di, 0xD0, 0xBD, upper);
            break;
        case 'o':
            emit_cyr(dst, di, 0xD0, 0xBE, upper);
            break;
        case 'p':
            emit_cyr(dst, di, 0xD0, 0xBF, upper);
            break;
        case 'q':
            emit_cyr(dst, di, 0xD0, 0xBA, upper);
            break;
        case 'r':
            emit_cyr(dst, di, 0xD1, 0x80, upper);
            break;
        case 's':
            emit_cyr(dst, di, 0xD1, 0x81, upper);
            break;
        case 't':
            emit_cyr(dst, di, 0xD1, 0x82, upper);
            break;
        case 'u':
            emit_cyr(dst, di, 0xD1, 0x83, upper);
            break;
        case 'v':
            emit_cyr(dst, di, 0xD0, 0xB2, upper);
            break;
        case 'w':
            emit_cyr(dst, di, 0xD0, 0xB2, upper);
            break;
        case 'x': {
            // x → кс
            bool second_upper = upper && si + 1 < src_len && src[si + 1] >= 'A' && src[si + 1] <= 'Z';
            emit_cyr(dst, di, 0xD0, 0xBA, upper);
            emit_cyr(dst, di, 0xD1, 0x81, second_upper);
            break;
        }
        case 'y':
            emit_cyr(dst, di, 0xD0, 0xB9, upper);
            break;
        case 'z':
            emit_cyr(dst, di, 0xD0, 0xB7, upper);
            break;
        default:
            dst[di++] = b;
            break;
        }
        si++;
    }

    return di;
}

StatusOr<ColumnPtr> StringFunctions::lat_to_cyr(FunctionContext* context, const Columns& columns) {
    RETURN_IF_COLUMNS_ONLY_NULL(columns);

    const auto& col = columns[0];
    const size_t num_rows = col->size();

    ColumnViewer<TYPE_VARCHAR> viewer(col);

    auto result = BinaryColumn::create();
    auto& result_offsets = result->get_offset();
    auto& result_bytes = result->get_bytes();

    // Worst-case expansion is 2x (j→дж is 1 byte → 4 bytes, but per-source-byte ratio is 4:1
    // for j; for typical strings the ratio stays close to 2:1 because Latin letters → 2-byte Cyrillic).
    size_t total_src_bytes = 0;
    for (size_t i = 0; i < num_rows; i++) {
        if (!viewer.is_null(i)) {
            total_src_bytes += viewer.value(i).size;
        }
    }
    result_bytes.resize(total_src_bytes * 4);
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

        if (result_bytes.size() < dst_offset + src_len * 4) {
            result_bytes.resize(dst_offset + src_len * 4);
        }

        uint8_t* dst_ptr = result_bytes.data() + dst_offset;
        size_t written = lat_to_cyr_fast(src_ptr, src_len, dst_ptr);

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
