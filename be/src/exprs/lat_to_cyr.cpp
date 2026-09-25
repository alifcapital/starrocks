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
// Greedy left-to-right: digraphs (kh, gh, zh, ch, sh, ya/yu/yo/ye, ts, oo, ee, o', g') match
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
    } else if (b0 == 0xD1 && b1 == 0x91) {
        dst[di++] = 0xD0; // ё -> Ё
        dst[di++] = 0x81;
    } else {
        dst[di++] = b0;
        dst[di++] = b1;
    }
}

// Only visually confusable Latin letters are eligible. Uppercase B/H/P have
// different phonetic transliterations, so examine the original case.
static inline uint32_t cyrillic_lookalike(uint8_t b) {
    switch (b) {
    case 'a':
    case 'A':
        return 0x0430;
    case 'B':
        return 0x0432;
    case 'c':
    case 'C':
        return 0x0441;
    case 'e':
    case 'E':
        return 0x0435;
    case 'H':
        return 0x043D;
    case 'k':
    case 'K':
        return 0x043A;
    case 'm':
    case 'M':
        return 0x043C;
    case 'o':
    case 'O':
        return 0x043E;
    case 'p':
    case 'P':
        return 0x0440;
    case 'T':
        return 0x0442;
    case 'x':
    case 'X':
        return 0x0445;
    case 'y':
    case 'Y':
        return 0x0443;
    default:
        return 0;
    }
}

// A name token consists of ASCII Latin and Cyrillic letters. Punctuation and
// spaces delimit tokens: one Cyrillic name must not change another Latin name's
// transliteration. Combining signs and Cyrillic numerals are not letters here.
static inline size_t name_letter_bytes(const uint8_t* src, size_t i, size_t n) {
    uint8_t b = src[i];
    if ((b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z')) return 1;
    if (i + 1 < n && b >= 0xD0 && b <= 0xD3 && (src[i + 1] & 0xC0) == 0x80) {
        uint32_t cp = ((b & 0x1F) << 6) | (src[i + 1] & 0x3F);
        if ((cp >= 0x0400 && cp <= 0x0481) || (cp >= 0x048A && cp <= 0x04FF)) return 2;
    }
    return 0;
}

static inline size_t lat_to_cyr_fast(const uint8_t* __restrict src, size_t src_len, uint8_t* __restrict dst) {
    size_t si = 0;
    size_t di = 0;
    size_t token_begin = 0;
    size_t token_end = 0;
    bool visual = false;

    while (si < src_len) {
        if (si >= token_end) {
            token_begin = token_end = si;
            size_t latin = 0;
            size_t cyrillic = 0;
            bool all_confusable = true;
            while (token_end < src_len) {
                size_t width = name_letter_bytes(src, token_end, src_len);
                if (width == 0) break;
                if (width == 1) {
                    ++latin;
                    all_confusable &= cyrillic_lookalike(src[token_end]) != 0;
                } else {
                    ++cyrillic;
                }
                token_end += width;
            }
            visual = latin > 0 && cyrillic >= latin && all_confusable;
        }
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

        if (visual) {
            uint32_t cp = cyrillic_lookalike(b);
            emit_cyr(dst, di, 0xC0 | (cp >> 6), 0x80 | (cp & 0x3F), upper);
            ++si;
            continue;
        }

        // Tajik/Uzbek initial x (Xasan, Xosiyat). Internal x remains ambiguous
        // across transliteration systems (Shaxlo vs Alexandr/Maxumova).
        if (lc == 'x' && si == token_begin) {
            emit_cyr(dst, di, 0xD1, 0x85, upper);
            ++si;
            continue;
        }

        // Two-letter digraphs (case based on first letter)
        if (si + 1 < src_len) {
            uint8_t nb = src[si + 1];
            uint8_t nlc = (nb >= 'A' && nb <= 'Z') ? (nb + 0x20) : nb;

            if (lc == 'y' && (nlc == 'a' || nlc == 'u' || nlc == 'o' || nlc == 'e')) {
                uint32_t cp = nlc == 'a' ? 0x044F : nlc == 'u' ? 0x044E : nlc == 'o' ? 0x0451 : 0x0435;
                emit_cyr(dst, di, 0xC0 | (cp >> 6), 0x80 | (cp & 0x3F), upper);
                si += 2;
                continue;
            }
            // Do not consume the s in t + sh: Davlatshoh -> Давлатшох.
            bool before_h = si + 2 < src_len && (src[si + 2] == 'h' || src[si + 2] == 'H');
            if (lc == 't' && nlc == 's' && !before_h) {
                emit_cyr(dst, di, 0xD1, 0x86, upper);
                si += 2;
                continue;
            }

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
    const size_t num_rows = col->is_constant() ? 1 : col->size();

    ColumnViewer<TYPE_VARCHAR> viewer(col);

    auto result = BinaryColumn::create();
    auto& result_offsets = result->get_offset();
    auto& result_bytes = result->get_bytes();

    // Worst-case expansion is 4x: j -> дж and x -> кс (one byte -> four bytes).
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

        uint8_t* dst_ptr = result_bytes.empty() ? nullptr : result_bytes.data() + dst_offset;
        size_t written = lat_to_cyr_fast(src_ptr, src_len, dst_ptr);

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
