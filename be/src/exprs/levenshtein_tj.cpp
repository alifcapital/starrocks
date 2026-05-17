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

// Weighted Levenshtein for Tajik/Cyrillic name matching. Costs calibrated on
// production transfer data (see scripts/tj_calibrate.py):
//
//   - Vowel↔vowel substitutions are cheap (0.3-0.5): а/о/и/е/у/й/ы/э/ю/я
//     swap freely across dialectal and transliteration variants.
//   - ж↔ч gets a soft 0.7: Ҷ (sound /dʒ/) and Ч (sound /tʃ/) are confused
//     by ear, common in Tajik recipient names. After norm_tj, Ҷ collapses
//     to ж, leaving ж↔ч as the residue.
//   - д↔т gets 1.5: voicing pair, occasional.
//   - Other consonant↔consonant and vowel↔consonant stay at 2.0 (default).
//   - Insert/delete a vowel costs 0.5, a consonant 1.0.
//
// UTF-8 codepoint-aware (not byte-level): operates on uint32 codepoints, so
// 2-byte Cyrillic letters count as one unit each.

#include <algorithm>
#include <cmath>
#include <vector>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/string_functions.h"

namespace starrocks {

using Codepoint = uint32_t;

// Decode UTF-8 bytes to a vector of codepoints. Invalid lead bytes are
// passed through one-by-one (matches what norm_tj does upstream).
static std::vector<Codepoint> decode_utf8(const char* data, size_t len) {
    std::vector<Codepoint> cps;
    cps.reserve(len);
    size_t i = 0;
    while (i < len) {
        uint8_t b0 = static_cast<uint8_t>(data[i]);
        Codepoint cp;
        size_t step;
        if (b0 < 0x80) {
            cp = b0;
            step = 1;
        } else if ((b0 & 0xE0) == 0xC0 && i + 1 < len) {
            cp = ((b0 & 0x1F) << 6) | (static_cast<uint8_t>(data[i + 1]) & 0x3F);
            step = 2;
        } else if ((b0 & 0xF0) == 0xE0 && i + 2 < len) {
            cp = ((b0 & 0x0F) << 12) | ((static_cast<uint8_t>(data[i + 1]) & 0x3F) << 6) |
                 (static_cast<uint8_t>(data[i + 2]) & 0x3F);
            step = 3;
        } else if ((b0 & 0xF8) == 0xF0 && i + 3 < len) {
            cp = ((b0 & 0x07) << 18) | ((static_cast<uint8_t>(data[i + 1]) & 0x3F) << 12) |
                 ((static_cast<uint8_t>(data[i + 2]) & 0x3F) << 6) | (static_cast<uint8_t>(data[i + 3]) & 0x3F);
            step = 4;
        } else {
            cp = b0;
            step = 1;
        }
        cps.push_back(cp);
        i += step;
    }
    return cps;
}

// й (U+0439) treated as a vowel for matching purposes — calibration showed it
// swaps with и at the same rate as plain V↔V soft pairs.
static inline bool is_vowel(Codepoint cp) {
    switch (cp) {
    case 0x0430:
    case 0x0435:
    case 0x0451:
    case 0x0438: // а е ё и
    case 0x043E:
    case 0x0443:
    case 0x044B:
    case 0x044D: // о у ы э
    case 0x044E:
    case 0x044F:
    case 0x0439: // ю я й
        return true;
    default:
        return false;
    }
}

// Substitution cost overrides (pair stored sorted, lo < hi).
struct SubCost {
    Codepoint lo;
    Codepoint hi;
    double cost;
};
static constexpr SubCost SUB_COSTS[] = {
        // Vowel↔vowel softs
        {0x0430, 0x043E, 0.3}, // а ↔ о  (Курбон↔Курбан, Тоҷик↔Таджик roots)
        {0x0435, 0x043E, 0.4}, // е ↔ о
        {0x0438, 0x0439, 0.4}, // и ↔ й
        {0x0438, 0x044B, 0.4}, // и ↔ ы
        {0x0430, 0x0443, 0.4}, // а ↔ у
        {0x0438, 0x043E, 0.5}, // и ↔ о
        {0x0430, 0x0438, 0.5}, // а ↔ и
        {0x0438, 0x0443, 0.5}, // и ↔ у
        {0x0439, 0x0443, 0.5}, // й ↔ у
        {0x0435, 0x0438, 0.4}, // е ↔ и
        // Yotated pairs (almost equivalent)
        {0x0430, 0x044F, 0.3}, // а ↔ я
        {0x0443, 0x044E, 0.3}, // у ↔ ю
        {0x043E, 0x0451, 0.3}, // о ↔ ё
        {0x0435, 0x0451, 0.3}, // е ↔ ё  (norm_tj folds ё→е already; kept for safety)
        {0x0435, 0x044D, 0.3}, // е ↔ э
        // Tajik-specific: Ҷ↔Ч confusion (post-norm_tj it surfaces as ж↔ч)
        {0x0436, 0x0447, 0.7},
        // Voicing pair
        {0x0434, 0x0442, 1.5}, // д ↔ т
};

static double get_sub_cost(Codepoint a, Codepoint b) {
    if (a == b) return 0.0;
    Codepoint lo = std::min(a, b);
    Codepoint hi = std::max(a, b);
    for (const auto& sc : SUB_COSTS) {
        if (sc.lo == lo && sc.hi == hi) return sc.cost;
    }
    bool va = is_vowel(a);
    bool vb = is_vowel(b);
    if (va && vb) return 0.5;
    return 2.0;
}

static inline double get_indel_cost(Codepoint c) {
    return is_vowel(c) ? 0.5 : 1.0;
}

static double weighted_distance(const std::vector<Codepoint>& a, const std::vector<Codepoint>& b) {
    size_t m = a.size();
    size_t n = b.size();
    if (m == 0) {
        double s = 0;
        for (Codepoint c : b) s += get_indel_cost(c);
        return s;
    }
    if (n == 0) {
        double s = 0;
        for (Codepoint c : a) s += get_indel_cost(c);
        return s;
    }
    std::vector<double> prev(n + 1);
    std::vector<double> cur(n + 1);
    prev[0] = 0.0;
    for (size_t j = 1; j <= n; j++) {
        prev[j] = prev[j - 1] + get_indel_cost(b[j - 1]);
    }
    for (size_t i = 1; i <= m; i++) {
        cur[0] = prev[0] + get_indel_cost(a[i - 1]);
        for (size_t j = 1; j <= n; j++) {
            double sub_c = prev[j - 1] + get_sub_cost(a[i - 1], b[j - 1]);
            double del_c = prev[j] + get_indel_cost(a[i - 1]);
            double ins_c = cur[j - 1] + get_indel_cost(b[j - 1]);
            cur[j] = std::min({sub_c, del_c, ins_c});
        }
        std::swap(prev, cur);
    }
    return prev[n];
}

StatusOr<ColumnPtr> StringFunctions::levenshtein_tj_distance(FunctionContext* context, const Columns& columns) {
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
        auto a = decode_utf8(s1.data, s1.size);
        auto b = decode_utf8(s2.data, s2.size);
        result->append(weighted_distance(a, b));
    }
    return result;
}

StatusOr<ColumnPtr> StringFunctions::levenshtein_tj_ratio(FunctionContext* context, const Columns& columns) {
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
        if (s1.size == 0 && s2.size == 0) {
            result->append(1.0);
            continue;
        }
        if (s1.size == 0 || s2.size == 0) {
            result->append(0.0);
            continue;
        }
        auto a = decode_utf8(s1.data, s1.size);
        auto b = decode_utf8(s2.data, s2.size);
        double dist = weighted_distance(a, b);
        // Normalize by total indel cost of both sides — bounds dist into [0, 1].
        double indel_total = 0.0;
        for (Codepoint c : a) indel_total += get_indel_cost(c);
        for (Codepoint c : b) indel_total += get_indel_cost(c);
        double ratio = (indel_total > 0.0) ? (1.0 - 2.0 * dist / indel_total) : 1.0;
        if (ratio < 0.0) ratio = 0.0;
        if (ratio > 1.0) ratio = 1.0;
        result->append(ratio);
    }
    return result;
}

} // namespace starrocks
