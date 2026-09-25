/** @brief Unicode 17 default lowercase and uppercase conversion. */
#ifndef STRINGZILLA_UTF8_CASE_SERIAL_H_
#define STRINGZILLA_UTF8_CASE_SERIAL_H_
#include "stringzilla/utf8_runes/serial.h"
#include "stringzilla/utf8_case/tables.h"
#include "stringzilla/utf8_case/initcap_tables.h"
#ifdef __cplusplus
extern "C" {
#endif

SZ_HELPER_AUTO sz_bool_t sz_unicode_case_has_property_(sz_rune_t rune, sz_unicode_case_range_t const *ranges,
                                                       sz_size_t count) {
    sz_size_t lo = 0, hi = count;
    while (lo < hi) {
        sz_size_t mid = lo + (hi - lo) / 2;
        if (rune < ranges[mid].first) hi = mid;
        else if (rune > ranges[mid].last) lo = mid + 1;
        else return sz_true_k;
    }
    return sz_false_k;
}
SZ_HELPER_AUTO sz_bool_t sz_unicode_case_ignorable_(sz_rune_t rune) {
    return sz_unicode_case_has_property_(
        rune, sz_unicode_case_ignorable_ranges_,
        sizeof(sz_unicode_case_ignorable_ranges_) / sizeof(sz_unicode_case_ignorable_ranges_[0]));
}
SZ_HELPER_AUTO sz_bool_t sz_unicode_case_cased_(sz_rune_t rune) {
    return sz_unicode_case_has_property_(rune, sz_unicode_cased_ranges_,
                                         sizeof(sz_unicode_cased_ranges_) / sizeof(sz_unicode_cased_ranges_[0]));
}

/** @brief Final_Sigma uses the original input, including across SIMD chunk boundaries. */
SZ_HELPER_AUTO sz_bool_t sz_utf8_case_final_sigma_(sz_cptr_t begin, sz_cptr_t sigma, sz_cptr_t end) {
    sz_cptr_t cursor = sigma;
    sz_bool_t preceded_by_cased = sz_false_k;
    while (cursor != begin) {
        sz_cptr_t previous = cursor - 1;
        unsigned int continuation_count = 0;
        while (previous != begin && sz_utf8_is_continuation_((sz_u8_t)*previous) && continuation_count < 3) {
            --previous;
            ++continuation_count;
        }
        sz_rune_t rune;
        sz_rune_length_t length = sz_rune_decode(previous, cursor, &rune);
        if (!length || (sz_size_t)length != (sz_size_t)(cursor - previous)) break;
        cursor = previous;
        if (sz_unicode_case_ignorable_(rune)) continue;
        preceded_by_cased = sz_unicode_case_cased_(rune);
        break;
    }
    if (!preceded_by_cased) return sz_false_k;
    cursor = sigma + 2;
    while (cursor != end) {
        sz_rune_t rune;
        sz_rune_length_t length = sz_rune_decode(cursor, end, &rune);
        if (!length) break;
        cursor += length;
        if (sz_unicode_case_ignorable_(rune)) continue;
        return (sz_bool_t)!sz_unicode_case_cased_(rune);
    }
    return sz_true_k;
}

SZ_HELPER_AUTO sz_size_t sz_unicode_case_map_(sz_rune_t rune, sz_bool_t upper, sz_rune_t mapped[3]) {
    sz_unicode_case_mapping_t const *entries = upper ? sz_unicode_upper_mappings_ : sz_unicode_lower_mappings_;
    sz_u16_t const *pages = upper ? sz_unicode_upper_pages_ : sz_unicode_lower_pages_;
    sz_size_t page_count = upper ? sizeof(sz_unicode_upper_pages_) / sizeof(sz_unicode_upper_pages_[0])
                                 : sizeof(sz_unicode_lower_pages_) / sizeof(sz_unicode_lower_pages_[0]);
    sz_size_t page = rune >> 8;
    if (page + 1 >= page_count) {
        mapped[0] = rune;
        return 1;
    }
    sz_size_t lo = pages[page], hi = pages[page + 1];
    // The previous range may cross the page boundary.
    if (lo) --lo;
    while (lo < hi) {
        sz_size_t mid = lo + (hi - lo) / 2;
        sz_unicode_case_mapping_t const *entry = entries + mid;
        if (rune < entry->first) hi = mid;
        else if (rune > entry->last) lo = mid + 1;
        else {
            if ((rune - entry->first) % entry->step) break;
            if (entry->count == 1) mapped[0] = (sz_rune_t)((sz_i32_t)rune + entry->values[0]);
            else
                for (sz_size_t i = 0; i != entry->count; ++i) mapped[i] = (sz_rune_t)entry->values[i];
            return entry->count;
        }
    }
    mapped[0] = rune;
    return 1;
}

SZ_HELPER_AUTO sz_size_t sz_utf8_case_one_rune_(sz_cptr_t begin, sz_cptr_t source, sz_cptr_t end, sz_ptr_t target,
                                                sz_bool_t upper, sz_size_t *consumed) {
    sz_u8_t byte = (sz_u8_t)*source;
    if (byte < 0x80) {
        sz_u8_t first = upper ? 'a' : 'A';
        *target = (char)(byte + ((sz_u8_t)(byte - first) < 26 ? (upper ? -32 : 32) : 0));
        *consumed = 1;
        return 1;
    }
    sz_rune_t rune;
    sz_rune_length_t length = sz_rune_decode(source, end, &rune);
    if (!length) {
        *target = *source;
        *consumed = 1;
        return 1;
    }
    *consumed = (sz_size_t)length;
    sz_rune_t mapped[3];
    sz_size_t count;
    if (!upper && rune == 0x03A3) {
        mapped[0] = sz_utf8_case_final_sigma_(begin, source, end) ? 0x03C2 : 0x03C3;
        count = 1;
    }
    else count = sz_unicode_case_map_(rune, upper, mapped);
    sz_ptr_t start = target;
    for (sz_size_t i = 0; i != count; ++i) target += sz_rune_encode(mapped[i], (sz_u8_t *)target);
    return (sz_size_t)(target - start);
}

SZ_HELPER_AUTO sz_size_t sz_utf8_case_serial_(sz_cptr_t source, sz_size_t source_length, sz_ptr_t target,
                                              sz_bool_t upper) {
    if (!source_length) return 0;
    sz_cptr_t begin = source, end = source + source_length;
    sz_ptr_t start = target;
    while (source != end) {
        sz_size_t consumed;
        target += sz_utf8_case_one_rune_(begin, source, end, target, upper, &consumed);
        source += consumed;
    }
    return (sz_size_t)(target - start);
}
SZ_API_COMPTIME sz_size_t sz_utf8_case_lower_serial(sz_cptr_t source, sz_size_t length, sz_ptr_t target) {
    return sz_utf8_case_serial_(source, length, target, sz_false_k);
}
SZ_API_COMPTIME sz_size_t sz_utf8_case_upper_serial(sz_cptr_t source, sz_size_t length, sz_ptr_t target) {
    return sz_utf8_case_serial_(source, length, target, sz_true_k);
}

/** @brief Flags expansions and contextual mappings that cannot use a fixed-width byte transform. */
SZ_HELPER_INLINE sz_bool_t sz_utf8_case_irregular_(sz_cptr_t source, sz_size_t length, sz_bool_t upper) {
    sz_u8_t lead = (sz_u8_t)source[0];
    if (lead >= 0xC2 && lead <= 0xDF && length >= 2) {
        sz_u64_t bits = upper ? sz_unicode_upper_two_byte_irregular_[lead - 0xC0]
                              : sz_unicode_lower_two_byte_irregular_[lead - 0xC0];
        return (sz_bool_t)((bits >> ((sz_u8_t)source[1] & 63)) & 1);
    }
    if (lead == 0xE1 && length >= 3) {
        sz_u64_t bits = upper ? sz_unicode_upper_e1_irregular_[(sz_u8_t)source[1] & 63]
                              : sz_unicode_lower_e1_irregular_[(sz_u8_t)source[1] & 63];
        return (sz_bool_t)((bits >> ((sz_u8_t)source[2] & 63)) & 1);
    }
    return sz_false_k;
}

/** @brief Selects the byte-family handler from the first non-ASCII sequence. */
SZ_HELPER_AUTO unsigned int sz_utf8_case_family_(sz_cptr_t source, sz_size_t length) {
    sz_u8_t first = (sz_u8_t)source[0], second = length > 1 ? (sz_u8_t)source[1] : 0;
    if (first == 0xC2 || first == 0xC3) return 7;
    if (first >= 0xC4 && first <= 0xC6) return 1;
    if (first >= 0xD0 && first <= 0xD3) return 2;
    if (first == 0xCE || first == 0xCF) return 8;
    if (first >= 0xD4 && first <= 0xD6) return 5;
    if (first == 0xE1) {
        if (second >= 0xB8 && second <= 0xBB) return 1;
        if (second >= 0xBC && second <= 0xBF) return 3;
        if (second == 0x82 || second == 0x83 || second == 0xB2 || second == 0xB3) return 4;
    }
    if (first == 0xE2 && second == 0xB4) return 4;
    if (first == 0xEF && (second == 0xBC || second == 0xBD)) return 6;
    return 0;
}
SZ_HELPER_AUTO sz_rune_t sz_unicode_simple_case_(sz_rune_t rune, sz_bool_t upper) {
    sz_unicode_simple_case_t const *entries = upper ? sz_unicode_simple_upper_ : sz_unicode_simple_lower_;
    sz_size_t lo = 0, hi = upper ? sizeof(sz_unicode_simple_upper_) / sizeof(sz_unicode_simple_upper_[0])
                                 : sizeof(sz_unicode_simple_lower_) / sizeof(sz_unicode_simple_lower_[0]);
    while (lo < hi) {
        sz_size_t mid = lo + (hi - lo) / 2;
        sz_unicode_simple_case_t const *entry = entries + mid;
        if (rune < entry->first) hi = mid;
        else if (rune > entry->last) lo = mid + 1;
        else return (rune - entry->first) % entry->step ? rune : (sz_rune_t)((sz_i32_t)rune + entry->delta);
    }
    return rune;
}

SZ_HELPER_AUTO sz_size_t sz_utf8_initcap_one_(sz_cptr_t source, sz_cptr_t end, sz_ptr_t target, sz_bool_t *word_start,
                                              sz_size_t *consumed) {
    sz_u8_t byte = (sz_u8_t)*source;
    if (byte < 0x80) {
        sz_u8_t lower = byte | 32;
        sz_bool_t letter = (sz_bool_t)((sz_u8_t)(lower - 'a') < 26);
        sz_bool_t alnum = (sz_bool_t)(letter || (sz_u8_t)(byte - '0') < 10);
        *target = letter ? (char)(lower - (*word_start ? 32 : 0)) : (char)byte;
        *word_start = (sz_bool_t)!alnum;
        *consumed = 1;
        return 1;
    }
    sz_rune_t rune;
    sz_rune_length_t length = sz_rune_decode(source, end, &rune);
    if (!length) return SZ_SIZE_MAX;
    *consumed = (sz_size_t)length;
    sz_bool_t alnum = sz_unicode_case_has_property_(
        rune, sz_unicode_alnum_ranges_, sizeof(sz_unicode_alnum_ranges_) / sizeof(sz_unicode_alnum_ranges_[0]));
    if (alnum) rune = sz_unicode_simple_case_(rune, *word_start);
    *word_start = (sz_bool_t)!alnum;
    return sz_rune_encode(rune, (sz_u8_t *)target);
}

SZ_API_COMPTIME sz_size_t sz_utf8_case_initcap_serial(sz_cptr_t source, sz_size_t length, sz_ptr_t target,
                                                      sz_size_t *error_offset) {
    if (error_offset) *error_offset = SZ_SIZE_MAX;
    if (!length) return 0;
    sz_cptr_t begin = source, end = source + length;
    sz_ptr_t start = target;
    sz_bool_t word_start = sz_true_k;
    while (source != end) {
        sz_size_t consumed, written = sz_utf8_initcap_one_(source, end, target, &word_start, &consumed);
        if (written == SZ_SIZE_MAX) {
            if (error_offset) *error_offset = (sz_size_t)(source - begin);
            return SZ_SIZE_MAX;
        }
        source += consumed;
        target += written;
    }
    return (sz_size_t)(target - start);
}

#ifdef __cplusplus
}
#endif
#endif
