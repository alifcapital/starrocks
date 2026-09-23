/** @brief Haswell (AVX2) backend for Unicode 17 lower/upper, using the UTF-8 folding chunk layout. */
#ifndef STRINGZILLA_UTF8_CASE_HASWELL_H_
#define STRINGZILLA_UTF8_CASE_HASWELL_H_
#include "stringzilla/utf8_case/serial.h"
#include "stringzilla/utf8_uncased_fold/haswell.h"
#ifdef __cplusplus
extern "C" {
#endif
#if SZ_USE_HASWELL
#if defined(__clang__)
#pragma clang attribute push(__attribute__((target("avx2,bmi,bmi2"))), apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("avx2", "bmi", "bmi2")
#endif

SZ_HELPER_INLINE __m256i sz_utf8_case_haswell_ascii_(__m256i v, sz_bool_t upper) {
    __m256i mask = sz_haswell_in_byte_range_(v, upper ? 'a' : 'A', 26);
    return _mm256_add_epi8(v, _mm256_and_si256(mask, _mm256_set1_epi8(upper ? -32 : 32)));
}
#include "stringzilla/utf8_case/haswell_mappings.h"
SZ_HELPER_AUTO sz_size_t sz_utf8_case_haswell_caseless_(__m256i v, sz_ptr_t target, sz_bool_t upper,
                                                        sz_size_t available) {
    sz_u32_t non_ascii = (sz_u32_t)_mm256_movemask_epi8(v);
    sz_utf8_uncased_fold_haswell_leads_t leads = sz_utf8_uncased_fold_haswell_classify_leads_(v, non_ascii);
    sz_u32_t two = leads.well_formed_lead_mask & leads.is_caseless_lead_mask & leads.is_two_byte_lead_mask;
    sz_u32_t three = leads.well_formed_lead_mask & leads.is_caseless_lead_mask & leads.is_three_byte_lead_mask;
    __m256i next = sz_haswell_next_bytes_(v);
    sz_u32_t supplementary = (sz_u32_t)_mm256_movemask_epi8(_mm256_or_si256(
        sz_haswell_in_byte_range_(v, 0xF1, 4), _mm256_and_si256(_mm256_cmpeq_epi8(v, _mm256_set1_epi8((char)0xF0)),
                                                                sz_haswell_in_byte_range_(next, 0x9F, 0x21))));
    sz_u32_t four = supplementary & leads.well_formed_lead_mask;
    sz_unused_(available);
    sz_u32_t stop = ~(~non_ascii | two | (two << 1) | three | (three << 1) | (three << 2) | four | (four << 1) |
                      (four << 2) | (four << 3));
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u32(stop) : 32;
    if (!length) return 0;
    _mm256_storeu_si256((__m256i *)target, sz_utf8_case_haswell_ascii_(v, upper));
    return length;
}
SZ_API_COMPTIME sz_size_t sz_utf8_case_lower_haswell(sz_cptr_t source, sz_size_t source_length, sz_ptr_t target) {
    if (!source_length) return 0;
    sz_cptr_t begin = source, end = source + source_length;
    sz_ptr_t start = target;
    while ((sz_size_t)(end - source) >= 32) {
        __m256i v = _mm256_loadu_si256((__m256i const *)source);
        sz_u32_t non_ascii = (sz_u32_t)_mm256_movemask_epi8(v);
        if (!non_ascii) {
            _mm256_storeu_si256((__m256i *)target, sz_utf8_case_haswell_ascii_(v, sz_false_k));
            source += 32;
            target += 32;
            continue;
        }
        sz_size_t first = (sz_size_t)_tzcnt_u32(non_ascii);
        if (first == 0 && sz_utf8_case_irregular_(source, (sz_size_t)(end - source), sz_false_k)) {
            sz_size_t consumed;
            target += sz_utf8_case_one_rune_(begin, source, end, target, sz_false_k, &consumed);
            source += consumed;
            continue;
        }
        unsigned int family = sz_utf8_case_family_(source + first, (sz_size_t)(end - source) - first);
        sz_size_t handled;
        switch (family) {
        case 1: handled = sz_utf8_lower_haswell_latin_(v, target, 32); break;
        case 2: handled = sz_utf8_lower_haswell_cyrillic_(v, target, 32); break;
        case 3: handled = sz_utf8_lower_haswell_greek_(v, target, 32); break;
        case 4: handled = sz_utf8_lower_haswell_georgian_(v, target, 32); break;
        case 5: handled = sz_utf8_lower_haswell_armenian_(v, target, 32); break;
        case 6: handled = sz_utf8_lower_haswell_fullwidth_(v, target, 32); break;
        case 7: handled = sz_utf8_lower_haswell_latin1_(v, target, 32); break;
        case 8: handled = sz_utf8_lower_haswell_greek_basic_(v, target, 32); break;
        default: handled = sz_utf8_case_haswell_caseless_(v, target, sz_false_k, 32); break;
        }
        if (handled) {
            source += handled;
            target += handled;
            continue;
        }
        sz_size_t consumed;
        target += sz_utf8_case_one_rune_(begin, source, end, target, sz_false_k, &consumed);
        source += consumed;
    }
    while (source != end) {
        sz_size_t consumed;
        target += sz_utf8_case_one_rune_(begin, source, end, target, sz_false_k, &consumed);
        source += consumed;
    }
    return (sz_size_t)(target - start);
}
SZ_API_COMPTIME sz_size_t sz_utf8_case_upper_haswell(sz_cptr_t source, sz_size_t source_length, sz_ptr_t target) {
    if (!source_length) return 0;
    sz_cptr_t begin = source, end = source + source_length;
    sz_ptr_t start = target;
    while ((sz_size_t)(end - source) >= 32) {
        __m256i v = _mm256_loadu_si256((__m256i const *)source);
        sz_u32_t non_ascii = (sz_u32_t)_mm256_movemask_epi8(v);
        if (!non_ascii) {
            _mm256_storeu_si256((__m256i *)target, sz_utf8_case_haswell_ascii_(v, sz_true_k));
            source += 32;
            target += 32;
            continue;
        }
        sz_size_t first = (sz_size_t)_tzcnt_u32(non_ascii);
        if (first == 0 && sz_utf8_case_irregular_(source, (sz_size_t)(end - source), sz_true_k)) {
            sz_size_t consumed;
            target += sz_utf8_case_one_rune_(begin, source, end, target, sz_true_k, &consumed);
            source += consumed;
            continue;
        }
        unsigned int family = sz_utf8_case_family_(source + first, (sz_size_t)(end - source) - first);
        sz_size_t handled;
        switch (family) {
        case 1: handled = sz_utf8_upper_haswell_latin_(v, target, 32); break;
        case 2: handled = sz_utf8_upper_haswell_cyrillic_(v, target, 32); break;
        case 3: handled = sz_utf8_upper_haswell_greek_(v, target, 32); break;
        case 4: handled = sz_utf8_upper_haswell_georgian_(v, target, 32); break;
        case 5: handled = sz_utf8_upper_haswell_armenian_(v, target, 32); break;
        case 6: handled = sz_utf8_upper_haswell_fullwidth_(v, target, 32); break;
        case 7: handled = sz_utf8_upper_haswell_latin1_(v, target, 32); break;
        case 8: handled = sz_utf8_upper_haswell_greek_basic_(v, target, 32); break;
        default: handled = sz_utf8_case_haswell_caseless_(v, target, sz_true_k, 32); break;
        }
        if (handled) {
            source += handled;
            target += handled;
            continue;
        }
        sz_size_t consumed;
        target += sz_utf8_case_one_rune_(begin, source, end, target, sz_true_k, &consumed);
        source += consumed;
    }
    while (source != end) {
        sz_size_t consumed;
        target += sz_utf8_case_one_rune_(begin, source, end, target, sz_true_k, &consumed);
        source += consumed;
    }
    return (sz_size_t)(target - start);
}
SZ_HELPER_INLINE __m256i sz_utf8_initcap_haswell_expand_mask_(sz_u32_t bits) {
    __m256i selectors = _mm256_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 2, 3, 3,
                                         3, 3, 3, 3, 3, 3);
    __m256i powers = _mm256_setr_epi8(1, 2, 4, 8, 16, 32, 64, (char)128, 1, 2, 4, 8, 16, 32, 64, (char)128, 1, 2, 4, 8,
                                      16, 32, 64, (char)128, 1, 2, 4, 8, 16, 32, 64, (char)128);
    __m256i bytes = _mm256_shuffle_epi8(_mm256_set1_epi32((int)bits), selectors);
    return _mm256_cmpeq_epi8(_mm256_and_si256(bytes, powers), powers);
}
#include "stringzilla/utf8_case/initcap_haswell_mappings.h"
SZ_API_COMPTIME sz_size_t sz_utf8_case_initcap_haswell(sz_cptr_t source, sz_size_t length, sz_ptr_t target,
                                                       sz_size_t *error_offset) {
    if (error_offset) *error_offset = SZ_SIZE_MAX;
    if (!length) return 0;
    sz_cptr_t begin = source, end = source + length;
    sz_ptr_t start = target;
    sz_bool_t word_start = sz_true_k;
    while (source != end) {
        if ((sz_size_t)(end - source) >= 32) {
            __m256i v = _mm256_loadu_si256((__m256i const *)source);
            if (!_mm256_movemask_epi8(v)) {
                __m256i lower = _mm256_or_si256(v, _mm256_set1_epi8(32));
                __m256i letters = sz_haswell_in_byte_range_(lower, 'a', 26);
                __m256i alnum = _mm256_or_si256(letters, sz_haswell_in_byte_range_(v, '0', 10));
                __m256i previous = _mm256_alignr_epi8(alnum, _mm256_permute2x128_si256(alnum, alnum, 0x08), 15);
                previous = _mm256_insert_epi8(previous, word_start ? 0 : -1, 0);
                __m256i capitalize = _mm256_andnot_si256(previous, letters);
                __m256i result = _mm256_blendv_epi8(v, lower, letters);
                result = _mm256_sub_epi8(result, _mm256_and_si256(capitalize, _mm256_set1_epi8(32)));
                _mm256_storeu_si256((__m256i *)target, result);
                word_start = (sz_bool_t) !((sz_u32_t)_mm256_movemask_epi8(alnum) >> 31);
                source += 32;
                target += 32;
                continue;
            }
            sz_u32_t non_ascii = (sz_u32_t)_mm256_movemask_epi8(v);
            sz_size_t first = (sz_size_t)_tzcnt_u32(non_ascii);
            unsigned int family = sz_utf8_case_family_(source + first, (sz_size_t)(end - source) - first);
            sz_size_t handled = 0;
            switch (family) {
            case 1: handled = sz_utf8_initcap_haswell_latin_(v, target, &word_start); break;
            case 2: handled = sz_utf8_initcap_haswell_cyrillic_(v, target, &word_start); break;
            case 3: handled = sz_utf8_initcap_haswell_greek_(v, target, &word_start); break;
            case 4: handled = sz_utf8_initcap_haswell_georgian_(v, target, &word_start); break;
            case 5: handled = sz_utf8_initcap_haswell_armenian_(v, target, &word_start); break;
            case 6: handled = sz_utf8_initcap_haswell_fullwidth_(v, target, &word_start); break;
            case 7: handled = sz_utf8_initcap_haswell_latin1_(v, target, &word_start); break;
            case 8: handled = sz_utf8_initcap_haswell_greek_basic_(v, target, &word_start); break;
            default: break;
            }
            if (handled) {
                source += handled;
                target += handled;
                continue;
            }
        }
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
#if defined(__clang__)
#pragma clang attribute pop
#elif defined(__GNUC__)
#pragma GCC pop_options
#endif
#endif
#ifdef __cplusplus
}
#endif
#endif
