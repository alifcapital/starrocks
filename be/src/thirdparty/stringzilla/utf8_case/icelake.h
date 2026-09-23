/** @brief Ice Lake (AVX-512) backend for Unicode 17 lower/upper, using the UTF-8 folding chunk layout. */
#ifndef STRINGZILLA_UTF8_CASE_ICELAKE_H_
#define STRINGZILLA_UTF8_CASE_ICELAKE_H_
#include "stringzilla/utf8_case/serial.h"
#include "stringzilla/utf8_uncased_fold/icelake.h"
#ifdef __cplusplus
extern "C" {
#endif
#if SZ_USE_ICELAKE
#if defined(__clang__) && SZ_CLANG_HAS_EVEX512_
#pragma clang attribute push(                                                                                    \
    __attribute__((                                                                                              \
        target("avx,avx512f,avx512vl,avx512bw,avx512dq,avx512vbmi,avx512vbmi2,bmi,bmi2,lzcnt,popcnt,evex512"))), \
    apply_to = function)
#elif defined(__clang__)
#pragma clang attribute push(                                                                                       \
    __attribute__((target("avx,avx512f,avx512vl,avx512bw,avx512dq,avx512vbmi,avx512vbmi2,bmi,bmi2,lzcnt,popcnt"))), \
    apply_to = function)
#elif defined(__GNUC__)
#pragma GCC push_options
#pragma GCC target("avx", "avx512f", "avx512vl", "avx512bw", "avx512dq", "avx512vbmi", "avx512vbmi2", "bmi", "bmi2", \
                   "lzcnt", "popcnt")
#endif

SZ_HELPER_INLINE __m512i sz_utf8_case_icelake_ascii_(__m512i v, sz_bool_t upper) {
    __mmask64 mask = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8(upper ? 'a' : 'A')),
                                            _mm512_set1_epi8(26));
    return _mm512_mask_add_epi8(v, mask, v, _mm512_set1_epi8(upper ? -32 : 32));
}
#include "stringzilla/utf8_case/icelake_mappings.h"
SZ_HELPER_AUTO sz_size_t sz_utf8_case_icelake_caseless_(__m512i v, sz_ptr_t target, sz_bool_t upper,
                                                        sz_size_t available) {
    sz_u64_t non_ascii = _mm512_movepi8_mask(v);
    sz_u64_t cont = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0x80)), _mm512_set1_epi8(0x40));
    sz_u64_t two_leads = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0xD7)), _mm512_set1_epi8(9));
    sz_u64_t three_leads =
        _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)0xE0)) |
        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0xE3)), _mm512_set1_epi8(7)) |
        _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0xEB)), _mm512_set1_epi8(4));
    sz_u64_t lt_a0 = _mm512_cmplt_epu8_mask(v, _mm512_set1_epi8((char)0xA0)) >> 1;
    sz_u64_t lt_90 = _mm512_cmplt_epu8_mask(v, _mm512_set1_epi8((char)0x90)) >> 1;
    sz_u64_t bad = (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)0xE0)) & lt_a0) |
                   (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)0xED)) & ~lt_a0);
    sz_u64_t two = two_leads & (cont >> 1);
    sz_u64_t three = three_leads & (cont >> 1) & (cont >> 2) & ~bad;
    sz_u64_t four_leads = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8((char)0xF1)),
                                                 _mm512_set1_epi8(4)) |
                          (_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)0xF0)) &
                           (_mm512_cmpge_epu8_mask(v, _mm512_set1_epi8((char)0x9F)) >> 1));
    sz_u64_t four = four_leads & (cont >> 1) & (cont >> 2) & (cont >> 3) &
                    ~(_mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8((char)0xF4)) & ~lt_90);
    non_ascii |= ~sz_u64_mask_until_(available);
    sz_u64_t stop = ~(~non_ascii | two | (two << 1) | three | (three << 1) | (three << 2) | four | (four << 1) |
                      (four << 2) | (four << 3));
    sz_size_t length = stop ? (sz_size_t)_tzcnt_u64(stop) : 64;
    if (!length) return 0;
    _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(length), sz_utf8_case_icelake_ascii_(v, upper));
    return length;
}
SZ_API_COMPTIME sz_size_t sz_utf8_case_lower_icelake(sz_cptr_t source, sz_size_t source_length, sz_ptr_t target) {
    if (!source_length) return 0;
    sz_cptr_t begin = source, end = source + source_length;
    sz_ptr_t start = target;
    while (source != end) {
        sz_size_t available = sz_min_of_two((sz_size_t)(end - source), 64);
        __m512i v = _mm512_maskz_loadu_epi8(sz_u64_mask_until_(available), source);
        sz_u64_t non_ascii = _mm512_movepi8_mask(v);
        if (!non_ascii) {
            _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(available), sz_utf8_case_icelake_ascii_(v, sz_false_k));
            source += available;
            target += available;
            continue;
        }
        sz_size_t first = (sz_size_t)_tzcnt_u64(non_ascii);
        if (first == 0 && sz_utf8_case_irregular_(source, (sz_size_t)(end - source), sz_false_k)) {
            sz_size_t consumed;
            target += sz_utf8_case_one_rune_(begin, source, end, target, sz_false_k, &consumed);
            source += consumed;
            continue;
        }
        unsigned int family = sz_utf8_case_family_(source + first, (sz_size_t)(end - source) - first);
        sz_size_t handled;
        switch (family) {
        case 1: handled = sz_utf8_lower_icelake_latin_(v, target, available); break;
        case 2: handled = sz_utf8_lower_icelake_cyrillic_(v, target, available); break;
        case 3: handled = sz_utf8_lower_icelake_greek_(v, target, available); break;
        case 4: handled = sz_utf8_lower_icelake_georgian_(v, target, available); break;
        case 5: handled = sz_utf8_lower_icelake_armenian_(v, target, available); break;
        case 6: handled = sz_utf8_lower_icelake_fullwidth_(v, target, available); break;
        case 7: handled = sz_utf8_lower_icelake_latin1_(v, target, available); break;
        case 8: handled = sz_utf8_lower_icelake_greek_basic_(v, target, available); break;
        default: handled = sz_utf8_case_icelake_caseless_(v, target, sz_false_k, available); break;
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
    return (sz_size_t)(target - start);
}
SZ_API_COMPTIME sz_size_t sz_utf8_case_upper_icelake(sz_cptr_t source, sz_size_t source_length, sz_ptr_t target) {
    if (!source_length) return 0;
    sz_cptr_t begin = source, end = source + source_length;
    sz_ptr_t start = target;
    while (source != end) {
        sz_size_t available = sz_min_of_two((sz_size_t)(end - source), 64);
        __m512i v = _mm512_maskz_loadu_epi8(sz_u64_mask_until_(available), source);
        sz_u64_t non_ascii = _mm512_movepi8_mask(v);
        if (!non_ascii) {
            _mm512_mask_storeu_epi8(target, sz_u64_mask_until_(available), sz_utf8_case_icelake_ascii_(v, sz_true_k));
            source += available;
            target += available;
            continue;
        }
        sz_size_t first = (sz_size_t)_tzcnt_u64(non_ascii);
        if (first == 0 && sz_utf8_case_irregular_(source, (sz_size_t)(end - source), sz_true_k)) {
            sz_size_t consumed;
            target += sz_utf8_case_one_rune_(begin, source, end, target, sz_true_k, &consumed);
            source += consumed;
            continue;
        }
        unsigned int family = sz_utf8_case_family_(source + first, (sz_size_t)(end - source) - first);
        sz_size_t handled;
        switch (family) {
        case 1: handled = sz_utf8_upper_icelake_latin_(v, target, available); break;
        case 2: handled = sz_utf8_upper_icelake_cyrillic_(v, target, available); break;
        case 3: handled = sz_utf8_upper_icelake_greek_(v, target, available); break;
        case 4: handled = sz_utf8_upper_icelake_georgian_(v, target, available); break;
        case 5: handled = sz_utf8_upper_icelake_armenian_(v, target, available); break;
        case 6: handled = sz_utf8_upper_icelake_fullwidth_(v, target, available); break;
        case 7: handled = sz_utf8_upper_icelake_latin1_(v, target, available); break;
        case 8: handled = sz_utf8_upper_icelake_greek_basic_(v, target, available); break;
        default: handled = sz_utf8_case_icelake_caseless_(v, target, sz_true_k, available); break;
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
    return (sz_size_t)(target - start);
}
#include "stringzilla/utf8_case/initcap_icelake_mappings.h"
SZ_API_COMPTIME sz_size_t sz_utf8_case_initcap_icelake(sz_cptr_t source, sz_size_t length, sz_ptr_t target,
                                                       sz_size_t *error_offset) {
    if (error_offset) *error_offset = SZ_SIZE_MAX;
    if (!length) return 0;
    sz_cptr_t begin = source, end = source + length;
    sz_ptr_t start = target;
    sz_bool_t word_start = sz_true_k;
    while (source != end) {
        if ((sz_size_t)(end - source) >= 64) {
            __m512i v = _mm512_loadu_si512((void const *)source);
            if (!_mm512_movepi8_mask(v)) {
                __m512i lower = _mm512_or_si512(v, _mm512_set1_epi8(32));
                sz_u64_t letters = _mm512_cmplt_epu8_mask(_mm512_sub_epi8(lower, _mm512_set1_epi8('a')),
                                                          _mm512_set1_epi8(26));
                sz_u64_t alnum = letters | _mm512_cmplt_epu8_mask(_mm512_sub_epi8(v, _mm512_set1_epi8('0')),
                                                                  _mm512_set1_epi8(10));
                sz_u64_t capitalize = letters & ~((alnum << 1) | (word_start ? 0ull : 1ull));
                __m512i result = _mm512_mask_mov_epi8(v, letters, lower);
                result = _mm512_mask_sub_epi8(result, capitalize, result, _mm512_set1_epi8(32));
                _mm512_storeu_si512((void *)target, result);
                word_start = (sz_bool_t) !(alnum >> 63);
                source += 64;
                target += 64;
                continue;
            }
            sz_u64_t non_ascii = _mm512_movepi8_mask(v);
            sz_size_t first = (sz_size_t)_tzcnt_u64(non_ascii);
            unsigned int family = sz_utf8_case_family_(source + first, (sz_size_t)(end - source) - first);
            sz_size_t handled = 0;
            switch (family) {
            case 1: handled = sz_utf8_initcap_icelake_latin_(v, target, &word_start); break;
            case 2: handled = sz_utf8_initcap_icelake_cyrillic_(v, target, &word_start); break;
            case 3: handled = sz_utf8_initcap_icelake_greek_(v, target, &word_start); break;
            case 4: handled = sz_utf8_initcap_icelake_georgian_(v, target, &word_start); break;
            case 5: handled = sz_utf8_initcap_icelake_armenian_(v, target, &word_start); break;
            case 6: handled = sz_utf8_initcap_icelake_fullwidth_(v, target, &word_start); break;
            case 7: handled = sz_utf8_initcap_icelake_latin1_(v, target, &word_start); break;
            case 8: handled = sz_utf8_initcap_icelake_greek_basic_(v, target, &word_start); break;
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
