/**
 *  @brief Unicode 17 UTF-8 default lowercase and uppercase conversion.
 *
 *  Uses full default case mappings, including expansions and context-sensitive Final_Sigma.
 *  No locale tailoring or normalization is applied. Malformed bytes are copied unchanged;
 *  each malformed byte breaks the casing context. Source and destination must not overlap.
 *  The destination must have at least source_length * 3 bytes. Returns bytes written.
 */
#ifndef STRINGZILLA_UTF8_CASE_H_
#define STRINGZILLA_UTF8_CASE_H_
#include "stringzilla/utf8_runes/serial.h"
#ifdef __cplusplus
extern "C" {
#endif
SZ_API_RUNTIME sz_size_t sz_utf8_case_lower(sz_cptr_t source, sz_size_t source_length, sz_ptr_t destination);
SZ_API_RUNTIME sz_size_t sz_utf8_case_upper(sz_cptr_t source, sz_size_t source_length, sz_ptr_t destination);
/**
 *  @brief Capitalizes runs of Unicode Letters and Decimal_Number characters with simple mappings.
 *  Separators are unchanged. This is not Unicode titlecase or locale-sensitive casing.
 *  Destination capacity must be at least 3 * length; buffers must not overlap.
 *  Returns bytes written, or SZ_SIZE_MAX for invalid UTF-8. error_offset, when non-null,
 *  receives the start of the invalid sequence, or SZ_SIZE_MAX on success.
 */
SZ_API_RUNTIME sz_size_t sz_utf8_case_initcap(sz_cptr_t source, sz_size_t length, sz_ptr_t target,
                                              sz_size_t *error_offset);
#include "stringzilla/utf8_case/serial.h"
#include "stringzilla/utf8_case/haswell.h"
#include "stringzilla/utf8_case/icelake.h"
#if !SZ_DYNAMIC_DISPATCH
SZ_API_RUNTIME sz_size_t sz_utf8_case_lower(sz_cptr_t source, sz_size_t length, sz_ptr_t target) {
#if SZ_USE_ICELAKE
    return sz_utf8_case_lower_icelake(source, length, target);
#elif SZ_USE_HASWELL
    return sz_utf8_case_lower_haswell(source, length, target);
#else
    return sz_utf8_case_lower_serial(source, length, target);
#endif
}
SZ_API_RUNTIME sz_size_t sz_utf8_case_upper(sz_cptr_t source, sz_size_t length, sz_ptr_t target) {
#if SZ_USE_ICELAKE
    return sz_utf8_case_upper_icelake(source, length, target);
#elif SZ_USE_HASWELL
    return sz_utf8_case_upper_haswell(source, length, target);
#else
    return sz_utf8_case_upper_serial(source, length, target);
#endif
}
SZ_API_RUNTIME sz_size_t sz_utf8_case_initcap(sz_cptr_t source, sz_size_t length, sz_ptr_t target,
                                              sz_size_t *error_offset) {
#if SZ_USE_ICELAKE
    return sz_utf8_case_initcap_icelake(source, length, target, error_offset);
#elif SZ_USE_HASWELL
    return sz_utf8_case_initcap_haswell(source, length, target, error_offset);
#else
    return sz_utf8_case_initcap_serial(source, length, target, error_offset);
#endif
}
#endif
#ifdef __cplusplus
}
#endif
#endif
