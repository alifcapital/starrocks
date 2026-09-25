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

#include "util/utf8_case.h"

#include <stdexcept>

#if defined(__x86_64__)
#define SZ_USE_HASWELL 1
#define SZ_USE_ICELAKE 1
#endif
#include "stringzilla/utf8_case.h"
#include "stringzilla/utf8_uncased_fold.h"

namespace starrocks {
namespace {
struct CaseConverters {
    UTF8CaseConverter lower = sz_utf8_case_lower_serial;
    UTF8CaseConverter upper = sz_utf8_case_upper_serial;
    UTF8CaseConverter fold = sz_utf8_uncased_fold_serial;
    sz_utf8_case_initcap_t initcap = sz_utf8_case_initcap_serial;

    CaseConverters() {
#if defined(__x86_64__)
        __builtin_cpu_init();
        if (__builtin_cpu_supports("avx2") && __builtin_cpu_supports("bmi") && __builtin_cpu_supports("bmi2")) {
            lower = sz_utf8_case_lower_haswell;
            upper = sz_utf8_case_upper_haswell;
            fold = sz_utf8_uncased_fold_haswell;
            initcap = sz_utf8_case_initcap_haswell;
            if (__builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl") &&
                __builtin_cpu_supports("avx512bw") && __builtin_cpu_supports("avx512dq") &&
                __builtin_cpu_supports("avx512vbmi") && __builtin_cpu_supports("avx512vbmi2") &&
                __builtin_cpu_supports("lzcnt") && __builtin_cpu_supports("popcnt")) {
                lower = sz_utf8_case_lower_icelake;
                upper = sz_utf8_case_upper_icelake;
                fold = sz_utf8_uncased_fold_icelake;
                initcap = sz_utf8_case_initcap_icelake;
            }
        }
#endif
    }
};

const CaseConverters& converters() {
    static const CaseConverters instance;
    return instance;
}
} // namespace

UTF8CaseConverter utf8_lower_converter() {
    return converters().lower;
}
UTF8CaseConverter utf8_upper_converter() {
    return converters().upper;
}
size_t utf8_initcap(const char* src, size_t length, char* dst, size_t* error_offset) {
    return converters().initcap(src, length, dst, error_offset);
}
void utf8_tolower(const char* src, size_t length, std::string& dst) {
    if (length > dst.max_size() / 3) {
        throw std::length_error("UTF-8 lowercase exceeds string capacity");
    }
    dst.resize(length * 3);
    if (length != 0) {
        dst.resize(utf8_lower_converter()(src, length, dst.data()));
    }
}

void utf8_casefold(const char* src, size_t length, std::string& dst) {
    if (length > dst.max_size() / 3) {
        throw std::length_error("UTF-8 case folding exceeds string capacity");
    }
    dst.resize(length * 3);
    if (length != 0) {
        dst.resize(converters().fold(src, length, dst.data()));
    }
}

} // namespace starrocks
