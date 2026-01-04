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

#include "simd/expand.h"

#include "simd/multi_version.h"
#include "simd/simd.h"
#include "util/int96.h"

#if defined(__AVX2__)
#include <immintrin.h>
#endif

#if defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

namespace SIMD::Expand {

#if defined(__AVX2__)
// Lookup table for AVX2 expand: for each 8-bit mask, stores the permutation indices
// to expand non-null values into their correct positions.
// Mutable - initialized at startup.
alignas(32) static uint32_t kExpandPermuteLUT[256][8];

// Count of valid (non-null) elements for each 8-bit mask
static const uint8_t kExpandCountLUT[256] = {
    0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
    4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
};

static const uint8_t kExpandCountLUT64[16] = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};

// Generate expand permutation table at program startup
struct ExpandLUTInitializer {
    ExpandLUTInitializer() {
        // Initialize 8-element LUT for int32
        for (uint32_t mask = 0; mask < 256; ++mask) {
            uint32_t src_idx = 0;
            for (uint32_t dst_idx = 0; dst_idx < 8; ++dst_idx) {
                if (mask & (1u << dst_idx)) {
                    kExpandPermuteLUT[mask][dst_idx] = src_idx++;
                } else {
                    kExpandPermuteLUT[mask][dst_idx] = 0;
                }
            }
        }
    }
};
static ExpandLUTInitializer g_expand_lut_init;
#endif // __AVX2__

MFV_DEFAULT(void expand_load_selection_i32(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                                           size_t count) { expand_load_branchless(dst_data, src_data, nulls, count); });

MFV_AVX2(void expand_load_selection_i32(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                                        size_t count) {
    // Pre-calculate total valid elements to avoid reading past src_data.
    // nulls[i] == 0 means "valid", so count_zero gives us total_valid.
    size_t total_valid = SIMD::count_zero(nulls, count);

    size_t cnt = 0;
    size_t i = 0;
    // Process 8 elements at a time using AVX2 permute with LUT
    // Only use SIMD while we have at least 8 valid elements remaining to load safely
    for (; i + 8 <= count && cnt + 8 <= total_valid; i += 8) {
        // Load 8 null flags and create bitmask using SIMD comparison
        __m128i null_vec = _mm_loadl_epi64(reinterpret_cast<const __m128i*>(&nulls[i]));
        __m128i zero_vec = _mm_setzero_si128();
        __m128i cmp = _mm_cmpeq_epi8(null_vec, zero_vec);
        uint32_t mask8 = _mm_movemask_epi8(cmp) & 0xFF;  // Only lower 8 bits

        // Load source data (safe: we verified cnt + 8 <= total_valid)
        __m256i src_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src_data + cnt));

        // Load permutation indices from LUT and permute
        __m256i perm = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(kExpandPermuteLUT[mask8]));
        __m256i dst_vec = _mm256_permutevar8x32_epi32(src_vec, perm);

        // Store result
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst_data + i), dst_vec);

        cnt += kExpandCountLUT[mask8];
    }
    // Handle remaining elements with scalar code
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
});

MFV_AVX512VLBW(void expand_load_selection_i32(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls,
                                              size_t count) {
    size_t cnt = 0;
    size_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __mmask16 null_mask;
        __m128i mask = _mm_loadu_epi8(&nulls[i]);
        null_mask = _mm_cmp_epi8_mask(mask, _mm_setzero_si128(), _MM_CMPINT_EQ);
        __m512i loaded = _mm512_maskz_expandloadu_epi32(null_mask, &src_data[cnt]);
        cnt += _mm_popcnt_u32(null_mask);
        _mm512_storeu_epi32(&dst_data[i], loaded);
    }
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
});

MFV_DEFAULT(void expand_load_selection_i64(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                                           size_t count) { expand_load_branchless(dst_data, src_data, nulls, count); });

MFV_AVX2(void expand_load_selection_i64(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                                        size_t count) {
    // Precomputed LUT for 64-bit expand: each entry is 8 x int32 indices for permutevar8x32
    // Maps 4-bit mask to permutation pattern for 4 x int64 elements (treated as 8 x int32)
    static const int32_t kPerm64LUT[16][8] = {
        {0, 1, 0, 1, 0, 1, 0, 1},  // 0b0000
        {0, 1, 0, 1, 0, 1, 0, 1},  // 0b0001
        {0, 1, 0, 1, 0, 1, 0, 1},  // 0b0010
        {0, 1, 2, 3, 0, 1, 0, 1},  // 0b0011
        {0, 1, 0, 1, 0, 1, 0, 1},  // 0b0100
        {0, 1, 0, 1, 2, 3, 0, 1},  // 0b0101
        {0, 1, 0, 1, 2, 3, 0, 1},  // 0b0110
        {0, 1, 2, 3, 4, 5, 0, 1},  // 0b0111
        {0, 1, 0, 1, 0, 1, 0, 1},  // 0b1000
        {0, 1, 0, 1, 0, 1, 2, 3},  // 0b1001
        {0, 1, 0, 1, 0, 1, 2, 3},  // 0b1010
        {0, 1, 2, 3, 0, 1, 4, 5},  // 0b1011
        {0, 1, 0, 1, 0, 1, 2, 3},  // 0b1100
        {0, 1, 0, 1, 2, 3, 4, 5},  // 0b1101
        {0, 1, 0, 1, 2, 3, 4, 5},  // 0b1110
        {0, 1, 2, 3, 4, 5, 6, 7},  // 0b1111
    };

    // Pre-calculate total valid elements to avoid reading past src_data.
    // nulls[i] == 0 means "valid", so count_zero gives us total_valid.
    size_t total_valid = SIMD::count_zero(nulls, count);

    size_t cnt = 0;
    size_t i = 0;
    // Process 4 elements at a time (4 x int64 = 256 bits)
    // Only use SIMD while we have at least 4 valid elements remaining to load safely
    for (; i + 4 <= count && cnt + 4 <= total_valid; i += 4) {
        // Load 4 null flags and create bitmask
        __m128i null_vec = _mm_cvtsi32_si128(*reinterpret_cast<const uint32_t*>(&nulls[i]));
        __m128i cmp = _mm_cmpeq_epi8(null_vec, _mm_setzero_si128());
        uint32_t mask4 = _mm_movemask_epi8(cmp) & 0xF;

        // Load source data (safe: we verified cnt + 4 <= total_valid)
        __m256i src_vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src_data + cnt));

        // Load permutation and shuffle
        __m256i perm = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(kPerm64LUT[mask4]));
        __m256i dst_vec = _mm256_permutevar8x32_epi32(src_vec, perm);

        // Store result
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst_data + i), dst_vec);

        cnt += kExpandCountLUT64[mask4];
    }
    // Handle remaining elements with scalar code
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
});

MFV_AVX512VLBW(void expand_load_selection_i64(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls,
                                              size_t count) {
    size_t cnt = 0;
    size_t i = 0;
    for (; i + 16 <= count; i += 16) {
        __m128i mask = _mm_loadu_epi8(&nulls[i]);
        __mmask16 null_mask = _mm_cmp_epi8_mask(mask, _mm_setzero_si128(), _MM_CMPINT_EQ);

        __m512i loaded;

        // process low mask
        __mmask8 mask_lo = null_mask;
        loaded = _mm512_maskz_expandloadu_epi64(mask_lo, &src_data[cnt]);
        _mm512_storeu_epi64(&dst_data[i], loaded);
        cnt += _mm_popcnt_u32(mask_lo);

        __mmask8 mask_hi = null_mask >> 8;
        loaded = _mm512_maskz_expandloadu_epi64(mask_hi, &src_data[cnt]);
        _mm512_storeu_epi64(&dst_data[i + 8], loaded);
        cnt += _mm_popcnt_u32(mask_hi);
    }
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
})

template <class DataType>
void expand_load_branchless(DataType* dst_data, const DataType* src_data, const uint8_t* nulls, size_t count) {
    size_t cnt = 0;
    for (size_t i = 0; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
}

void expand_load_simd(int32_t* dst_data, const int32_t* src_data, const uint8_t* nulls, size_t count) {
#if defined(__ARM_NEON) && defined(__aarch64__)
    // ARM NEON implementation using tbl for permutation
    // LUT for 4-element expand: 16 possible masks, each with 4 byte offsets (for tbl)
    // Each int32 needs 4 consecutive byte indices, so we store base byte offset
    static const uint8_t kNeonExpandLUT[16][4] = {
        {0, 0, 0, 0},     // 0b0000: all null
        {0, 0, 0, 0},     // 0b0001: pos 0 valid
        {0, 0, 0, 0},     // 0b0010: pos 1 valid
        {0, 4, 0, 0},     // 0b0011: pos 0,1 valid
        {0, 0, 0, 0},     // 0b0100: pos 2 valid
        {0, 0, 4, 0},     // 0b0101: pos 0,2 valid
        {0, 0, 4, 0},     // 0b0110: pos 1,2 valid
        {0, 4, 8, 0},     // 0b0111: pos 0,1,2 valid
        {0, 0, 0, 0},     // 0b1000: pos 3 valid
        {0, 0, 0, 4},     // 0b1001: pos 0,3 valid
        {0, 0, 0, 4},     // 0b1010: pos 1,3 valid
        {0, 4, 0, 8},     // 0b1011: pos 0,1,3 valid
        {0, 0, 0, 4},     // 0b1100: pos 2,3 valid
        {0, 0, 4, 8},     // 0b1101: pos 0,2,3 valid
        {0, 0, 4, 8},     // 0b1110: pos 1,2,3 valid
        {0, 4, 8, 12},    // 0b1111: all valid
    };
    static const uint8_t kNeonCountLUT[16] = {0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4};

    // Pre-calculate total valid elements to avoid reading past src_data.
    // nulls[i] == 0 means "valid", so count_zero gives us total_valid.
    size_t total_valid = SIMD::count_zero(nulls, count);

    size_t cnt = 0;
    size_t i = 0;
    // Process 4 elements at a time (4 x int32 = 128 bits for NEON)
    // Only use SIMD while we have at least 4 valid elements remaining to load safely
    for (; i + 4 <= count && cnt + 4 <= total_valid; i += 4) {
        // Create 4-bit mask: bit j is set if nulls[i+j] == 0 (not null = valid)
        uint32_t mask4 = ((nulls[i] == 0) ? 1 : 0) |
                         ((nulls[i+1] == 0) ? 2 : 0) |
                         ((nulls[i+2] == 0) ? 4 : 0) |
                         ((nulls[i+3] == 0) ? 8 : 0);

        // Load 4 source elements (safe: we verified cnt + 4 <= total_valid)
        uint32x4_t src_vec = vld1q_u32(reinterpret_cast<const uint32_t*>(src_data + cnt));

        // Create byte indices for tbl: each int32 = 4 consecutive bytes
        uint8_t b0 = kNeonExpandLUT[mask4][0];
        uint8_t b1 = kNeonExpandLUT[mask4][1];
        uint8_t b2 = kNeonExpandLUT[mask4][2];
        uint8_t b3 = kNeonExpandLUT[mask4][3];
        uint8x16_t idx = {b0, static_cast<uint8_t>(b0+1), static_cast<uint8_t>(b0+2), static_cast<uint8_t>(b0+3),
                         b1, static_cast<uint8_t>(b1+1), static_cast<uint8_t>(b1+2), static_cast<uint8_t>(b1+3),
                         b2, static_cast<uint8_t>(b2+1), static_cast<uint8_t>(b2+2), static_cast<uint8_t>(b2+3),
                         b3, static_cast<uint8_t>(b3+1), static_cast<uint8_t>(b3+2), static_cast<uint8_t>(b3+3)};

        // Permute using tbl
        uint8x16_t src_bytes = vreinterpretq_u8_u32(src_vec);
        uint8x16_t dst_bytes = vqtbl1q_u8(src_bytes, idx);
        uint32x4_t dst_vec = vreinterpretq_u32_u8(dst_bytes);

        // Store result
        vst1q_u32(reinterpret_cast<uint32_t*>(dst_data + i), dst_vec);

        cnt += kNeonCountLUT[mask4];
    }
    // Handle remaining elements with scalar code
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
#else
    expand_load_selection_i32(dst_data, src_data, nulls, count);
#endif
}

void expand_load_simd(int64_t* dst_data, const int64_t* src_data, const uint8_t* nulls, size_t count) {
#if defined(__ARM_NEON) && defined(__aarch64__)
    // ARM NEON implementation for int64
    static const uint8_t kNeonCountLUT[4] = {0, 1, 1, 2};

    // Pre-calculate total valid elements to avoid reading past src_data.
    // nulls[i] == 0 means "valid", so count_zero gives us total_valid.
    size_t total_valid = SIMD::count_zero(nulls, count);

    size_t cnt = 0;
    size_t i = 0;
    // Process 2 elements at a time (2 x int64 = 128 bits for NEON)
    // Only use SIMD while we have at least 2 valid elements remaining to load safely
    for (; i + 2 <= count && cnt + 2 <= total_valid; i += 2) {
        // Load 2 null flags
        uint8_t n0 = nulls[i];
        uint8_t n1 = nulls[i + 1];
        uint32_t mask2 = (n0 == 0 ? 1 : 0) | (n1 == 0 ? 2 : 0);

        // Load source data (safe: we verified cnt + 2 <= total_valid)
        uint64x2_t src_vec = vld1q_u64(reinterpret_cast<const uint64_t*>(src_data + cnt));

        // Simple 2-element permutation
        uint64x2_t dst_vec;
        switch (mask2) {
            case 0:  // both null - don't care
                dst_vec = src_vec;
                break;
            case 1:  // only first valid
                dst_vec = src_vec;
                break;
            case 2:  // only second valid
                dst_vec = vdupq_laneq_u64(src_vec, 0);  // src[0] -> both
                break;
            case 3:  // both valid
                dst_vec = src_vec;
                break;
            default:
                dst_vec = src_vec;
                break;
        }

        // Store result
        vst1q_u64(reinterpret_cast<uint64_t*>(dst_data + i), dst_vec);

        cnt += kNeonCountLUT[mask2];
    }
    // Handle remaining elements with scalar code
    for (; i < count; ++i) {
        dst_data[i] = src_data[cnt];
        cnt += !nulls[i];
    }
#else
    expand_load_selection_i64(dst_data, src_data, nulls, count);
#endif
}

template void expand_load_branchless<int32_t>(int32_t*, const int32_t*, const uint8_t*, size_t);
template void expand_load_branchless<uint32_t>(uint32_t*, const uint32_t*, const uint8_t*, size_t);
template void expand_load_branchless<double>(double*, const double*, const uint8_t*, size_t);
template void expand_load_branchless<float>(float*, const float*, const uint8_t*, size_t);
template void expand_load_branchless<int64_t>(int64_t*, const int64_t*, const uint8_t*, size_t);
using int96_t = starrocks::int96_t;
template void expand_load_branchless<int96_t>(int96_t*, const int96_t*, const uint8_t*, size_t);

} // namespace SIMD::Expand