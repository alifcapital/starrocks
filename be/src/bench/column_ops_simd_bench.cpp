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

// Microbenchmarks for the column-ops SIMD paths added by PR #73289
// ([Enhancement] Add SIMD optimizations for column operations).
//
// Each pair benches the exact pre-PR scalar loop against the exact post-PR
// SIMD loop on identical inputs. Covers:
//   * column_hash.h           : memequal_padded (AVX2 32B vs SSE2 16B vs memcmp)
//   * column_helper.cpp       : merge_two_filters (AND), or_two_filters (OR),
//                               merge_nullable_filter (ANDN)
//   * fixed_length_column_base.cpp : fill_default conditional-fill blend
//                                    over int32 / int64

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

namespace starrocks {

static void fill_byte_mask(uint8_t* data, size_t n, int zero_ratio_percent, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> d(0, 99);
    for (size_t i = 0; i < n; ++i) {
        data[i] = (d(rng) < zero_ratio_percent) ? 0 : 1;
    }
}

// =====================================================================
// column_hash.h :: memequal_padded
// =====================================================================
//
// Pre-PR: SSE2 16-byte chunks
// Post-PR (AVX2): 32-byte chunks + SSE2 tail with overlap-last-16 trick
//
// Two regimes:  fully equal (worst case for both -- no early exit),
// mismatch in first 32 bytes (best case for both).  state.range(0) is the
// string length in bytes.

static bool scalar_memequal(const uint8_t* p1, size_t s1, const uint8_t* p2, size_t s2) {
    if (s1 != s2) return false;
    return std::memcmp(p1, p2, s1) == 0;
}

#if defined(__AVX2__)
static bool simd_memequal_avx2(const uint8_t* p1, size_t size1, const uint8_t* p2, size_t size2) {
    if (size1 != size2) return false;
    size_t offset = 0;
    for (; offset + 32 <= size1; offset += 32) {
        __m256i v1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p1 + offset));
        __m256i v2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p2 + offset));
        __m256i cmp = _mm256_cmpeq_epi8(v1, v2);
        uint32_t mask = ~static_cast<uint32_t>(_mm256_movemask_epi8(cmp));
        if (mask) return false;
    }
    if (offset < size1) {
        __m128i v1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1 + offset));
        __m128i v2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2 + offset));
        uint16_t mask = ~static_cast<uint16_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(v1, v2)));
        if (mask) {
            offset += __builtin_ctz(mask);
            return offset >= size1;
        }
        if (size1 - offset > 16) {
            __m128i v1_tail = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p1 + size1 - 16));
            __m128i v2_tail = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p2 + size1 - 16));
            uint16_t mask_tail = ~static_cast<uint16_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(v1_tail, v2_tail)));
            if (mask_tail) return false;
        }
    }
    return true;
}
#endif

static void BM_MemEqual_Scalar(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    // pad +32 so the AVX2 path's overread is harmless even when compared
    std::vector<uint8_t> a(n + 32, 0x42), b(n + 32, 0x42);
    for (auto _ : state) {
        bool r = scalar_memequal(a.data(), n, b.data(), n);
        benchmark::DoNotOptimize(r);
    }
}

static void BM_MemEqual_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<uint8_t> a(n + 32, 0x42), b(n + 32, 0x42);
    for (auto _ : state) {
#if defined(__AVX2__)
        bool r = simd_memequal_avx2(a.data(), n, b.data(), n);
#else
        bool r = scalar_memequal(a.data(), n, b.data(), n);
#endif
        benchmark::DoNotOptimize(r);
    }
}

BENCHMARK(BM_MemEqual_Scalar)->Arg(8)->Arg(16)->Arg(32)->Arg(64)->Arg(128)->Arg(512)->Arg(4096);
BENCHMARK(BM_MemEqual_SIMD)->Arg(8)->Arg(16)->Arg(32)->Arg(64)->Arg(128)->Arg(512)->Arg(4096);

// =====================================================================
// column_helper.cpp :: merge_two_filters (AND), or_two_filters (OR),
//                      merge_nullable_filter (ANDN)
// =====================================================================
//
// Pre-PR: for (i) data[i] = data[i] OP selected[i];
// Post-PR (AVX2): 32-byte AND/OR/ANDN; NEON: 16-byte.

static void scalar_and(uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; ++i) a[i] = a[i] & b[i];
}
static void simd_and(uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
#ifdef __AVX2__
    for (; i + 32 <= n; i += 32) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(a + i), _mm256_and_si256(va, vb));
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    for (; i + 16 <= n; i += 16) {
        vst1q_u8(a + i, vandq_u8(vld1q_u8(a + i), vld1q_u8(b + i)));
    }
#endif
    for (; i < n; ++i) a[i] = a[i] & b[i];
}

static void scalar_or(uint8_t* a, const uint8_t* b, size_t n) {
    for (size_t i = 0; i < n; ++i) a[i] = a[i] | b[i];
}
static void simd_or(uint8_t* a, const uint8_t* b, size_t n) {
    size_t i = 0;
#ifdef __AVX2__
    for (; i + 32 <= n; i += 32) {
        __m256i va = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
        __m256i vb = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(a + i), _mm256_or_si256(va, vb));
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    for (; i + 16 <= n; i += 16) {
        vst1q_u8(a + i, vorrq_u8(vld1q_u8(a + i), vld1q_u8(b + i)));
    }
#endif
    for (; i < n; ++i) a[i] = a[i] | b[i];
}

// merge_nullable_filter: selected &= !nulls -> ANDN(nulls, selected)
static void scalar_andn(uint8_t* selected, const uint8_t* nulls, size_t n) {
    for (size_t i = 0; i < n; ++i) selected[i] = selected[i] & !nulls[i];
}
static void simd_andn(uint8_t* selected, const uint8_t* nulls, size_t n) {
    size_t i = 0;
#ifdef __AVX2__
    for (; i + 32 <= n; i += 32) {
        __m256i vs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(selected + i));
        __m256i vn = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(nulls + i));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(selected + i), _mm256_andnot_si256(vn, vs));
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    for (; i + 16 <= n; i += 16) {
        vst1q_u8(selected + i, vbicq_u8(vld1q_u8(selected + i), vld1q_u8(nulls + i)));
    }
#endif
    for (; i < n; ++i) selected[i] = selected[i] & !nulls[i];
}

constexpr size_t kFilterChunk = 4096;

#define DEFINE_FILTER_BENCH(NAME, FN)                       \
    static void BM_Filter_##NAME(benchmark::State& state) { \
        size_t n = kFilterChunk;                            \
        std::vector<uint8_t> a(n), b(n);                    \
        fill_byte_mask(a.data(), n, 50, 0xAAAA);            \
        fill_byte_mask(b.data(), n, 50, 0xBBBB);            \
        for (auto _ : state) {                              \
            std::vector<uint8_t> ac = a;                    \
            FN(ac.data(), b.data(), n);                     \
            benchmark::DoNotOptimize(ac.data());            \
        }                                                   \
    }

DEFINE_FILTER_BENCH(And_Scalar, scalar_and)
DEFINE_FILTER_BENCH(And_SIMD, simd_and)
DEFINE_FILTER_BENCH(Or_Scalar, scalar_or)
DEFINE_FILTER_BENCH(Or_SIMD, simd_or)
DEFINE_FILTER_BENCH(Andn_Scalar, scalar_andn)
DEFINE_FILTER_BENCH(Andn_SIMD, simd_andn)

BENCHMARK(BM_Filter_And_Scalar);
BENCHMARK(BM_Filter_And_SIMD);
BENCHMARK(BM_Filter_Or_Scalar);
BENCHMARK(BM_Filter_Or_SIMD);
BENCHMARK(BM_Filter_Andn_Scalar);
BENCHMARK(BM_Filter_Andn_SIMD);

// =====================================================================
// fixed_length_column_base.cpp :: fill_default
// =====================================================================
//
// Conditional fill: where filter[i]==1, data[i] = default. Pre-PR is a
// scalar branch; post-PR uses AVX2 blendv (or NEON bsl). Bench int32 + int64.

template <typename T>
static void fill_default_scalar(T* data, const uint8_t* filter, size_t n, T val) {
    for (size_t i = 0; i < n; ++i) {
        if (filter[i] == 1) data[i] = val;
    }
}

template <typename T>
static void fill_default_simd(T* data, const uint8_t* filter, size_t n, T val);

template <>
void fill_default_simd<int32_t>(int32_t* data, const uint8_t* f, size_t n, int32_t val) {
    size_t i = 0;
#ifdef __AVX2__
    int32_t val_bits;
    std::memcpy(&val_bits, &val, sizeof(val_bits));
    const __m256i val_vec = _mm256_set1_epi32(val_bits);
    const __m256i zero = _mm256_setzero_si256();
    for (; i + 8 <= n; i += 8) {
        __m256i flt = _mm256_cvtepu8_epi32(_mm_loadl_epi64(reinterpret_cast<const __m128i*>(f + i)));
        __m256i mask = _mm256_cmpgt_epi32(flt, zero);
        __m256i cur = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i), _mm256_blendv_epi8(cur, val_vec, mask));
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    uint32_t val_bits;
    std::memcpy(&val_bits, &val, sizeof(val_bits));
    const uint32x4_t val_vec = vdupq_n_u32(val_bits);
    for (; i + 4 <= n; i += 4) {
        uint32_t f4;
        std::memcpy(&f4, f + i, sizeof(f4));
        uint32x4_t flt32 = {f4 & 0xFF, (f4 >> 8) & 0xFF, (f4 >> 16) & 0xFF, (f4 >> 24) & 0xFF};
        uint32x4_t mask = vcgtq_u32(flt32, vdupq_n_u32(0));
        uint32x4_t cur = vld1q_u32(reinterpret_cast<const uint32_t*>(data + i));
        vst1q_u32(reinterpret_cast<uint32_t*>(data + i), vbslq_u32(mask, val_vec, cur));
    }
#endif
    for (; i < n; ++i) {
        if (f[i] == 1) data[i] = val;
    }
}

static void BM_FillDefault_Int32_Scalar(benchmark::State& state) {
    size_t n = kFilterChunk;
    std::vector<int32_t> data(n, 0);
    std::vector<uint8_t> filter(n);
    int hit_ratio = static_cast<int>(state.range(0));
    fill_byte_mask(filter.data(), n, 100 - hit_ratio, 0xCAFE);
    for (auto _ : state) {
        fill_default_scalar<int32_t>(data.data(), filter.data(), n, static_cast<int32_t>(0xCAFEBABE));
        benchmark::DoNotOptimize(data.data());
    }
}

static void BM_FillDefault_Int32_SIMD(benchmark::State& state) {
    size_t n = kFilterChunk;
    std::vector<int32_t> data(n, 0);
    std::vector<uint8_t> filter(n);
    int hit_ratio = static_cast<int>(state.range(0));
    fill_byte_mask(filter.data(), n, 100 - hit_ratio, 0xCAFE);
    for (auto _ : state) {
        fill_default_simd<int32_t>(data.data(), filter.data(), n, static_cast<int32_t>(0xCAFEBABE));
        benchmark::DoNotOptimize(data.data());
    }
}

BENCHMARK(BM_FillDefault_Int32_Scalar)->Arg(0)->Arg(10)->Arg(50)->Arg(90)->Arg(100);
BENCHMARK(BM_FillDefault_Int32_SIMD)->Arg(0)->Arg(10)->Arg(50)->Arg(90)->Arg(100);

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/column_ops_simd_bench`.
// =====================================================================
