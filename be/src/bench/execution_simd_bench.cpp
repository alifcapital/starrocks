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

// Microbenchmarks for the execution-layer SIMD paths added by PR #73293
// ([Enhancement] Add SIMD optimizations for execution layer).
//
// Covers two paths unique to this PR:
//   * sorting/compare_column.cpp :: compare_integral_column_simd
//       SIMD-packed cmp for an int8 column against a scalar pivot, updating
//       a cmp_vector that the next sort column will refine. Adaptive: when
//       cmp_vector is mostly zeros (rows still equal at previous columns),
//       SIMD-packed compare; otherwise scalar skip.
//   * data_sinks/tablet_sink.cpp :: _validate_data selection normalize
//       validate_selection[j] &= 0x1 (AND-with-1) packed into AVX2/NEON.
//
// Other paths in this PR (local_exchange XOR, padding_char_column adaptive
// sparse-null, validate_selection bitwise) reuse the same byte-OR / byte-AND /
// count_nonzero-+-N/8 patterns already covered by column_ops_simd_bench and
// agg_simd_bench from earlier PRs in the stack.

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <vector>

namespace starrocks {

constexpr size_t kChunk = 4096;

static void fill_byte_mask(uint8_t* data, size_t n, int zero_ratio_percent, uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> d(0, 99);
    for (size_t i = 0; i < n; ++i) {
        data[i] = (d(rng) < zero_ratio_percent) ? 0 : 1;
    }
}

// =====================================================================
// sorting/compare_column.cpp :: compare_integral_column_simd (int8)
// =====================================================================
//
// cmp_vector[i] holds the running compare result for row i (-1 / 0 / +1).
// For a second sort column, we only need to recompute cmp where the
// previous columns already tied (cmp_vector[i] == 0). The adaptive SIMD:
//   * If at least 87.5% of cmp_vector is zero, AVX2-packed compare.
//   * Otherwise scalar skip-non-zeros.
//
// state.range(0) is the percentage of rows still equal (i.e., cmp == 0).

template <typename T>
static inline int8_t scalar_cmp(T lhs, T rhs) {
    return lhs < rhs ? int8_t{-1} : (lhs > rhs ? int8_t{1} : int8_t{0});
}

static void compare_int8_scalar(int8_t* cmp_vector, const int8_t* lhs, int8_t rhs, size_t n) {
    for (size_t i = 0; i < n; ++i) {
        if (cmp_vector[i] == 0) cmp_vector[i] = scalar_cmp<int8_t>(lhs[i], rhs);
    }
}

static void compare_int8_simd(int8_t* cmp_vector, const int8_t* lhs, int8_t rhs, size_t n) {
    size_t i = 0;
#ifdef __AVX2__
    constexpr size_t kBlock = 32;
    const __m256i zero = _mm256_setzero_si256();
    const __m256i rhs_vec = _mm256_set1_epi8(rhs);
    const __m256i ones = _mm256_set1_epi8(1);
    for (; i + kBlock <= n; i += kBlock) {
        __m256i cmp_bytes = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(cmp_vector + i));
        __m256i zeros = _mm256_cmpeq_epi8(cmp_bytes, zero);
        uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(zeros));
        if (mask != 0xFFFFFFFFu) {
            // Mixed block: fall back to scalar so we don't overwrite non-zero cmp entries.
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int8_t>(lhs[j], rhs);
            }
            continue;
        }
        __m256i lhs_v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + i));
        __m256i gt = _mm256_cmpgt_epi8(lhs_v, rhs_vec);
        __m256i lt = _mm256_cmpgt_epi8(rhs_vec, lhs_v);
        __m256i gt01 = _mm256_and_si256(gt, ones);
        __m256i lt01 = _mm256_and_si256(lt, ones);
        __m256i result = _mm256_sub_epi8(gt01, lt01); // +1 / 0 / -1
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(cmp_vector + i), result);
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    constexpr size_t kBlock = 16;
    const int8x16_t zero = vdupq_n_s8(0);
    const int8x16_t rhs_vec = vdupq_n_s8(rhs);
    const int8x16_t ones = vdupq_n_s8(1);
    for (; i + kBlock <= n; i += kBlock) {
        int8x16_t cmp_bytes = vld1q_s8(cmp_vector + i);
        uint8x16_t zeros = vceqq_s8(cmp_bytes, zero);
        if (vminvq_u8(zeros) != 0xFF) {
            for (size_t j = i; j < i + kBlock; ++j) {
                if (cmp_vector[j] == 0) cmp_vector[j] = scalar_cmp<int8_t>(lhs[j], rhs);
            }
            continue;
        }
        int8x16_t lhs_v = vld1q_s8(lhs + i);
        int8x16_t gt = vreinterpretq_s8_u8(vcgtq_s8(lhs_v, rhs_vec));
        int8x16_t lt = vreinterpretq_s8_u8(vcltq_s8(lhs_v, rhs_vec));
        int8x16_t gt01 = vandq_s8(gt, ones);
        int8x16_t lt01 = vandq_s8(lt, ones);
        vst1q_s8(cmp_vector + i, vsubq_s8(gt01, lt01));
    }
#endif
    for (; i < n; ++i) {
        if (cmp_vector[i] == 0) cmp_vector[i] = scalar_cmp<int8_t>(lhs[i], rhs);
    }
}

static void prepare_compare(std::vector<int8_t>& cmp, std::vector<int8_t>& lhs, int zero_ratio_percent) {
    cmp.resize(kChunk);
    lhs.resize(kChunk);
    std::mt19937_64 rng(0x501234DE);
    std::uniform_int_distribution<int> rd(0, 99);
    std::uniform_int_distribution<int> vd(-128, 127);
    for (size_t i = 0; i < kChunk; ++i) {
        cmp[i] = (rd(rng) < zero_ratio_percent) ? int8_t{0} : (rd(rng) < 50 ? int8_t{-1} : int8_t{1});
        lhs[i] = static_cast<int8_t>(vd(rng));
    }
}

static void BM_CompareInt8_Scalar(benchmark::State& state) {
    std::vector<int8_t> cmp_init, lhs;
    prepare_compare(cmp_init, lhs, static_cast<int>(state.range(0)));
    for (auto _ : state) {
        std::vector<int8_t> cmp = cmp_init;
        compare_int8_scalar(cmp.data(), lhs.data(), int8_t{0}, kChunk);
        benchmark::DoNotOptimize(cmp.data());
    }
}

static void BM_CompareInt8_SIMD(benchmark::State& state) {
    std::vector<int8_t> cmp_init, lhs;
    prepare_compare(cmp_init, lhs, static_cast<int>(state.range(0)));
    for (auto _ : state) {
        std::vector<int8_t> cmp = cmp_init;
        compare_int8_simd(cmp.data(), lhs.data(), int8_t{0}, kChunk);
        benchmark::DoNotOptimize(cmp.data());
    }
}

BENCHMARK(BM_CompareInt8_Scalar)->Arg(0)->Arg(50)->Arg(90)->Arg(99)->Arg(100);
BENCHMARK(BM_CompareInt8_SIMD)->Arg(0)->Arg(50)->Arg(90)->Arg(99)->Arg(100);

// =====================================================================
// data_sinks/tablet_sink.cpp :: _validate_data selection normalize
// =====================================================================
//
// Pre-PR: for (j) validate_selection[j] &= 0x1;
// Post-PR (AVX2): 32-byte broadcast-AND. NEON: 16-byte.
//
// Used to mask validation bits back to {0,1} between validation passes.

static void normalize_selection_scalar(uint8_t* sel, size_t n) {
    for (size_t i = 0; i < n; ++i) sel[i] &= 0x1;
}

static void normalize_selection_simd(uint8_t* sel, size_t n) {
    size_t i = 0;
#ifdef __AVX2__
    const __m256i and_mask = _mm256_set1_epi8(0x1);
    for (; i + 32 <= n; i += 32) {
        __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(sel + i));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(sel + i), _mm256_and_si256(data, and_mask));
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    const uint8x16_t and_mask = vdupq_n_u8(0x1);
    for (; i + 16 <= n; i += 16) {
        vst1q_u8(sel + i, vandq_u8(vld1q_u8(sel + i), and_mask));
    }
#endif
    for (; i < n; ++i) sel[i] &= 0x1;
}

static void BM_NormalizeSelection_Scalar(benchmark::State& state) {
    std::vector<uint8_t> sel(kChunk);
    fill_byte_mask(sel.data(), kChunk, 50, 0xAB);
    for (auto _ : state) {
        std::vector<uint8_t> s = sel;
        normalize_selection_scalar(s.data(), kChunk);
        benchmark::DoNotOptimize(s.data());
    }
}

static void BM_NormalizeSelection_SIMD(benchmark::State& state) {
    std::vector<uint8_t> sel(kChunk);
    fill_byte_mask(sel.data(), kChunk, 50, 0xAB);
    for (auto _ : state) {
        std::vector<uint8_t> s = sel;
        normalize_selection_simd(s.data(), kChunk);
        benchmark::DoNotOptimize(s.data());
    }
}

BENCHMARK(BM_NormalizeSelection_Scalar);
BENCHMARK(BM_NormalizeSelection_SIMD);

} // namespace starrocks

BENCHMARK_MAIN();

// =====================================================================
// Paste results below after `./build_Release/src/bench/output/execution_simd_bench`.
// =====================================================================
