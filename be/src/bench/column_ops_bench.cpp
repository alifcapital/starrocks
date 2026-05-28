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

// Microbenchmarks for the column-ops SIMD paths shipping in PR #73289.
//
// Each pair benches the inline pre-PR scalar form against the post-PR SIMD
// form on identical inputs.  Covers (one pair per file touched):
//
//   * column.cpp                     -- null-vector materialisation bit-pack
//   * chunk.cpp                      -- Chunk::filter all-ones short-circuit
//   * column_hash/column_hash.cpp    -- null-run boundary detection
//   * fixed_length_column_base.cpp   -- replicate() inner fill
//   * column_helper.cpp              -- bytewise AND / OR / ANDN (scalar only;
//                                       documents the no-win observation that
//                                       made us keep the hand-rolled SIMD out)

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "base/simd/simd.h"
#include "base/simd/simd_utils.h"

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
// column.cpp :: null-vector materialisation bit-pack
// =====================================================================
//
// Pre-PR: byte-by-byte loop over null_data with a branchy offset check.
// Post-PR (AVX2): pack 32 null bytes into a 32-bit mask via cmpgt +
//                 movemask, walk only set bits via ctz + clear-lowest.
// state.range(0) = null density in percent.

static void scan_nulls_scalar(const uint8_t* null_data, const uint32_t* offsets, size_t size, bool& need_empty) {
    for (size_t i = 0; i < size && !need_empty; ++i) {
        if (null_data[i] && offsets[i + 1] != offsets[i]) {
            need_empty = true;
        }
    }
}

#if defined(__AVX2__)
static void scan_nulls_avx2(const uint8_t* null_data, const uint32_t* offsets, size_t size, bool& need_empty) {
    const __m256i zero = _mm256_setzero_si256();
    size_t i = 0;
    for (; i + 32 <= size && !need_empty; i += 32) {
        __m256i v_null = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(null_data + i));
        uint32_t null_mask = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpgt_epi8(v_null, zero)));
        while (null_mask && !need_empty) {
            uint32_t bit_pos = __builtin_ctz(null_mask);
            size_t idx = i + bit_pos;
            if (offsets[idx + 1] != offsets[idx]) {
                need_empty = true;
            }
            null_mask &= null_mask - 1;
        }
    }
    for (; i < size && !need_empty; ++i) {
        if (null_data[i] && offsets[i + 1] != offsets[i]) {
            need_empty = true;
        }
    }
}
#endif

template <bool simd>
static void do_NullScan(benchmark::State& state) {
    const int null_pct = state.range(0);
    std::vector<uint8_t> null_data(kChunk);
    std::vector<uint32_t> offsets(kChunk + 1);
    fill_byte_mask(null_data.data(), kChunk, 100 - null_pct, 0xBEEF);
    offsets[0] = 0;
    for (size_t i = 0; i < kChunk; ++i) offsets[i + 1] = offsets[i] + 4; // every row non-empty
    for (auto _ : state) {
        bool need_empty = false;
        if constexpr (simd) {
#if defined(__AVX2__)
            scan_nulls_avx2(null_data.data(), offsets.data(), kChunk, need_empty);
#else
            scan_nulls_scalar(null_data.data(), offsets.data(), kChunk, need_empty);
#endif
        } else {
            scan_nulls_scalar(null_data.data(), offsets.data(), kChunk, need_empty);
        }
        benchmark::DoNotOptimize(need_empty);
    }
}

static void BM_NullScan_Scalar(benchmark::State& state) {
    do_NullScan<false>(state);
}
static void BM_NullScan_SIMD(benchmark::State& state) {
    do_NullScan<true>(state);
}

BENCHMARK(BM_NullScan_Scalar)->Arg(0)->Arg(10)->Arg(50)->Arg(90);
BENCHMARK(BM_NullScan_SIMD)->Arg(0)->Arg(10)->Arg(50)->Arg(90);

// =====================================================================
// chunk.cpp :: filter all-ones short-circuit
// =====================================================================
//
// Pre-PR: `SIMD::count_zero(selection) == 0` -- always full scan to count.
// Post-PR: `SIMD::all_ones(selection)` -- memchr early-exits at first zero.
// state.range(0) = zero density in percent (0 == all-ones, the worst case
// for memchr).

static void BM_AllOnes_CountZero(benchmark::State& state) {
    const int zero_pct = state.range(0);
    std::vector<uint8_t> sel(kChunk);
    fill_byte_mask(sel.data(), kChunk, zero_pct, 0xCAFE);
    for (auto _ : state) {
        bool all_ones = SIMD::count_zero(sel) == 0;
        benchmark::DoNotOptimize(all_ones);
    }
}

static void BM_AllOnes_Memchr(benchmark::State& state) {
    const int zero_pct = state.range(0);
    std::vector<uint8_t> sel(kChunk);
    fill_byte_mask(sel.data(), kChunk, zero_pct, 0xCAFE);
    for (auto _ : state) {
        bool all_ones = SIMD::all_ones(sel);
        benchmark::DoNotOptimize(all_ones);
    }
}

BENCHMARK(BM_AllOnes_CountZero)->Arg(0)->Arg(1)->Arg(10)->Arg(50);
BENCHMARK(BM_AllOnes_Memchr)->Arg(0)->Arg(1)->Arg(10)->Arg(50);

// =====================================================================
// column_hash/column_hash.cpp :: null-run boundary detection
// =====================================================================
//
// Pre-PR: `while (next < to && null_data[next] == null_data[cursor]) ++next;`
//         a per-byte equality loop until the run flips.
// Post-PR: `SIMD::find_zero` / `SIMD::find_nonzero` on the null array.
// state.range(0) = null density in percent (drives average run length).

static void scan_runs_scalar(const uint8_t* null_data, size_t size, size_t& runs) {
    size_t cursor = 0;
    while (cursor < size) {
        size_t next = cursor + 1;
        while (next < size && null_data[next] == null_data[cursor]) ++next;
        ++runs;
        cursor = next;
    }
}

static void scan_runs_simd(const std::vector<uint8_t>& null_data, size_t& runs) {
    const size_t size = null_data.size();
    size_t cursor = 0;
    while (cursor < size) {
        size_t next;
        if (null_data[cursor]) {
            next = SIMD::find_zero(null_data, cursor + 1, size - cursor - 1);
        } else {
            next = SIMD::find_nonzero(null_data, cursor + 1, size - cursor - 1);
        }
        ++runs;
        cursor = next;
    }
}

static void BM_RunBoundary_Scalar(benchmark::State& state) {
    const int null_pct = state.range(0);
    std::vector<uint8_t> null_data(kChunk);
    fill_byte_mask(null_data.data(), kChunk, 100 - null_pct, 0xDEAD);
    for (auto _ : state) {
        size_t runs = 0;
        scan_runs_scalar(null_data.data(), kChunk, runs);
        benchmark::DoNotOptimize(runs);
    }
}

static void BM_RunBoundary_SIMD(benchmark::State& state) {
    const int null_pct = state.range(0);
    std::vector<uint8_t> null_data(kChunk);
    fill_byte_mask(null_data.data(), kChunk, 100 - null_pct, 0xDEAD);
    for (auto _ : state) {
        size_t runs = 0;
        scan_runs_simd(null_data, runs);
        benchmark::DoNotOptimize(runs);
    }
}

BENCHMARK(BM_RunBoundary_Scalar)->Arg(1)->Arg(10)->Arg(50)->Arg(90);
BENCHMARK(BM_RunBoundary_SIMD)->Arg(1)->Arg(10)->Arg(50)->Arg(90);

// =====================================================================
// fixed_length_column_base.cpp :: replicate() inner fill
// =====================================================================
//
// Pre-PR: nested scalar loop `dest[j] = src[i]` for j in [offsets[i], offsets[i+1]).
// Post-PR: `SIMDUtils::simd_fill<T>(dest + offsets[i], src[i], fill_count)`.
// state.range(0) = fill_count per source element (constant for the bench).

template <typename T>
static void replicate_scalar(T* dest, const T* src, const uint32_t* offsets, size_t orig_size) {
    for (size_t i = 0; i < orig_size; ++i) {
        for (uint32_t j = offsets[i]; j < offsets[i + 1]; ++j) {
            dest[j] = src[i];
        }
    }
}

template <typename T>
static void replicate_simd_fill(T* dest, const T* src, const uint32_t* offsets, size_t orig_size) {
    for (size_t i = 0; i < orig_size; ++i) {
        size_t fill_count = offsets[i + 1] - offsets[i];
        SIMDUtils::simd_fill(dest + offsets[i], src[i], fill_count);
    }
}

constexpr size_t kSrcRows = 256;

template <typename T, void (*Impl)(T*, const T*, const uint32_t*, size_t)>
static void do_Replicate(benchmark::State& state) {
    const uint32_t fill = static_cast<uint32_t>(state.range(0));
    std::vector<T> src(kSrcRows, T{42});
    std::vector<uint32_t> offsets(kSrcRows + 1);
    for (size_t i = 0; i <= kSrcRows; ++i) offsets[i] = static_cast<uint32_t>(i) * fill;
    std::vector<T> dest(kSrcRows * fill);
    for (auto _ : state) {
        Impl(dest.data(), src.data(), offsets.data(), kSrcRows);
        benchmark::DoNotOptimize(dest.data());
    }
}

BENCHMARK_TEMPLATE(do_Replicate, int32_t, replicate_scalar<int32_t>)
        ->Name("BM_Replicate_Int32_Scalar")
        ->Arg(1)
        ->Arg(4)
        ->Arg(16)
        ->Arg(64);
BENCHMARK_TEMPLATE(do_Replicate, int32_t, replicate_simd_fill<int32_t>)
        ->Name("BM_Replicate_Int32_SIMD")
        ->Arg(1)
        ->Arg(4)
        ->Arg(16)
        ->Arg(64);
BENCHMARK_TEMPLATE(do_Replicate, int64_t, replicate_scalar<int64_t>)
        ->Name("BM_Replicate_Int64_Scalar")
        ->Arg(1)
        ->Arg(4)
        ->Arg(16)
        ->Arg(64);
BENCHMARK_TEMPLATE(do_Replicate, int64_t, replicate_simd_fill<int64_t>)
        ->Name("BM_Replicate_Int64_SIMD")
        ->Arg(1)
        ->Arg(4)
        ->Arg(16)
        ->Arg(64);

// =====================================================================
// column_helper.cpp :: bytewise AND / OR / ANDN
// =====================================================================
//
// Reproduces the no-win observation that made us keep the hand-rolled SIMD
// path out of `merge_two_filters` / `or_two_filters` / `merge_nullable_filter`:
// the compiler auto-vectorises the bytewise op to vpand / vpor / vpandn,
// matching a hand AVX2 store stream byte-for-byte (microbench was 132 ns vs
// 132 ns at chunk size 4096).  No SIMD variant -- only the scalar form that
// ships in the PR.

static void BM_FilterAnd_Scalar(benchmark::State& state) {
    std::vector<uint8_t> filter(kChunk);
    std::vector<uint8_t> selected(kChunk);
    fill_byte_mask(filter.data(), kChunk, 50, 0xAAAA);
    fill_byte_mask(selected.data(), kChunk, 50, 0xBBBB);
    for (auto _ : state) {
        uint8_t* f = filter.data();
        const uint8_t* s = selected.data();
        for (size_t i = 0; i < kChunk; ++i) f[i] = f[i] & s[i];
        benchmark::DoNotOptimize(filter.data());
    }
}

static void BM_FilterOr_Scalar(benchmark::State& state) {
    std::vector<uint8_t> filter(kChunk);
    std::vector<uint8_t> selected(kChunk);
    fill_byte_mask(filter.data(), kChunk, 50, 0xAAAA);
    fill_byte_mask(selected.data(), kChunk, 50, 0xBBBB);
    for (auto _ : state) {
        uint8_t* f = filter.data();
        const uint8_t* s = selected.data();
        for (size_t i = 0; i < kChunk; ++i) f[i] |= s[i];
        benchmark::DoNotOptimize(filter.data());
    }
}

static void BM_FilterAndN_Scalar(benchmark::State& state) {
    // mirrors merge_nullable_filter pattern: selected[i] = selected[i] & !nulls[i]
    std::vector<uint8_t> selected(kChunk);
    std::vector<uint8_t> nulls(kChunk);
    fill_byte_mask(selected.data(), kChunk, 50, 0xCCCC);
    fill_byte_mask(nulls.data(), kChunk, 50, 0xDDDD);
    for (auto _ : state) {
        uint8_t* s = selected.data();
        const uint8_t* n = nulls.data();
        for (size_t i = 0; i < kChunk; ++i) s[i] = static_cast<uint8_t>(s[i] & !n[i]);
        benchmark::DoNotOptimize(selected.data());
    }
}

BENCHMARK(BM_FilterAnd_Scalar);
BENCHMARK(BM_FilterOr_Scalar);
BENCHMARK(BM_FilterAndN_Scalar);

} // namespace starrocks

BENCHMARK_MAIN();
