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

// Microbench for the merge-side no-null delegation in NullableAggregateFunction
// (merge_batch_single_state). It models the two code paths exactly:
//
//   PerRow    (before): the wrapper loops nested_function->merge() once per row.
//                       nested_function is held by pointer, so every row is a
//                       virtual dispatch that the compiler cannot devirtualise
//                       or vectorise.
//   Delegated (after) : the wrapper hands the whole no-null run to
//                       nested_function->merge_batch_single_state() in a single
//                       virtual call; the reduction loop inside is monomorphic
//                       and auto-vectorises.
//
// The nested op is the real reducer body of sum/min over a fixed-width column
// (merge() of sum/min/max is byte-identical to update() -- see sum.h/maxmin.h).
//
// Expected from the standalone asm study (x86-64 AVX2, no -ffast-math):
//   * int64 sum/min : the delegated loop vectorises -> large win.
//   * double sum/min: stays scalar (FP non-associativity / NaN), so only the
//                     dispatch overhead (N virtual calls -> 1) is removed.
// Both regimes are benched so a reviewer can read off win and confirm the
// double case does not regress.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <limits>
#include <random>
#include <vector>

namespace starrocks {

constexpr size_t kChunkSize = 4096;

// Abstract nested function held by pointer -- reproduces the virtual dispatch of
// NullableAggregateFunctionBase::nested_function (an AggregateFunctionPtr).
struct INested {
    virtual ~INested() = default;
    virtual void merge_row(const void* col, void* state, size_t row) const = 0;
    virtual void merge_run(void* state, const void* col, size_t start, size_t size) const = 0;
};

template <typename T, bool IsMin>
struct Reducer final : INested {
    static inline void op(T& s, T x) {
        if constexpr (IsMin) {
            s = x < s ? x : s;
        } else {
            s += x;
        }
    }
    void merge_row(const void* col, void* state, size_t row) const override {
        op(*static_cast<T*>(state), static_cast<const T*>(col)[row]);
    }
    void merge_run(void* state, const void* col, size_t start, size_t size) const override {
        const T* d = static_cast<const T*>(col);
        T s = *static_cast<T*>(state);
        for (size_t i = start; i < start + size; ++i) op(s, d[i]);
        *static_cast<T*>(state) = s;
    }
};

template <typename T>
static std::vector<T> make_data(uint64_t seed) {
    std::mt19937_64 rng(seed);
    std::vector<T> v(kChunkSize);
    for (auto& x : v) x = static_cast<T>(rng() & 0xFFFFFF);
    return v;
}

template <typename T, bool IsMin>
static T init_state() {
    return IsMin ? std::numeric_limits<T>::max() : T{0};
}

// before: per-row virtual merge
template <typename T, bool IsMin>
static void BM_PerRow(benchmark::State& state) {
    auto data = make_data<T>(0x9E3779B97F4A7C15ull);
    Reducer<T, IsMin> impl;
    const INested* f = &impl;
    benchmark::DoNotOptimize(f); // keep f opaque so the calls stay virtual (no devirt)
    for (auto _ : state) {
        T st = init_state<T, IsMin>();
        for (size_t i = 0; i < kChunkSize; ++i) f->merge_row(data.data(), &st, i);
        benchmark::DoNotOptimize(st);
    }
    state.SetItemsProcessed(state.iterations() * kChunkSize);
}

// after: one delegated batch call
template <typename T, bool IsMin>
static void BM_Delegated(benchmark::State& state) {
    auto data = make_data<T>(0x9E3779B97F4A7C15ull);
    Reducer<T, IsMin> impl;
    const INested* f = &impl;
    benchmark::DoNotOptimize(f);
    for (auto _ : state) {
        T st = init_state<T, IsMin>();
        f->merge_run(&st, data.data(), 0, kChunkSize);
        benchmark::DoNotOptimize(st);
    }
    state.SetItemsProcessed(state.iterations() * kChunkSize);
}

BENCHMARK_TEMPLATE(BM_PerRow, int64_t, false); // sum  BIGINT
BENCHMARK_TEMPLATE(BM_Delegated, int64_t, false);
BENCHMARK_TEMPLATE(BM_PerRow, double, false); // sum  DOUBLE
BENCHMARK_TEMPLATE(BM_Delegated, double, false);
BENCHMARK_TEMPLATE(BM_PerRow, int64_t, true); // min  BIGINT
BENCHMARK_TEMPLATE(BM_Delegated, int64_t, true);
BENCHMARK_TEMPLATE(BM_PerRow, double, true); // min  DOUBLE
BENCHMARK_TEMPLATE(BM_Delegated, double, true);

} // namespace starrocks

BENCHMARK_MAIN();
