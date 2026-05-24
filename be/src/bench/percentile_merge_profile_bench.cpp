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

// Honest time breakdown of PercentileApproxAggregateFunction::merge() per row.
// Every variant grows `target` identically (kRows merges of the same partial),
// so the delta between adjacent variants isolates one component:
//
//   A  MergeOnly      : target.merge(prebuilt scratch)         -> irreducible tdigest math
//   B  Deserialize    : scratch.deserialize(blob); merge       -> B-A = deserialize cost
//   C  AllocDeser     : make_unique scratch; deserialize; merge-> C-B = per-row scratch alloc
//   D  MemAccount     : B + 2x mem_usage() + add_mem_usage      -> D-B = mem-accounting cost
//
// Read per-row ns = Time / kRows. Arg(0) = centroids per partial blob
// (1 = pass-through singleton, the common high-cardinality shape).

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "types/tdigest.h"

namespace starrocks {

static constexpr size_t kRows = 4096;
static constexpr double kC = 10000.0;

static std::vector<uint8_t> make_blob(int ncent) {
    TDigest t(kC);
    for (int i = 0; i < ncent; ++i) t.add(static_cast<float>((i * 7) % 1000));
    std::vector<uint8_t> b(t.serialize_size());
    t.serialize(b.data());
    return b;
}

// A: pure merge math (scratch built once, merged kRows times).
static void BM_A_MergeOnly(benchmark::State& st) {
    auto blob = make_blob(static_cast<int>(st.range(0)));
    for (auto _ : st) {
        TDigest target(kC);
        TDigest scratch(kC);
        scratch.deserialize(reinterpret_cast<const char*>(blob.data()));
        for (size_t i = 0; i < kRows; ++i) target.merge(&scratch);
        benchmark::DoNotOptimize(&target);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

// B: + per-row deserialize into a reused scratch.
static void BM_B_Deserialize(benchmark::State& st) {
    auto blob = make_blob(static_cast<int>(st.range(0)));
    const char* d = reinterpret_cast<const char*>(blob.data());
    for (auto _ : st) {
        TDigest target(kC);
        TDigest scratch(kC);
        for (size_t i = 0; i < kRows; ++i) {
            scratch.deserialize(d);
            target.merge(&scratch);
        }
        benchmark::DoNotOptimize(&target);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

// C: + per-row scratch allocation (this is the current merge() code).
static void BM_C_AllocDeser(benchmark::State& st) {
    auto blob = make_blob(static_cast<int>(st.range(0)));
    const char* d = reinterpret_cast<const char*>(blob.data());
    for (auto _ : st) {
        TDigest target(kC);
        for (size_t i = 0; i < kRows; ++i) {
            auto src = std::make_unique<TDigest>(kC);
            src->deserialize(d);
            target.merge(src.get());
        }
        benchmark::DoNotOptimize(&target);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

// D: B + the per-row mem-usage accounting merge() does
// (prev = mem_usage(); merge; add_mem_usage(mem_usage() - prev)).
// mem_usage() == 1 + tdigest.serialize_size().
static void BM_D_MemAccount(benchmark::State& st) {
    auto blob = make_blob(static_cast<int>(st.range(0)));
    const char* d = reinterpret_cast<const char*>(blob.data());
    int64_t mem_counter = 0;
    for (auto _ : st) {
        TDigest target(kC);
        TDigest scratch(kC);
        for (size_t i = 0; i < kRows; ++i) {
            scratch.deserialize(d);
            int64_t prev = 1 + static_cast<int64_t>(target.serialize_size());
            target.merge(&scratch);
            int64_t after = 1 + static_cast<int64_t>(target.serialize_size());
            mem_counter += after - prev;
        }
        benchmark::DoNotOptimize(&target);
        benchmark::DoNotOptimize(mem_counter);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

BENCHMARK(BM_A_MergeOnly)->Arg(1)->Arg(100);
BENCHMARK(BM_B_Deserialize)->Arg(1)->Arg(100);
BENCHMARK(BM_C_AllocDeser)->Arg(1)->Arg(100);
BENCHMARK(BM_D_MemAccount)->Arg(1)->Arg(100);

} // namespace starrocks

BENCHMARK_MAIN();
