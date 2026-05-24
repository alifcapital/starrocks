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

// Microbench for the percentile_approx merge-side scratch allocation.
//
// PercentileApproxAggregateFunction::merge() deserialises each incoming
// intermediate blob into a *fresh* scratch state every row:
//
//     PercentileApproxState src_percentile(compression);   // heap-allocs
//     src_percentile.percentile->deserialize(...);          // grows vectors
//     state.percentile->merge(src_percentile.percentile.get());
//
// On the high-volume GROUP BY merge path this is one (or more) heap allocation
// per merged row. A merge_batch override could keep a single scratch digest and
// re-deserialise into it (TDigest::deserialize uses resize(), so the centroid
// vectors retain capacity across rows).
//
//   PerRow  (before): fresh std::make_unique<TDigest> per row.
//   Reused  (after) : one TDigest reused across the whole batch.
//
// Arg(0) = centroids carried by each partial blob:
//   1   -> pass-through singleton (the common high-cardinality shape), where the
//          per-row allocation dominates the tiny merge work.
//   20/100 -> pre-aggregated partials, where the tdigest merge/process work is
//          larger and the allocation is a smaller fraction. Benching the range
//          shows how much of the win is allocation vs digest math.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "types/tdigest.h"

namespace starrocks {

static constexpr size_t kRows = 4096;
static constexpr double kCompression = 10000.0; // PercentileApprox DEFAULT_COMPRESSION_FACTOR

static std::vector<uint8_t> make_blob(int ncent) {
    TDigest t(kCompression);
    for (int i = 0; i < ncent; ++i) t.add(static_cast<float>((i * 7) % 1000));
    std::vector<uint8_t> buf(t.serialize_size());
    t.serialize(buf.data());
    return buf;
}

static void BM_PerRowScratch(benchmark::State& state) {
    auto blob = make_blob(static_cast<int>(state.range(0)));
    const char* data = reinterpret_cast<const char*>(blob.data());
    for (auto _ : state) {
        TDigest target(kCompression);
        for (size_t i = 0; i < kRows; ++i) {
            auto src = std::make_unique<TDigest>(kCompression);
            src->deserialize(data);
            target.merge(src.get());
        }
        benchmark::DoNotOptimize(&target);
    }
    state.SetItemsProcessed(state.iterations() * kRows);
}

static void BM_ReusedScratch(benchmark::State& state) {
    auto blob = make_blob(static_cast<int>(state.range(0)));
    const char* data = reinterpret_cast<const char*>(blob.data());
    for (auto _ : state) {
        TDigest target(kCompression);
        TDigest scratch(kCompression);
        for (size_t i = 0; i < kRows; ++i) {
            scratch.deserialize(data);
            target.merge(&scratch);
        }
        benchmark::DoNotOptimize(&target);
    }
    state.SetItemsProcessed(state.iterations() * kRows);
}

BENCHMARK(BM_PerRowScratch)->Arg(1)->Arg(20)->Arg(100);
BENCHMARK(BM_ReusedScratch)->Arg(1)->Arg(20)->Arg(100);

} // namespace starrocks

BENCHMARK_MAIN();
