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

// Where percentile_approx merge time actually goes.
//
// TDigest::merge(other) -> add({other}) runs, per call: a priority-queue setup,
// mergeProcessed/mergeUnprocessed, processIfNecessary, and updateCumulative()
// (O(processed)). Doing this once per row (vs once per batch) is the suspected
// cost. Variants:
//
//   PerMerge        : kRows x target.merge(p)              -- current code path
//   PerMergeDeferred: kRows x target.merge_deferred(p) + finalize_cumulative()
//                     -- skips the per-row updateCumulative; BIT-IDENTICAL to
//                        PerMerge (same bytes + quantiles, verified).
//   BatchAdd        : target.add(begin,end) once           -- TDigest's own batch
//                        API; faster but REORDERS processing -> shifts the result.
//
// Partials carry VARIED values (normal dist) so the digest actually grows to
// ~compression centroids like a real high-cardinality merge. (Identical values
// collapse to a single centroid and measure nothing.)
//
// Arg(0) = TDigest compression (1000 = TDigest() default; 10000 =
// percentile_approx DEFAULT_COMPRESSION_FACTOR). Per-row ns = Time / kRows.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <memory>
#include <random>
#include <vector>

#include "types/tdigest.h"

namespace starrocks {

static constexpr size_t kRows = 4096;

// kRows varied single-centroid partials (one per incoming merge row).
static std::vector<std::unique_ptr<TDigest>> make_partials(double compression) {
    std::mt19937_64 rng(0x9E3779B97F4A7C15ull);
    std::normal_distribution<double> dist(100.0, 30.0);
    std::vector<std::unique_ptr<TDigest>> parts;
    parts.reserve(kRows);
    for (size_t i = 0; i < kRows; ++i) {
        auto t = std::make_unique<TDigest>(compression);
        t->add(static_cast<float>(dist(rng)));
        parts.push_back(std::move(t));
    }
    return parts;
}

static std::vector<const TDigest*> ptrs_of(const std::vector<std::unique_ptr<TDigest>>& parts) {
    std::vector<const TDigest*> p;
    p.reserve(parts.size());
    for (const auto& up : parts) p.push_back(up.get());
    return p;
}

static void BM_PerMerge(benchmark::State& st) {
    const double c = static_cast<double>(st.range(0));
    auto parts = make_partials(c);
    auto ptrs = ptrs_of(parts);
    for (auto _ : st) {
        TDigest target(c);
        for (const auto* p : ptrs) target.merge(p);
        benchmark::DoNotOptimize(&target);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

static void BM_PerMergeDeferred(benchmark::State& st) {
    const double c = static_cast<double>(st.range(0));
    auto parts = make_partials(c);
    auto ptrs = ptrs_of(parts);
    for (auto _ : st) {
        TDigest target(c);
        for (const auto* p : ptrs) target.merge_deferred(p);
        target.finalize_cumulative();
        benchmark::DoNotOptimize(&target);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

static void BM_BatchAdd(benchmark::State& st) {
    const double c = static_cast<double>(st.range(0));
    auto parts = make_partials(c);
    auto ptrs = ptrs_of(parts);
    for (auto _ : st) {
        TDigest target(c);
        target.add(ptrs.cbegin(), ptrs.cend());
        benchmark::DoNotOptimize(&target);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

BENCHMARK(BM_PerMerge)->Arg(1000)->Arg(10000);
BENCHMARK(BM_PerMergeDeferred)->Arg(1000)->Arg(10000);
BENCHMARK(BM_BatchAdd)->Arg(1000)->Arg(10000);

} // namespace starrocks

BENCHMARK_MAIN();
