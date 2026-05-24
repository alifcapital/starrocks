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
// The earlier breakdown showed scratch alloc / deserialize / mem-accounting are
// all noise (<=2%); ~96% is the tdigest merge math. This bench drills into that:
// TDigest::merge(other) -> add({other}) runs updateCumulative() (O(processed))
// on EVERY call, and process() runs it again. updateCumulative is only needed
// before quantile()/serialize(), so per-row merge is O(rows * processed) when it
// could be O(rows + processed).
//
//   PerMerge : kRows x target.merge(&scratch)            -- current code path
//   BatchAdd : target.add(begin,end) once over kRows ptrs -- TDigest's own
//              "merge in the most efficient manner" batch API: one updateCumulative.
//
// Arg(0) = TDigest compression (1000 = TDigest() default; 10000 =
// percentile_approx DEFAULT_COMPRESSION_FACTOR). Partial = a singleton centroid
// (the common high-cardinality pass-through shape). Per-row ns = Time / kRows.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <vector>

#include "types/tdigest.h"

namespace starrocks {

static constexpr size_t kRows = 4096;

static std::vector<uint8_t> make_singleton_blob(double compression) {
    TDigest t(compression);
    t.add(42.0f);
    std::vector<uint8_t> b(t.serialize_size());
    t.serialize(b.data());
    return b;
}

// current: merge the partial in one row at a time.
static void BM_PerMerge(benchmark::State& st) {
    const double c = static_cast<double>(st.range(0));
    auto blob = make_singleton_blob(c);
    TDigest scratch(c);
    scratch.deserialize(reinterpret_cast<const char*>(blob.data()));
    for (auto _ : st) {
        TDigest target(c);
        for (size_t i = 0; i < kRows; ++i) target.merge(&scratch);
        benchmark::DoNotOptimize(&target);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

// batched: hand all partials to TDigest's constant-space batch merge in one call.
static void BM_BatchAdd(benchmark::State& st) {
    const double c = static_cast<double>(st.range(0));
    auto blob = make_singleton_blob(c);
    TDigest scratch(c);
    scratch.deserialize(reinterpret_cast<const char*>(blob.data()));
    std::vector<const TDigest*> ptrs(kRows, &scratch);
    for (auto _ : st) {
        TDigest target(c);
        target.add(ptrs.cbegin(), ptrs.cend());
        benchmark::DoNotOptimize(&target);
    }
    st.SetItemsProcessed(st.iterations() * kRows);
}

BENCHMARK(BM_PerMerge)->Arg(1000)->Arg(10000);
BENCHMARK(BM_BatchAdd)->Arg(1000)->Arg(10000);

} // namespace starrocks

BENCHMARK_MAIN();
