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

// Isolated bench for the cache-conscious top-n hot path, comparing it against what StarRocks does
// today for `SELECT x, count(*) c FROM t GROUP BY x ORDER BY c DESC LIMIT k`:
//
//   BM_FullAggTopK (baseline):    aggregate every group into one phmap, then partial_sort top-k.
//                                 At high cardinality the table is RAM-sized and every probe is a
//                                 random miss to L3/RAM -- the cliff Zippy targets.
//   BM_CacheConsciousTopK:        model the operator hot path -- fill a small phmap up to the FA
//                                 budget (the flip point), then probe-only against that frozen FA
//                                 (hit -> ++, miss -> CacheConsciousCa::route), then finalize-prune.
//                                 Random access stays inside the L2-resident FA; the cold tail is a
//                                 sequential vector append, then pruned without full ranking.
//
// Both consume the SAME pre-generated key stream so the per-row numbers are directly comparable.
// Headline win zone: high cardinality (table >> L2) AND value skew (a few groups dominate count).
//   * Zipf high-card   -> cache-conscious should win (FA stays hot, tail pruned).
//   * Uniform high-card -> no skew, nothing prunes; cache-conscious carries pure overhead (the
//                          honest "fall back to plain aggregation" case -- it must NOT win here).
//
// NOTE: this measures the CURRENT engine, whose CA is a plain std::vector append + unordered_map
// resolve. The non-temporal / cache-line radix partitioning (M4) is not built yet; this bench is
// the baseline that work must beat. No FE, no scan I/O, no pipeline operator overhead.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

#include "base/phmap/phmap.h"
#include "exec/cache_conscious_topn.h"

namespace starrocks {

using Group = CacheConsciousTopN::Group;

enum class Distribution : int {
    Uniform = 0, // high-card, no skew -> cache-conscious must fall back, not win
    Zipf = 1,    // high-card, skewed (s=1.2) -> the win zone
};

// FA budget = how many distinct groups fit the frozen-table flip point. The operator flips when
// the live table first exceeds kCacheConsciousL2TargetBytes (512 KiB); at ~16 B per group slot
// that is ~32k groups. Modeled directly here so the bench mirrors the runtime flip size.
inline constexpr size_t kFaBudget = 32768;
inline constexpr size_t kFanout = 256;

bool by_count_desc(const Group& a, const Group& b) {
    return a.count != b.count ? a.count > b.count : a.key < b.key;
}

// Pre-generate the key stream once (outside the timed region). Uniform spreads rows evenly across
// `distinct` keys; Zipf concentrates ~most rows on the lowest-ranked keys via inverse-CDF sampling
// over the full `distinct` range, so cardinality stays high while counts are skewed.
class KeyStream {
public:
    KeyStream(int64_t num_rows, int distinct, Distribution dist) {
        _keys.reserve(num_rows);
        std::mt19937_64 rng(0xC0FFEE);
        if (dist == Distribution::Uniform || distinct <= 1) {
            std::uniform_int_distribution<uint64_t> uni(0, distinct > 0 ? distinct - 1 : 0);
            for (int64_t i = 0; i < num_rows; ++i) {
                _keys.push_back(uni(rng));
            }
            return;
        }
        // Zipf(s) cumulative weights over [0, distinct): cum[k] = sum_{j<=k} 1/(j+1)^s.
        constexpr double s = 1.2;
        std::vector<double> cum(distinct);
        double total = 0.0;
        for (int k = 0; k < distinct; ++k) {
            total += 1.0 / std::pow(k + 1, s);
            cum[k] = total;
        }
        std::uniform_real_distribution<double> uni(0.0, total);
        for (int64_t i = 0; i < num_rows; ++i) {
            const double r = uni(rng);
            int lo = 0, hi = distinct - 1;
            while (lo < hi) {
                const int mid = (lo + hi) >> 1;
                if (cum[mid] < r) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            // Scramble rank -> key so heavy keys are not contiguous (matches arbitrary group ids).
            _keys.push_back(static_cast<uint64_t>(lo) * 2654435761ull + 1);
        }
    }

    const std::vector<uint64_t>& keys() const { return _keys; }

private:
    std::vector<uint64_t> _keys;
};

// Baseline: full aggregation into one phmap + partial_sort top-k. The phmap grows to the full
// distinct cardinality, so at high card every insert/probe is a random RAM access.
std::vector<Group> full_agg_top_k(const std::vector<uint64_t>& keys, int64_t k, size_t* distinct_out) {
    phmap::flat_hash_map<uint64_t, int64_t> ht;
    for (const uint64_t key : keys) {
        ++ht[key];
    }
    if (distinct_out != nullptr) {
        *distinct_out = ht.size();
    }
    std::vector<Group> groups;
    groups.reserve(ht.size());
    for (const auto& [key, count] : ht) {
        groups.push_back({key, count});
    }
    const size_t n = std::min(static_cast<size_t>(k), groups.size());
    std::partial_sort(groups.begin(), groups.begin() + n, groups.end(), by_count_desc);
    groups.resize(n);
    return groups;
}

// Cache-conscious: fill the FA phmap up to kFaBudget distinct keys (the flip), then probe-only
// against the frozen FA and route misses to the CA, then prune. Mirrors the operator hot path.
std::vector<Group> cache_conscious_top_k(const std::vector<uint64_t>& keys, int64_t k, size_t* pruned_out) {
    phmap::flat_hash_map<uint64_t, int64_t> fa;
    CacheConsciousCa ca(k, kFaBudget, kFanout);
    bool flipped = false;
    for (const uint64_t key : keys) {
        if (!flipped) {
            ++fa[key];
            if (fa.size() >= kFaBudget) {
                flipped = true;
            }
        } else {
            auto it = fa.find(key);
            if (it != fa.end()) {
                ++it->second;
            } else {
                ca.route(key, 1);
            }
        }
    }
    std::vector<Group> fa_groups;
    fa_groups.reserve(fa.size());
    for (const auto& [key, count] : fa) {
        fa_groups.push_back({key, count});
    }
    return ca.finalize(std::move(fa_groups), pruned_out);
}

static void BM_FullAggTopK(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));
    const int64_t k = state.range(3);

    KeyStream stream(num_rows, distinct, dist);
    size_t final_distinct = 0;
    int64_t checksum = 0;
    for (auto _ : state) {
        auto top = full_agg_top_k(stream.keys(), k, &final_distinct);
        for (const auto& g : top) {
            checksum += g.count;
        }
        benchmark::DoNotOptimize(checksum);
        benchmark::ClobberMemory();
    }
    benchmark::DoNotOptimize(checksum);
    state.SetItemsProcessed(num_rows * state.iterations());
    state.counters["distinct"] = final_distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["k"] = k;
}

static void BM_CacheConsciousTopK(benchmark::State& state) {
    const int64_t num_rows = state.range(0);
    const int distinct = static_cast<int>(state.range(1));
    const Distribution dist = static_cast<Distribution>(state.range(2));
    const int64_t k = state.range(3);

    KeyStream stream(num_rows, distinct, dist);

    // Correctness (top-n matches the baseline) is covered by the engine unit test and fuzz; this
    // bench measures time only.
    size_t final_pruned = 0;
    int64_t checksum = 0;
    for (auto _ : state) {
        auto top = cache_conscious_top_k(stream.keys(), k, &final_pruned);
        for (const auto& g : top) {
            checksum += g.count;
        }
        benchmark::DoNotOptimize(checksum);
        benchmark::ClobberMemory();
    }
    benchmark::DoNotOptimize(checksum);
    state.SetItemsProcessed(num_rows * state.iterations());
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["k"] = k;
    state.counters["pruned_groups"] = final_pruned;
}

// High-cardinality sweep. The baseline phmap holds ~24 B per entry (16 B key+value slot at a
// ~0.5-0.7 load factor plus control byte), so the distinct points are sized by the resulting table
// footprint relative to L3, picking the largest so the win survives even big server parts:
//   1M  distinct -> ~24 MB  : fits L3 -> in-cache reference, little/no win expected
//   16M distinct -> ~384 MB : straddles the L3 of large EPYC parts
//   64M distinct -> ~1.5 GB : past any current L3 -> the headline thrash-vs-resident case
// rows = 200M keeps real aggregation even at 64M distinct (~3 rows/group) while the heavy keys
// dominate under Zipf. k is fixed at 100 (top-k size barely moves the aggregation-bound cost; the
// distinct x distribution axes are what matter). Heavy: ~1.6 GB stream + up to ~1.5 GB table per
// point; filter with --benchmark_filter=... to run a subset.
static void RegisterArgs(benchmark::internal::Benchmark* b) {
    b->ArgNames({"rows", "distinct", "dist", "k"});
    constexpr int64_t kRows = 200'000'000;
    constexpr int64_t kK = 100;
    for (int distinct : {1'000'000, 16'000'000, 64'000'000}) {
        for (int d : {static_cast<int>(Distribution::Uniform), static_cast<int>(Distribution::Zipf)}) {
            b->Args({kRows, distinct, d, kK});
        }
    }
    b->Unit(benchmark::kMillisecond);
    b->Iterations(3);
}

BENCHMARK(BM_FullAggTopK)->Apply(RegisterArgs);
BENCHMARK(BM_CacheConsciousTopK)->Apply(RegisterArgs);

} // namespace starrocks

BENCHMARK_MAIN();
