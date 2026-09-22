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

#include "exec/cache_conscious_topn.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "exec/cache_conscious_fa.h"

namespace starrocks {

using Group = CacheConsciousTopN::Group;

namespace {

// Reference top-n: full ranking by count desc, ties broken by smaller key (matches the
// engine's deterministic order), truncated to k.
std::vector<Group> brute_force_top_n(std::vector<Group> groups, int64_t k) {
    std::sort(groups.begin(), groups.end(),
              [](const Group& a, const Group& b) { return a.count != b.count ? a.count > b.count : a.key < b.key; });
    if (static_cast<size_t>(k) < groups.size()) {
        groups.resize(k);
    }
    return groups;
}

// Reference for a stream of (key, count) events: aggregate by key, then top-n.
std::vector<Group> brute_force_stream_top_n(const std::vector<std::pair<uint64_t, int64_t>>& events, int64_t k) {
    std::unordered_map<uint64_t, int64_t> agg;
    for (const auto& e : events) {
        agg[e.first] += e.second;
    }
    std::vector<Group> groups;
    for (const auto& [key, count] : agg) {
        groups.push_back({key, count});
    }
    return brute_force_top_n(std::move(groups), k);
}

void expect_same(const std::vector<Group>& got, const std::vector<Group>& want) {
    ASSERT_EQ(got.size(), want.size());
    for (size_t i = 0; i < want.size(); ++i) {
        EXPECT_EQ(got[i].key, want[i].key) << "at rank " << i;
        EXPECT_EQ(got[i].count, want[i].count) << "at rank " << i;
    }
}

} // namespace

TEST(CacheConsciousTopNTest, ArenaAppendAfterPartialFlush) {
    // Partial reads must not leave holes when later batches cross a block boundary.
    for (size_t prefix : {1, 2, 3}) {
        CacheConsciousTopN::GroupArena arena;
        const size_t rows = arena.block_slots() * 3 + prefix;
        for (size_t i = 0; i < rows; ++i) {
            arena.push_back({i, static_cast<int64_t>(i % 17)});
            if (i + 1 == prefix) arena.flush();
        }
        size_t i = 0;
        for (Group g : arena) {
            ASSERT_LT(i, rows);
            EXPECT_EQ(i, g.key);
            EXPECT_EQ(i % 17, g.count);
            ++i;
        }
        ASSERT_EQ(rows, i);
        for (i = 0; i < rows; ++i) {
            EXPECT_EQ(i, arena.at(i).key);
        }
        CacheConsciousTopN::GroupArena copy(arena);
        CacheConsciousTopN::GroupArena moved(std::move(copy));
        i = 0;
        for (Group g : moved) EXPECT_EQ(i++, g.key);
        EXPECT_EQ(rows, i);
        EXPECT_TRUE(copy.empty());
    }
}

TEST(CacheConsciousFaTest, HistogramHintsDoNotCreateGroups) {
    CacheConsciousFa fa;
    fa.build({{1, 0}, {2, 10}});
    fa.seed_pinned(1); // A real COUNT(nullable) group may have a zero count.
    fa.seed_pinned(3); // This histogram key never reaches the aggregate.
    fa.seed_pinned(4);
    fa.build_bloom();
    fa.set_bloom_active(true);
    EXPECT_EQ(INT64_MIN, fa.kth_largest_count(3));
    uint64_t keys[] = {4, 2, 99};
    int64_t weights[] = {0, 3, 0};
    uint8_t selection[3] = {};
    EXPECT_EQ(2, fa.probe_and_count(keys, weights, selection, 3));
    EXPECT_EQ(0, selection[0]);
    EXPECT_EQ(1, selection[2]);
    std::vector<std::pair<uint64_t, int64_t>> collected;
    fa.collect(&collected);
    std::sort(collected.begin(), collected.end());
    const std::vector<std::pair<uint64_t, int64_t>> expected{{1, 0}, {2, 13}, {4, 0}};
    EXPECT_EQ(expected, collected);
    EXPECT_EQ(0, fa.kth_largest_count(3));
    EXPECT_EQ(INT64_MIN, fa.kth_largest_count(4));
}

TEST(CacheConsciousTopNTest, SkewTestUniformVsSkewed) {
    // Uniform: the top-k holds only k/n of the mass (10/1000), far below the 0.15 gate -> not skewed.
    std::vector<int64_t> uniform(1000, 7);
    EXPECT_FALSE(CacheConsciousTopN::is_skewed(uniform, /*k=*/10));

    // Skewed: a handful of heavy groups dominate a long tail of count-1 groups.
    std::vector<int64_t> skewed(1000, 1);
    for (int i = 0; i < 10; ++i) {
        skewed[i] = 1'000'000 - i;
    }
    EXPECT_TRUE(CacheConsciousTopN::is_skewed(skewed, /*k=*/10));

    // Gated on mass share alone, not on the distinct count: a skewed input whose distinct keys
    // would fit FA still flips (the prune just resolves a small tail).
    std::vector<int64_t> skewed_small(40, 1);
    for (int i = 0; i < 4; ++i) {
        skewed_small[i] = 1000;
    }
    EXPECT_TRUE(CacheConsciousTopN::is_skewed(skewed_small, /*k=*/4));

    // Degenerate guard: k covers every group, nothing to rank.
    EXPECT_FALSE(CacheConsciousTopN::is_skewed(skewed, /*k=*/1000));
    // Small uniform input: top-k mass (4/32 = 0.125) is below the 0.15 gate -> not skewed.
    std::vector<int64_t> small(32, 5);
    EXPECT_FALSE(CacheConsciousTopN::is_skewed(small, /*k=*/4));
}

TEST(CacheConsciousTopNTest, SkewedMatchesBruteForceAndPrunes) {
    std::vector<Group> groups;
    for (uint64_t i = 0; i < 5000; ++i) {
        groups.push_back({i, 1});
    }
    for (uint64_t i = 0; i < 8; ++i) {
        groups[i].count = 10'000 - static_cast<int64_t>(i);
    }

    CacheConsciousTopN engine(/*k=*/5, /*fa_capacity=*/64, /*fanout=*/256);
    size_t pruned = 0;
    auto got = engine.top_n(groups, &pruned);

    expect_same(got, brute_force_top_n(groups, 5));
    // The long count-1 tail must be pruned without exact ranking.
    EXPECT_GT(pruned, 0u);
}

TEST(CacheConsciousTopNTest, ModestHeadHugeTailStillPrunes) {
    // The heavy groups are only modestly larger than the per-partition totals a single
    // radix level would produce, so pruning requires multi-level re-partitioning: the cold
    // tail must be split until partition totals fall below the k-th highest exact count.
    std::vector<Group> groups;
    for (uint64_t i = 0; i < 100000; ++i) {
        groups.push_back({i * 2654435761ull + 1, 1});
    }
    for (uint64_t i = 0; i < 20; ++i) {
        groups[i].count = 200 + static_cast<int64_t>(i); // k-th highest exact ~ a couple hundred
    }
    // Exercise radix pruning directly: this distribution intentionally fails the separate
    // 15% skew gate, so top_n() would correctly use its full-ranking fallback.
    CacheConsciousCa ca(/*k=*/5, /*fa_capacity=*/128, /*fanout=*/256);
    std::vector<Group> fa(groups.begin(), groups.begin() + 20);
    for (size_t i = 20; i < groups.size(); ++i) ca.route(groups[i].key, groups[i].count);
    size_t pruned = 0;
    auto got = ca.finalize(std::move(fa), &pruned);

    expect_same(got, brute_force_top_n(groups, 5));
    // Most of the count-1 tail must be pruned, not resolved one partition at a time.
    EXPECT_GT(pruned, groups.size() / 2);
}

TEST(CacheConsciousTopNTest, UniformFallsBackButStaysCorrect) {
    std::vector<Group> groups;
    for (uint64_t i = 0; i < 2000; ++i) {
        groups.push_back({i, 42});
    }
    CacheConsciousTopN engine(/*k=*/10, /*fa_capacity=*/64, /*fanout=*/256);
    size_t pruned = 0;
    auto got = engine.top_n(groups, &pruned);

    expect_same(got, brute_force_top_n(groups, 10));
    EXPECT_EQ(pruned, 0u); // fallback path prunes nothing
}

TEST(CacheConsciousTopNTest, TiesAtBoundaryKeepAllWinners) {
    // Several groups share the k-th largest count; none of the tied winners may be dropped
    // and a tail group equal to the boundary must not be pruned away.
    std::vector<Group> groups;
    for (uint64_t i = 0; i < 1000; ++i) {
        groups.push_back({i, 1});
    }
    for (uint64_t i = 0; i < 3; ++i) {
        groups[i].count = 100; // clear winners
    }
    for (uint64_t i = 3; i < 9; ++i) {
        groups[i].count = 50; // six-way tie around the k-th boundary
    }
    CacheConsciousTopN engine(/*k=*/5, /*fa_capacity=*/64, /*fanout=*/256);
    auto got = engine.top_n(groups);
    expect_same(got, brute_force_top_n(groups, 5));
}

TEST(CacheConsciousTopNTest, FewerGroupsThanK) {
    std::vector<Group> groups{{1, 9}, {2, 4}, {3, 7}};
    CacheConsciousTopN engine(/*k=*/10, /*fa_capacity=*/64, /*fanout=*/256);
    auto got = engine.top_n(groups);
    expect_same(got, brute_force_top_n(groups, 10));
}

TEST(CacheConsciousCaTest, LateColdKeyBelowThresholdIsPruned) {
    // FA holds the frozen hot keys (exact). A key seen only in CA after the flip totals below
    // the k-th FA count, so it is correctly pruned and excluded. `events` mirrors FA + CA for
    // the brute-force reference (aggregated by key).
    std::vector<Group> fa;
    std::vector<std::pair<uint64_t, int64_t>> events;
    for (uint64_t h = 1; h <= 5; ++h) {
        fa.push_back({h, 300}); // hot keys 1..5 -> 300 each
        events.push_back({h, 300});
    }
    CacheConsciousCa ca(/*k=*/5, /*fa_capacity=*/64, /*fanout=*/64);
    for (uint64_t i = 0; i < 50000; ++i) {
        ca.route(i + 1000, 1); // long cold tail
        events.push_back({i + 1000, 1});
    }
    for (int r = 0; r < 250; ++r) {
        ca.route(999999, 1); // late key, 250 < 300 -> not a winner
        events.push_back({999999, 1});
    }
    size_t pruned = 0;
    auto got = ca.finalize(fa, &pruned);
    expect_same(got, brute_force_stream_top_n(events, 5));
    EXPECT_GT(pruned, 0u);
}

TEST(CacheConsciousCaTest, LateColdKeyAboveThresholdWins) {
    // Same shape, but the late CA key dominates: it must survive pruning and rank first.
    std::vector<Group> fa;
    std::vector<std::pair<uint64_t, int64_t>> events;
    for (uint64_t h = 1; h <= 5; ++h) {
        fa.push_back({h, 300});
        events.push_back({h, 300});
    }
    CacheConsciousCa ca(/*k=*/5, /*fa_capacity=*/64, /*fanout=*/64);
    for (uint64_t i = 0; i < 50000; ++i) {
        ca.route(i + 1000, 1);
        events.push_back({i + 1000, 1});
    }
    for (int r = 0; r < 500; ++r) {
        ca.route(999999, 1); // late key, 500 > 300 -> top-1
        events.push_back({999999, 1});
    }
    auto got = ca.finalize(fa);
    expect_same(got, brute_force_stream_top_n(events, 5));
    ASSERT_FALSE(got.empty());
    EXPECT_EQ(got[0].key, 999999u);
    EXPECT_EQ(got[0].count, 500);
}

TEST(CacheConsciousCaTest, FuzzStreamMatchesBruteForce) {
    std::mt19937_64 rng(0xBADF00D);
    for (int trial = 0; trial < 300; ++trial) {
        const int64_t k = 1 + static_cast<int64_t>(rng() % 20);
        const size_t fa_cap = 1 + rng() % 32;
        const size_t fanout = 2 + rng() % 16;

        // FA: frozen exact groups with distinct keys in [0, fa_cap); CA: cold rows with keys
        // disjoint from FA (>= 1000) and repeats to force re-aggregation.
        std::vector<Group> fa;
        std::vector<std::pair<uint64_t, int64_t>> events;
        const size_t fa_n = rng() % (fa_cap + 1);
        for (uint64_t i = 0; i < fa_n; ++i) {
            const int64_t c = 1 + static_cast<int64_t>(rng() % 1000);
            fa.push_back({i, c});
            events.push_back({i, c});
        }
        CacheConsciousCa ca(k, fa_cap, fanout);
        const size_t cold_rows = rng() % 4000;
        for (size_t i = 0; i < cold_rows; ++i) {
            const uint64_t key = 1000 + rng() % 4000;
            const int64_t partial = 1 + static_cast<int64_t>(rng() % 4);
            ca.route(key, partial);
            events.push_back({key, partial});
        }
        expect_same(ca.finalize(fa), brute_force_stream_top_n(events, k));
    }
}

// The spill primitives the operator relies on (take/restore + the logical stat that drives prune
// while tuples are on disk). take_ moves tuples out without touching the stat; restore_ appends
// without bumping it; physical_tuples_bytes tracks only the in-RAM tuples (the revocable size).
TEST(CacheConsciousCaTest, SpillStatAndBytesInvariants) {
    CacheConsciousCa ca(/*k=*/5, /*fa_capacity=*/64, /*fanout=*/16);
    constexpr int n = 1000;
    for (int i = 0; i < n; ++i) {
        ca.route(100 + i % 50, 1); // 50 distinct keys, n routed rows total
    }
    auto total_ub = [&]() {
        int64_t s = 0;
        for (size_t pid = 0; pid < ca.fanout(); ++pid) s += ca.partition_upper_bound(pid);
        return s;
    };
    // Revocable bytes include whole blocks, not just their occupied rows.
    EXPECT_EQ(total_ub(), n);
    EXPECT_GT(ca.physical_tuples_bytes(), static_cast<size_t>(n) * sizeof(Group));

    // Spill every partition out: the stat stays (prune still works), the RAM bytes drop to zero.
    std::vector<std::pair<uint64_t, int64_t>> spilled;
    for (size_t pid = 0; pid < ca.fanout(); ++pid) {
        for (const auto& g : ca.take_partition_tuples(pid)) spilled.emplace_back(g.key, g.count);
    }
    EXPECT_EQ(total_ub(), n);                  // stat unchanged by take_
    EXPECT_EQ(ca.physical_tuples_bytes(), 0u); // tuples gone from RAM

    // Restore: bytes come back, the stat is NOT double-counted (restore_ does not bump it).
    for (const auto& [key, partial] : spilled) ca.restore_tuple(key, partial);
    EXPECT_EQ(total_ub(), n);
    EXPECT_GT(ca.physical_tuples_bytes(), static_cast<size_t>(n) * sizeof(Group));
}

// Spilling the CA (take_) then restoring it must not change the local top-n: identical to a run
// that never spilled.
TEST(CacheConsciousCaTest, TakeRestoreRoundtripMatchesNoSpill) {
    std::mt19937_64 rng(0x5EED);
    for (int trial = 0; trial < 200; ++trial) {
        const int64_t k = 1 + static_cast<int64_t>(rng() % 16);
        const size_t fa_cap = 1 + rng() % 32;
        const size_t fanout = 2 + rng() % 16;
        std::vector<Group> fa;
        for (uint64_t i = 0, fa_n = rng() % (fa_cap + 1); i < fa_n; ++i) {
            fa.push_back({i, 1 + static_cast<int64_t>(rng() % 500)});
        }
        std::vector<std::pair<uint64_t, int64_t>> cold;
        for (size_t i = 0, cold_rows = rng() % 3000; i < cold_rows; ++i) {
            cold.emplace_back(1000 + rng() % 3000, 1 + static_cast<int64_t>(rng() % 4));
        }

        CacheConsciousCa no_spill(k, fa_cap, fanout);
        CacheConsciousCa spilled(k, fa_cap, fanout);
        for (const auto& [key, partial] : cold) {
            no_spill.route(key, partial);
            spilled.route(key, partial);
        }
        // Round-trip the spilled CA through take_/restore_ (every partition).
        std::vector<std::pair<uint64_t, int64_t>> out;
        for (size_t pid = 0; pid < spilled.fanout(); ++pid) {
            for (const auto& g : spilled.take_partition_tuples(pid)) out.emplace_back(g.key, g.count);
        }
        for (const auto& [key, partial] : out) spilled.restore_tuple(key, partial);

        expect_same(spilled.finalize(fa), no_spill.finalize(fa));
    }
}

// Cyclic spill: route, spill (take_), keep routing into the now-empty partitions (the stat keeps
// accumulating across disk + RAM), then restore the spilled tuples. finalize must match a brute
// force over every routed row plus FA.
TEST(CacheConsciousCaTest, CyclicSpillRouteAfterTake) {
    CacheConsciousCa ca(/*k=*/5, /*fa_capacity=*/64, /*fanout=*/32);
    std::vector<Group> fa;
    std::vector<std::pair<uint64_t, int64_t>> events;
    for (uint64_t h = 1; h <= 5; ++h) {
        fa.push_back({h, 1000});
        events.emplace_back(h, 1000);
    }
    // Batch 1.
    for (uint64_t i = 0; i < 20000; ++i) {
        ca.route(1000 + i % 4000, 1);
        events.emplace_back(1000 + i % 4000, 1);
    }
    // Spill batch 1 out.
    std::vector<std::pair<uint64_t, int64_t>> spilled;
    for (size_t pid = 0; pid < ca.fanout(); ++pid) {
        for (const auto& g : ca.take_partition_tuples(pid)) spilled.emplace_back(g.key, g.count);
    }
    // Batch 2 routed into the emptied partitions (cyclic) — overlapping and new keys.
    for (uint64_t i = 0; i < 15000; ++i) {
        ca.route(1000 + i % 6000, 1);
        events.emplace_back(1000 + i % 6000, 1);
    }
    // Late heavy key only in CA, above the FA k-th count -> must win.
    for (int i = 0; i < 1500; ++i) {
        ca.route(999999, 1);
        events.emplace_back(999999, 1);
    }
    // Restore batch 1.
    for (const auto& [key, partial] : spilled) ca.restore_tuple(key, partial);

    expect_same(ca.finalize(fa), brute_force_stream_top_n(events, 5));
}

// Fuzz the full spill cycle: random FA + cold stream, spill the whole CA out and back, compare to
// a from-scratch brute force.
TEST(CacheConsciousCaTest, FuzzTakeRestoreRoundtrip) {
    std::mt19937_64 rng(0xD00D);
    for (int trial = 0; trial < 300; ++trial) {
        const int64_t k = 1 + static_cast<int64_t>(rng() % 20);
        const size_t fa_cap = 1 + rng() % 32;
        const size_t fanout = 2 + rng() % 16;
        std::vector<Group> fa;
        std::vector<std::pair<uint64_t, int64_t>> events;
        for (uint64_t i = 0, fa_n = rng() % (fa_cap + 1); i < fa_n; ++i) {
            const int64_t c = 1 + static_cast<int64_t>(rng() % 1000);
            fa.push_back({i, c});
            events.emplace_back(i, c);
        }
        CacheConsciousCa ca(k, fa_cap, fanout);
        for (size_t i = 0, cold_rows = rng() % 4000; i < cold_rows; ++i) {
            const uint64_t key = 1000 + rng() % 4000;
            const int64_t partial = 1 + static_cast<int64_t>(rng() % 4);
            ca.route(key, partial);
            events.emplace_back(key, partial);
        }
        std::vector<std::pair<uint64_t, int64_t>> spilled;
        for (size_t pid = 0; pid < ca.fanout(); ++pid) {
            for (const auto& g : ca.take_partition_tuples(pid)) spilled.emplace_back(g.key, g.count);
        }
        for (const auto& [key, partial] : spilled) ca.restore_tuple(key, partial);
        expect_same(ca.finalize(fa), brute_force_stream_top_n(events, k));
    }
}

// CacheConsciousFa: build from a snapshot, probe a stream (hit -> inline increment + sel 0, miss ->
// sel 1), collect back. Focused checks on the membership mask, the fused increment, and the
// 2-phase partial-weight path.
TEST(CacheConsciousFaTest, BuildProbeCollect) {
    CacheConsciousFa fa;
    fa.build({{10, 3}, {20, 5}, {30, 1}});
    EXPECT_EQ(fa.size(), 3u);

    // Stream: hits on 10 and 20, a miss on 99. 1-phase (partials null -> weight 1).
    const int64_t keys[] = {10, 99, 20, 10};
    uint8_t sel[] = {9, 9, 9, 9};
    fa.probe_and_count<int64_t>(keys, nullptr, sel, 4);
    EXPECT_EQ(sel[0], 0); // 10 hit
    EXPECT_EQ(sel[1], 1); // 99 miss
    EXPECT_EQ(sel[2], 0); // 20 hit
    EXPECT_EQ(sel[3], 0); // 10 hit again

    // 2-phase: a partial weight added on the hit; the miss is still flagged.
    const int64_t mkeys[] = {30, 77};
    const int64_t parts[] = {4, 2};
    uint8_t msel[] = {9, 9};
    fa.probe_and_count<int64_t>(mkeys, parts, msel, 2);
    EXPECT_EQ(msel[0], 0);
    EXPECT_EQ(msel[1], 1);

    std::unordered_map<uint64_t, int64_t> got;
    std::vector<std::pair<uint64_t, int64_t>> pairs;
    fa.collect(&pairs);
    for (const auto& [k, c] : pairs) got[k] = c;
    EXPECT_EQ(got[10], 3 + 2); // seed 3 + two 1-weight hits
    EXPECT_EQ(got[20], 5 + 1);
    EXPECT_EQ(got[30], 1 + 4); // seed 1 + one 4-weight hit
    EXPECT_EQ(got.size(), 3u); // misses never enter FA
}

// End-to-end, mirroring the operator: FA seed = aggregate of the pre-flip prefix; post-flip rows
// probe the dense FA (hit -> increment, miss -> routed to CA); finalize prunes FA + CA. Compared
// to a brute force over the whole stream, both 1-phase (weight 1) and 2-phase (weighted) shapes.
TEST(CacheConsciousFaTest, FuzzProbeRouteFinalizeMatchesBruteForce) {
    std::mt19937_64 rng(0xDE5EU);
    for (int trial = 0; trial < 2000; ++trial) {
        const int64_t k = 1 + static_cast<int64_t>(rng() % 15);
        const size_t fa_cap = 1 + rng() % 32;
        const size_t fanout = 1ull << (1 + rng() % 5); // power of two 2..32
        const bool one_phase = rng() & 1;
        const size_t n = 1 + rng() % 1500;
        const uint64_t key_space = 1 + rng() % 400;

        std::vector<std::pair<uint64_t, int64_t>> events;
        events.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            const uint64_t key = rng() % key_space;
            const int64_t w = one_phase ? 1 : static_cast<int64_t>(1 + rng() % 5);
            events.emplace_back(key, w);
        }
        const size_t flip = 1 + rng() % n;

        std::unordered_map<uint64_t, int64_t> seed_map;
        for (size_t i = 0; i < flip; ++i) seed_map[events[i].first] += events[i].second;
        std::vector<std::pair<uint64_t, int64_t>> seed(seed_map.begin(), seed_map.end());
        std::unordered_set<uint64_t> seed_keys;
        for (const auto& p : seed) seed_keys.insert(p.first);

        CacheConsciousFa fa;
        fa.build(seed);
        CacheConsciousCa ca(k, fa_cap, fanout);

        size_t i = flip;
        while (i < n) {
            size_t bn = 1 + rng() % 64;
            if (i + bn > n) bn = n - i;
            std::vector<int64_t> keys(bn), parts(bn);
            std::vector<uint8_t> sel(bn, 0);
            for (size_t j = 0; j < bn; ++j) {
                keys[j] = static_cast<int64_t>(events[i + j].first);
                parts[j] = events[i + j].second;
            }
            const int64_t* pp = one_phase ? nullptr : parts.data();
            fa.probe_and_count<int64_t>(keys.data(), pp, sel.data(), bn);
            for (size_t j = 0; j < bn; ++j) {
                const bool in_fa = seed_keys.count(static_cast<uint64_t>(keys[j])) > 0;
                ASSERT_EQ(sel[j] == 0, in_fa) << "trial " << trial;
            }
            ca.route_batch(keys.data(), pp, sel.data(), bn);
            i += bn;
        }

        std::vector<std::pair<uint64_t, int64_t>> fap;
        fa.collect(&fap);
        std::vector<Group> fg;
        for (const auto& p : fap) fg.push_back({p.first, p.second});
        expect_same(ca.finalize(std::move(fg)), brute_force_stream_top_n(events, k));
    }
}

TEST(CacheConsciousFaTest, SwapBloomAndHistogramMatchBruteForce) {
    std::mt19937_64 rng(0x51A9);
    size_t promotions = 0;
    size_t evictions = 0;
    for (int trial = 0; trial < 200; ++trial) {
        SCOPED_TRACE(trial);
        const int64_t k = 1 + rng() % 7;
        const size_t capacity = 8;
        CacheConsciousFa fa;
        CacheConsciousCa ca(k, capacity, 1 + rng() % 8, /*swap_cooldown_chunks=*/1);
        std::vector<std::pair<uint64_t, int64_t>> events;
        for (uint64_t key = 0; key < capacity; ++key) events.emplace_back(key, rng() % 10);
        fa.build(events);
        fa.seed_pinned(0);
        fa.seed_pinned(20);
        fa.seed_pinned(1000); // An absent histogram value must never become an output group.
        fa.build_bloom();
        fa.set_bloom_active(true);
        ca.prime_swap(fa.kth_largest_count(k));
        for (size_t batch = 0; batch < 80; ++batch) {
            const size_t n = 1 + rng() % 100;
            std::vector<uint64_t> keys(n);
            std::vector<int64_t> weights(n);
            std::vector<uint8_t> selection(n);
            for (size_t i = 0; i < n; ++i) {
                // Rotate the hot key so promotions, evictions and re-promotions all occur.
                keys[i] = rng() % 5 ? 10 + batch / 10 : rng() % 25;
                weights[i] = trial % 5 == 0 ? 0 : rng() % 4;
                events.emplace_back(keys[i], weights[i]);
            }
            fa.probe_and_count(keys.data(), weights.data(), selection.data(), n);
            ca.route_batch(keys.data(), weights.data(), selection.data(), n);
            ca.swap_pass(&fa);
        }
        promotions += ca.swap_promotions();
        evictions += ca.swap_evictions();
        std::vector<std::pair<uint64_t, int64_t>> pairs;
        fa.collect(&pairs);
        std::vector<Group> groups;
        for (const auto& [key, count] : pairs) groups.push_back({key, count});
        expect_same(ca.finalize(std::move(groups)), brute_force_stream_top_n(events, k));
    }
    EXPECT_GT(promotions, 0);
    EXPECT_GT(evictions, 0);
}

TEST(CacheConsciousTopNTest, FuzzMatchesBruteForce) {
    std::mt19937_64 rng(0xC0FFEE);
    for (int trial = 0; trial < 200; ++trial) {
        std::uniform_int_distribution<int> n_dist(1, 3000);
        std::uniform_int_distribution<int> k_dist(1, 50);
        // Mix of distributions: heavy skew, mild skew, near-uniform.
        std::uniform_int_distribution<int64_t> tail(1, 5);
        std::uniform_int_distribution<int64_t> head(1, 1'000'000);

        const int n = n_dist(rng);
        const int64_t k = k_dist(rng);
        std::vector<Group> groups;
        groups.reserve(n);
        for (int i = 0; i < n; ++i) {
            int64_t c = (i < 20) ? head(rng) : tail(rng);
            groups.push_back({static_cast<uint64_t>(i) * 2654435761ull + 1, c});
        }
        std::shuffle(groups.begin(), groups.end(), rng);

        CacheConsciousTopN engine(k, /*fa_capacity=*/128, /*fanout=*/64);
        auto got = engine.top_n(groups);
        expect_same(got, brute_force_top_n(groups, k));
    }
}

} // namespace starrocks
