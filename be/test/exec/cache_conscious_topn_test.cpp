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
#include <utility>
#include <vector>

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

TEST(CacheConsciousTopNTest, SkewTestUniformVsSkewed) {
    // Uniform: every group ties at the k-th value, so the candidate set spans the whole
    // input and overflows FA -> not skewed.
    std::vector<int64_t> uniform(1000, 7);
    EXPECT_FALSE(CacheConsciousTopN::is_skewed(uniform, /*k=*/10, /*fa_capacity=*/64));

    // Skewed: a handful of heavy groups dominate a long tail of count-1 groups.
    std::vector<int64_t> skewed(1000, 1);
    for (int i = 0; i < 10; ++i) {
        skewed[i] = 1'000'000 - i;
    }
    EXPECT_TRUE(CacheConsciousTopN::is_skewed(skewed, /*k=*/10, /*fa_capacity=*/64));

    // Degenerate guards: k covers everything, or the whole input already fits FA.
    EXPECT_FALSE(CacheConsciousTopN::is_skewed(skewed, /*k=*/1000, /*fa_capacity=*/64));
    std::vector<int64_t> small(32, 5);
    EXPECT_FALSE(CacheConsciousTopN::is_skewed(small, /*k=*/4, /*fa_capacity=*/64));
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
    CacheConsciousTopN engine(/*k=*/5, /*fa_capacity=*/128, /*fanout=*/256);
    size_t pruned = 0;
    auto got = engine.top_n(groups, &pruned);

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

TEST(CacheConsciousAccumulatorTest, LateColdKeyBelowThresholdIsPruned) {
    // Hot keys arrive first and fill FA; a key that shows up only after the flip stays in CA.
    // Here its total is below the k-th FA count, so it is correctly pruned and excluded.
    std::vector<std::pair<uint64_t, int64_t>> events;
    for (int r = 0; r < 300; ++r) {
        for (uint64_t h = 1; h <= 5; ++h) {
            events.push_back({h, 1}); // keys 1..5 -> 300 each
        }
    }
    for (uint64_t i = 0; i < 50000; ++i) {
        events.push_back({i + 1000, 1}); // long cold tail
    }
    for (int r = 0; r < 250; ++r) {
        events.push_back({999999, 1}); // late key, 250 < 300 -> not a winner
    }
    CacheConsciousTopnAccumulator acc(/*k=*/5, /*fa_capacity=*/64, /*fanout=*/64);
    for (const auto& e : events) {
        acc.add(e.first, e.second);
    }
    size_t pruned = 0;
    auto got = acc.finalize(&pruned);
    expect_same(got, brute_force_stream_top_n(events, 5));
    EXPECT_GT(pruned, 0u);
}

TEST(CacheConsciousAccumulatorTest, LateColdKeyAboveThresholdWins) {
    // Same shape, but the late CA key dominates: it must survive pruning and rank first.
    std::vector<std::pair<uint64_t, int64_t>> events;
    for (int r = 0; r < 300; ++r) {
        for (uint64_t h = 1; h <= 5; ++h) {
            events.push_back({h, 1});
        }
    }
    for (uint64_t i = 0; i < 50000; ++i) {
        events.push_back({i + 1000, 1});
    }
    for (int r = 0; r < 500; ++r) {
        events.push_back({999999, 1}); // late key, 500 > 300 -> top-1
    }
    CacheConsciousTopnAccumulator acc(/*k=*/5, /*fa_capacity=*/64, /*fanout=*/64);
    for (const auto& e : events) {
        acc.add(e.first, e.second);
    }
    auto got = acc.finalize();
    expect_same(got, brute_force_stream_top_n(events, 5));
    ASSERT_FALSE(got.empty());
    EXPECT_EQ(got[0].key, 999999u);
    EXPECT_EQ(got[0].count, 500);
}

TEST(CacheConsciousAccumulatorTest, FuzzStreamMatchesBruteForce) {
    std::mt19937_64 rng(0xBADF00D);
    for (int trial = 0; trial < 300; ++trial) {
        std::uniform_int_distribution<int> events_dist(1, 8000);
        std::uniform_int_distribution<int64_t> k_dist(1, 20);
        std::uniform_int_distribution<uint64_t> key_dist(1, 400); // repeats force re-aggregation
        std::uniform_int_distribution<int64_t> count_dist(1, 4);

        const int m = events_dist(rng);
        const int64_t k = k_dist(rng);
        std::vector<std::pair<uint64_t, int64_t>> events;
        events.reserve(m);
        for (int i = 0; i < m; ++i) {
            events.push_back({key_dist(rng), count_dist(rng)});
        }
        CacheConsciousTopnAccumulator acc(k, /*fa_capacity=*/32, /*fanout=*/16);
        for (const auto& e : events) {
            acc.add(e.first, e.second);
        }
        expect_same(acc.finalize(), brute_force_stream_top_n(events, k));
    }
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
