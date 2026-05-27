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

#pragma once

#include <algorithm>
#include <cstdint>
#include <queue>
#include <unordered_map>
#include <vector>

namespace starrocks {

// Cache-conscious top-n for count(*)-style aggregation: keep only the candidate top-n
// groups exact (Fine-grained Aggregates) and prune the long tail held as coarse partition
// upper bounds, instead of fully ranking every group. Pure logic with no pipeline
// dependencies so it is unit-testable in isolation and can be driven by the spillable
// blocking aggregate operator.
//
// Soundness of pruning (the crux): the prune threshold is the k-th highest *lower* bound.
// For count(*) an FA group's exact count is its own lower bound, while a partition's total
// count is an *upper* bound on any single group inside it. A partition is pruned iff its
// upper bound is strictly below the k-th highest exact count: there are then already >= k
// groups at least that large, so no group in the partition can reach the top-n. Partition
// upper bounds must never raise the threshold, otherwise a loose bound could prune a
// partition that holds a true winner.
class CacheConsciousTopN {
public:
    struct Group {
        uint64_t key; // opaque group id (the encoded group-by key or its hash)
        int64_t count;
    };

    // A coarse partition: the logical upper bound stat (`upper_bound` = sum of contained counts) and the
    // physical tuples. In the operator the stat lives in RAM always while the tuples may
    // spill; here both are in memory, but prune only ever consults `upper_bound`.
    struct Partition {
        int64_t upper_bound = 0; // sum of group counts; an upper bound on any single group inside
        int level = 0;           // radix level already consumed
        std::vector<Group> groups;
        // max-heap by upper bound
        bool operator<(const Partition& o) const { return upper_bound < o.upper_bound; }
    };

    CacheConsciousTopN(int64_t k, size_t fa_capacity, size_t partition_fanout)
            : _k(k), _fa_capacity(fa_capacity), _fanout(std::max<size_t>(1, partition_fanout)) {}

    size_t fanout() const { return _fanout; }

    // Level-0 bucket for a key: the operator uses this to route a miss row to its partition
    // at push time. Re-partitioning at deeper levels re-salts (see _prune), so a key stays
    // in one bucket per level but redistributes across passes.
    size_t bucket(uint64_t key) const { return _mix(key) % _fanout; }

    // Skew test = the flip decision. `counts` are the exact (prefix) counts observed at the
    // flip point. Returns true iff the candidate set {count >= k-th highest count} fits the
    // FA budget, i.e. a small set of groups provably dominates. A uniform distribution ties
    // every group at the k-th value, so the candidate set spans the whole input and exceeds
    // fa_capacity -> not skewed -> do not flip.
    static bool is_skewed(const std::vector<int64_t>& counts, int64_t k, size_t fa_capacity) {
        if (k <= 0) {
            return false;
        }
        const size_t n = counts.size();
        // Everything is already in the top-k, or the whole input fits FA: nothing to prune.
        if (static_cast<size_t>(k) >= n || n <= fa_capacity) {
            return false;
        }
        std::vector<int64_t> c(counts);
        std::nth_element(c.begin(), c.begin() + (k - 1), c.end(), std::greater<int64_t>());
        const int64_t l_k = c[k - 1];
        size_t candidates = 0;
        for (const int64_t v : counts) {
            candidates += (v >= l_k);
        }
        return candidates <= fa_capacity;
    }

    // Exact top-n by count descending (ties broken by smaller key for determinism). When the
    // input is not skewed it falls back to a full ranking. `pruned_groups`, if non-null,
    // reports how many tail groups were skipped without exact ranking (an efficiency signal,
    // never a correctness one).
    //
    // Tail handling is best-first multi-level: cold groups are radix-partitioned and the
    // partition with the largest total (its upper bound) is expanded first. A partition is
    // pruned once its total drops below the k-th highest exact count, resolved once it is
    // small enough to aggregate, otherwise re-partitioned on the next radix level so its
    // total shrinks. count(*) needs this re-partitioning: a single level leaves the whole
    // tail in a few partitions whose totals stay above the threshold even though every group
    // in them is tiny.
    std::vector<Group> top_n(const std::vector<Group>& groups, size_t* pruned_groups = nullptr) const {
        if (pruned_groups != nullptr) {
            *pruned_groups = 0;
        }
        if (_k <= 0 || groups.empty()) {
            return {};
        }

        std::vector<int64_t> counts;
        counts.reserve(groups.size());
        for (const auto& g : groups) {
            counts.push_back(g.count);
        }
        if (!is_skewed(counts, _k, _fa_capacity)) {
            return _full_top_n(groups);
        }

        // Seed FA with the top fa_capacity groups by count (exact); the rest are the cold
        // tail. (In the operator FA is instead the set frozen at the flip; rank() works for
        // any FA/cold split, so the same prune core serves both.)
        std::vector<Group> sorted(groups);
        const size_t fa_n = std::min(_fa_capacity, sorted.size());
        std::partial_sort(sorted.begin(), sorted.begin() + fa_n, sorted.end(), _by_count_desc);
        std::vector<Group> fa(sorted.begin(), sorted.begin() + fa_n);
        std::vector<Group> cold(sorted.begin() + fa_n, sorted.end());
        return rank(std::move(fa), std::move(cold), pruned_groups);
    }

    // Exact top-n given an already-chosen FA set (exact groups) and the cold tail as a flat
    // vector. Builds a single seed partition from the tail and prunes it best-first.
    std::vector<Group> rank(std::vector<Group> fa, std::vector<Group> cold, size_t* pruned_groups = nullptr) const {
        std::vector<Partition> seed;
        if (!cold.empty()) {
            Partition c;
            for (const auto& g : cold) {
                c.upper_bound += g.count;
            }
            c.groups = std::move(cold);
            seed.push_back(std::move(c));
        }
        return _prune(std::move(fa), std::move(seed), pruned_groups);
    }

    // Exact top-n given an FA set and the cold tail already partitioned at push time (the
    // two-layer CA: each partition carries its logical upper bound stat and its tuples). This is the
    // path the operator uses — partitioning happened on push, not here. Empty partitions are
    // skipped; the prune core is identical to the flat-vector rank().
    std::vector<Group> rank_partitions(std::vector<Group> fa, std::vector<Partition> partitions,
                                       size_t* pruned_groups = nullptr) const {
        std::vector<Partition> seed;
        seed.reserve(partitions.size());
        for (auto& p : partitions) {
            if (!p.groups.empty()) {
                p.level = 0;
                seed.push_back(std::move(p));
            }
        }
        return _prune(std::move(fa), std::move(seed), pruned_groups);
    }

private:
    static bool _by_count_desc(const Group& a, const Group& b) {
        return a.count != b.count ? a.count > b.count : a.key < b.key;
    }

    // splitmix64 finalizer: scrambles a key into well-distributed radix bits.
    static uint64_t _mix(uint64_t x) {
        x += 0x9E3779B97F4A7C15ull;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
        return x ^ (x >> 31);
    }

    // A partition this small is aggregated exactly instead of being split further.
    size_t _resolve_threshold() const { return std::max<size_t>(_fa_capacity, 1); }

    // Safety cap on radix depth; distinct keys separate well before this with re-salting.
    static constexpr int _max_level() { return 16; }

    void _push_kheap(std::priority_queue<int64_t, std::vector<int64_t>, std::greater<int64_t>>& kheap,
                     int64_t count) const {
        if (kheap.size() < static_cast<size_t>(_k)) {
            kheap.push(count);
        } else if (count > kheap.top()) {
            kheap.pop();
            kheap.push(count);
        }
    }

    std::vector<Group> _full_top_n(const std::vector<Group>& groups) const {
        std::vector<Group> out(groups);
        const size_t n = std::min(static_cast<size_t>(_k), out.size());
        std::partial_sort(out.begin(), out.begin() + n, out.end(), _by_count_desc);
        out.resize(n);
        return out;
    }

    // Best-first multi-level prune shared by rank()/rank_partitions(): expand the partition
    // with the largest upper bound first, prune when even that cannot reach the k-th highest exact
    // count, resolve small/exhausted partitions exactly, re-partition the rest on the next
    // radix level (re-salted) so their totals shrink.
    std::vector<Group> _prune(std::vector<Group> fa, std::vector<Partition> seed, size_t* pruned_groups) const {
        if (pruned_groups != nullptr) {
            *pruned_groups = 0;
        }
        std::vector<Group> resolved = std::move(fa);
        // Min-heap holding the k largest exact counts seen so far; its top is the k-th
        // highest exact value = the sound prune threshold.
        std::priority_queue<int64_t, std::vector<int64_t>, std::greater<int64_t>> kheap;
        for (const auto& g : resolved) {
            _push_kheap(kheap, g.count);
        }

        std::priority_queue<Partition> pq;
        for (auto& p : seed) {
            if (!p.groups.empty()) {
                pq.push(std::move(p));
            }
        }

        while (!pq.empty()) {
            const int64_t threshold = (kheap.size() >= static_cast<size_t>(_k)) ? kheap.top() : INT64_MIN;
            // Best-first: the top of pq has the largest upper bound, so if it cannot reach the
            // threshold neither can anything else still queued.
            if (pq.top().upper_bound < threshold) {
                if (pruned_groups != nullptr) {
                    while (!pq.empty()) {
                        *pruned_groups += pq.top().groups.size();
                        pq.pop();
                    }
                }
                break;
            }
            Partition p = pq.top();
            pq.pop();

            // Resolve exactly when small enough to aggregate, or when the radix is exhausted.
            // A key always hashes to the same bucket at every level, so all of its rows are
            // in this partition: aggregating by key here yields its exact count even when the
            // cold tail carried a key as several separate rows.
            if (p.groups.size() <= _resolve_threshold() || p.level >= _max_level()) {
                std::unordered_map<uint64_t, int64_t> exact;
                for (const auto& g : p.groups) {
                    exact[g.key] += g.count;
                }
                for (const auto& [key, count] : exact) {
                    resolved.push_back({key, count});
                    _push_kheap(kheap, count);
                }
                continue;
            }

            // Re-partition on the next radix level so the sub-partition totals shrink. The
            // key is re-hashed with a per-level salt, so each level redistributes groups
            // independently instead of relying on a fixed slice of hash bits.
            std::vector<Partition> sub(_fanout);
            for (const auto& g : p.groups) {
                const size_t b = _mix(g.key + static_cast<uint64_t>(p.level) * 0x9E3779B97F4A7C15ull) % _fanout;
                sub[b].groups.push_back(g);
                sub[b].upper_bound += g.count;
            }
            for (auto& s : sub) {
                if (!s.groups.empty()) {
                    s.level = p.level + 1;
                    pq.push(std::move(s));
                }
            }
        }

        return _full_top_n(resolved);
    }

    int64_t _k;
    size_t _fa_capacity;
    size_t _fanout;
};

// Coarse-grained aggregates as two layers: a logical
// per-partition stat (the count upper bound, always in RAM, the only thing prune consults) and the
// physical tuples (here in RAM; the operator layer spills these on memory pressure). The
// crux is that partitioning happens at routing time — route() is called per miss row on
// push — so the upper bound is maintained incrementally and is available without ever reading the
// tuples. finalize() hands the partitions to the engine's prune core.
class CacheConsciousCa {
public:
    using Group = CacheConsciousTopN::Group;
    using Partition = CacheConsciousTopN::Partition;

    CacheConsciousCa(int64_t k, size_t fa_capacity, size_t fanout)
            : _engine(k, fa_capacity, fanout), _partitions(_engine.fanout()) {}

    // Route a miss row to its level-0 partition: bump the logical upper bound stat and append the
    // tuple. The tuple is what spills; the stat is what stays and drives prune.
    void route(uint64_t key, int64_t partial) {
        const size_t pid = _engine.bucket(key);
        _partitions[pid].upper_bound += partial;
        _partitions[pid].groups.push_back({key, partial});
    }

    // Logical upper bound of a partition without touching its tuples — what the spill path
    // reports and what prune compares against the threshold.
    int64_t partition_upper_bound(size_t pid) const { return _partitions[pid].upper_bound; }
    size_t fanout() const { return _engine.fanout(); }

    // Spill support: move a partition's physical tuples out (to spill them) and back in (on
    // restore). The logical upper-bound stat stays in the partition either way, so prune keeps
    // working while the tuples are on disk. The operator owns the actual block I/O; a partition
    // left empty after take_ is simply skipped by finalize (e.g. a pruned one never restored).
    std::vector<Group> take_partition_tuples(size_t pid) { return std::move(_partitions[pid].groups); }
    void set_partition_tuples(size_t pid, std::vector<Group> tuples) { _partitions[pid].groups = std::move(tuples); }

    // Re-append a previously spilled tuple to its partition WITHOUT bumping the stat — the stat
    // was already accumulated by route() before the spill, so restore must not double-count it.
    void restore_tuple(uint64_t key, int64_t partial) {
        _partitions[_engine.bucket(key)].groups.push_back({key, partial});
    }

    // Total bytes of physical tuples held in RAM across partitions — what the operator reports
    // as revocable and sheds on spill. The logical stats are O(fanout) and non-revocable.
    size_t physical_tuples_bytes() const {
        size_t rows = 0;
        for (const auto& p : _partitions) {
            rows += p.groups.size();
        }
        return rows * sizeof(Group);
    }

    std::vector<Group> finalize(std::vector<Group> fa, size_t* pruned = nullptr) {
        return _engine.rank_partitions(std::move(fa), std::move(_partitions), pruned);
    }

private:
    CacheConsciousTopN _engine;
    std::vector<Partition> _partitions;
};

} // namespace starrocks
