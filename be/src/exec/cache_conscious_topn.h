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
#include <memory>
#include <new>
#include <numeric>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#if defined(__x86_64__)
#include <immintrin.h>
#endif

#include "exec/cache_conscious_fa.h"

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
    // The CA wrapper drives the mid-run swap and reuses the private locality probe + thresholds.
    friend class CacheConsciousCa;

    struct Group {
        uint64_t key; // opaque group id (the encoded group-by key or its hash)
        int64_t count;
    };

    // Append-only block arena for the cold partition tuples, with a per-arena software
    // write-combine buffer ("staging") at the front. The cold tail is written once on the scan and
    // then mostly pruned WITHOUT being read back, so the write path is what matters; an earlier
    // shape that issued a single 8/16-byte NT store per row against each of 256 partition tails
    // overflowed the CPU's hardware write-combine slots (a few per core), evicted half-full
    // bursts, and stalled the routing loop.
    //
    // The fix is the paper's design: each arena keeps one 64-byte staging line resident in L1 and
    // accumulates up to a full cache-line's worth of rows there; when it fills, the whole 64-byte
    // burst is committed to the current 64 KiB block as one aligned NT store, so every store
    // costs one burst (not one row), and the staging line itself is the only hot write the loop
    // touches per row.
    //
    // One slot layout: a 16-byte {key, count} pair per row, 4 rows per 64-byte staging burst. The
    // 2-phase merge path carries a per-row partial count; the 1-phase colocate count(*) path stores
    // each miss row with count 1. Both share this layout, so the engine code reads a stored pair.
    //
    // Storage is a uint64_t* block (64 KiB, 64-byte aligned). Block capacity is `block_slots()`
    // = 4096 pairs.
    //
    // begin() flushes any partial staging before iterating so a finalized arena exposes the full
    // set, including the last < staging_slots() rows that never completed a burst. take_partition
    // does the same via the public flush() entry point.
    //
    // Drop-in for the few std::vector<Group> ops the engine uses (push_back, range-for, size,
    // empty, copy/move, build-from-vector). Copyable because _prune lifts a surviving partition
    // out of the priority queue by value; copies are survivor-only (a fully pruned tail never
    // copies) and deep-copy with plain stores.
    class GroupArena {
    public:
        static constexpr size_t kBlockBytes = 64 * 1024;
        // Staging burst = one cache line: 4 {key, count} pairs.
        static constexpr size_t kStagingU64s = 8;

        GroupArena() = default;
        ~GroupArena() { _free(); }
        GroupArena(GroupArena&& o) noexcept { _steal(o); }
        GroupArena& operator=(GroupArena&& o) noexcept {
            if (this != &o) {
                _free();
                _steal(o);
            }
            return *this;
        }
        GroupArena(const GroupArena& o) : GroupArena() {
            for (Group g : o) push_back(g);
        }
        GroupArena& operator=(const GroupArena& o) {
            if (this != &o) {
                *this = GroupArena(o);
            }
            return *this;
        }
        explicit GroupArena(const std::vector<Group>& v) : GroupArena() {
            for (const auto& g : v) push_back(g);
        }

        size_t block_slots() const { return kBlockBytes / 16; }
        size_t staging_slots() const { return kStagingU64s / 2; }

        void push_back(const Group& g) {
            // Hot path: one cache-line write to staging + one byte bump. The branch on a full
            // staging line is taken once per `staging_slots()` rows and is well-predicted.
            _staging[_stage_n * 2] = g.key;
            _staging[_stage_n * 2 + 1] = static_cast<uint64_t>(g.count);
            ++_stage_n;
            ++_size;
            if (_stage_n == staging_slots()) {
                _flush_staging();
            }
        }

        // Flush any partial staging to the current block. Must be called before iteration / take
        // so the trailing rows that never completed a burst are visible. Public so callers that
        // already know they want a flushed view (the operator at finalize) can drive it.
        void flush() {
            if (_stage_n != 0) _flush_staging();
        }

        size_t size() const { return _size; }
        size_t allocated_bytes() const { return _blocks.size() * kBlockBytes + _blocks.capacity() * sizeof(Block); }
        // Rows already committed to a block. The trailing < staging_slots() rows still in the
        // staging line are not addressable by at(), so a reader that must not disturb the staging
        // line (a probe of a partition that keeps receiving rows) iterates only [0, flushed_size()).
        size_t flushed_size() const { return _size - _stage_n; }
        bool empty() const { return _size == 0; }

        class const_iterator {
        public:
            const_iterator(const GroupArena* a, size_t blk, size_t idx) : _a(a), _blk(blk), _idx(idx) {}
            Group operator*() const {
                const uint64_t* blk = _a->_blocks[_blk].get();
                return Group{blk[_idx * 2], static_cast<int64_t>(blk[_idx * 2 + 1])};
            }
            const_iterator& operator++() {
                ++_idx;
                const size_t cap = (_blk + 1 == _a->_blocks.size()) ? _a->_tail_n : _a->block_slots();
                if (_idx >= cap) {
                    ++_blk;
                    _idx = 0;
                }
                return *this;
            }
            bool operator!=(const const_iterator& o) const { return _blk != o._blk || _idx != o._idx; }

        private:
            const GroupArena* _a;
            size_t _blk;
            size_t _idx;
        };
        const_iterator begin() const {
            // Iteration sees everything written so far, including the tail rows still sitting in
            // staging. Mutating in begin() is a const-correctness hack here, but the alternative
            // (require callers to flush()) breaks the std::vector-shaped API the engine uses.
            const_cast<GroupArena*>(this)->flush();
#if defined(__x86_64__)
            _mm_sfence(); // make the non-temporal staging bursts visible before reading them back
#endif
            if (_size == 0) return end();
            return const_iterator(this, 0, 0);
        }
        const_iterator end() const { return const_iterator(this, _blocks.size(), 0); }

        // Random access by logical index, valid only after flush(): non-last blocks are exactly
        // block_slots() full (staging bursts divide a block evenly), so the index math is exact.
        // Lets the prune locality probe read scattered windows without iterating the whole arena.
        Group at(size_t i) const {
            const size_t bs = block_slots();
            const uint64_t* blk = _blocks[i / bs].get();
            const size_t j = i % bs;
            return Group{blk[j * 2], static_cast<int64_t>(blk[j * 2 + 1])};
        }

    private:
        // Move `_stage_n` rows from the staging line into the current block as one cache-line burst.
        // A full burst goes out as streaming (MOVNTDQ) stores so the write-once cold tail never
        // pollutes L1. A partial burst uses memcpy instead: a partial NT store would leave the
        // write-combine buffer half-full, and the next partition's burst is as likely to evict it as
        // our own, so the bytes get written back anyway without the streaming benefit.
        //
        // MOVNTDQ also faults on a destination that is not 16-byte aligned. Each row is a 16-byte
        // pair and `_tail_n` advances in whole rows, so the byte offset `_tail_n * 16` is always a
        // multiple of 16; the alignment test below is a defensive guard on that invariant.
        void _flush_staging() {
            if (_stage_n == 0) return;
            size_t copied = 0;
            while (copied < _stage_n) {
                if (_blocks.empty() || _tail_n == block_slots()) {
                    Block block(static_cast<uint64_t*>(::operator new(kBlockBytes, std::align_val_t(64))));
                    _blocks.push_back(std::move(block));
                    _tail = _blocks.back().get();
                    _tail_n = 0;
                }
                // An earlier partial flush may leave fewer than four slots in this block.
                // Fill it completely before allocating the next: logical indexing assumes
                // every block except the last is full.
                const size_t count = std::min(_stage_n - copied, block_slots() - _tail_n);
                uint64_t* dst = _tail + _tail_n * 2;
                const uint64_t* src = _staging + copied * 2;
#if defined(__x86_64__)
                if (count == staging_slots() && (reinterpret_cast<uintptr_t>(dst) & 63) == 0) {
                    for (size_t i = 0; i < kStagingU64s; i += 2) {
                        _mm_stream_si128(reinterpret_cast<__m128i*>(dst + i),
                                         _mm_load_si128(reinterpret_cast<const __m128i*>(src + i)));
                    }
                } else {
                    std::memcpy(dst, src, count * sizeof(Group));
                }
#else
                std::memcpy(dst, src, count * sizeof(Group));
#endif
                _tail_n += count;
                copied += count;
            }
            _stage_n = 0;
        }

        void _steal(GroupArena& o) {
            _blocks = std::move(o._blocks);
            _tail = o._tail;
            _tail_n = o._tail_n;
            _size = o._size;
            _stage_n = o._stage_n;
            std::memcpy(_staging, o._staging, sizeof(_staging));
            o._tail = nullptr;
            o._tail_n = 0;
            o._size = 0;
            o._stage_n = 0;
        }
        void _free() {
            _blocks.clear();
            _tail = nullptr;
            _tail_n = 0;
            _size = 0;
            _stage_n = 0;
        }
        // 64-byte staging line first so push_back's hot store hits the first cache line of the
        // arena object. The rest of the metadata follows in line 1.
        alignas(64) uint64_t _staging[kStagingU64s] = {};
        size_t _stage_n = 0;
        struct BlockDeleter {
            void operator()(uint64_t* block) const { ::operator delete(block, std::align_val_t(64)); }
        };
        using Block = std::unique_ptr<uint64_t, BlockDeleter>;
        std::vector<Block> _blocks;
        uint64_t* _tail = nullptr;
        size_t _tail_n = 0;
        size_t _size = 0;
    };

    // A coarse partition: the logical upper bound stat (`upper_bound` = sum of contained counts) and the
    // physical tuples. In the operator the stat lives in RAM always while the tuples may
    // spill; here both are in memory, but prune only ever consults `upper_bound`.
    struct Partition {
        int64_t upper_bound = 0; // sum of group counts; an upper bound on any single group inside
        int level = 0;           // radix level already consumed
        GroupArena groups;
        // max-heap by upper bound
        bool operator<(const Partition& o) const { return upper_bound < o.upper_bound; }
    };

    CacheConsciousTopN(int64_t k, size_t fa_capacity, size_t partition_fanout)
            : _k(k), _fa_capacity(fa_capacity), _fanout(std::max<size_t>(1, partition_fanout)) {
        // A power-of-two fanout lets the per-row partition assignment use a mask instead of a
        // modulo. The `%` is by a runtime value, so it compiles to a ~20-40 cycle integer divide
        // executed for every cold row (tens of millions) -- it showed up as a large slice of the
        // routing cost; the mask removes it for the common power-of-two fanout.
        _fanout_mask = ((_fanout & (_fanout - 1)) == 0) ? (_fanout - 1) : 0;
        _fanout_shift = _fanout_mask ? (64 - __builtin_ctzll(_fanout)) : 0;
    }

    size_t fanout() const { return _fanout; }

    // Level-0 bucket for a key: the operator uses this to route a miss row to its partition
    // at push time. Re-partitioning at deeper levels re-salts (see _prune), so a key stays
    // in one bucket per level but redistributes across passes.
    //
    // Fibonacci hash (Knuth multiplicative): one multiply and a shift of the top bits select the
    // partition for a power-of-two fanout, spreading sequential cold keys evenly. The deeper-level
    // re-salt (step()) stays on splitmix64 _mix, whose avalanche separates keys that share a
    // level-0 bucket; a fibonacci re-salt could not, since the k*phi difference is salt-invariant
    // in the top bits. The re-salt runs only on surviving partitions.
    size_t bucket(uint64_t key) const {
        if (_fanout_mask) {
            return static_cast<size_t>((key * 0x9E3779B97F4A7C15ull) >> _fanout_shift);
        }
        return _reduce(_mix(key));
    }

    // Skew test = the flip decision. `counts` are the exact (prefix) counts observed at the
    // flip point. Returns true iff the top k groups together account for at least `mass_fraction` of
    // the mass (default 0.15 -- measured: cc already wins once the top-k holds ~15-20% of the mass).
    // That is the property the cc operator exploits: when a few groups dominate the rows, FA
    // captures them exactly while CA holds the long tail, and the prune sees most of the tail
    // off without resolving it.
    //
    // The decision rests on mass share alone -- not on the distinct-key count. The flip is triggered
    // by the live map outgrowing the L2 budget, which by itself establishes the working set does not
    // fit cache, so a separate "does the whole input fit FA" guard would only restate the trigger --
    // and, since hitting the budget pins the distinct count at ~= capacity at that instant, it would
    // veto the flip on exactly the skewed inputs cc is built for.
    //
    // The earlier test compared every observed count against the k-th highest. For a distribution
    // with fewer than k distinct hot keys followed by a long tail of cold count=1 keys (a very
    // common shape for `GROUP BY id ORDER BY count(*) DESC`), the k-th highest collapses to 1,
    // so the candidate set spans the whole input and the test always misfires -- the natural
    // flip never fired despite obvious skew, and ON matched OFF. The mass-share test is robust
    // to the long tail of 1s, and a uniform distribution still falls back: top-k mass / total
    // approaches k/n, well below any sane fraction once n >> k.
    static bool is_skewed(const std::vector<int64_t>& counts, int64_t k, double mass_fraction = 0.15) {
        if (k <= 0) {
            return false;
        }
        const size_t n = counts.size();
        // <= k distinct groups: everything is already in the top-k, nothing to prune.
        if (static_cast<size_t>(k) >= n) {
            return false;
        }
        std::vector<int64_t> c(counts);
        std::nth_element(c.begin(), c.begin() + (k - 1), c.end(), std::greater<int64_t>());
        int64_t topk_sum = 0;
        for (size_t i = 0; i < static_cast<size_t>(k); ++i) {
            topk_sum += c[i];
        }
        const int64_t total = std::accumulate(counts.begin(), counts.end(), int64_t{0});
        // Skewed iff the top-k holds at least `mass_fraction` of the mass. A lower fraction flips on
        // weaker skew (reaching the late-riser case for the swap); a uniform stream still falls back,
        // since top-k mass / total approaches k/n, far below any sane fraction once n >> k.
        return total > 0 && static_cast<double>(topk_sum) >= static_cast<double>(total) * mass_fraction;
    }

    // The sound prune threshold: the k-th highest exact count in FA. Partitions whose upper bound
    // is strictly below this cannot host a top-k winner (there are already >= k exact counts at or
    // above the threshold). Returns INT64_MIN when FA has fewer than k entries -- no threshold can
    // be claimed yet and every partition is potentially a winner. The threshold is monotone
    // non-decreasing as resolved partitions add exact counts to FA, so a partition below threshold
    // at this snapshot stays pruned for the rest of the run -- which is what makes early-prune at
    // restore time sound.
    int64_t topk_threshold(const std::vector<Group>& fa) const {
        if (_k <= 0 || fa.size() < static_cast<size_t>(_k)) {
            return INT64_MIN;
        }
        std::vector<int64_t> counts;
        counts.reserve(fa.size());
        for (const auto& g : fa) counts.push_back(g.count);
        std::nth_element(counts.begin(), counts.begin() + (_k - 1), counts.end(), std::greater<int64_t>());
        return counts[_k - 1];
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
        if (!is_skewed(counts, _k)) {
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
            c.groups = GroupArena(cold);
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

    // Multi-step prune driven by the operator's source side -- one PQ pop per `step()` so a long
    // surviving CA does not monopolize the driver thread inside one sink finalize call. The state
    // (resolved FA, k-heap of exact counts, max-heap of partitions by upper bound) is the same as
    // `_prune` keeps locally; the difference is who owns the loop. `finish()` does the final
    // partial sort on `resolved`.
    class PruneSession {
    public:
        PruneSession() = default;

        bool done() const { return _pq.empty(); }
        size_t pruned_groups() const { return _pruned_groups; }
        size_t resolved_partitions() const { return _resolved_partitions; }
        size_t repartitioned_partitions() const { return _repartitioned_partitions; }
        size_t pruned_partitions() const { return _pruned_partitions; }
        size_t reprocessed_tuples() const { return _reprocessed_tuples; }
        int max_radix_level() const { return _max_radix_level; }

        void init(const CacheConsciousTopN* engine, std::vector<Group> fa, std::vector<Partition> seed) {
            _engine = engine;
            _resolved = std::move(fa);
            for (const auto& g : _resolved) _engine->_push_kheap(_kheap, g.count);
            for (auto& p : seed) {
                if (!p.groups.empty()) _pq.push(std::move(p));
            }
            _drop_losers();
        }

        // One step: pop the largest-UB partition and either resolve it exactly (small / out of
        // radix levels) or re-partition into the next radix level. After every step the queue
        // is re-pruned against the (possibly raised) threshold so resolves are amortized.
        void step() {
            if (_pq.empty()) return;
            Partition p = _pq.top();
            _pq.pop();
            if (static_cast<int>(p.level) > _max_radix_level) _max_radix_level = static_cast<int>(p.level);
            bool aggregate = p.groups.size() <= _engine->_resolve_threshold() || p.level >= _engine->_max_level();
            if (!aggregate) {
                // Decide exact-aggregate vs re-partition by collapsibility, not raw tuple count: a
                // buffer of many copies of a few keys (low locality) collapses on aggregation,
                // whereas testing tuple count would re-partition it -- and identical keys all hash
                // to one child, so the split makes no progress and the recursion spins to the depth
                // cap (the concentrated-CA reprocess blow-up).
                p.groups.flush();
                const double locality = CacheConsciousTopN::probe_locality(
                        p.groups, CacheConsciousTopN::kProbeSegmentLen, CacheConsciousTopN::kProbeSegments);
                aggregate = locality < CacheConsciousTopN::kLocalityAggregateThreshold;
            }
            if (aggregate) {
                ++_resolved_partitions;
                _reprocessed_tuples += p.groups.size();
                std::unordered_map<uint64_t, int64_t> exact;
                for (Group g : p.groups) exact[g.key] += g.count;
                for (const auto& [key, count] : exact) {
                    _resolved.push_back({key, count});
                    _engine->_push_kheap(_kheap, count);
                }
            } else {
                ++_repartitioned_partitions;
                _reprocessed_tuples += p.groups.size();
                std::vector<Partition> sub(_engine->_fanout);
                const uint64_t salt = static_cast<uint64_t>(p.level + 1) * 0x9E3779B97F4A7C15ull;
                for (Group g : p.groups) {
                    const size_t b = _engine->_reduce(_engine->_mix(g.key + salt));
                    sub[b].groups.push_back(g);
                    sub[b].upper_bound += g.count;
                }
                for (auto& s : sub) {
                    if (!s.groups.empty()) {
                        s.level = p.level + 1;
                        _pq.push(std::move(s));
                    }
                }
            }
            _drop_losers();
        }

        std::vector<Group> finish() { return _engine->_full_top_n(_resolved); }

    private:
        void _drop_losers() {
            const int64_t threshold = (_kheap.size() >= static_cast<size_t>(_engine->_k)) ? _kheap.top() : INT64_MIN;
            while (!_pq.empty() && _pq.top().upper_bound < threshold) {
                _pruned_groups += _pq.top().groups.size();
                ++_pruned_partitions;
                _pq.pop();
            }
        }

        const CacheConsciousTopN* _engine = nullptr;
        std::vector<Group> _resolved;
        std::priority_queue<int64_t, std::vector<int64_t>, std::greater<int64_t>> _kheap;
        std::priority_queue<Partition> _pq;
        size_t _pruned_groups = 0;
        // Phase-3 work counters (surfaced in the AGGREGATION profile): how many partitions were
        // resolved exactly vs re-partitioned to a deeper radix level, how many were pruned whole,
        // total tuples re-touched across all steps, and the deepest radix level reached.
        size_t _resolved_partitions = 0;
        size_t _repartitioned_partitions = 0;
        size_t _pruned_partitions = 0;
        size_t _reprocessed_tuples = 0;
        int _max_radix_level = 0;
    };

    PruneSession begin_prune(std::vector<Group> fa, std::vector<Partition> seed) const {
        PruneSession s;
        std::vector<Partition> non_empty;
        non_empty.reserve(seed.size());
        for (auto& p : seed) {
            if (!p.groups.empty()) {
                p.level = 0;
                non_empty.push_back(std::move(p));
            }
        }
        s.init(this, std::move(fa), std::move(non_empty));
        return s;
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

    // Map a mixed hash to a partition: a mask for a power-of-two fanout (no divide), else modulo.
    size_t _reduce(uint64_t h) const { return _fanout_mask ? (h & _fanout_mask) : (h % _fanout); }

    // A partition this small is aggregated exactly instead of being split further.
    size_t _resolve_threshold() const { return std::max<size_t>(_fa_capacity, 1); }

    // A partition larger than the resolve threshold is exact-aggregated (collapsed) rather than
    // re-partitioned iff its sampled locality is below this threshold -- it is concentrated or
    // locally clustered enough to shrink on aggregation. Re-partitioning is left for genuinely
    // scattered high-distinct buffers. The threshold is a starting value tuned per machine.
    static constexpr size_t kProbeSegmentLen = 256;
    static constexpr size_t kProbeSegments = 8;
    static constexpr double kLocalityAggregateThreshold = 0.5;

    // Sample n_segments contiguous windows of seg_len tuples, spread evenly across the (flushed)
    // buffer, and return its locality = mean(distinct / window size). Low means collapsible (few
    // distinct per window: one dominant key, or keys clustered into contiguous runs); near 1 means
    // scattered. O(n_segments * seg_len), not O(size) -- a small slice of an already-materialized
    // buffer, no source re-scan. Distinct-per-window, not global distinct, is the signal, so a
    // high-distinct buffer whose keys arrive in runs still reads as collapsible.
    static double probe_locality(const GroupArena& buf, size_t seg_len, size_t n_segments, uint64_t* top_key = nullptr,
                                 double* top_fraction = nullptr) {
#if defined(__x86_64__)
        _mm_sfence(); // Complete streaming writes before sampling the flushed prefix.
#endif
        const size_t n = buf.flushed_size();
        if (n == 0) {
            if (top_key != nullptr) *top_key = 0;
            if (top_fraction != nullptr) *top_fraction = 0.0;
            return 1.0;
        }
        seg_len = std::min(seg_len, n);
        n_segments = std::max<size_t>(1, std::min(n_segments, n / seg_len));
        const size_t span = n - seg_len;
        const bool want_dominant = top_key != nullptr || top_fraction != nullptr;
        std::unordered_set<uint64_t> seg;
        // When the caller wants the dominant key (the swap), tally frequencies across the same
        // sampled rows and hand back the most common one plus its share of the sample. A
        // concentrated buffer's dominant key recurs in every window, so the sample names it -- and
        // estimates its count as share * partition_total -- without a full pass over the buffer.
        std::unordered_map<uint64_t, uint32_t> freq;
        double locality_sum = 0.0;
        for (size_t s = 0; s < n_segments; ++s) {
            const size_t start = (n_segments == 1) ? 0 : (s * span) / (n_segments - 1);
            seg.clear();
            for (size_t i = 0; i < seg_len; ++i) {
                const uint64_t key = buf.at(start + i).key;
                seg.insert(key);
                if (want_dominant) ++freq[key];
            }
            locality_sum += static_cast<double>(seg.size()) / static_cast<double>(seg_len);
        }
        if (want_dominant) {
            uint64_t best_key = 0;
            uint32_t best_count = 0;
            for (const auto& [key, count] : freq) {
                if (count > best_count) {
                    best_count = count;
                    best_key = key;
                }
            }
            if (top_key != nullptr) *top_key = best_key;
            if (top_fraction != nullptr) {
                *top_fraction = static_cast<double>(best_count) / static_cast<double>(n_segments * seg_len);
            }
        }
        return locality_sum / static_cast<double>(n_segments);
    }

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
                for (Group g : p.groups) {
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
            // independently instead of relying on a fixed slice of hash bits. The salt is
            // (level+1)-based: a pushed partition arrives at level 0 already split by
            // bucket() = _mix(key), so a salt of level*0 would reproduce that exact split (a
            // wasted pass that only bumps the level); offsetting by one makes the first
            // re-partition actually redistribute.
            std::vector<Partition> sub(_fanout);
            for (Group g : p.groups) {
                const size_t b = _reduce(_mix(g.key + static_cast<uint64_t>(p.level + 1) * 0x9E3779B97F4A7C15ull));
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
    size_t _fanout_mask = 0; // _fanout-1 when _fanout is a power of two, else 0 (use modulo)
    int _fanout_shift = 0;   // 64 - log2(fanout) for the fibonacci level-0 bucket; 0 if not pow2
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
    using GroupArena = CacheConsciousTopN::GroupArena;

    // Upper bound on the partition fanout: the 1-phase router's per-chunk scratch counter is a stack
    // array of this size, and bucket() masks into [0, fanout). The operator derives the actual fanout
    // from the cache budget and never exceeds this.
    static constexpr size_t kMaxFanout = 8192;

    CacheConsciousCa(int64_t k, size_t fa_capacity, size_t fanout, int64_t swap_cooldown_chunks = 8)
            : _engine(k, fa_capacity, fanout),
              _partitions(_engine.fanout()),
              _k(k),
              _fa_capacity(fa_capacity),
              _swap_cooldown_chunks(std::max<int64_t>(1, swap_cooldown_chunks)),
              _cooldown_until(_engine.fanout(), 0),
              _backoff(_engine.fanout(), std::max<int64_t>(1, swap_cooldown_chunks)) {}

    // Level-0 partition for a key, exposed so the restore path can re-derive the routing decision
    // without re-mixing in the operator. Deterministic in the key alone: a row spilled out of pid X
    // re-buckets to pid X on restore.
    size_t bucket(uint64_t key) const { return _engine.bucket(key); }

    // The sound prune threshold given FA's current contents -- delegated to the engine. Provided
    // here so the operator can compute a pruned-partition mask off the threshold + this CA's stats
    // before restore starts, without instantiating a temporary engine.
    int64_t topk_threshold(const std::vector<Group>& fa) const { return _engine.topk_threshold(fa); }

    // The bitmap of partitions that cannot host a top-k winner: their upper bound is strictly
    // below `threshold` (the k-th highest exact FA count). Used to short-circuit restore on a
    // spilled CA so pruned partitions' tuples are never re-routed back into the arena -- the disk
    // bytes are still read by the spill engine (a separate per-partition block-group writer is
    // the way to skip them too; not done here), but the dominant per-row CPU on the restore loop
    // is gone.
    std::vector<uint8_t> pruned_mask(int64_t threshold) const {
        std::vector<uint8_t> out(_partitions.size(), 0);
        for (size_t pid = 0; pid < _partitions.size(); ++pid) {
            out[pid] = (_partitions[pid].upper_bound < threshold) ? 1 : 0;
        }
        return out;
    }

    // Route a miss row to its level-0 partition: bump the logical upper bound stat and append the
    // tuple. The tuple is what spills; the stat is what stays and drives prune. For 1-phase
    // count(*) the upstream has no partial-count column, so the caller passes a weight of 1.
    void route(uint64_t key, int64_t partial) {
        const size_t pid = _engine.bucket(key);
        _partitions[pid].upper_bound += partial;
        _partitions[pid].groups.push_back({key, partial});
    }

    // Batched router for a whole chunk's miss rows. The operator's per-row loop dispatched
    // through ca->route per row, and tens of millions of those dominated the routing CPU. Two
    // structural fixes here:
    //  * batch the `upper_bound` update: incrementing it per row was a random write into the
    //    Partition struct on every row, a second cache-line scatter besides the arena tail; the
    //    1-phase case here uses a stack-resident `chunk_count[]` to keep that scatter inside one
    //    L1-hot 1 KiB array and apply the totals per partition at the end of the chunk;
    //  * route each row through the arena's per-partition staging cache line (added in
    //    GroupArena), so the per-row store hits a single L1-resident 64-byte line per pid,
    //    and the actual block writes amortize as one 64-byte burst per `staging_slots()` rows.
    // Templated on the column scalar type so the integer cast is a no-op (Int8/16/32/64Column
    // matches the gated key types). When `partials` is null the row weight is 1 (1-phase colocate
    // count(*)); otherwise each row carries its own partial from the upstream count column. The
    // partial-counts variant still does the scalar add per row because the partial value is the
    // weight, so it cannot be reduced into a single per-pid count.
    template <typename KeyT>
    void route_batch(const KeyT* keys, const int64_t* partials, const uint8_t* sel, size_t n) {
        Partition* parts = _partitions.data();
        if (partials != nullptr) {
            for (size_t i = 0; i < n; ++i) {
                if (!sel[i]) continue;
                const uint64_t k = static_cast<uint64_t>(keys[i]);
                const int64_t p = partials[i];
                const size_t pid = _engine.bucket(k);
                parts[pid].upper_bound += p;
                parts[pid].groups.push_back({k, p});
            }
            return;
        }
        // 1-phase: weight is 1 per row, so the scalar add reduces to a per-partition row count. The
        // scratch counter is sized to the fanout cap; only its live prefix is cleared, so a small
        // fanout keeps the touched range cache-resident across the chunk.
        uint32_t chunk_count[kMaxFanout];
        const size_t f = _engine.fanout();
        std::fill_n(chunk_count, f, uint32_t{0});
        for (size_t i = 0; i < n; ++i) {
            if (!sel[i]) continue;
            const uint64_t k = static_cast<uint64_t>(keys[i]);
            const size_t pid = _engine.bucket(k);
            ++chunk_count[pid];
            parts[pid].groups.push_back({k, 1});
        }
        for (size_t pid = 0; pid < f; ++pid) {
            parts[pid].upper_bound += chunk_count[pid];
        }
    }

    // Logical upper bound of a partition without touching its tuples — what the spill path
    // reports and what prune compares against the threshold.
    int64_t partition_upper_bound(size_t pid) const { return _partitions[pid].upper_bound; }
    size_t fanout() const { return _engine.fanout(); }

    // Spill support: move a partition's physical tuples out (to spill them) and back in (on
    // restore). The logical upper-bound stat stays in the partition either way, so prune keeps
    // working while the tuples are on disk. The operator owns the actual block I/O; a partition
    // left empty after take_ is simply skipped by finalize (e.g. a pruned one never restored).
    std::vector<Group> take_partition_tuples(size_t pid) {
        std::vector<Group> out;
        out.reserve(_partitions[pid].groups.size());
        for (Group g : _partitions[pid].groups) out.push_back(g);
        _partitions[pid].groups = GroupArena();
        return out;
    }
    void set_partition_tuples(size_t pid, std::vector<Group> tuples) { _partitions[pid].groups = GroupArena(tuples); }

    // Re-append a previously spilled tuple to its partition WITHOUT bumping the stat — the stat
    // was already accumulated by route() before the spill, so restore must not double-count it.
    void restore_tuple(uint64_t key, int64_t partial) {
        _partitions[_engine.bucket(key)].groups.push_back({key, partial});
    }

    // Dynamic arena storage released on spill, including unused capacity in allocated blocks.
    // The inline staging lines and logical statistics remain resident and are not revocable.
    size_t physical_tuples_bytes() const {
        size_t bytes = 0;
        for (const auto& p : _partitions) {
            bytes += p.groups.allocated_bytes();
        }
        return bytes;
    }

    std::vector<Group> finalize(std::vector<Group> fa, size_t* pruned = nullptr) {
        return _engine.rank_partitions(std::move(fa), std::move(_partitions), pruned);
    }

    // Hand the partitions to the engine as a session the operator's source side drives one step
    // at a time; the CA's partitions are consumed. Same semantics as finalize() in aggregate, but
    // driven by pull_chunk so a large surviving tail does not block the driver thread.
    CacheConsciousTopN::PruneSession begin_finalize(std::vector<Group> fa) {
        return _engine.begin_prune(std::move(fa), std::move(_partitions));
    }

    // Prime the swap watermark at flip with the k-th largest seed count. Until a partition first
    // crosses it the whole mechanism is dormant -- swap_pass does one O(fanout) compare and returns
    // -- so when the hot set is captured before the flip the swap costs nothing for the whole run.
    void prime_swap(int64_t topk_bound_flip) { _topk_bound = topk_bound_flip; }

    // One post-flip pass of the late-hot-key promotion. Dormant until a partition's count first
    // crosses the watermark; that crossing arms a live, doubling-cadence recompute of the bound (the
    // rising bound throttles late, marginal promotions). On a crossing not in cooldown: probe the
    // buffer's locality -- a scattered partition is left for the end-of-input prune; a concentrated
    // one is aggregated, and if its dominant key beats the live bound that key moves into FA carrying
    // its exact count, the partition is rebuilt without it, and FA's smallest key (below the bound,
    // so never a winner) is evicted back to its CA partition as one pre-aggregated {key, count} tuple
    // to keep FA at capacity. A key lives in FA xor CA throughout (I1) -- the promoted key leaves CA,
    // the evicted key leaves FA -- so no key is dropped or counted twice.
    void swap_pass(CacheConsciousFa* fa) {
        ++_post_flip_chunks;
        if (!_swap_armed) {
            bool crossed = false;
            for (const auto& p : _partitions) {
                if (p.upper_bound > _topk_bound) {
                    crossed = true;
                    break;
                }
            }
            if (!crossed) {
                return; // nothing has out-accumulated the k-th winner yet
            }
            _swap_armed = true;
            _swap_arm_chunk = _post_flip_chunks;
            _recompute_topk_bound(fa);
            _next_recompute = _post_flip_chunks * 2;
        } else if (_post_flip_chunks >= _next_recompute) {
            _recompute_topk_bound(fa);
            _next_recompute = _post_flip_chunks * 2;
        }
        // Two ways to rest a partition that was examined but not promoted:
        //  * scattered (no dominant key -- a uniform cold buffer, e.g. every CA partition on a
        //    no-early-skew input): nothing to estimate, so back off exponentially (double the wait
        //    each time) toward never. Stops the swap churning a garbage-FA flip.
        //  * concentrated but the dominant key is still below the bound: the sample already gave its
        //    count estimate, so re-probe when it is *projected* to reach the bound. Assuming the key
        //    grows in step with the partition, it needs the partition ~bound/est-fold larger, so the
        //    next wait is base * (bound/est) -- soon when it is close, far when it is far. No blind
        //    doubling; the estimate paces it.
        // A promotion resets the partition to the base cadence (it may hold more risers).
        auto rest_scattered = [&](size_t p) {
            _cooldown_until[p] = _post_flip_chunks + _backoff[p];
            _backoff[p] = std::min(_backoff[p] * 2, kMaxBackoff);
        };
        auto project_partition = [&](size_t p, int64_t dominant_count) {
            const int64_t ratio = _topk_bound / std::max<int64_t>(1, dominant_count);
            _cooldown_until[p] =
                    _post_flip_chunks + std::min(kMaxBackoff, _swap_cooldown_chunks * std::max<int64_t>(1, ratio));
        };
        for (size_t pid = 0; pid < _partitions.size(); ++pid) {
            if (_partitions[pid].upper_bound <= _topk_bound || _cooldown_until[pid] > _post_flip_chunks) {
                continue;
            }
            // Probe the committed rows in place. The sample reads the flushed prefix and leaves the
            // staging line untouched, so a partition left here (scattered) keeps the full-burst write
            // alignment for the rows that keep arriving into it.
            uint64_t cand = 0;
            double cand_fraction = 0.0;
            const double locality =
                    CacheConsciousTopN::probe_locality(_partitions[pid].groups, CacheConsciousTopN::kProbeSegmentLen,
                                                       CacheConsciousTopN::kProbeSegments, &cand, &cand_fraction);
            if (locality >= CacheConsciousTopN::kLocalityAggregateThreshold) {
                ++_swap_skipped_scattered; // no single dominant key; leave it for the end prune
                rest_scattered(pid);
                continue;
            }
            // Concentrated: estimate the dominant key's count from its sample share without a full
            // pass -- est = share * partition total. If it cannot beat the bound yet, skip the scan
            // entirely and re-probe only when the key is projected to reach the bound.
            const int64_t est = static_cast<int64_t>(cand_fraction * static_cast<double>(_partitions[pid].upper_bound));
            if (est < _topk_bound) {
                ++_swap_estimate_skipped; // dominant key not yet a contender; no scan done
                project_partition(pid, est);
                continue;
            }
            // The estimate clears the bound, so the key earns an exact count. One scan counts it and
            // moves every other row, unchanged, into the rebuilt buffer -- the dominant key (most of
            // the rows) leaves for FA; the minority tail stays put, raw, with its counts intact.
            GroupArena rest;
            int64_t cand_count = 0;
            size_t scanned = 0;
            for (Group g : _partitions[pid].groups) {
                ++scanned;
                if (g.key == cand) {
                    cand_count += g.count;
                } else {
                    rest.push_back(g);
                }
            }
            _swap_reaggregated_tuples += scanned; // one pass over the partition, not two
            // Exact count fell short (the sample over-estimated): re-project on the true count.
            if (cand_count < _topk_bound) {
                ++_swap_declined; // `rest` is dropped; the original buffer stays intact
                project_partition(pid, cand_count);
                continue;
            }
            // Promote the dominant key into FA carrying its exact count, and rebuild the partition
            // without it (its upper bound drops by the moved count). FA xor CA still holds (I1).
            fa->promote(cand, cand_count);
            fa->bloom_add(cand); // keep the bloom in sync, else the promoted key's later rows route to CA (I1)
            _partitions[pid].groups = std::move(rest);
            _partitions[pid].upper_bound -= cand_count;
            ++_swap_promotions;
            // Productive examination: reset this partition to the base cadence -- it may hold more
            // risers, so re-check it promptly rather than backing off.
            _cooldown_until[pid] = _post_flip_chunks + _swap_cooldown_chunks;
            _backoff[pid] = _swap_cooldown_chunks;
            // When FA was full at the flip (the common case -- distinct keys at the flip ~= capacity)
            // the promotion overfills it by one slot. Evict the smallest FA key -- below the bound,
            // never a winner -- back to its CA partition as one pre-aggregated {key, count} tuple: that
            // partition's upper bound grows by the count and finalize re-aggregates it (plus any later
            // rows of the same key). Net FA size returns to capacity, and the evicted key now lives only
            // in CA (I1). When the flip froze FA below capacity, the guard skips the eviction and the
            // promotion just fills a free slot. Routing the tuple after the std::move above means an
            // eviction that lands back in `pid` appends to the rebuilt buffer.
            if (fa->size() > _fa_capacity) {
                const std::pair<uint64_t, int64_t> evicted = fa->evict_min();
                const size_t epid = _engine.bucket(evicted.first);
                _partitions[epid].upper_bound += evicted.second;
                _partitions[epid].groups.push_back({evicted.first, evicted.second});
                ++_swap_evictions;
            }
        }
    }

    // Swap telemetry surfaced in the profile: keys promoted CA->FA, crossings left in place because
    // the buffer was scattered or the dominant key did not earn a free FA slot, and the post-flip
    // chunk the swap armed at (-1 if it never armed -- the whole mechanism stayed dormant).
    size_t swap_promotions() const { return _swap_promotions; }
    size_t swap_evictions() const { return _swap_evictions; }
    size_t swap_skipped_scattered() const { return _swap_skipped_scattered; }
    size_t swap_declined() const { return _swap_declined; }
    size_t swap_estimate_skipped() const { return _swap_estimate_skipped; }
    int64_t swap_arm_chunk() const { return _swap_arm_chunk; }
    // Tuples the swap re-touched across all its aggregate + rebuild passes -- the swap-side analogue
    // of the end-prune's reprocessed-tuples, so the two re-aggregation costs compare in one unit.
    size_t swap_reaggregated_tuples() const { return _swap_reaggregated_tuples; }

private:
    // Refresh the live watermark from FA's current k-th largest count (O(FA), only on the doubling
    // recompute cadence). Keep the prior bound if FA somehow holds fewer than k keys.
    void _recompute_topk_bound(CacheConsciousFa* fa) {
        const int64_t t = fa->kth_largest_count(_k);
        if (t != INT64_MIN) {
            _topk_bound = t;
        }
    }

    CacheConsciousTopN _engine;
    std::vector<Partition> _partitions;
    int64_t _k = 0;
    size_t _fa_capacity = 0;
    int64_t _topk_bound = INT64_MAX; // swap watermark; primed at flip, then live-recomputed
    int64_t _post_flip_chunks = 0;
    int64_t _next_recompute = 0;
    bool _swap_armed = false;
    int64_t _swap_arm_chunk = -1;         // post-flip chunk the swap armed at; -1 until it arms
    size_t _swap_promotions = 0;          // keys moved CA->FA
    size_t _swap_evictions = 0;           // FA keys flushed back to CA to make room for a promotion
    size_t _swap_skipped_scattered = 0;   // crossings left in place (no dominant key)
    size_t _swap_declined = 0;            // crossings examined but not promoted (dominant key below bound)
    size_t _swap_estimate_skipped = 0;    // concentrated crossings whose sample estimate skipped the scan
    size_t _swap_reaggregated_tuples = 0; // tuples re-touched across the swap's aggregate + rebuild passes
    int64_t _swap_cooldown_chunks = 8;    // base post-flip chunks a partition rests before the swap revisits it
    std::vector<int64_t> _cooldown_until; // per-partition: chunk index until which pid is cooling
    std::vector<int64_t> _backoff;        // per-partition: current cooldown, doubled on each unproductive probe
    static constexpr int64_t kMaxBackoff = 1LL << 40; // cap so the doubling never overflows int64
};

} // namespace starrocks
