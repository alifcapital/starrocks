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
#include <cstddef>
#include <cstdint>
#include <functional>
#include <utility>
#include <vector>

#include "util/hash.h"
#include "util/phmap/phmap.h"

namespace starrocks {

// Frozen Fine-grained Aggregates for the cache-conscious top-n count(*) case, backed by phmap's
// SwissTable with the count stored INLINE as the value (not an AggDataPtr). It is the count(*)
// specialization of the generic aggregate path: that path stores an AggDataPtr per group -- a
// pointer to a generic state blob, uniform across all aggregate types -- so it needs two passes
// (the probe fills a pointer array, then update_batch increments the counter through each
// pointer). Here the value IS the int64 counter, so post-flip work collapses to one fused pass:
// find + increment, on the raw group key (no bit-compress encode). The open-addressing table
// itself is phmap's (a well-tuned SwissTable -- a hand-rolled flat array lost to it on the
// miss-heavy probe); only the fused batch probe loop here is ours.
class CacheConsciousFa {
public:
    // Key is the real group value widened to uint64 -- the same representation route_cold_rows /
    // route_batch use for CA, so FA and CA share one key space (a key lives in exactly one). Every supported
    // integral key (int8..int64, gated by cache_conscious_group_key_supported) round-trips through
    // uint64 exactly. Value is the count inline (no AggDataPtr indirection).
    // phmap's default integer hash is identity, and its internal mixing is disabled here.
    // Adjacent keys then share the high bits used to select control groups, producing long
    // probe chains even in an L2-sized FA. Use the same mixing as ordinary integer aggregation.
    using Hash = StdHashWithSeed<uint64_t, PhmapSeed1>;
    using Map = phmap::flat_hash_map<uint64_t, int64_t, Hash>;

    // Build from the FA snapshot taken at the flip: distinct real group keys with counts >= 1.
    void build(const std::vector<std::pair<uint64_t, int64_t>>& seed) {
        _map.reserve(seed.size());
        for (const auto& [key, count] : seed) {
            _map[key] = count;
        }
    }

    // Probe + count over a chunk's raw integral keys in one fused pass: hash the key, look it up,
    // and bump the inline counter on a hit; a miss sets sel[i]=1 so the caller routes the row to
    // CA. No software prefetch and no precomputed-hash buffer -- the frozen FA is sized to L2, so
    // its buckets are already cache-resident and a prefetch is wasted work, while out-of-order
    // execution hides the L2 bucket load across independent rows. `partials` is the per-row weight
    // for the 2-phase merge path (the partial count to add on a hit); null for 1-phase colocate
    // count(*) where every row weighs 1. Returns the number of FA hits (rows aggregated exactly
    // here); misses == n - hits are the rows routed to CA, so the caller derives both counters.
    template <typename KeyT>
    size_t probe_and_count(const KeyT* keys, const int64_t* partials, uint8_t* sel, size_t n) {
        const auto& hasher = _map.hash_function();
        size_t hits = 0;
        for (size_t i = 0; i < n; ++i) {
            const uint64_t key = static_cast<uint64_t>(keys[i]);
            const size_t h = hasher(key);
            // Optional bloom pre-filter: a definite miss
            // skips the SwissTable group scan entirely -- the dominant probe cost on a cold-tail
            // stream. The operator enables it at the flip by default, or after an observation window.
            if (_bloom_active && !_bloom_maybe(h)) {
                sel[i] = 1;
                continue;
            }
            auto it = _map.find(key, h);
            if (it != _map.end()) {
                if (!_unseen_pinned.empty()) _unseen_pinned.erase(key);
                it->second += (partials != nullptr) ? partials[i] : 1;
                sel[i] = 0;
                ++hits;
            } else {
                sel[i] = 1;
            }
        }
        return hits;
    }

    // (real key, count) pairs for the prune. Keys are already real group values -- no decode.
    void collect(std::vector<std::pair<uint64_t, int64_t>>* out) const {
        out->reserve(_map.size());
        for (const auto& [key, count] : _map) {
            if (!_unseen_pinned.empty() && _unseen_pinned.count(key) != 0) continue;
            out->emplace_back(key, count);
        }
    }

    size_t size() const { return _map.size(); }

    // The k-th largest count currently in FA -- the swap watermark (topKBound). INT64_MIN when FA
    // holds fewer than k keys (no bound can be claimed). O(size); called only on the swap's
    // doubling recompute cadence, not per row.
    int64_t kth_largest_count(int64_t k) const {
        if (k <= 0 || _map.size() < static_cast<size_t>(k)) {
            return INT64_MIN;
        }
        std::vector<int64_t> c;
        c.reserve(_map.size());
        for (const auto& kv : _map) {
            if (!_unseen_pinned.empty() && _unseen_pinned.count(kv.first) != 0) continue;
            c.push_back(kv.second);
        }
        if (c.size() < static_cast<size_t>(k)) return INT64_MIN;
        std::nth_element(c.begin(), c.begin() + (k - 1), c.end(), std::greater<int64_t>());
        return c[k - 1];
    }

    // Mid-run promotion (the CA->FA swap): a key discovered hot in CA is inserted with its
    // aggregated count. The key is guaranteed absent -- it routed to CA, so it was never an FA
    // member (FA members' rows hit FA, not CA) -- so this is a fresh slot, not a merge. The swap
    // only promotes into a FA that still has a free slot, so an inserted key never displaces one.
    void promote(uint64_t key, int64_t count) { _map[key] = count; }

    // Pin a FE-supplied MCV (known-hot) key into FA before the post-flip probe: insert it at count 0
    // if absent -- only the key matters, the exact probe fills the count -- and mark it un-evictable so
    // a later swap promotion cannot flush it before its rows arrive (a count-0 key is otherwise the
    // first evict_min victim). A key already present (its rows came pre-flip) keeps its real count and
    // is just pinned. Call before build_bloom so the bloom covers the seeded key.
    void seed_pinned(uint64_t key) {
        if (_map.try_emplace(key, 0).second) {
            _unseen_pinned.insert(key);
        }
        _pinned.insert(key);
    }

    // Evict the smallest-count entry and return its (key, count). The mid-run swap calls this to
    // free a slot for a promoted CA key when FA is at capacity: the evicted key is the FA member
    // least likely to be a top-k winner (its count is below the k-th-largest bound), and its exact
    // count rides back into CA as a pre-aggregated tuple, so no count is lost and FA xor CA holds.
    // O(size), called only on a promotion into a full FA -- never per row. FA must be non-empty.
    std::pair<uint64_t, int64_t> evict_min() {
        auto min_it = _map.end();
        for (auto it = _map.begin(); it != _map.end(); ++it) {
            if (!_pinned.empty() && _pinned.count(it->first) > 0) {
                continue; // never evict a pinned (FE-supplied MCV) key
            }
            if (min_it == _map.end() || it->second < min_it->second) {
                min_it = it;
            }
        }
        // Every entry pinned (degenerate -- the MCV count is << FA capacity, so unreachable in
        // practice): fall back to the global minimum so the swap can still free a slot.
        if (min_it == _map.end()) {
            min_it = _map.begin();
        }
        const std::pair<uint64_t, int64_t> out{min_it->first, min_it->second};
        _map.erase(min_it);
        return out;
    }

    // Build a small (L1-resident) bloom over the frozen FA keys: ~16 bits/key, 2 probes, power-of-two
    // sized. No false negatives -- the FA is frozen, so every member is in the bloom -- so a bloom
    // miss is a definite FA miss. The operator builds it when activating the filter.
    void build_bloom() {
        size_t nbits = 1024;
        while (nbits < _map.size() * 16) nbits <<= 1;
        _bloom_mask = nbits - 1;
        _bloom_shift = 64 - __builtin_ctzll(nbits);
        _bloom.assign(nbits >> 6, 0);
        const auto& hasher = _map.hash_function();
        for (const auto& kv : _map) {
            _bloom_set(hasher(kv.first));
        }
    }
    void set_bloom_active(bool active) { _bloom_active = active; }
    bool bloom_active() const { return _bloom_active; }

    // A mid-run swap promotion adds a key to FA after the bloom was built; it must also enter the
    // bloom, or its later rows would bloom-miss and route to CA -- splitting the promoted key's
    // count across FA and CA (an I1 violation). No-op when the bloom is inactive.
    void bloom_add(uint64_t key) {
        if (_bloom_active) _bloom_set(_map.hash_function()(key));
    }

private:
    // Two bit positions from one hash: the low bits, and the top bits of a golden-ratio multiply --
    // the latter stays well-mixed even if the table hash is near-identity on small integer keys.
    void _bloom_set(size_t h) {
        const uint64_t b1 = h & _bloom_mask;
        const uint64_t b2 = (h * 0x9E3779B97F4A7C15ull) >> _bloom_shift;
        _bloom[b1 >> 6] |= 1ull << (b1 & 63);
        _bloom[b2 >> 6] |= 1ull << (b2 & 63);
    }
    bool _bloom_maybe(size_t h) const {
        const uint64_t b1 = h & _bloom_mask;
        if (((_bloom[b1 >> 6] >> (b1 & 63)) & 1ull) == 0) return false; // early reject on the first bit
        const uint64_t b2 = (h * 0x9E3779B97F4A7C15ull) >> _bloom_shift;
        return ((_bloom[b2 >> 6] >> (b2 & 63)) & 1ull) != 0;
    }

    Map _map;
    // Histogram keys are hints; a group exists only after an input row reaches it.
    phmap::flat_hash_set<uint64_t, Hash> _unseen_pinned;
    // FE-supplied MCV (known-hot) keys, pinned into FA and exempt from swap eviction. Small (<= the
    // histogram MCV size, ~100), so the membership check in evict_min is cheap.
    phmap::flat_hash_set<uint64_t, Hash> _pinned;
    std::vector<uint64_t> _bloom;
    uint64_t _bloom_mask = 0;
    int _bloom_shift = 0;
    bool _bloom_active = false;
};

} // namespace starrocks
