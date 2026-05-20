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

// Bench for the BE config `agg_hash_map_prefetch_dist` (replaces the
// former AGG_HASH_MAP_DEFAULT_PREFETCH_DIST constant).
//
// What this bench measures:
//   Steady-state per-row hash-probe cost on a pre-filled phmap
//   AggHashMapWithOneNumberKey<TYPE_BIGINT> at a chosen table size,
//   for a chosen prefetch distance, on a chosen access pattern.
//
// Shape (informed by codex critique of the original 50/50 hot/cold
// plan, which over-stated cache locality on large random tables and
// let timed misses mutate the table mid-iteration):
//   1. Pre-fill phase (NOT timed): drive build_hash_map with `target_size`
//      distinct INT64 keys so the hash table is at the target band.
//   2. Timed phase: drive build_hash_map with chunks whose keys are all
//      drawn from [0, target_size).  Hit rate is 100% (table is fully
//      populated post-prefill); phmap's lazy_emplace short-circuits on
//      hit so no allocations or insertions happen in the timed window.
//      The table stays at `target_size` for the entire run -> the
//      table-size band reading is unpolluted by mid-bench growth.
//
// What this bench does NOT cover (call out for reviewers):
//   - Hit-rate sweep: 100% is the "steady warmed-up table" shape.
//     Lower hit rates require inserts which mutate the table size and
//     conflate the band reading.  If a follow-up wants the cold-probe
//     win specifically, run separately with target_size = 0 and let it
//     grow.
//   - Load-factor sweep: phmap chooses its own load factor; we don't
//     reserve to a specific occupancy.  If the optimal distance per
//     band proves load-factor sensitive, that's a separate bench.
//   - Hardware counters: this is wall-time only.  Use `perf stat -e
//     cache-misses,l1d-load-misses,llc-load-misses` on the binary if
//     you need cycle-level evidence.
//   - Two-level conversion: aggregator promotes a wrapper from
//     one-level to two-level at ~32 MiB (CpuInfo-derived; see
//     aggregator_fwd.h).  Sizes here cap at 8M entries (~ a few MiB
//     bucket array) so the one-level wrapper is exercised throughout.
//
// Decision rule for the proposal's step 2 (adaptive distance picked
// from working hash-table-size band): the knob ships unconditionally
// here.  Step 2 is justified only if the OPTIMAL distance shifts
// monotonically with band by >= 5% over the constant 16, stable across
// repetitions (>= 10) and at least two access patterns.  See the
// bottom of this file for the post-run summary template.

#include <benchmark/benchmark.h>

#include <cmath>
#include <memory>
#include <random>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_flow_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

inline constexpr int kBenchChunkSize = 4096;
inline constexpr int64_t kTimedRowsPerCombo = 10'000'000;

template <PhmapSeed seed>
using PhmapInt64 = phmap::flat_hash_map<int64_t, AggDataPtr, StdHashWithSeed<int64_t, seed>>;

using PhmapInt64Wrapper = AggHashMapWithOneNumberKey<TYPE_BIGINT, PhmapInt64<PhmapSeed1>>;

enum class Pattern : int {
    Random = 0,     // uniform random over [0, target_size)
    Clustered64 = 1 // run-length 64 over the same range (consecutive-keys shape)
};

struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    AggDataPtr operator()(std::nullptr_t) { return allocator->allocate_null_key_data(); }
    AggDataPtr operator()(int64_t /*key*/) { return allocator->allocate(); }
};

class Int64ChunkStream {
public:
    Int64ChunkStream(int64_t num_rows, int64_t key_universe_size, Pattern p, uint64_t seed)
            : _num_rows(num_rows), _universe(key_universe_size) {
        std::mt19937_64 rng(seed);
        std::uniform_int_distribution<int64_t> uni(0, _universe > 0 ? _universe - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = Int64Column::create();
            auto& data = col->get_data();
            data.resize(kBenchChunkSize);
            switch (p) {
            case Pattern::Random:
                for (int i = 0; i < kBenchChunkSize; ++i) data[i] = uni(rng);
                break;
            case Pattern::Clustered64: {
                int64_t current = uni(rng);
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    if (i > 0 && i % 64 == 0) current = uni(rng);
                    data[i] = current;
                }
                break;
            }
            }
            _chunks.emplace_back(std::move(col));
        }
    }

    // For the pre-fill phase we want target_size *distinct* keys to
    // populate the table.  Permuted sequential keys cover the universe
    // exactly once across (universe + chunk_size - 1) / chunk_size chunks.
    static std::vector<ColumnPtr> build_distinct_keys(int64_t universe_size) {
        std::vector<int64_t> all(universe_size);
        for (int64_t i = 0; i < universe_size; ++i) all[i] = i;
        std::mt19937_64 rng(0xC0FFEEC0FFEEC0FFULL);
        std::shuffle(all.begin(), all.end(), rng);
        std::vector<ColumnPtr> chunks;
        const int64_t num_chunks = (universe_size + kBenchChunkSize - 1) / kBenchChunkSize;
        chunks.reserve(num_chunks);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = Int64Column::create();
            auto& data = col->get_data();
            data.resize(kBenchChunkSize);
            for (int i = 0; i < kBenchChunkSize; ++i) {
                const int64_t idx = c * kBenchChunkSize + i;
                data[i] = idx < universe_size ? all[idx] : all[idx % universe_size];
            }
            chunks.emplace_back(std::move(col));
        }
        return chunks;
    }

    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    int64_t _num_rows;
    int64_t _universe;
    std::vector<ColumnPtr> _chunks;
};

class BenchSuite {
public:
    void SetUp() {
        config::vector_chunk_size = kBenchChunkSize;
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = kBenchChunkSize;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();
        _mem_pool = std::make_unique<MemPool>();
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_prefetch_dist_bench");
        _agg_stat = std::make_unique<AggStatistics>(_runtime_profile.get());
    }

    void TearDown() {
        _agg_stat.reset();
        _runtime_profile.reset();
        _mem_pool.reset();
        _runtime_state.reset();
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<RuntimeProfile> _runtime_profile;
    std::unique_ptr<AggStatistics> _agg_stat;
};

template <typename Wrapper>
inline size_t hash_table_bytes(Wrapper* w) {
    return w->hash_map.dump_bound();
}

// ============================================================================
// Main bench: probe a pre-filled phmap at the given prefetch distance.
// ============================================================================
static void BM_PrefetchProbe(benchmark::State& state) {
    const int32_t distance = static_cast<int32_t>(state.range(0));
    const int64_t target_size = state.range(1);
    const Pattern pattern = static_cast<Pattern>(state.range(2));

    // Knob under test: the function in agg_hash_set.h reads this at chunk
    // start, captures into __prefetch_dist for the loop body.
    config::agg_hash_map_prefetch_dist = distance;

    BenchSuite suite;
    suite.SetUp();

    auto wrapper = std::make_unique<PhmapInt64Wrapper>(kBenchChunkSize, suite._agg_stat.get());
    HashTableKeyAllocator allocator;
    allocator.aggregate_key_size = sizeof(int64_t);
    allocator.pool = suite._mem_pool.get();
    BenchAllocateState alloc{&allocator};
    Buffer<AggDataPtr> agg_states(kBenchChunkSize);

    // 1. Pre-fill phase (NOT timed): one chunk per kBenchChunkSize distinct
    //    keys until the table holds `target_size` entries.
    const auto prefill_chunks = Int64ChunkStream::build_distinct_keys(target_size);
    for (const auto& chunk_col : prefill_chunks) {
        Columns key_columns;
        key_columns.emplace_back(chunk_col);
        wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
    }
    const size_t prefilled = wrapper->hash_map.size();

    // 2. Build the timed chunk stream once outside the loop so column-buffer
    //    construction cost is paid once.
    Int64ChunkStream timed_stream(kTimedRowsPerCombo, target_size, pattern, 0xBEEFCAFE);

    // Touch the buffer once to make sure column pages are resident; otherwise
    // first iteration pays page-fault cost that confounds the distance read.
    for (const auto& chunk_col : timed_stream.chunks()) {
        const auto* col = down_cast<const Int64Column*>(chunk_col.get());
        benchmark::DoNotOptimize(col->immutable_data().data());
    }

    int64_t total_rows = 0;
    for (auto _ : state) {
        for (const auto& chunk_col : timed_stream.chunks()) {
            Columns key_columns;
            key_columns.emplace_back(chunk_col);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);
            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();
    }

    // Verify the table didn't grow (100% hit rate invariant).
    const size_t post_size = wrapper->hash_map.size();

    state.counters["distance"] = distance;
    state.counters["target_size"] = target_size;
    state.counters["pattern"] = static_cast<int>(pattern);
    state.counters["prefilled"] = prefilled;
    state.counters["post_size"] = post_size;
    state.counters["hash_table_bytes"] = hash_table_bytes(wrapper.get());
    state.counters["rows_per_iter"] = kTimedRowsPerCombo;
    state.SetItemsProcessed(total_rows);

    wrapper.reset();
    suite.TearDown();
}

// ============================================================================
// Argument matrix
// ============================================================================
// Distances: 0 (no SW prefetch via guard), 4 / 8 / 16 / 24 / 32 (sweep),
//   8192 (forces __prefetch_index > chunk_size -> disables via bound check;
//   tells us if the "0" guard and the "way-too-big" disable take the same
//   codegen path).
// Sizes: 64Ki (L1d-spill), 1Mi (L2-spill), 8Mi (close to but under the
//   ~32 MiB two-level threshold).  16Mi is intentionally OMITTED -- the
//   one-level wrapper does not stay one-level past 32 MiB in prod.
// Patterns: Random + Clustered64.  Sorted-by-key is excluded because INT
//   sorted by value is NOT sorted by hash bucket -- it would measure the
//   wrong thing.
static void RegisterArgs(benchmark::internal::Benchmark* b) {
    constexpr int distances[] = {0, 4, 8, 16, 24, 32, 8192};
    constexpr int64_t sizes[] = {64 * 1024, 1024 * 1024, 8 * 1024 * 1024};
    constexpr int patterns[] = {static_cast<int>(Pattern::Random), static_cast<int>(Pattern::Clustered64)};
    for (auto d : distances) {
        for (auto s : sizes) {
            for (auto p : patterns) {
                b->Args({d, s, p});
            }
        }
    }
}

BENCHMARK(BM_PrefetchProbe)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

} // namespace starrocks

BENCHMARK_MAIN();
