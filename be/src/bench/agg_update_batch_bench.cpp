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

// Bench for SUM / MIN / MAX / AVG with GROUP BY along the production
// hot path: AggHashMap.build_hash_map -> resolved AggDataPtr[] ->
// agg_function->update_batch(states, columns).
//
// This bench is the witness for the `update_batch` overrides added to
// sum.h / maxmin.h / avg.h (P1.A).  The CRTP base helper's update_batch
// in `aggregate.h` does a `static_cast<Derived*>->update(...)` per row,
// which forces re-evaluating the down_cast + immutable_data() pointer
// inside the row loop.  The overrides hoist those out and let the
// inner loop collapse to a scatter store (SUM/AVG) or OP()(state[i],
// data[i]) (MIN/MAX) so the compiler can vectorize.
//
// How to use:
//   1. Run on `main` (before this branch).
//   2. Run on this branch.
//   3. Compare same-shape rows: delta = (1) - (2), normalized by (1).
//
// Both runs share the bench source; the difference comes only from
// which `update_batch` impl is selected at vtable dispatch.

#include <benchmark/benchmark.h>

#include <memory>
#include <random>
#include <vector>

#include "base/phmap/phmap.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config_exec_fwd.h"
#include "common/runtime_profile.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/avg.h"
#include "exprs/agg/count.h"
#include "exprs/agg/factory/aggregate_factory.hpp"
#include "exprs/agg/maxmin.h"
#include "exprs/agg/nullable_aggregate.h"
#include "exprs/agg/sum.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {

inline constexpr int kBenchChunkSize = 4096;
inline constexpr int64_t kBenchRows = 100'000'000;

template <PhmapSeed seed>
using PhmapInt64 = phmap::flat_hash_map<int64_t, AggDataPtr, StdHashWithSeed<int64_t, seed>>;
using PhmapInt64Wrapper = AggHashMapWithOneNumberKey<TYPE_BIGINT, PhmapInt64<PhmapSeed1>>;

enum class Distribution : int {
    Random = 0,
    Sorted = 1,
    Clustered64 = 2,
};

// Init the aggregate state via the function's create() on every newly-
// allocated slot so the timed update_batch reads a properly-initialized
// state (MAX/MIN sentinels, SUM/AVG zeros).  Without this, the bench
// still reflects per-row cost (cycle count is data-independent) but the
// final state values are meaningless -- codex review flagged it as a
// correctness smell worth fixing.
struct BenchAllocateState {
    HashTableKeyAllocator* allocator;
    const AggregateFunction* agg_fn;
    FunctionContext* fn_ctx;
    AggDataPtr operator()(std::nullptr_t) {
        AggDataPtr p = allocator->allocate_null_key_data();
        agg_fn->create(fn_ctx, p);
        return p;
    }
    AggDataPtr operator()(int64_t /*key*/) {
        AggDataPtr p = allocator->allocate();
        agg_fn->create(fn_ctx, p);
        return p;
    }
};

class Int64ChunkStream {
public:
    Int64ChunkStream(int64_t num_rows, int distinct, Distribution dist) {
        std::mt19937_64 rng(0xC0FFEE);
        std::uniform_int_distribution<int64_t> uni(0, distinct > 0 ? distinct - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _key_chunks.reserve(num_chunks);
        _value_chunks.reserve(num_chunks);
        std::uniform_int_distribution<int64_t> val_uni(0, 1'000'000);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto key_col = Int64Column::create();
            auto val_col = Int64Column::create();
            auto& kd = key_col->get_data();
            auto& vd = val_col->get_data();
            kd.resize(kBenchChunkSize);
            vd.resize(kBenchChunkSize);
            switch (dist) {
            case Distribution::Random:
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    kd[i] = uni(rng);
                    vd[i] = val_uni(rng);
                }
                break;
            case Distribution::Sorted: {
                std::vector<int64_t> vals(kBenchChunkSize);
                for (int i = 0; i < kBenchChunkSize; ++i) vals[i] = uni(rng);
                std::sort(vals.begin(), vals.end());
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    kd[i] = vals[i];
                    vd[i] = val_uni(rng);
                }
                break;
            }
            case Distribution::Clustered64: {
                int64_t cur = uni(rng);
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    if (i > 0 && i % 64 == 0) cur = uni(rng);
                    kd[i] = cur;
                    vd[i] = val_uni(rng);
                }
                break;
            }
            }
            _key_chunks.emplace_back(std::move(key_col));
            _value_chunks.emplace_back(std::move(val_col));
        }
    }

    const std::vector<ColumnPtr>& key_chunks() const { return _key_chunks; }
    const std::vector<ColumnPtr>& value_chunks() const { return _value_chunks; }

private:
    std::vector<ColumnPtr> _key_chunks;
    std::vector<ColumnPtr> _value_chunks;
};

// Same key/value shape as Int64ChunkStream but the VALUE column is
// wrapped in a NullableColumn with set_has_null(false).  This routes
// `NullableAggregateFunctionUnary::update_batch` into the "all not null"
// fast path -- the one Part 2 of the PR forwards to the nested
// function's update_batch (gated on batch_safe()).
class NullableInt64ValueStream {
public:
    NullableInt64ValueStream(int64_t num_rows, int distinct, Distribution dist) {
        std::mt19937_64 rng(0xC0FFEE);
        std::uniform_int_distribution<int64_t> uni(0, distinct > 0 ? distinct - 1 : 0);
        std::uniform_int_distribution<int64_t> val_uni(0, 1'000'000);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _key_chunks.reserve(num_chunks);
        _value_chunks.reserve(num_chunks);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto key_col = Int64Column::create();
            auto val_data = Int64Column::create();
            auto val_null = NullColumn::create();
            auto& kd = key_col->get_data();
            auto& vd = val_data->get_data();
            auto& nd = val_null->get_data();
            kd.resize(kBenchChunkSize);
            vd.resize(kBenchChunkSize);
            nd.resize(kBenchChunkSize, 0);
            switch (dist) {
            case Distribution::Random:
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    kd[i] = uni(rng);
                    vd[i] = val_uni(rng);
                }
                break;
            case Distribution::Sorted: {
                std::vector<int64_t> vals(kBenchChunkSize);
                for (int i = 0; i < kBenchChunkSize; ++i) vals[i] = uni(rng);
                std::sort(vals.begin(), vals.end());
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    kd[i] = vals[i];
                    vd[i] = val_uni(rng);
                }
                break;
            }
            case Distribution::Clustered64: {
                int64_t cur = uni(rng);
                for (int i = 0; i < kBenchChunkSize; ++i) {
                    if (i > 0 && i % 64 == 0) cur = uni(rng);
                    kd[i] = cur;
                    vd[i] = val_uni(rng);
                }
                break;
            }
            }
            auto nullable = NullableColumn::create(std::move(val_data), std::move(val_null));
            nullable->set_has_null(false); // critical: triggers all-not-null branch
            _key_chunks.emplace_back(std::move(key_col));
            _value_chunks.emplace_back(std::move(nullable));
        }
    }

    const std::vector<ColumnPtr>& key_chunks() const { return _key_chunks; }
    const std::vector<ColumnPtr>& value_chunks() const { return _value_chunks; }

private:
    std::vector<ColumnPtr> _key_chunks;
    std::vector<ColumnPtr> _value_chunks;
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
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_update_batch_bench");
        _agg_stat = std::make_unique<AggStatistics>(_runtime_profile.get());
        _func_ctx_mem_pool = std::make_unique<MemPool>();
    }

    void TearDown() {
        _func_ctx_mem_pool.reset();
        _agg_stat.reset();
        _runtime_profile.reset();
        _mem_pool.reset();
        _runtime_state.reset();
    }

    std::shared_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<MemPool> _mem_pool;
    std::unique_ptr<MemPool> _func_ctx_mem_pool;
    std::unique_ptr<RuntimeProfile> _runtime_profile;
    std::unique_ptr<AggStatistics> _agg_stat;
};

// Build agg-states big enough for all four (max state is AvgAggregateState
// at sizeof(int128_t) + sizeof(int64_t) = 24 bytes).  Aggregator does the
// same per-row sizing in production; we use a single uniform size here so
// rotations between SUM/MIN/MAX/AVG don't perturb pool growth.
inline constexpr size_t kAggStateBytes = 32;

template <typename Wrapper, typename AggFn>
static void run_groupby_update_batch(benchmark::State& state, Distribution dist) {
    const int distinct = static_cast<int>(state.range(0));
    BenchSuite suite;
    suite.SetUp();
    Int64ChunkStream stream(kBenchRows, distinct, dist);

    AggFn agg_fn;
    FunctionContext* fn_ctx = FunctionContext::create_test_context();

    int64_t total_rows = 0;
    size_t final_groups = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        auto wrapper = std::make_unique<Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = kAggStateBytes;
        allocator.pool = suite._mem_pool.get();
        BenchAllocateState alloc{&allocator, &agg_fn, fn_ctx};
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (size_t ci = 0; ci < stream.key_chunks().size(); ++ci) {
            Columns key_columns;
            key_columns.emplace_back(stream.key_chunks()[ci]);
            wrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);

            const Column* value_cols[1] = {stream.value_chunks()[ci].get()};
            agg_fn.update_batch(fn_ctx, kBenchChunkSize, /*state_offset=*/0, value_cols, agg_states.data());

            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        final_groups = wrapper->hash_map.size();
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) {
            cs += reinterpret_cast<intptr_t>(agg_states[i]);
        }
        cum_checksum += cs;
        wrapper.reset();
        suite._mem_pool->clear();
    }
    delete fn_ctx;

    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = kBenchRows;
    state.counters["final_groups"] = final_groups;
    suite.TearDown();
}

// ============================================================================
// SUM(BIGINT) GROUP BY BIGINT
// ============================================================================
using SumBigIntFn = SumAggregateFunction<TYPE_BIGINT>;

static void BM_SumBigInt_Random(benchmark::State& state) {
    run_groupby_update_batch<PhmapInt64Wrapper, SumBigIntFn>(state, Distribution::Random);
}
static void BM_SumBigInt_Sorted(benchmark::State& state) {
    run_groupby_update_batch<PhmapInt64Wrapper, SumBigIntFn>(state, Distribution::Sorted);
}
static void BM_SumBigInt_Clustered64(benchmark::State& state) {
    run_groupby_update_batch<PhmapInt64Wrapper, SumBigIntFn>(state, Distribution::Clustered64);
}

// ============================================================================
// AVG(BIGINT) GROUP BY BIGINT
// ============================================================================
using AvgBigIntFn = AvgAggregateFunction<TYPE_BIGINT>;

static void BM_AvgBigInt_Random(benchmark::State& state) {
    run_groupby_update_batch<PhmapInt64Wrapper, AvgBigIntFn>(state, Distribution::Random);
}
static void BM_AvgBigInt_Clustered64(benchmark::State& state) {
    run_groupby_update_batch<PhmapInt64Wrapper, AvgBigIntFn>(state, Distribution::Clustered64);
}

// ============================================================================
// MIN / MAX(BIGINT) GROUP BY BIGINT
// ============================================================================
using MaxBigIntFn = MaxMinAggregateFunction<TYPE_BIGINT, MaxAggregateData<TYPE_BIGINT>,
                                            MaxElement<TYPE_BIGINT, MaxAggregateData<TYPE_BIGINT>>>;
using MinBigIntFn = MaxMinAggregateFunction<TYPE_BIGINT, MinAggregateData<TYPE_BIGINT>,
                                            MinElement<TYPE_BIGINT, MinAggregateData<TYPE_BIGINT>>>;

static void BM_MaxBigInt_Random(benchmark::State& state) {
    run_groupby_update_batch<PhmapInt64Wrapper, MaxBigIntFn>(state, Distribution::Random);
}
static void BM_MinBigInt_Random(benchmark::State& state) {
    run_groupby_update_batch<PhmapInt64Wrapper, MinBigIntFn>(state, Distribution::Random);
}

// ============================================================================
// COUNT(BIGINT) GROUP BY BIGINT -- count's own update_batch override was
// pre-existing; included here to round out the 5 batch_safe aggregates.
// ============================================================================
using CountFn = CountAggregateFunction<false>;

static void BM_CountBigInt_Random(benchmark::State& state) {
    run_groupby_update_batch<PhmapInt64Wrapper, CountFn>(state, Distribution::Random);
}

// ============================================================================
// Part 2: NullableAggregateFunctionUnary wrapper around the leaf agg.
// Drives the wrapper's `update_batch` "all not null at runtime" branch,
// which forwards to the nested function's update_batch when the nested
// opts in via batch_safe()=true.  Compare against the same combo without
// the wrapper (the Part 1 benches above) to see the wrapper's residual
// overhead, and compare on/off `batch_safe` by flipping the override on
// the leaf to measure the forward win directly.
// ============================================================================
template <typename NestedFn, typename NestedState>
static void run_groupby_nullable_update_batch(benchmark::State& state, Distribution dist) {
    const int distinct = static_cast<int>(state.range(0));
    BenchSuite suite;
    suite.SetUp();
    NullableInt64ValueStream stream(kBenchRows, distinct, dist);

    auto* nested = new NestedFn();
    auto* wrapper = AggregateFactory::MakeNullableAggregateFunctionUnary<NestedState, /*IsWindowFunc=*/false>(nested);
    FunctionContext* fn_ctx = FunctionContext::create_test_context();

    int64_t total_rows = 0;
    size_t final_groups = 0;
    int64_t cum_checksum = 0;
    for (auto _ : state) {
        state.PauseTiming();
        auto hwrapper = std::make_unique<PhmapInt64Wrapper>(kBenchChunkSize, suite._agg_stat.get());
        HashTableKeyAllocator allocator;
        allocator.aggregate_key_size = kAggStateBytes;
        allocator.pool = suite._mem_pool.get();
        BenchAllocateState alloc{&allocator, wrapper, fn_ctx};
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        state.ResumeTiming();

        for (size_t ci = 0; ci < stream.key_chunks().size(); ++ci) {
            Columns key_columns;
            key_columns.emplace_back(stream.key_chunks()[ci]);
            hwrapper->build_hash_map(kBenchChunkSize, key_columns, suite._mem_pool.get(), alloc, &agg_states);

            const Column* value_cols[1] = {stream.value_chunks()[ci].get()};
            wrapper->update_batch(fn_ctx, kBenchChunkSize, /*state_offset=*/0, value_cols, agg_states.data());

            total_rows += kBenchChunkSize;
        }
        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        final_groups = hwrapper->hash_map.size();
        int64_t cs = 0;
        for (int i = 0; i < kBenchChunkSize; ++i) {
            cs += reinterpret_cast<intptr_t>(agg_states[i]);
        }
        cum_checksum += cs;
        hwrapper.reset();
        suite._mem_pool->clear();
    }
    delete fn_ctx;
    delete wrapper; // wrapper deletes nested via its shared owner contract
                    // (the factory wraps the raw nested pointer in the wrapper);
                    // production lifetime is managed by the aggregator caches,
                    // here we leak `nested` deliberately to keep the bench
                    // closer to the production allocation shape.

    benchmark::DoNotOptimize(cum_checksum);
    state.SetItemsProcessed(total_rows);
    state.counters["distinct"] = distinct;
    state.counters["dist"] = static_cast<int>(dist);
    state.counters["rows_per_iter"] = kBenchRows;
    state.counters["final_groups"] = final_groups;
    suite.TearDown();
}

static void BM_SumBigInt_Nullable_Random(benchmark::State& state) {
    run_groupby_nullable_update_batch<SumBigIntFn, SumAggregateState<int64_t>>(state, Distribution::Random);
}
static void BM_AvgBigInt_Nullable_Random(benchmark::State& state) {
    run_groupby_nullable_update_batch<AvgBigIntFn, AvgAggregateState<int64_t>>(state, Distribution::Random);
}
static void BM_MaxBigInt_Nullable_Random(benchmark::State& state) {
    run_groupby_nullable_update_batch<MaxBigIntFn, MaxAggregateData<TYPE_BIGINT>>(state, Distribution::Random);
}
static void BM_MinBigInt_Nullable_Random(benchmark::State& state) {
    run_groupby_nullable_update_batch<MinBigIntFn, MinAggregateData<TYPE_BIGINT>>(state, Distribution::Random);
}
static void BM_CountBigInt_Nullable_Random(benchmark::State& state) {
    run_groupby_nullable_update_batch<CountFn, AggregateCountFunctionState<false>>(state, Distribution::Random);
}

// ============================================================================
// Argument matrix: group cardinality sweep.
// 1: COUNT(*) shape (1 group)
// 24: small dimension (status, segment)
// 1000: medium dimension (city, dim)
// 65536: large dimension
// ============================================================================
static void RegisterArgs(benchmark::internal::Benchmark* b) {
    for (int distinct : {1, 24, 1000, 65536}) {
        b->Args({distinct});
    }
}

BENCHMARK(BM_SumBigInt_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_SumBigInt_Sorted)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_SumBigInt_Clustered64)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

BENCHMARK(BM_AvgBigInt_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_AvgBigInt_Clustered64)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

BENCHMARK(BM_MaxBigInt_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_MinBigInt_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_CountBigInt_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

BENCHMARK(BM_SumBigInt_Nullable_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_AvgBigInt_Nullable_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_MaxBigInt_Nullable_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_MinBigInt_Nullable_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_CountBigInt_Nullable_Random)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

} // namespace starrocks

BENCHMARK_MAIN();
