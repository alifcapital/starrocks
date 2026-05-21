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

// Bench for the two-level conversion threshold change (P2.F).
//
// `aggregator_fwd.h::two_level_memory_threshold()` used to be a hard 32
// MiB constant; the patch sizes it from `CpuInfo::get_cache_sizes()`
// (L3) with a 512 MiB cap.  The hypothesis is that a host with a larger
// L3 (e.g. 54 MiB on c5.2xlarge / 1 NUMA node) keeps the flat
// SerializedKeyAggHashMap resident longer and pays less aggregate
// pointer-chase cost than the 32 MiB cutoff.
//
// This bench drives a string GROUP BY through `AggHashMapVariant`'s
// `phase1_string -> phase1_string_two_level` path and times the build
// across the full data set for a sweep of threshold values.  Compare
// the wall-clock time at the host's L3 against the 32 MiB threshold.

#include <benchmark/benchmark.h>

#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/system/cpu_info.h"
#include "exec/aggregate/agg_hash_map.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator_fwd.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"

namespace starrocks {

inline constexpr int kBenchChunkSize = 4096;
// Total rows per bench iteration.  Cardinality is controlled by the
// `distinct` arg; this bound caps the wall-clock work above the
// largest configured distinct so all threshold variants see the same
// post-conversion residual.
inline constexpr int64_t kBenchRows = 8'000'000;
// kAggStateBytes: per-group state size MemPool reserves.  Production
// sizes this from the aggregate function; we pick a uniform 16 bytes
// (matches simple sum/avg state) so MemPool growth is reproducible.
inline constexpr size_t kAggStateBytes = 16;

class StringChunkStream {
public:
    // Distinct controls cardinality; rows fill kBenchRows by cycling
    // through generated unique strings.  Strings are short ("k_<id>")
    // so the slice column overhead is small enough that the HT itself
    // dominates memory growth past 32 MiB.
    StringChunkStream(int64_t num_rows, int distinct) {
        std::vector<std::string> keys(distinct);
        for (int i = 0; i < distinct; ++i) {
            keys[i] = "k_" + std::to_string(i);
        }
        std::mt19937_64 rng(0xC0FFEE);
        std::uniform_int_distribution<int> uni(0, distinct > 0 ? distinct - 1 : 0);
        const int64_t num_chunks = (num_rows + kBenchChunkSize - 1) / kBenchChunkSize;
        _chunks.reserve(num_chunks);
        _storage.reserve(num_chunks);
        for (int64_t c = 0; c < num_chunks; ++c) {
            auto col = BinaryColumn::create();
            // Reserve owning string storage so the Slice pointers stay
            // valid until TearDown.
            auto storage = std::make_unique<std::vector<std::string>>();
            storage->reserve(kBenchChunkSize);
            for (int i = 0; i < kBenchChunkSize; ++i) {
                storage->emplace_back(keys[uni(rng)]);
            }
            for (auto& s : *storage) {
                col->append(Slice{s.data(), s.size()});
            }
            _storage.emplace_back(std::move(storage));
            _chunks.emplace_back(std::move(col));
        }
    }

    const std::vector<ColumnPtr>& chunks() const { return _chunks; }

private:
    std::vector<ColumnPtr> _chunks;
    std::vector<std::unique_ptr<std::vector<std::string>>> _storage;
};

class BenchSuite {
public:
    void SetUp() {
        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = kBenchChunkSize;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();
        _mem_pool = std::make_unique<MemPool>();
        _runtime_profile = std::make_unique<RuntimeProfile>("agg_two_level_threshold_bench");
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

// AggHashMapVariant::visit instantiates the lambda body for every
// wrapper in the variant -- int64 / decimal / date / Slice / ... --
// and each wrapper's CRTP build_hash_map enforces an
// `AllocFunc<Impl>` concept on the allocator's call operator. Use a
// templated `operator()` so the concept is satisfied for every
// KeyType; this bench only ever invokes the path under the Slice
// wrappers (phase1_string -> phase1_string_two_level) but the
// substitution must succeed for the others or the visit lambda
// itself will not compile.
struct BenchAllocate {
    MemPool* pool;
    template <typename K>
    AggDataPtr operator()(const K&) {
        return pool->allocate(kAggStateBytes);
    }
    AggDataPtr operator()(std::nullptr_t) { return pool->allocate(kAggStateBytes); }
};

// Drives a single (threshold, distinct) sample.  Each timed iteration
// builds the full chunk stream through phase1_string, fires
// convert_to_two_level once when the variant's reserved_memory_usage
// crosses the threshold, then continues with the two-level wrapper.
static void BM_TwoLevelThreshold(benchmark::State& state) {
    const size_t threshold = static_cast<size_t>(state.range(0));
    const int distinct = static_cast<int>(state.range(1));
    BenchSuite suite;
    suite.SetUp();
    StringChunkStream stream(kBenchRows, distinct);

    int64_t total_rows = 0;
    size_t last_convert_bytes = 0;
    size_t final_size = 0;
    bool converted_at_least_once = false;

    for (auto _ : state) {
        state.PauseTiming();
        suite._mem_pool->clear();
        AggHashMapVariant variant;
        variant.init(suite._runtime_state.get(), AggHashMapVariant::Type::phase1_string, suite._agg_stat.get());
        Buffer<AggDataPtr> agg_states(kBenchChunkSize);
        bool converted = false;
        state.ResumeTiming();

        for (auto& chunk : stream.chunks()) {
            Columns cols;
            cols.emplace_back(chunk);
            variant.visit([&](auto& wrapper) {
                // Only the Slice-keyed flat / two-level wrappers see real
                // BinaryColumn input here; the other variants would fail
                // their down_cast at runtime. The visit body still needs
                // to compile for every variant in the std::variant.
                using W = std::decay_t<decltype(*wrapper)>;
                using KT = typename W::HashMapType::key_type;
                if constexpr (std::is_same_v<KT, Slice>) {
                    // Pass a fresh temporary so Func deduces as
                    // BenchAllocate (rvalue), not BenchAllocate&;
                    // AggHashMapWithSerializedKey internally
                    // `std::move`s the allocator into helper
                    // routines and that fails to bind a
                    // non-const lvalue reference.
                    wrapper->build_hash_map(kBenchChunkSize, cols, suite._mem_pool.get(),
                                            BenchAllocate{suite._mem_pool.get()}, &agg_states);
                }
            });

            if (!converted) {
                size_t used = variant.reserved_memory_usage(suite._mem_pool.get());
                if (used > threshold) {
                    last_convert_bytes = used;
                    variant.convert_to_two_level(suite._runtime_state.get());
                    converted = true;
                    converted_at_least_once = true;
                }
            }
            total_rows += kBenchChunkSize;
        }

        benchmark::DoNotOptimize(agg_states.data());
        benchmark::ClobberMemory();

        state.PauseTiming();
        final_size = variant.size();
        state.ResumeTiming();
    }

    state.SetItemsProcessed(total_rows);
    state.counters["threshold_mb"] = threshold / (1024.0 * 1024.0);
    state.counters["distinct"] = distinct;
    state.counters["converted"] = converted_at_least_once ? 1 : 0;
    state.counters["convert_at_mb"] = last_convert_bytes / (1024.0 * 1024.0);
    state.counters["final_groups"] = final_size;
    state.counters["rows_per_iter"] = kBenchRows;
    suite.TearDown();
}

// Threshold sweep includes the historical 32 MiB constant, common L3
// sizes (16 / 54 / 128 MiB), and a sentinel large enough that the
// conversion never fires (acts as a baseline for the cost of growing
// the flat phmap past L3).  Distinct sweep spans small (fits L3),
// boundary (crosses L3), and large (forces conversion regardless of
// threshold for the small thresholds).
static void RegisterArgs(benchmark::internal::Benchmark* b) {
    constexpr int64_t MB = 1024 * 1024;
    constexpr int64_t kNeverConvert = 4LL * 1024 * MB;
    std::vector<int64_t> thresholds_mb = {16, 32, 54, 128};
    std::vector<int> distincts = {500'000, 2'000'000, 5'000'000};
    for (auto t_mb : thresholds_mb) {
        for (auto d : distincts) {
            b->Args({t_mb * MB, d});
        }
    }
    // Baseline: never convert. Same distinct sweep.
    for (auto d : distincts) {
        b->Args({kNeverConvert, d});
    }
}

BENCHMARK(BM_TwoLevelThreshold)->Apply(RegisterArgs)->Unit(benchmark::kMillisecond);

} // namespace starrocks

BENCHMARK_MAIN();
