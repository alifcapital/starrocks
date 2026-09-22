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

#include <any>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "common/statusor.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/aggregator_fwd.h"
#include "exec/cache_conscious_fa.h"
#include "exec/cache_conscious_topn.h"
#include "exec/limited_pipeline_chunk_buffer.h"
#include "exec/pipeline/context_with_dependency.h"
#include "exec/pipeline/schedule/observer.h"
#include "exec/pipeline/spill_process_channel.h"
#include "exprs/agg/aggregate.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/memory/counting_allocator.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/phmap/phmap.h"

namespace starrocks {
class RuntimeFilter;
class AggTopNRuntimeFilterBuilder;
class AggInRuntimeFilterMerger;
struct HashTableKeyAllocator;
class VectorizedLiteral;

struct RawHashTableIterator {
    RawHashTableIterator(HashTableKeyAllocator* alloc_, size_t x_, int y_) : alloc(alloc_), x(x_), y(y_) {}
    bool operator==(const RawHashTableIterator& other) { return x == other.x && y == other.y; }
    bool operator!=(const RawHashTableIterator& other) { return !this->operator==(other); }
    inline void next();
    // return alloc[x]->states[y]
    inline uint8_t* value();
    HashTableKeyAllocator* alloc;
    size_t x;
    int y;
};

struct HashTableKeyAllocator {
    // number of states allocated consecutively in a single alloc
    static auto constexpr alloc_batch_size = 1024;
    // memory aligned when allocate
    static size_t constexpr aligned = 16;

    int aggregate_key_size = 0;
    std::vector<std::pair<void*, int>> vecs;
    MemPool* pool = nullptr;

    RawHashTableIterator begin() { return {this, 0, 0}; }

    RawHashTableIterator end() { return {this, vecs.size(), 0}; }

    AggDataPtr allocate() {
        if (vecs.empty() || vecs.back().second == alloc_batch_size) {
            uint8_t* mem = pool->allocate_aligned(alloc_batch_size * aggregate_key_size, aligned);
            if (mem == nullptr) {
                throw std::bad_alloc();
            }
            vecs.emplace_back(mem, 0);
        }
        return static_cast<AggDataPtr>(vecs.back().first) + aggregate_key_size * vecs.back().second++;
    }

    AggDataPtr allocate_null_key_data() { return pool->allocate_aligned(aggregate_key_size, aligned); }

    void reset() { vecs.clear(); }

    void rollback() {
        DCHECK(!vecs.empty());
        DCHECK_GT(vecs.back().second, 0);
        vecs.back().second--;
        if (vecs.back().second == 0) {
            vecs.pop_back();
        }
    }
};

inline void RawHashTableIterator::next() {
    y++;
    if (y == alloc->vecs[x].second) {
        y = 0;
        x++;
    }
}

inline uint8_t* RawHashTableIterator::value() {
    return static_cast<uint8_t*>(alloc->vecs[x].first) + alloc->aggregate_key_size * y;
}

struct AggFunctionTypes {
    TypeDescriptor result_type;
    TypeDescriptor serde_type; // for serialize
    std::vector<FunctionContext::TypeDesc> arg_typedescs;
    bool has_nullable_child;
    bool is_nullable; // whether result of agg function is nullable
    // hold order-by info
    std::vector<bool> is_asc_order;
    std::vector<bool> nulls_first;

    bool is_distinct = false;
    bool is_always_nullable_result = false;
    bool serialize_always_nullable = false;

    template <bool UseIntermediateAsOutput>
    bool is_result_nullable() const {
        if constexpr (UseIntermediateAsOutput) {
            // If using intermediate results as output, no output will be generated and only the input will be serialized.
            // Therefore, only judge whether the input is nullable to decide whether to serialize null data.
            return has_nullable_child || serialize_always_nullable;
        } else {
            // `is_nullable` means whether the output MAY be nullable. It will be false only when the output is always non-nullable.
            // Therefore, we need to decide whether the output is really nullable case by case:
            // 1. Same as input: `has_nullable_child` = `has_nullable_child && is_nullable(true)`.
            // 2. Always non-nullable: `false` = `has_nullable_child && is_nullable(false)`, eg. count, count distinct, and bitmap_union_int.
            // 3. Always nullable: `is_always_nullable_result`.
            return (has_nullable_child && is_nullable) || is_always_nullable_result;
        }
    }
    bool use_nullable_fn(bool use_intermediate_as_output) const;
};

struct ColumnType {
    TypeDescriptor result_type;
    bool is_nullable;
};

enum AggrMode {
    AM_DEFAULT, // normal mode(cache feature turn off)
    // A blocking operator is split into a pair {blocking operator(before cache), blocking operator(after cache)]
    // process non-passthrough chunks: (pre-cache: input-->intermediate) => (post-cache: intermediate->output)
    // process passthrough chunks: (pre-cache: input-->input) => (post-cache: input--> output)
    AM_BLOCKING_PRE_CACHE,
    AM_BLOCKING_POST_CACHE,
    // A streaming operator is split into a pair {streaming operator(before cache), streaming operator(after cache)]
    // process non-passthrough chunks: (pre-cache: input-->intermediate) => (post-cache: intermediate->intermediate)
    // process passthrough chunks: (pre-cache: input-->input) = > (post-cache: input-->intermediate)
    AM_STREAMING_PRE_CACHE,
    AM_STREAMING_POST_CACHE
};

enum AggrAutoState { INIT_PREAGG = 0, ADJUST, PASS_THROUGH, FORCE_PREAGG, PREAGG, SELECTIVE_PREAGG };

struct AggrAutoContext {
    static constexpr size_t ContinuousUpperLimit = 10000;
    static constexpr int ForcePreaggLimit = 3;
    static constexpr int PreaggLimit = 100;
    static constexpr int AdjustLimit = 100;
    static constexpr double LowReduction = 0.2;
    static constexpr double HighReduction = 0.9;
    static constexpr size_t MaxHtSize = 64 * 1024 * 1024; // 64 MB
    static constexpr int StableLimit = 5;
    std::string get_auto_state_string(const AggrAutoState& state);
    size_t get_continuous_limit();
    void update_continuous_limit();
    bool is_high_reduction(const size_t agg_count, const size_t chunk_size);
    bool is_low_reduction(const size_t agg_count, const size_t chunk_size);
    size_t init_preagg_count = 0;
    size_t adjust_count = 0;
    size_t pass_through_count = 0;
    size_t force_preagg_count = 0;
    size_t preagg_count = 0;
    size_t selective_preagg_count = 0;
    size_t continuous_limit = 100;
};

struct StreamingHtMinReductionEntry {
    int min_ht_mem;
    double streaming_ht_min_reduction;
};

static const StreamingHtMinReductionEntry STREAMING_HT_MIN_REDUCTION[] = {
        {0, 0.0},
        {256 * 1024, 1.1},
        {2 * 1024 * 1024, 2.0},
};

static const int STREAMING_HT_MIN_REDUCTION_SIZE =
        sizeof(STREAMING_HT_MIN_REDUCTION) / sizeof(STREAMING_HT_MIN_REDUCTION[0]);

struct LimitedMemAggState {
    size_t limited_memory_size{};
    bool has_limited(const Aggregator& aggregator) const;

    // Compute the LIMITED_MEM budget as min(memory_usage, cap), floored at 1. Both streaming
    // pre-agg sinks cap by config::streaming_agg_limited_memory_size. The floor is essential:
    // set_execute_mode() latches this budget exactly once (enter_low_memory_mode() is a
    // one-shot latch) and has_limited() tests `limited_memory_size > 0 && ...`, so a 0 budget
    // would make has_limited() permanently false, silently degrading LIMITED_MEM back to AUTO
    // with no memory cap and reintroducing the streaming pre-aggregation OOM.
    static size_t clamp_budget(int64_t memory_usage, int64_t cap);
};

using AggregatorPtr = std::shared_ptr<Aggregator>;

struct AggregatorParams {
    bool needs_finalize;
    bool has_outer_join_child;
    int64_t limit;
    bool enable_pipeline_share_limit;
    TStreamingPreaggregationMode::type streaming_preaggregation_mode;
    TupleId intermediate_tuple_id;
    TupleId output_tuple_id;
    std::string sql_grouping_keys;
    std::string sql_aggregate_functions;
    std::vector<TExpr> conjuncts;
    std::vector<TExpr> grouping_exprs;
    std::vector<TExpr> aggregate_functions;
    std::vector<TExpr> intermediate_aggr_exprs;
    std::vector<TExpr> grouping_min_max;

    // Cache-conscious top-n aggregation: when enabled, the global aggregation fuses
    // the downstream TopN and keeps only the candidate top-n groups exact. The limit
    // is the fused TopN's k (it lives on the SortNode, not on this node's limit).
    bool enable_cache_conscious_topn = false;
    int64_t cache_conscious_topn_limit = -1;
    // Test/debug only: force the flip past the limit, bypassing the L2-budget and skew gates.
    bool cache_conscious_topn_force_flip = false;
    // Most-common values of the group-by column from the histogram statistics: hot-key literals
    // (parallel to cc_mcv_counts) for seeding the FA. Empty when no histogram is available.
    std::vector<TExpr> cc_mcv_keys;
    std::vector<int64_t> cc_mcv_counts;

    // Incremental MV
    // Whether it's testing, use MemStateTable in testing, instead use IMTStateTable.
    bool is_testing;
    // Whether input is only append-only or with retract messages.
    bool is_append_only;
    // Whether output is generated with retract or without retract messages.
    bool is_generate_retract;
    // The agg index of count agg function.
    int32_t count_agg_idx;

    // aggregate function types
    // only invalid after inited
    std::vector<AggFunctionTypes> agg_fn_types;
    // group by types
    // only invalid after inited
    std::vector<ColumnType> group_by_types;

    bool has_nullable_key;

    // FE NDV estimate of the group-by key, safe to reserve from per the FE gate. -1
    // means absent/unsafe; the BE only reserves the hash table when this is > 0.
    int64_t estimated_cardinality = -1;

    void init();
};
using AggregatorParamsPtr = std::shared_ptr<AggregatorParams>;
AggregatorParamsPtr convert_to_aggregator_params(const TPlanNode& tnode);

// it contains common data struct and algorithm of aggregation
class Aggregator : public pipeline::ContextWithDependency {
public:
    Aggregator(AggregatorParamsPtr params);

    ~Aggregator() noexcept override {
        if (_state != nullptr) {
            close(_state);
        }
    }

    virtual Status open(RuntimeState* state);
    Status prepare(RuntimeState* state, RuntimeProfile* runtime_profile);
    void close(RuntimeState* state) override;

    // Local degree of parallelism of the agg pipeline, set idempotently from the
    // operator factory's create(dop, seq). Used as the reserve divisor: on a single
    // BE it is the exact number of drivers sharing the keyspace after local-shuffle.
    void set_degree_of_parallelism(int32_t dop) {
        if (_degree_of_parallelism <= 0) {
            _degree_of_parallelism = dop;
        } else {
            DCHECK_EQ(_degree_of_parallelism, dop);
        }
    }

    // Reserve the hash map's initial capacity once from the FE NDV estimate, capped by
    // config::agg_hashtable_reserve_max_bytes. Triggered from the hash-blocking sink
    // after open(); a no-op for streaming, distinct (set), or unsafe variants.
    // Returns a MemoryLimitExceeded status if the (best-effort) reserve hit the
    // mem limit; the caller treats that as non-fatal and falls back to growth.
    Status reserve_hash_table_from_estimate();

    // Finish the reserve the cache-conscious L2 cap deferred, once the flip verdict
    // lands "not skewed": the map keeps growing organically from here, so jump it to
    // the full estimate in one rehash instead of a doubling chain. No-op when the
    // initial reserve was not capped.
    Status complete_cache_conscious_deferred_reserve();

    const MemPool* mem_pool() const { return _mem_pool.get(); }
    bool is_none_group_by_exprs() { return _group_by_expr_ctxs.empty(); }
    bool only_group_by_exprs() { return _is_only_group_by_columns; }
    const std::vector<ExprContext*>& conjunct_ctxs() { return _conjunct_ctxs; }
    const std::vector<ExprContext*>& group_by_expr_ctxs() { return _group_by_expr_ctxs; }
    const std::vector<FunctionContext*>& agg_fn_ctxs() { return _agg_fn_ctxs; }
    const std::vector<std::vector<ExprContext*>>& agg_expr_ctxs() { return _agg_expr_ctxs; }
    int64_t limit() { return _limit; }
    bool needs_finalize() { return _needs_finalize; }
    // Cache-conscious top-n: the gated extension point for the blocking/spillable agg
    // operators. When enabled, the global aggregation keeps only the candidate top-n
    // groups exact and prunes the tail; the limit is the fused TopN's k.
    bool enable_cache_conscious_topn() const { return _params->enable_cache_conscious_topn; }
    int64_t cache_conscious_topn_limit() const { return _params->cache_conscious_topn_limit; }
    bool cache_conscious_topn_force_flip() const { return _params->cache_conscious_topn_force_flip; }
    // After the flip the live hash map is frozen as FA; post-flip miss rows are routed into CA
    // partitions (a logical count stat + physical tuples) on push. finalize prunes FA + CA into
    // the local top-n the source emits in place of the normal convert path.
    bool cache_conscious_topn_active() const { return _cache_conscious_active; }
    // Freeze FA and create the CA sized to the given FA candidate capacity.
    void activate_cache_conscious_topn(size_t fa_capacity);
    // Post-flip per-row FA work: probe the dense FA for each row, bump its inline counter on a
    // hit, and mark misses in _streaming_selection for route_cache_conscious_cold_rows. Replaces
    // the build_hash_map_with_selection + compute_batch_agg_states_with_selection pair (the phmap
    // probe + the separate count-update pass) on the cc path. Evaluates the agg input columns the
    // 2-phase route reads for the partial count (a no-op for argument-less count(*)).
    Status probe_cache_conscious_fa(Chunk* chunk, size_t chunk_size);
    // Route post-flip miss rows (streaming_selection == 1) to their CA partitions, bumping each
    // partition's logical count stat. Called per chunk on push.
    void route_cache_conscious_cold_rows(size_t chunk_size);
    // Post-flip late-hot-key promotion (CA->FA swap); dormant until a CA partition out-accumulates
    // the k-th FA winner, so a no-op on workloads whose hot set is captured before the flip.
    void maybe_swap_cache_conscious();
    // True only for a single integral group-by key that fits a uint64 exactly (so the engine
    // can use it as a group id without collisions). LARGEINT/strings are unsupported.
    bool cache_conscious_group_key_supported() const;
    // Bytes of CA physical tuples held in RAM — the operator reports this as revocable. The
    // logical stats are O(fanout) and stay (prune needs them), so they are not revocable.
    int64_t cache_conscious_revocable_bytes() const {
        return _cache_conscious_ca ? static_cast<int64_t>(_cache_conscious_ca->physical_tuples_bytes()) : 0;
    }
    bool cache_conscious_ca_spilled() const { return _cache_conscious_ca_spilled; }
    // On memory pressure: spill the CA partitions' tuples to the spiller as (key, partial) chunks
    // and free the RAM (the logical stats stay, so prune still works). Spills inline while the
    // spiller is not full, then hands the remainder to the spill channel so backpressure
    // (need_input gates on is_full / has_task) paces it instead of bursting past the mem-table
    // pool. The partition stays routable — later misses refill it and can be spilled again (cyclic).
    Status spill_cache_conscious_ca(RuntimeState* state);
    // Queue the remaining resident tail after prior drain tasks and before the final flush.
    void queue_cache_conscious_ca_tail(RuntimeState* state);
    // Called once after the sink is complete: start the multi-pass prune of FA + CA. Non-spill
    // path: opens a PruneSession the source drives one step per pull. Spill path: returns OK and
    // the source drives restore + finalize_cache_conscious_ca instead.
    Status finalize_cache_conscious_topn(RuntimeState* state);
    // Merge the next sorted spill chunk into one pending cold group and a bounded top-k heap.
    // Whole partitions whose upper bounds cannot reach the FA threshold are skipped.
    // Call only while !is_spilled_eos() and the spill reader has output available.
    Status restore_cache_conscious_chunk(RuntimeState* state);
    // At spill EOF, finish the pending group and build the result from the bounded heap.
    Status finalize_cache_conscious_ca(RuntimeState* state);
    // Source side, non-spill: while the prune session has work, run one step per call (one
    // partition resolve / re-partition). When the session finishes, builds the local top-n
    // result chunk so the next pull emits it. The driver yields between calls so a large CA
    // does not monopolize this driver thread.
    bool cache_conscious_prune_active() const { return _prune_session != nullptr; }
    Status advance_cache_conscious_prune();
    // The source drives emission: a ready result is pulled exactly once, then EOS.
    bool cache_conscious_result_ready() const { return _cache_conscious_result_ready; }
    bool cache_conscious_result_emitted() const { return _cache_conscious_result_emitted; }
    ChunkPtr pull_cache_conscious_result_chunk(size_t chunk_size) {
        const size_t remaining = _cache_conscious_result_chunk->num_rows() - _cc_result_offset;
        const size_t n = std::min(chunk_size, remaining);
        ChunkPtr chunk;
        if (_cc_result_offset == 0 && n == remaining) {
            chunk = std::move(_cache_conscious_result_chunk);
        } else {
            chunk = _cache_conscious_result_chunk->clone_empty();
            chunk->append(*_cache_conscious_result_chunk, _cc_result_offset, n);
        }
        _cc_result_offset += n;
        update_num_rows_returned(n);
        if (n == remaining) {
            _cache_conscious_result_chunk.reset();
            _cache_conscious_result_emitted = true;
            set_ht_eos();
        }
        return chunk;
    }
    bool is_ht_eos() { return _is_ht_eos; }
    void set_ht_eos() { _is_ht_eos = true; }
    bool is_sink_complete() { return _is_sink_complete.load(std::memory_order_acquire); }
    int64_t num_input_rows() { return _num_input_rows; }
    int64_t num_rows_returned() { return _num_rows_returned; }
    void update_num_rows_returned(int64_t increment) { _num_rows_returned += increment; };
    void update_num_input_rows(int64_t increment) { _num_input_rows += increment; }
    int64_t num_pass_through_rows() { return _num_pass_through_rows; }
    void set_aggr_phase(AggrPhase aggr_phase) { _aggr_phase = aggr_phase; }
    AggrPhase get_aggr_phase() { return _aggr_phase; }

    bool is_hash_set() const { return _is_only_group_by_columns; }
    const int64_t hash_map_memory_usage() const { return _hash_map_variant.reserved_memory_usage(mem_pool()); }
    const int64_t hash_set_memory_usage() const { return _hash_set_variant.reserved_memory_usage(mem_pool()); }
    const int64_t agg_state_memory_usage() const { return _agg_state_mem_usage; }
    const int64_t allocator_memory_usage() const { return _allocator->memory_usage(); }

    const int64_t memory_usage() const {
        if (is_hash_set()) {
            return hash_set_memory_usage() + agg_state_memory_usage() + allocator_memory_usage();
        } else if (!_group_by_expr_ctxs.empty()) {
            return hash_map_memory_usage() + agg_state_memory_usage() + allocator_memory_usage();
        } else {
            return 0;
        }
    }
    size_t size() const {
        if (is_hash_set()) {
            return _hash_set_variant.size();
        } else {
            return _hash_map_variant.size();
        }
    }

    TStreamingPreaggregationMode::type& streaming_preaggregation_mode() { return _streaming_preaggregation_mode; }
    TStreamingPreaggregationMode::type streaming_preaggregation_mode() const { return _streaming_preaggregation_mode; }
    const AggHashMapVariant& hash_map_variant() { return _hash_map_variant; }
    const AggHashSetVariant& hash_set_variant() { return _hash_set_variant; }
    std::any& it_hash() { return _it_hash; }
    const Filter& streaming_selection() { return _streaming_selection; }
    RuntimeProfile::Counter* agg_compute_timer() { return _agg_stat->agg_compute_timer; }
    RuntimeProfile::Counter* agg_expr_timer() { return _agg_stat->agg_function_compute_timer; }
    RuntimeProfile::Counter* streaming_timer() { return _agg_stat->streaming_timer; }
    RuntimeProfile::Counter* input_row_count() { return _agg_stat->input_row_count; }
    RuntimeProfile::Counter* rows_returned_counter() { return _agg_stat->rows_returned_counter; }
    RuntimeProfile::Counter* hash_table_size() { return _agg_stat->hash_table_size; }
    RuntimeProfile::Counter* pass_through_row_count() { return _agg_stat->pass_through_row_count; }
    RuntimeProfile::Counter* consecutive_keys_cache_hits() { return _agg_stat->consecutive_keys_cache_hits; }
    RuntimeProfile::Counter* consecutive_keys_cache_misses() { return _agg_stat->consecutive_keys_cache_misses; }

    // Snapshot hash-map runtime stats into RuntimeProfile counters. Call from
    // sink/source operators that already publish HashTableSize at chunk and
    // finalize boundaries. Hides the variant-specific getters from operators.
    void update_hash_map_profile_counters() {
        COUNTER_SET(hash_table_size(), (int64_t)_hash_map_variant.size());
        COUNTER_SET(consecutive_keys_cache_hits(), (int64_t)_hash_map_variant.consecutive_keys_cache_hits());
        COUNTER_SET(consecutive_keys_cache_misses(), (int64_t)_hash_map_variant.consecutive_keys_cache_misses());
    }

    // Same shape for the distinct/set side; called from
    // aggregate_distinct_* sink/source operators.
    void update_hash_set_profile_counters() {
        COUNTER_SET(hash_table_size(), (int64_t)_hash_set_variant.size());
        COUNTER_SET(consecutive_keys_cache_hits(), (int64_t)_hash_set_variant.consecutive_keys_cache_hits());
        COUNTER_SET(consecutive_keys_cache_misses(), (int64_t)_hash_set_variant.consecutive_keys_cache_misses());
    }

    void sink_complete() { _is_sink_complete.store(true, std::memory_order_release); }

    bool is_chunk_buffer_empty();
    ChunkPtr poll_chunk_buffer();
    void offer_chunk_to_buffer(const ChunkPtr& chunk);
    bool is_chunk_buffer_full();

    bool should_expand_preagg_hash_tables(size_t prev_row_returned, size_t input_chunk_size, int64_t ht_mem,
                                          int64_t ht_rows) const;

    // For aggregate without group by
    Status compute_single_agg_state(Chunk* chunk, size_t chunk_size);
    // For aggregate with group by
    Status compute_batch_agg_states(Chunk* chunk, size_t chunk_size);
    Status compute_batch_agg_states_with_selection(Chunk* chunk, size_t chunk_size);

    RuntimeFilter* build_in_filters(RuntimeState* state, RuntimeFilterBuildDescriptor* desc);
    RuntimeFilter* build_topn_filters(RuntimeState* state, RuntimeFilterBuildDescriptor* desc);
    AggTopNRuntimeFilterBuilder* topn_runtime_filter_builder() { return _topn_runtime_filter_builder; }

    // Convert one row agg states to chunk
    Status convert_to_chunk_no_groupby(ChunkPtr* chunk);

    void process_limit(ChunkPtr* chunk);

    Status evaluate_groupby_exprs(Chunk* chunk);
    Status evaluate_agg_fn_exprs(Chunk* chunk);
    Status evaluate_agg_fn_exprs(Chunk* chunk, bool use_intermediate);
    Status evaluate_agg_input_column(Chunk* chunk, std::vector<ExprContext*>& agg_expr_ctxs, int i);

    Status output_chunk_by_streaming(Chunk* input_chunk, ChunkPtr* chunk,
                                     bool force_use_intermediate_as_output = false);
    Status output_chunk_by_streaming(Chunk* input_chunk, ChunkPtr* chunk, size_t num_input_rows, bool use_selection,
                                     bool force_use_intermediate_as_output = false);

    // convert input chunk to spill format
    Status convert_to_spill_format(Chunk* input_chunk, ChunkPtr* chunk);

    // Elements queried in HashTable will be added to HashTable,
    // elements that cannot be queried are not processed,
    // and are mainly used in the first stage of two-stage aggregation when aggr reduction is low
    // selection[i] = 0: found in hash table
    // selection[1] = 1: not found in hash table
    Status output_chunk_by_streaming_with_selection(Chunk* input_chunk, ChunkPtr* chunk,
                                                    bool force_use_intermediate_as_output = false);

    // At first, we use single hash map, if hash map is too big,
    // we convert the single hash map to two level hash map.
    // two level hash map is better in large data set.
    void try_convert_to_two_level_map();
    void try_convert_to_two_level_set();

    Status check_has_error();

    void set_aggr_mode(AggrMode aggr_mode) { _aggr_mode = aggr_mode; }
    // reset_state is used to clear the internal state of the Aggregator, then it can process new tablet, in
    // multi-version cache, we should refill the chunks (i.e.partial-hit result) from the stale cache back to
    // the pre-cache agg, after that, the incremental rowsets are read out and merged with these partial state
    // to produce the final result that will be populated into the cache.
    // refill_chunk: partial-hit result of stale version.
    // refill_op: pre-cache agg operator, Aggregator's holder.
    // reset_sink_complete: reset sink_complete state. sometimes if operator sink has complete we don't have to reset sink state
    Status reset_state(RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks, pipeline::Operator* refill_op,
                       bool reset_sink_complete = true);

    const AggregatorParamsPtr& params() const { return _params; }

    bool is_full() { return _spiller != nullptr && _spiller->is_full(); }

    const std::shared_ptr<spill::Spiller>& spiller() const { return _spiller; }
    void set_spiller(std::shared_ptr<spill::Spiller> spiller) { _spiller = std::move(spiller); }

    const SpillProcessChannelPtr spill_channel() const { return _spill_channel; }
    void set_spill_channel(SpillProcessChannelPtr channel) { _spill_channel = std::move(channel); }

    Status spill_aggregate_data(RuntimeState* state, std::function<StatusOr<ChunkPtr>()> chunk_provider);

    bool has_pending_data() const { return _spiller != nullptr && _spiller->has_pending_data(); }
    bool has_pending_restore() const { return _spiller != nullptr && !_spiller->restore_finished(); }
    bool is_spilled_eos() const {
        return _spiller == nullptr || _spiller->spilled_append_rows() == _spiller->restore_read_rows();
    }

    void set_streaming_all_states(bool streaming_all_states) { _streaming_all_states = streaming_all_states; }

    bool is_streaming_all_states() const { return _streaming_all_states; }

    HashTableKeyAllocator& state_allocator() { return _state_allocator; }

    void attach_sink_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
        _pip_observable.attach_sink_observer(state, observer);
    }
    void attach_source_observer(RuntimeState* state, pipeline::PipelineObserver* observer) {
        _pip_observable.attach_source_observer(state, observer);
    }
    auto defer_notify_source() { return _pip_observable.defer_notify_source(); }
    auto defer_notify_sink() { return _pip_observable.defer_notify_sink(); }

protected:
    AggregatorParamsPtr _params;

    bool _is_closed = false;
    RuntimeState* _state = nullptr;

    // Expr/Object pool owned by Aggregator.
    // Used to allocate ExprContext and other helper objects whose lifetime
    // is tied to the Aggregator itself rather than a specific operator.
    std::unique_ptr<ObjectPool> _pool;
    std::unique_ptr<MemPool> _mem_pool;
    // used to count heap memory usage of agg states
    std::unique_ptr<CountingAllocatorWithHook> _allocator;

    HashTableKeyAllocator _state_allocator;
    // When true, the single qualifying aggregate runs the inline fast path: the hash-map
    // slot holds its accumulator directly (no arena state, no key dup). Set in open() for a
    // supported op over a supported fixed-size key (numeric, or a multi-column
    // serialized/compressed key).
    bool _inline_agg = false;
    // Which op policy drives the inline path (resolved in open() from the function name).
    // kCountStar: plain count -- constant +1 deltas, fused into the build on update chunks
    // (the only fused op: its delta needs no input column).
    // kCountCol: count_nullable -- the delta of a row is !null[i] of the input column, so
    // update chunks go through the deferred fold in compute (where the input column lives).
    // kSumInt / kSumDouble: plain sum with a BIGINT / DOUBLE accumulator -- the delta is the
    // input value itself (widened for narrow ints), deferred fold like kCountCol; the
    // nullable wrapper never reaches here (gate by resolved name), so there is no null mask
    // and no NULL partials. Merge chunks fold the intermediate partials with the same typed
    // += for every op.
    // kMin / kMax: plain min/max -- the slot holds the value type T itself, the delta is the
    // input value (zero-copy), identity is the type's limit; like the other per-row-delta ops
    // the update fold is deferred to compute. _inline_minmax_lt carries the concrete T.
    enum class InlineOpKind : uint8_t { kCountStar, kCountCol, kSumInt, kSumDouble, kMin, kMax };
    InlineOpKind _inline_op = InlineOpKind::kCountStar;
    LogicalType _inline_minmax_lt = TYPE_UNKNOWN;
    // Multi-aggregate pack: 2..4 additive int64 aggregates (count(*) / count(col) /
    // sum(int->BIGINT)) share one 32-byte map cell; field k of the cell is aggregate k.
    // The single-op fields above are not used while _inline_pack is set.
    bool _inline_pack = false;
    uint8_t _inline_pack_n = 0;
    InlineOpKind _inline_pack_ops[4] = {};
    // A pack is build-fused only when EVERY field is count(*): that is the only delta
    // needing no input column. Any other member defers the whole pack's fold to compute.
    bool _inline_pack_fused = false;
    bool _inline_op_is_fused() const {
        return _inline_pack ? _inline_pack_fused : _inline_op == InlineOpKind::kCountStar;
    }
    template <typename HTBuildOp>
    void _inline_pack_dispatch_build(size_t chunk_size, Filter* not_founds, size_t limit, bool fused);
    // Materialize the chunk's per-row pack deltas (AoS) into the delta scratch: on an update
    // chunk field k's delta comes from aggregate k's input column, on a merge chunk from its
    // intermediate partial column.
    StatusOr<const InlinePackDelta*> _compute_pack_deltas(Chunk* chunk, size_t chunk_size, bool is_merge);
    // Per-chunk materialized deltas for the per-row-delta ops (8B per row; doubles reuse the
    // same bytes). Lives until the end of the compute call that filled it.
    Buffer<int64_t> _inline_delta_scratch;
    const int64_t* _compute_count_col_deltas(size_t chunk_size);
    template <typename DeltaT, typename ColumnT>
    const DeltaT* _compute_sum_deltas(size_t chunk_size);
    template <typename Op, typename PartialColumnT>
    Status _inline_fold_merge_chunk(Chunk* chunk, size_t chunk_size, const Filter* selection);
    template <typename Op, typename DeltaColumnT>
    Status _inline_fold_sum_update_chunk(Chunk* chunk, size_t chunk_size, const Filter* selection);
    Status _inline_sum_int_update(Chunk* chunk, size_t chunk_size, const Filter* selection);
    Status _inline_sum_double_update(Chunk* chunk, size_t chunk_size, const Filter* selection);
    Status _inline_minmax_update(Chunk* chunk, size_t chunk_size, const Filter* selection);
    Status _inline_minmax_merge(Chunk* chunk, size_t chunk_size, const Filter* selection);
    template <typename HTBuildOp>
    void _inline_dispatch_build(size_t chunk_size, Filter* not_founds, size_t limit, bool fused_count);
    // Sorted streaming aggregation (including spill restore) still uses ordinary aggregate-function
    // states; it never stores the count in a hash-map value slot. Subclasses can turn this off before open().
    bool _allow_inline_agg = true;
    // Whether the CURRENT chunk carries merge/intermediate input (partial counts to fold with
    // slot += partial) rather than raw update rows (+1 each). Decided per chunk -- exactly like
    // the general path's update-vs-merge dispatch in compute_batch_agg_states -- because
    // _use_intermediate_as_input() can flip after open(): a PRE_CACHE aggregator starts on raw
    // rows and is refilled with intermediate chunks on a query-cache hit
    // (begin_pending_reset_state). Evaluated ONCE per chunk by the build entry and captured in
    // _inline_chunk; compute uses the captured value, never re-evaluates.
    bool _inline_agg_merge_chunk() { return _is_merge_funcs[0] || _use_intermediate_as_input(); }
    // Per-chunk protocol between the build entry and the matching compute call. Every build
    // entry overwrites the whole struct for the current chunk; compute consumes it (fold ->
    // kConsumed). The pairing is guaranteed by the operators: every compute_*(chunk) is
    // preceded by a build entry for the SAME chunk, and the only path that skips compute is
    // "stream the whole chunk" after a classify build, whose kFoldSelection state is legally
    // overwritten by the next build. Carries no pointers/iterators/views.
    struct InlineChunkState {
        enum Fold : uint8_t {
            kConsumed,      // nothing pending (initial; after compute consumed the chunk)
            kCommitted,     // build counted in place; compute is a no-op fold
            kFoldAll,       // build created/skipped; compute folds every row
            kFoldSelection, // build classified; compute folds/commits kept rows (selection==0)
        };
        Fold fold = kConsumed;
        bool is_merge = false; // captured _inline_agg_merge_chunk() of the current chunk
    };
    InlineChunkState _inline_chunk;
    Status _inline_pack_compute(Chunk* chunk, size_t chunk_size, const Filter* selection, InlineChunkState chunk_state);
    // The open phase still relies on the TFunction object for some initialization operations
    std::vector<TFunction> _fns;

    RuntimeProfile* _runtime_profile;

    int64_t _limit = -1;
    int64_t _num_rows_returned = 0;
    int64_t _num_rows_processed = 0;

    // only used in pipeline engine
    std::atomic<bool> _is_sink_complete = false;
    // only used in pipeline engine
    std::unique_ptr<LimitedPipelineChunkBuffer<AggStatistics>> _limited_buffer;

    // Certain aggregates require a finalize step, which is the final step of the
    // aggregate after consuming all input rows. The finalize step converts the aggregate
    // value into its final form. This is true if this node contains aggregate that requires
    // a finalize step.
    bool _needs_finalize;
    // Indicate whether data of the hash table has been taken out or reach limit
    bool _is_ht_eos = false;
    std::atomic_bool _streaming_all_states = false;
    bool _is_only_group_by_columns = false;
    // At least one group by column is nullable
    bool _has_nullable_key = false;
    int64_t _num_input_rows = 0;
    int64_t _num_pass_through_rows = 0;

    TStreamingPreaggregationMode::type _streaming_preaggregation_mode;

    // The key is all group by column, the value is all agg function column
    AggHashMapVariant _hash_map_variant;
    AggHashSetVariant _hash_set_variant;
    std::any _it_hash;

    // Cache-conscious top-n state (see enable_cache_conscious_topn). Once the sink flips, the
    // hash map above is frozen as FA and post-flip cold miss chunks accumulate here as CA.
    // finalize_cache_conscious_topn prunes them into the local top-n result chunk the source
    // emits in place of the normal convert path.
    bool _cache_conscious_active = false;
    bool _cache_conscious_ca_spilled = false;
    bool _cc_bloom_decided = false; // whether the post-flip bloom-activation decision has run
    int64_t _cc_postflip_rows = 0;  // rows probed since the flip (the bloom decision window)
    int64_t _cc_postflip_hits = 0;  // of those, rows that hit FA (miss rate = 1 - hits/rows)
    std::unique_ptr<CacheConsciousCa> _cache_conscious_ca;
    // The frozen FA as a phmap-backed count(*) table with the count inline (built at the flip from
    // the live map's (key, count) snapshot). Once set it is the source of truth for FA counts:
    // post-flip chunks probe + count through it, and collect_cache_conscious_topn_groups reads it
    // rather than the now-dormant hash map.
    std::unique_ptr<CacheConsciousFa> _cache_conscious_fa;
    Int64Column::Ptr _cc_count_deltas;
    const Int64Column* _cc_input_counts = nullptr;
    ChunkPtr _cache_conscious_result_chunk;
    size_t _cc_result_offset = 0;
    bool _cache_conscious_result_ready = false;
    bool _cache_conscious_result_emitted = false;
    // Pre-computed at the first restore call from the frozen FA's k-th-highest count: each pid
    // whose partition stat is already below threshold is marked so the restore loop drops its
    // rows without re-routing them back into the CA. The mask only ever grows tighter (the FA
    // threshold is monotone non-decreasing), so it is computed once.
    std::vector<uint8_t> _cache_conscious_pruned_mask;
    // Multi-step prune state: live across pull_chunk calls so the source can yield between
    // partitions instead of monopolizing the driver thread inside a single sink finalize. Set
    // up by finalize_cache_conscious_topn (non-spill) and reset once the result chunk is built.
    std::unique_ptr<CacheConsciousTopN::PruneSession> _prune_session;
    std::unique_ptr<CacheConsciousTopN::SpillMerge> _cc_spill_merge;
    // Cache-conscious top-n profile counters: registered in prepare() only when
    // enable_cache_conscious_topn, null and untouched otherwise. They expose where the rows went
    // (pre-flip build / FA hit / CA route) and how hard the end-of-input Phase-3 prune worked, so a
    // cc query's behavior is readable straight off EXPLAIN ANALYZE.
    RuntimeProfile::Counter* _cc_flipped = nullptr;                  // 1 if the flip engaged
    RuntimeProfile::Counter* _cc_preflip_rows = nullptr;             // rows aggregated into the FA seed before the flip
    RuntimeProfile::Counter* _cc_fa_keys = nullptr;                  // distinct keys frozen in FA at the flip
    RuntimeProfile::Counter* _cc_fa_hit_rows = nullptr;              // post-flip rows that hit FA (exact)
    RuntimeProfile::Counter* _cc_ca_routed_rows = nullptr;           // post-flip rows routed to CA (miss)
    RuntimeProfile::Counter* _cc_bloom_active = nullptr;             // 1 if the bloom pre-filter was activated
    RuntimeProfile::Counter* _cc_ca_partitions = nullptr;            // CA partition fanout
    RuntimeProfile::Counter* _cc_partitions_resolved = nullptr;      // Phase-3 partitions aggregated exactly
    RuntimeProfile::Counter* _cc_partitions_repartitioned = nullptr; // Phase-3 partitions split to a deeper level
    RuntimeProfile::Counter* _cc_partitions_pruned = nullptr;        // partitions dropped whole by the UB prune
    RuntimeProfile::Counter* _cc_reprocessed_tuples = nullptr;       // tuples re-touched across all Phase-3 steps
    RuntimeProfile::Counter* _cc_pruned_groups = nullptr;            // groups inside pruned partitions
    RuntimeProfile::Counter* _cc_max_radix_level = nullptr;          // deepest radix level reached
    RuntimeProfile::Counter* _cc_mcv_keys = nullptr;                 // FE-supplied histogram MCV hot keys received
    RuntimeProfile::Counter* _cc_mcv_seeded = nullptr; // MCV keys pinned into FA (not already live at flip)
    // Late hot-key swap (CA->FA promotion): whether it ever armed and what it did over the run.
    RuntimeProfile::Counter* _cc_swap_arm_chunk = nullptr;         // post-flip chunk the swap armed at (-1 if never)
    RuntimeProfile::Counter* _cc_swap_promotions = nullptr;        // keys promoted CA->FA mid-run
    RuntimeProfile::Counter* _cc_swap_evictions = nullptr;         // FA keys flushed back to CA to free a slot
    RuntimeProfile::Counter* _cc_swap_skipped_scattered = nullptr; // crossings left in place (no dominant key)
    RuntimeProfile::Counter* _cc_swap_declined = nullptr;          // crossings not promoted (dominant key below bound)
    RuntimeProfile::Counter* _cc_swap_estimate_skipped =
            nullptr; // concentrated crossings whose sample estimate skipped the scan
    RuntimeProfile::Counter* _cc_swap_reaggregated_tuples = nullptr; // tuples re-scanned across the swap's passes
    // CA spill: whether the cold tail spilled and what restore re-routed vs early-pruned off disk.
    RuntimeProfile::Counter* _cc_ca_spilled = nullptr;             // 1 if the CA tuples spilled to disk
    RuntimeProfile::Counter* _cc_ca_restored_rows = nullptr;       // spilled rows re-routed into the CA on restore
    RuntimeProfile::Counter* _cc_ca_restore_pruned_rows = nullptr; // spilled rows dropped at restore by the prune mask

    // The offset of the n-th aggregate function in a row of aggregate functions.
    std::vector<size_t> _agg_states_offsets;
    // The total size of the row for the aggregate function state.
    size_t _agg_states_total_size = 0;
    // The max align size for all aggregate state
    size_t _max_agg_state_align_size = 1;
    // The followings are aggregate function information:
    std::vector<FunctionContext*> _agg_fn_ctxs;
    std::vector<const AggregateFunction*> _agg_functions;
    // agg state when no group by columns
    AggDataPtr _single_agg_state = nullptr;
    // The expr used to evaluate agg input columns
    // one agg function could have multi input exprs
    std::vector<std::vector<ExprContext*>> _agg_expr_ctxs;
    std::vector<Columns> _agg_input_columns;
    //raw pointers in order to get multi-column values
    std::vector<std::vector<const Column*>> _agg_input_raw_columns;
    // The expr used to evaluate agg intermediate columns.
    std::vector<std::vector<ExprContext*>> _intermediate_agg_expr_ctxs;

    // Indicates we should use update or merge method to process aggregate column data
    std::vector<bool> _is_merge_funcs;
    // In order batch update agg states
    Buffer<AggDataPtr> _tmp_agg_states;
    std::vector<AggFunctionTypes> _agg_fn_types;

    // Exprs used to evaluate conjunct
    std::vector<ExprContext*> _conjunct_ctxs;

    // Exprs used to evaluate group by column
    std::vector<ExprContext*> _group_by_expr_ctxs;
    std::vector<ExprContext*> _group_by_min_max;
    // Cache-conscious top-n MCV (most-common group-by values) supplied by the FE from the histogram
    // statistics: hot-key literal exprs (parallel to _cc_mcv_counts). Received and counted; the FA
    // seeding that consumes them is a follow-on.
    std::vector<ExprContext*> _cc_mcv_key_ctxs;
    std::vector<int64_t> _cc_mcv_counts;
    std::vector<std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>>> _ranges;
    Columns _group_by_columns;
    std::vector<ColumnType> _group_by_types;

    // Tuple into which Update()/Merge()/Serialize() results are stored.
    TupleId _intermediate_tuple_id;
    TupleDescriptor* _intermediate_tuple_desc = nullptr;

    // Tuple into which Finalize() results are stored. Possibly the same as
    // the intermediate tuple.
    TupleId _output_tuple_id;
    TupleDescriptor* _output_tuple_desc = nullptr;

    // used for blocking aggregate
    AggrPhase _aggr_phase = AggrPhase1;
    AggrMode _aggr_mode = AM_DEFAULT;
    bool _is_passthrough = false;
    bool _is_pending_reset_state = false;
    Filter _streaming_selection;

    bool _has_udaf = false;

    AggStatistics* _agg_stat;

    std::shared_ptr<spill::Spiller> _spiller;
    SpillProcessChannelPtr _spill_channel;
    bool _is_opened = false;
    bool _is_prepared = false;
    int64_t _agg_state_mem_usage = 0;

    // Local DOP of the agg pipeline (reserve divisor); 0 until set by the factory.
    int32_t _degree_of_parallelism = 0;
    // Guards the initial-capacity reserve so it runs at most once over the
    // aggregator's life.
    bool _initial_reserve_applied = false;
    // Last observed hash-map capacity, to count rehashes (HashTableGrowCount).
    size_t _prev_hash_map_capacity = 0;
    // Full-estimate slot count the cache-conscious L2 cap held back from the initial
    // reserve; consumed by complete_cache_conscious_deferred_reserve() on a
    // "not skewed" flip verdict. 0 = nothing deferred.
    int64_t _cc_deferred_reserve_slots = 0;

    // aggregate combinator functions since they are not persisted in agg hash map
    std::vector<const AggregateFunction*> _combinator_function;

    pipeline::PipeObservable _pip_observable;
    // used to build the topn runtime filter
    AggTopNRuntimeFilterBuilder* _topn_runtime_filter_builder = nullptr;

public:
    void build_hash_map(size_t chunk_size, bool agg_group_by_with_limit = false);
    void build_hash_map(size_t chunk_size, std::atomic<int64_t>& shared_limit_countdown, bool agg_group_by_with_limit);
    void build_hash_map_with_selection(size_t chunk_size);
    void build_hash_map_with_selection_and_allocation(size_t chunk_size, bool agg_group_by_with_limit = false);
    void build_hash_map_with_topn_runtime_filter(size_t chunk_size);
    Status convert_hash_map_to_chunk(int32_t chunk_size, ChunkPtr* chunk,
                                     bool force_use_intermediate_as_output = false);

    // Read the current per-group count(*) values straight out of the live hash table, used by
    // the cache-conscious top-n flip decision. Only valid for a single count(*) aggregate
    // (the FE gating guarantees this); the count state is the int64 at the first agg offset.
    // The optional NULL-key group is skipped — one group cannot change the skew verdict.
    void collect_cache_conscious_topn_counts(std::vector<int64_t>* counts);

    // Read (group key, count) pairs out of the live hash table, used to seed the frozen FA
    // and to emit. Only integral group keys are supported (the key is exact as a uint64, so
    // there are no identity collisions); returns false for string/serialized keys, letting
    // the operator fall back to plain aggregation. NULL-key group is skipped.
    bool collect_cache_conscious_topn_groups(std::vector<std::pair<uint64_t, int64_t>>* groups);

    void build_hash_set(size_t chunk_size);
    void build_hash_set_with_selection(size_t chunk_size);
    void convert_hash_set_to_chunk(int32_t chunk_size, ChunkPtr* chunk);

    bool is_pre_cache() { return _aggr_mode == AM_BLOCKING_PRE_CACHE || _aggr_mode == AM_STREAMING_PRE_CACHE; }
    MutableColumns create_group_by_columns(size_t num_rows) const { return _create_group_by_columns(num_rows); }

protected:
    bool _reached_limit() { return _limit != -1 && _num_rows_returned >= _limit; }

    void _build_hash_map_with_shared_limit(size_t chunk_size, std::atomic<int64_t>& shared_limit_countdown);

    // Count hash-map capacity growths (rehashes) into HashTableGrowCount. Idempotent:
    // capacity is monotonic, so a repeat call with no growth adds nothing.
    void _update_hash_table_grow_count();

    bool _use_intermediate_as_input() {
        if (is_pending_reset_state()) {
            DCHECK(_aggr_mode == AM_BLOCKING_PRE_CACHE || _aggr_mode == AM_STREAMING_PRE_CACHE);
            return true;
        } else {
            return ((_aggr_mode == AM_BLOCKING_POST_CACHE) || (_aggr_mode == AM_STREAMING_POST_CACHE)) &&
                   !_is_passthrough;
        }
    }

    bool _use_intermediate_as_output() {
        return _aggr_mode == AM_STREAMING_PRE_CACHE || _aggr_mode == AM_BLOCKING_PRE_CACHE || !_needs_finalize;
    }

    Status _reset_state(RuntimeState* state, bool reset_sink_complete);

    // initial const columns for i'th FunctionContext.
    Status _evaluate_const_columns(int i);

    // Create new aggregate function result column by type
    MutableColumns _create_agg_result_columns(size_t num_rows, bool use_intermediate);
    MutableColumns _create_group_by_columns(size_t num_rows) const;

    void _serialize_to_chunk(ConstAggDataPtr __restrict state, MutableColumns& agg_result_columns);
    void _finalize_to_chunk(ConstAggDataPtr __restrict state, MutableColumns& agg_result_columns);
    void _destroy_state(AggDataPtr __restrict state);

    ChunkPtr _build_output_chunk(const Columns& group_by_columns, const Columns& agg_result_columns,
                                 bool use_intermediate);
    ChunkPtr _build_output_chunk(MutableColumns&& group_by_columns, MutableColumns&& agg_result_columns,
                                 bool use_intermediate);
    // Materialize the pruned local top-n (key, count) pairs into the result chunk.
    Status _build_cache_conscious_result_chunk(const std::vector<std::pair<uint64_t, int64_t>>& result);
    // Shared tail of both finalize paths (in-memory and post-restore): read the frozen FA out of
    // the hash map, prune it against the CA, build the result chunk, and release the CA.

    // Resumable generator yielding the CA partitions' tuples as (key, partial) intermediate
    // chunks (or EndOfFile when drained). The caller spills the returned chunks; the spill
    // channel drives the same generator for whatever did not fit inline (backpressure).
    std::function<StatusOr<ChunkPtr>()> _build_cache_conscious_ca_spill_task(RuntimeState* state);

    void _set_passthrough(bool flag) { _is_passthrough = flag; }
    bool is_passthrough() const { return _is_passthrough; }

    void begin_pending_reset_state() { _is_pending_reset_state = true; }
    void end_pending_reset_state() { _is_pending_reset_state = false; }
    bool is_pending_reset_state() { return _is_pending_reset_state; }

    void _reset_exprs();
    Status _evaluate_group_by_exprs(Chunk* chunk);

    // Choose different agg hash map/set by different group by column's count, type, nullable
    template <typename HashVariantType>
    void _init_agg_hash_variant(HashVariantType& hash_variant, bool want_pack = false);
    // get spec hash table/set type
    template <typename HashVariantType>
    typename HashVariantType::Type _get_hash_table_type();

    template <typename HashVariantType>
    typename HashVariantType::Type _try_to_apply_fixed_size_opt(typename HashVariantType::Type type,
                                                                bool* has_null_column, int* fixed_byte_size);
    struct CompressKeyContext {
        std::vector<int> offsets;
        std::vector<int> used_bits;
        std::vector<std::any> bases;
    };
    template <typename HashVariantType>
    typename HashVariantType::Type _try_to_apply_compressed_key_opt(typename HashVariantType::Type input_type,
                                                                    CompressKeyContext* ctx);
    template <typename HashVariantType>
    void _build_hash_variant(HashVariantType& hash_variant, typename HashVariantType::Type type,
                             CompressKeyContext&& context);

    void _release_agg_memory();

    bool _is_agg_result_nullable(const TExpr& desc, const AggFunctionTypes& agg_func_type);

    Status _create_aggregate_function(starrocks::RuntimeState* state, const TFunction& fn, bool is_result_nullable,
                                      const AggregateFunction** ret);

    int64_t get_two_level_threahold() {
        if (config::two_level_memory_threshold < 0) {
            return agg::two_level_memory_threshold();
        }
        return config::two_level_memory_threshold;
    }

    template <class HashMapWithKey>
    friend struct AllocateState;
};

inline bool LimitedMemAggState::has_limited(const Aggregator& aggregator) const {
    return limited_memory_size > 0 && aggregator.memory_usage() >= limited_memory_size;
}

inline size_t LimitedMemAggState::clamp_budget(int64_t memory_usage, int64_t cap) {
    if (memory_usage > cap) {
        return cap;
    }
    return memory_usage > 0 ? static_cast<size_t>(memory_usage) : 1;
}

template <class T>
class AggregatorFactoryBase {
public:
    using Ptr = std::shared_ptr<T>;
    AggregatorFactoryBase(const TPlanNode& tnode)
            : _tnode(tnode), _aggregator_param(convert_to_aggregator_params(_tnode)) {
        _shared_limit_countdown.store(_aggregator_param->limit);
    }

    Ptr get_or_create(size_t id) {
        auto it = _aggregators.find(id);
        if (it != _aggregators.end()) {
            return it->second;
        }
        auto aggregator = std::make_shared<T>(_aggregator_param);
        aggregator->set_aggr_mode(_aggr_mode);
        _aggregators[id] = aggregator;
        return aggregator;
    }

    void set_aggr_mode(AggrMode aggr_mode) { _aggr_mode = aggr_mode; }

    const AggregatorParamsPtr& aggregator_param() { return _aggregator_param; }

    const TPlanNode& t_node() { return _tnode; }
    const AggrMode aggr_mode() { return _aggr_mode; }

    std::atomic<int64_t>& get_shared_limit_countdown() { return _shared_limit_countdown; }

private:
    const TPlanNode& _tnode;
    AggregatorParamsPtr _aggregator_param;
    std::unordered_map<size_t, Ptr> _aggregators;
    AggrMode _aggr_mode = AggrMode::AM_DEFAULT;
    std::atomic<int64_t> _shared_limit_countdown;
};

} // namespace starrocks
