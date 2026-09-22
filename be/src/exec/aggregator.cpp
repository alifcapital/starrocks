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

#include "aggregator.h"

#include <algorithm>
#include <memory>
#include <type_traits>
#include <utility>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/agg_runtime_filter_builder.h"
#include "exec/aggregate/agg_hash_variant.h"
#include "exec/aggregate/agg_profile.h"
#include "exec/cache_conscious_topn.h"
#include "exec/exec_node.h"
#include "exec/pipeline/operator.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/combinator/agg_state_utils.h"
#include "exprs/literal.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/current_thread.h"
#include "runtime/descriptors.h"
#include "types/logical_type.h"
#include "udf/java/utils.h"
#include "util/defer_op.h"
#include "util/runtime_profile.h"

namespace starrocks {

static const std::unordered_set<std::string> ALWAYS_NULLABLE_RESULT_AGG_FUNCS = {
        "variance_samp", "var_samp", "stddev_samp", "covar_samp", "corr", "max_by_v2", "min_by_v2"};

static const std::string FUNCTION_COUNT = "count";

template <class HashMapWithKey>
struct AllocateState {
    AllocateState(Aggregator* aggregator_) : aggregator(aggregator_) {}
    inline AggDataPtr operator()(const typename HashMapWithKey::KeyType& key);
    inline AggDataPtr operator()(std::nullptr_t);

private:
    Aggregator* aggregator;
};

template <class HashMapWithKey>
inline AggDataPtr AllocateState<HashMapWithKey>::operator()(const typename HashMapWithKey::KeyType& key) {
    AggDataPtr agg_state = aggregator->_state_allocator.allocate();
    *reinterpret_cast<typename HashMapWithKey::KeyType*>(agg_state) = key;
    size_t created = 0;
    size_t aggregate_function_sz = aggregator->_agg_fn_ctxs.size();
    try {
        for (int i = 0; i < aggregate_function_sz; i++) {
            aggregator->_agg_functions[i]->create(aggregator->_agg_fn_ctxs[i],
                                                  agg_state + aggregator->_agg_states_offsets[i]);
            created++;
        }
        return agg_state;
    } catch (std::bad_alloc& e) {
        for (size_t i = 0; i < created; ++i) {
            aggregator->_agg_functions[i]->destroy(aggregator->_agg_fn_ctxs[i],
                                                   agg_state + aggregator->_agg_states_offsets[i]);
        }
        aggregator->_state_allocator.rollback();
        throw;
    }
}

template <class HashMapWithKey>
inline AggDataPtr AllocateState<HashMapWithKey>::operator()(std::nullptr_t) {
    AggDataPtr agg_state = aggregator->_state_allocator.allocate_null_key_data();
    size_t created = 0;
    size_t aggregate_function_sz = aggregator->_agg_fn_ctxs.size();
    try {
        for (int i = 0; i < aggregate_function_sz; i++) {
            aggregator->_agg_functions[i]->create(aggregator->_agg_fn_ctxs[i],
                                                  agg_state + aggregator->_agg_states_offsets[i]);
            created++;
        }
        return agg_state;
    } catch (std::bad_alloc& e) {
        for (int i = 0; i < created; i++) {
            aggregator->_agg_functions[i]->destroy(aggregator->_agg_fn_ctxs[i],
                                                   agg_state + aggregator->_agg_states_offsets[i]);
        }
        throw;
    }
}

bool AggFunctionTypes::use_nullable_fn(bool use_intermediate_as_output) const {
    // The non-nullable version functions assume that both the input and output are non-nullable, while the nullable version
    // functions support nullable input or nullable output, which will judge whether the input and output are nullable.
    //
    // NOTE that for the case of `is_always_nullable_result=true`, the function created with `use_intermediate_as_output=true`
    // also needs to use `is_result_nullable<true>` when getting the finalize result.
    // Because for the case of `is_always_nullable_result=true and has_nullable_child=false`, the function is the non-nullable version,
    // which causes only non-nullable output can be created.
    if (use_intermediate_as_output) {
        return has_nullable_child || is_result_nullable<true>();
    } else {
        return has_nullable_child || is_result_nullable<false>();
    }
}

std::string AggrAutoContext::get_auto_state_string(const AggrAutoState& state) {
    switch (state) {
    case INIT_PREAGG:
        return "INIT_PREAGG";
    case ADJUST:
        return "ADJUST";
    case PASS_THROUGH:
        return "PASS_THROUGH";
    case FORCE_PREAGG:
        return "FORCE_PREAGG";
    case PREAGG:
        return "PREAGG";
    case SELECTIVE_PREAGG:
        return "SELECTIVE_PREAGG";
    }
    return "UNKNOWN";
}

void AggrAutoContext::update_continuous_limit() {
    continuous_limit = continuous_limit * 2 > ContinuousUpperLimit ? ContinuousUpperLimit : continuous_limit * 2;
}

size_t AggrAutoContext::get_continuous_limit() {
    return continuous_limit;
}

bool AggrAutoContext::is_high_reduction(const size_t agg_count, const size_t chunk_size) {
    return agg_count >= HighReduction * chunk_size;
}

bool AggrAutoContext::is_low_reduction(const size_t agg_count, const size_t chunk_size) {
    return agg_count <= LowReduction * chunk_size;
}

Status init_udaf_context(int64_t fid, const std::string& url, const std::string& checksum, const std::string& symbol,
                         FunctionContext* context, const TCloudConfiguration& cloud_configuration,
                         bool use_cache = false, bool* cache_hit_out = nullptr);

AggregatorParamsPtr convert_to_aggregator_params(const TPlanNode& tnode) {
    auto params = std::make_shared<AggregatorParams>();
    params->conjuncts = tnode.conjuncts;
    params->limit = tnode.limit;

    // TODO: STREAM_AGGREGATION_NODE will be added later.
    DCHECK_EQ(tnode.node_type, TPlanNodeType::AGGREGATION_NODE);
    switch (tnode.node_type) {
    case TPlanNodeType::AGGREGATION_NODE: {
        params->needs_finalize = tnode.agg_node.need_finalize;
        params->streaming_preaggregation_mode = tnode.agg_node.streaming_preaggregation_mode;
        params->intermediate_tuple_id = tnode.agg_node.intermediate_tuple_id;
        params->output_tuple_id = tnode.agg_node.output_tuple_id;
        params->sql_grouping_keys = tnode.agg_node.__isset.sql_grouping_keys ? tnode.agg_node.sql_grouping_keys : "";
        params->sql_aggregate_functions =
                tnode.agg_node.__isset.sql_aggregate_functions ? tnode.agg_node.sql_aggregate_functions : "";
        params->has_outer_join_child =
                tnode.agg_node.__isset.has_outer_join_child && tnode.agg_node.has_outer_join_child;
        params->grouping_exprs = tnode.agg_node.grouping_exprs;
        params->aggregate_functions = tnode.agg_node.aggregate_functions;
        params->intermediate_aggr_exprs = tnode.agg_node.intermediate_aggr_exprs;
        params->enable_pipeline_share_limit =
                tnode.agg_node.__isset.enable_pipeline_share_limit ? tnode.agg_node.enable_pipeline_share_limit : false;
        params->grouping_min_max =
                tnode.agg_node.__isset.group_by_min_max ? tnode.agg_node.group_by_min_max : std::vector<TExpr>{};
        params->estimated_cardinality =
                tnode.agg_node.__isset.estimated_cardinality ? tnode.agg_node.estimated_cardinality : -1;
        params->enable_cache_conscious_topn =
                tnode.agg_node.__isset.enable_cache_conscious_topn && tnode.agg_node.enable_cache_conscious_topn;
        params->cache_conscious_topn_limit =
                tnode.agg_node.__isset.cache_conscious_topn_limit ? tnode.agg_node.cache_conscious_topn_limit : -1;
        params->cache_conscious_topn_force_flip = tnode.agg_node.__isset.cache_conscious_topn_force_flip &&
                                                  tnode.agg_node.cache_conscious_topn_force_flip;
        params->cc_mcv_keys = tnode.agg_node.__isset.cache_conscious_topn_mcv_keys
                                      ? tnode.agg_node.cache_conscious_topn_mcv_keys
                                      : std::vector<TExpr>{};
        params->cc_mcv_counts = tnode.agg_node.__isset.cache_conscious_topn_mcv_counts
                                        ? tnode.agg_node.cache_conscious_topn_mcv_counts
                                        : std::vector<int64_t>{};

        break;
    }
    default:
        __builtin_unreachable();
    }
    params->init();
    return params;
}

void AggregatorParams::init() {
    size_t agg_size = aggregate_functions.size();
    agg_fn_types.resize(agg_size);
    // init aggregate function types
    for (size_t i = 0; i < agg_size; ++i) {
        const TExpr& desc = aggregate_functions[i];
        const TFunction& fn = desc.nodes[0].fn;

        if (AggStateUtils::is_count_function(fn.name.function_name)) {
            // count family serializes a non-nullable BIGINT. count_combine's NULL-skipping is
            // re-derived from the real input nullability in _is_agg_result_nullable, not here.
            agg_fn_types[i] = {TypeDescriptor(TYPE_BIGINT), TypeDescriptor(TYPE_BIGINT), {}, false, false};
        } else {
            // whether agg function has nullable child
            const bool has_nullable_child = has_outer_join_child || desc.nodes[0].has_nullable_child;
            // whether agg function is nullable
            bool is_nullable = desc.nodes[0].is_nullable;
            // collect arg_typedescs for aggregate function.
            std::vector<FunctionContext::TypeDesc> arg_typedescs;
            for (auto& type : fn.arg_types) {
                arg_typedescs.push_back(TypeDescriptor::from_thrift(type));
            }
            TypeDescriptor return_type = TypeDescriptor::from_thrift(fn.ret_type);
            TypeDescriptor serde_type = TypeDescriptor::from_thrift(fn.aggregate_fn.intermediate_type);
            agg_fn_types[i] = {return_type, serde_type, arg_typedescs, has_nullable_child, is_nullable};
            agg_fn_types[i].is_always_nullable_result =
                    ALWAYS_NULLABLE_RESULT_AGG_FUNCS.contains(fn.name.function_name);
            if (fn.__isset.agg_state_desc && AggStateUtils::is_agg_state_if(fn.name.function_name)) {
                agg_fn_types[i].is_always_nullable_result = true;
                agg_fn_types[i].serialize_always_nullable = true;
            }
            if (fn.name.function_name == "array_agg" || fn.name.function_name == "group_concat") {
                // set order by info
                if (fn.aggregate_fn.__isset.is_asc_order && fn.aggregate_fn.__isset.nulls_first &&
                    !fn.aggregate_fn.is_asc_order.empty()) {
                    agg_fn_types[i].is_asc_order = fn.aggregate_fn.is_asc_order;
                    agg_fn_types[i].nulls_first = fn.aggregate_fn.nulls_first;
                }
                if (fn.aggregate_fn.__isset.is_distinct) {
                    agg_fn_types[i].is_distinct = fn.aggregate_fn.is_distinct;
                }
            }
        }
        VLOG_ROW << fn.name.function_name << ", param_arg_nullable:" << desc.nodes[0].has_nullable_child
                 << ", param_result_nullable " << desc.nodes[0].is_nullable
                 << ", is_always_nullable_result: " << agg_fn_types[i].is_always_nullable_result
                 << ", has_nullable_child:" << agg_fn_types[i].has_nullable_child
                 << ", is_nullable:" << agg_fn_types[i].is_nullable;
    }

    // init group by types
    size_t group_by_size = grouping_exprs.size();
    group_by_types.resize(group_by_size);
    for (size_t i = 0; i < group_by_size; ++i) {
        TExprNode expr = grouping_exprs[i].nodes[0];
        group_by_types[i].result_type = TypeDescriptor::from_thrift(expr.type);
        group_by_types[i].is_nullable = expr.is_nullable || has_outer_join_child;
        has_nullable_key = has_nullable_key || group_by_types[i].is_nullable;
        VLOG_ROW << "group by column " << i << " result_type " << group_by_types[i].result_type << " is_nullable "
                 << expr.is_nullable;
    }

    VLOG_ROW << "has_nullable_key " << has_nullable_key;
}

#define ALIGN_TO(size, align) ((size + align - 1) / align * align)
#define PAD(size, align) (align - (size % align)) % align;

Aggregator::Aggregator(AggregatorParamsPtr params) : _params(std::move(params)) {
    _allocator = std::make_unique<CountingAllocatorWithHook>();
}

Status Aggregator::reserve_hash_table_from_estimate() {
    // Apply the initial-capacity reserve at most once over this aggregator's life,
    // even across repeated triggers.
    if (_initial_reserve_applied) {
        return Status::OK();
    }
    _initial_reserve_applied = true;

    // Only the hash MAP with group-by keys is in scope: distinct (set) reserve and
    // the no-group-by single-state path are excluded.
    if (is_hash_set() || is_none_group_by_exprs()) {
        return Status::OK();
    }
    // FE only sets a positive estimate when it proved the value safe to reserve from.
    const int64_t est = _params->estimated_cardinality;
    if (est <= 0) {
        return Status::OK();
    }
    // The final variant type (after two-level / compressed-key rewrites) must support
    // reserve -- true for every phmap-backed map, false only for fixed-size-small maps.
    if (!_hash_map_variant.supports_reserve()) {
        return Status::OK();
    }

    // Divisor = local DOP: drivers split the keyspace after local-shuffle, so each
    // driver only needs its share. The estimate is already per-instance when the input
    // is hash-shuffled (the FE divides the cluster-wide NDV by the fragment instance
    // count), so est/dop is the per-driver share NDV/(instances*dop). The estimate
    // under-counts more often than it over-counts (clamped to input rows, post-HAVING),
    // so under-reserve is the common, harmless case; the byte cap below bounds over-estimate.
    const int64_t dop = _degree_of_parallelism > 0 ? _degree_of_parallelism : 1;
    constexpr int64_t kMaxReserveSlots = int64_t{1} << 30; // overflow / absurd-estimate guard
    int64_t reserve_slots = std::min(est / dop, kMaxReserveSlots);

    // A limit-bounded group-by keeps at most `limit` groups in the table (the phase-2
    // _agg_group_by_with_limit path in the blocking sink), while the FE cardinality is
    // the full NDV -- not capped by the agg limit. Never reserve beyond the limit.
    if (limit() != -1 && conjunct_ctxs().empty() && get_aggr_phase() == AggrPhase2) {
        reserve_slots = std::min(reserve_slots, limit());
    }

    // Reserving at/below the current capacity (the chunk_size baseline) buys nothing.
    if (reserve_slots <= static_cast<int64_t>(_hash_map_variant.capacity())) {
        return Status::OK();
    }

    // Cap the reserve so one aggregator never pre-allocates more than the configured
    // ceiling, bounding wasted memory when the estimate overshoots.
    const int64_t cap_bytes = config::agg_hashtable_reserve_max_bytes;
    if (cap_bytes <= 0) {
        return Status::OK();
    }
    const int64_t want_bytes = static_cast<int64_t>(_hash_map_variant.reserve_bytes_estimate(reserve_slots));
    if (want_bytes > cap_bytes) {
        reserve_slots = static_cast<int64_t>(static_cast<__int128>(reserve_slots) * cap_bytes / want_bytes);
        if (reserve_slots <= static_cast<int64_t>(_hash_map_variant.capacity())) {
            return Status::OK();
        }
    }

    // A cache-conscious top-n candidate needs the flip verdict to come from an
    // L2-resident probe: the verdict fires on the first growth past the L2 budget and
    // reads the counts accumulated by then. A full-estimate reserve would start the
    // table past the budget and the verdict would fire on a near-empty map, killing
    // the flip for every table with stats. Cap the initial reserve at the L2 budget
    // and remember the full size; a "not skewed" verdict completes it in one rehash
    // (cheap -- it only re-buckets an L2-sized table), a flip freezes the map anyway.
    if (enable_cache_conscious_topn() && cache_conscious_group_key_supported() && needs_finalize() && !is_pre_cache()) {
        const int64_t l2_budget = config::cache_conscious_topn_l2_budget_bytes;
        const int64_t l2_want = static_cast<int64_t>(_hash_map_variant.reserve_bytes_estimate(reserve_slots));
        if (l2_budget > 0 && l2_want > l2_budget) {
            _cc_deferred_reserve_slots = reserve_slots;
            reserve_slots = static_cast<int64_t>(static_cast<__int128>(reserve_slots) * l2_budget / l2_want);
            if (reserve_slots <= static_cast<int64_t>(_hash_map_variant.capacity())) {
                return Status::OK();
            }
        }
    }

    // Best-effort: a reserve OOM is reported but treated as non-fatal by the caller,
    // which falls back to incremental growth. phmap::reserve is exception-safe, so
    // the map is left intact on failure. The thread-local mem tracker set by the
    // driver around prepare_local_state covers this allocation (same as open()).
    TRY_CATCH_BAD_ALLOC(_hash_map_variant.reserve(static_cast<size_t>(reserve_slots)));
    // Baseline the grow counter at the reserved capacity so the reserve itself is not
    // counted as rehashes -- only later organic growth past it is.
    _prev_hash_map_capacity = _hash_map_variant.capacity();
    return Status::OK();
}

Status Aggregator::complete_cache_conscious_deferred_reserve() {
    const int64_t slots = _cc_deferred_reserve_slots;
    _cc_deferred_reserve_slots = 0;
    if (slots <= static_cast<int64_t>(_hash_map_variant.capacity())) {
        return Status::OK();
    }
    // Same best-effort contract as the initial reserve: an OOM falls back to growth.
    TRY_CATCH_BAD_ALLOC(_hash_map_variant.reserve(static_cast<size_t>(slots)));
    _prev_hash_map_capacity = _hash_map_variant.capacity();
    return Status::OK();
}

Status Aggregator::open(RuntimeState* state) {
    if (_is_opened) {
        return Status::OK();
    }
    _is_opened = true;
    RETURN_IF_ERROR(Expr::open(_group_by_expr_ctxs, state));
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        RETURN_IF_ERROR(Expr::open(_agg_expr_ctxs[i], state));
        RETURN_IF_ERROR(_evaluate_const_columns(i));
    }
    for (auto& _intermediate_agg_expr_ctx : _intermediate_agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::open(_intermediate_agg_expr_ctx, state));
    }
    RETURN_IF_ERROR(Expr::open(_conjunct_ctxs, state));

    // init function context
    _has_udaf = std::any_of(_fns.begin(), _fns.end(),
                            [](const auto& ctx) { return ctx.binary_type == TFunctionBinaryType::SRJAR; });
#ifndef __APPLE__
    if (_has_udaf) {
        auto& opts = state->query_options();
        bool enable_cache = opts.__isset.enable_cache_udaf && opts.enable_cache_udaf;
        auto promise_st = call_function_in_pthread(state, [this, enable_cache]() {
            for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
                if (_fns[i].binary_type == TFunctionBinaryType::SRJAR) {
                    const auto& fn = _fns[i];
                    // use_cache only when isolation is explicitly shared and enable_cache_udaf is set
                    bool use_cache = enable_cache && fn.__isset.isolated && !fn.isolated;
                    bool cache_hit = false;
                    Status st;
                    {
                        SCOPED_TIMER(_agg_stat->udaf_load_timer);
                        st = init_udaf_context(fn.fid, fn.hdfs_location, fn.checksum, fn.aggregate_fn.symbol,
                                               _agg_fn_ctxs[i], fn.cloud_configuration, use_cache,
                                               use_cache ? &cache_hit : nullptr);
                    }
                    if (use_cache) {
                        if (cache_hit) {
                            COUNTER_UPDATE(_agg_stat->udaf_cache_hit_count, 1);
                        } else {
                            COUNTER_UPDATE(_agg_stat->udaf_cache_populate_count, 1);
                        }
                    }
                    RETURN_IF_ERROR(st);
                }
            }
            return Status::OK();
        });
        RETURN_IF_ERROR(promise_st->get_future().get());
    }
#endif

    // open() may follow a reset; determine inline eligibility from the newly selected map.
    _inline_agg = false;
    _inline_pack = false;
    _inline_pack_n = 0;
    _inline_pack_fused = false;

    // For SQL: select distinct id from table or select id from from table group by id;
    // we don't need to allocate memory for agg states.
    if (_is_only_group_by_columns) {
        TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_set_variant));
    } else {
        TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_map_variant));
    }

    // Inline-agg fast path: a single qualifying aggregate over a supported group-by key
    // (numeric, single string, or a multi-column serialized/compressed fixed-size key) keeps
    // its accumulator in the hash-map slot itself (no arena state; a string key is still
    // duplicated into the pool -- the slot replaces the state indirection, not the key
    // copy). supports_inline_agg() decides which
    // key variants qualify; the op is resolved from the resolved function name. Gated by the
    // enable_agg_inline_accumulator session variable; everything else is unchanged.
    // Update-vs-merge is NOT decided here: it is a per-chunk property (_inline_agg_merge_chunk),
    // because a query-cache PRE_CACHE aggregator switches from raw rows to intermediate input at
    // refill time, after open(). The gate only requires one agg function, which also keeps the
    // _is_merge_funcs[0] read inside _inline_agg_merge_chunk in bounds.
    const bool inline_candidate = _allow_inline_agg && state->enable_agg_inline_accumulator() &&
                                  !_is_only_group_by_columns && !_group_by_expr_ctxs.empty() &&
                                  _agg_fn_ctxs.size() == 1 && !_agg_fn_types[0].is_distinct &&
                                  _hash_map_variant.supports_inline_agg();
    if (inline_candidate && _agg_functions[0]->get_name() == "count") {
        // count(*) / count(1) / count(non-null col): the constant +1 op on every stage.
        _inline_agg = true;
        _inline_op = InlineOpKind::kCountStar;
    } else if (inline_candidate && _agg_functions[0]->get_name() == "count_nullable") {
        // count(col): the per-row delta is the input column's !null[i] on update chunks; on
        // merge chunks the fold of NULL-free int64 partials is byte-identical to plain
        // count's. (The FE resolves count_nullable for a nullable input; with honest producer
        // nullability a NOT NULL input's merge stage arrives as plain count, branch above.)
        _inline_agg = true;
        _inline_op = InlineOpKind::kCountCol;
    } else if (inline_candidate && _fns.size() == 1 && _fns[0].name.function_name == "sum" &&
               _agg_functions[0]->get_name() == "sum") {
        // Plain (non-nullable-wrapped) sum. The resolved name alone is NOT enough: the registry
        // also carries "sum" entries with 16-byte accumulators (largeint -> LARGEINT,
        // decimalv2 -> DECIMALV2) and the storage variant (result == input), so only the
        // accumulator types that fit the slot pass. The plan-merge stage passes here too when
        // the FE marked the producer's slots with their real nullability (NOT NULL input, no
        // outer join / repeat below); a nullable input or context resolves "nullable sum" and
        // falls back.
        const LogicalType result_lt = _agg_fn_types[0].result_type.type;
        if (result_lt == TYPE_BIGINT) {
            _inline_agg = true;
            _inline_op = InlineOpKind::kSumInt;
        } else if (result_lt == TYPE_DOUBLE) {
            _inline_agg = true;
            _inline_op = InlineOpKind::kSumDouble;
        }
    } else if (inline_candidate && _fns.size() == 1 &&
               (_fns[0].name.function_name == "min" || _fns[0].name.function_name == "max") &&
               _agg_functions[0]->get_name() == "maxmin") {
        // Plain (non-nullable-wrapped) min/max. The BE-resolved name is "maxmin" for BOTH ops,
        // so the op identity comes from the FE function name. The whitelist keeps the value
        // types whose slot image and combine are proven against the general path; merge-stage
        // eligibility follows the resolved name exactly like sum.
        const LogicalType lt = _agg_fn_types[0].result_type.type;
        switch (lt) {
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            _inline_agg = true;
            _inline_op = _fns[0].name.function_name == "min" ? InlineOpKind::kMin : InlineOpKind::kMax;
            _inline_minmax_lt = lt;
            break;
        default:
            break;
        }
    }
    // Multi-aggregate pack gate: 2..4 aggregates, every one an additive int64 op
    // (count(*) / count(col) / sum(int family -> BIGINT)), over a key that has a pack
    // variant twin. The variant was initialized for the general path above; on success it
    // is re-initialized onto the pack twin (the map is still empty in open), and a key
    // with no twin (string, compressed, direct-array) re-resolves to the same type, which
    // is detected via is_inline_pack() and keeps the general path.
    if (!_inline_agg && _allow_inline_agg && state->enable_agg_inline_accumulator() && !_is_only_group_by_columns &&
        !_group_by_expr_ctxs.empty() && !_is_merge_funcs.empty() && _agg_fn_ctxs.size() >= 2 &&
        _agg_fn_ctxs.size() <= 4) {
        bool all_additive = true;
        bool all_count_star = true;
        InlineOpKind kinds[4] = {};
        for (size_t i = 0; i < _agg_fn_ctxs.size() && all_additive; ++i) {
            if (_agg_fn_types[i].is_distinct || _is_merge_funcs[i] != _is_merge_funcs[0]) {
                all_additive = false;
                break;
            }
            const std::string& resolved = _agg_functions[i]->get_name();
            if (resolved == "count") {
                kinds[i] = InlineOpKind::kCountStar;
            } else if (resolved == "count_nullable") {
                kinds[i] = InlineOpKind::kCountCol;
                all_count_star = false;
            } else if (resolved == "sum" && i < _fns.size() && _fns[i].name.function_name == "sum" &&
                       _agg_fn_types[i].result_type.type == TYPE_BIGINT) {
                kinds[i] = InlineOpKind::kSumInt;
                all_count_star = false;
            } else {
                all_additive = false;
            }
        }
        if (all_additive) {
            TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_map_variant, /*want_pack=*/true));
            if (_hash_map_variant.is_inline_pack()) {
                _inline_agg = true;
                _inline_pack = true;
                _inline_pack_n = static_cast<uint8_t>(_agg_fn_ctxs.size());
                _inline_pack_fused = all_count_star;
                for (size_t i = 0; i < _agg_fn_ctxs.size(); ++i) {
                    _inline_pack_ops[i] = kinds[i];
                }
            }
        }
    }

    static constexpr const char* kInlineOpNames[] = {"count(*)", "count(col)", "sum(int)", "sum(double)", "min", "max"};
    _runtime_profile->add_info_string(
            "InlineAggOptimization",
            _inline_agg ? (_inline_pack ? "pack" : kInlineOpNames[static_cast<int>(_inline_op)]) : "false");

    {
        _agg_states_total_size = 16;
        _max_agg_state_align_size = 8;
        if (!_is_only_group_by_columns && !_inline_agg) {
            _hash_map_variant.visit([&](auto& variant) {
                auto& hash_map_with_key = *variant;
                using HashMapWithKey = std::remove_reference_t<decltype(hash_map_with_key)>;
                _agg_states_total_size = sizeof(typename HashMapWithKey::KeyType);
                _max_agg_state_align_size = alignof(typename HashMapWithKey::KeyType);
            });

            DCHECK_GT(_agg_fn_ctxs.size(), 0);
            _max_agg_state_align_size = std::max(_max_agg_state_align_size, _agg_functions[0]->alignof_size());
            _agg_states_total_size += PAD(_agg_states_total_size, _agg_functions[0]->alignof_size());

            // compute agg state total size and offsets
            for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
                _agg_states_offsets[i] = _agg_states_total_size;
                _agg_states_total_size += _agg_functions[i]->size();
                _max_agg_state_align_size = std::max(_max_agg_state_align_size, _agg_functions[i]->alignof_size());

                // If not the last aggregate_state, we need pad it so that next aggregate_state will be aligned.
                if (i + 1 < _agg_fn_ctxs.size()) {
                    size_t next_state_align_size = _agg_functions[i + 1]->alignof_size();
                    // Extend total_size to next alignment requirement
                    // Add padding by rounding up '_agg_states_total_size' to be a multiplier of next_state_align_size.
                    _agg_states_total_size = ALIGN_TO(_agg_states_total_size, next_state_align_size);
                }
            }
            _agg_states_total_size = ALIGN_TO(_agg_states_total_size, _max_agg_state_align_size);
            _state_allocator.aggregate_key_size = _agg_states_total_size;
            _state_allocator.pool = _mem_pool.get();
        }
    }

    // AggregateFunction::create needs to call create in JNI,
    // but prepare is executed in bthread, which will cause the JNI code to crash

    if (_group_by_expr_ctxs.empty()) {
        _single_agg_state = _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
        RETURN_IF_UNLIKELY_NULL(_single_agg_state, Status::MemoryAllocFailed("alloc single agg state failed"));
        auto call_agg_create = [this]() {
            size_t created = 0;
            try {
                for (int i = 0; i < _agg_functions.size(); i++) {
                    _agg_functions[i]->create(_agg_fn_ctxs[i], _single_agg_state + _agg_states_offsets[i]);
                    created++;
                }
            } catch (std::bad_alloc& e) {
                tls_thread_status.set_is_catched(false);
                for (int i = 0; i < created; i++) {
                    _agg_functions[i]->destroy(_agg_fn_ctxs[i], _single_agg_state + _agg_states_offsets[i]);
                }
                _single_agg_state = nullptr;
                return Status::MemoryLimitExceeded("aggregate::create allocate failed");
            }

            return Status::OK();
        };
#ifdef __APPLE__
        RETURN_IF_ERROR(call_agg_create());
#else
        if (_has_udaf) {
            auto promise_st = call_function_in_pthread(state, call_agg_create);
            RETURN_IF_ERROR(promise_st->get_future().get());
        } else {
            RETURN_IF_ERROR(call_agg_create());
        }
#endif

        if (_agg_expr_ctxs.empty()) {
            return Status::InternalError("Invalid agg query plan");
        }
    }

    RETURN_IF_ERROR(check_has_error());

    _limited_buffer = std::make_unique<LimitedPipelineChunkBuffer<AggStatistics>>(
            _agg_stat, 1, config::local_exchange_buffer_mem_limit_per_driver,
            state->chunk_size() * config::streaming_agg_chunk_buffer_size);

    return Status::OK();
}

Status Aggregator::prepare(RuntimeState* state, RuntimeProfile* runtime_profile) {
    if (_is_prepared) {
        return Status::OK();
    }
    _is_prepared = true;
    _state = state;
    _pool = std::make_unique<ObjectPool>();
    _runtime_profile = runtime_profile;

    _limit = _params->limit;
    _needs_finalize = _params->needs_finalize;
    _streaming_preaggregation_mode = _params->streaming_preaggregation_mode;
    _intermediate_tuple_id = _params->intermediate_tuple_id;
    _output_tuple_id = _params->output_tuple_id;

    RETURN_IF_ERROR(Expr::create_expr_trees(_pool.get(), _params->conjuncts, &_conjunct_ctxs, state, true));
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool.get(), _params->grouping_exprs, &_group_by_expr_ctxs, state, true));
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool.get(), _params->grouping_min_max, &_group_by_min_max, state, true));
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool.get(), _params->cc_mcv_keys, &_cc_mcv_key_ctxs, state, true));
    _cc_mcv_counts = _params->cc_mcv_counts;
    _ranges.resize(_group_by_expr_ctxs.size());
    if (_group_by_min_max.size() == _group_by_expr_ctxs.size() * 2) {
        for (size_t i = 0; i < _group_by_expr_ctxs.size(); ++i) {
            std::pair<VectorizedLiteral*, VectorizedLiteral*> range;
            range.first = down_cast<VectorizedLiteral*>(_group_by_min_max[i * 2]->root());
            range.second = down_cast<VectorizedLiteral*>(_group_by_min_max[i * 2 + 1]->root());
            _ranges[i] = range;
        }
    }

    // add profile attributes
    if (!_params->sql_grouping_keys.empty()) {
        _runtime_profile->add_info_string("GroupingKeys", _params->sql_grouping_keys);
    }
    if (!_params->sql_aggregate_functions.empty()) {
        _runtime_profile->add_info_string("AggregateFunctions", _params->sql_aggregate_functions);
    }

    // Cache-conscious top-n profile counters: rows distribution (pre-flip build / FA hit / CA
    // route) and end-of-input Phase-3 prune work. Registered only on the cc path so normal aggs
    // stay uncluttered; populated at flip, on each post-flip probe, and when the prune drains.
    if (_params->enable_cache_conscious_topn) {
        auto* p = _runtime_profile;
        _cc_flipped = ADD_COUNTER(p, "CCFlipped", TUnit::UNIT);
        _cc_preflip_rows = ADD_COUNTER(p, "CCPreFlipRows", TUnit::UNIT);
        _cc_fa_keys = ADD_COUNTER(p, "CCFaKeys", TUnit::UNIT);
        _cc_fa_hit_rows = ADD_COUNTER(p, "CCFaHitRows", TUnit::UNIT);
        _cc_ca_routed_rows = ADD_COUNTER(p, "CCCaRoutedRows", TUnit::UNIT);
        _cc_bloom_active = ADD_COUNTER(p, "CCBloomActive", TUnit::UNIT);
        _cc_ca_partitions = ADD_COUNTER(p, "CCCaPartitions", TUnit::UNIT);
        _cc_partitions_resolved = ADD_COUNTER(p, "CCPartitionsResolved", TUnit::UNIT);
        _cc_partitions_repartitioned = ADD_COUNTER(p, "CCPartitionsRepartitioned", TUnit::UNIT);
        _cc_partitions_pruned = ADD_COUNTER(p, "CCPartitionsPruned", TUnit::UNIT);
        _cc_reprocessed_tuples = ADD_COUNTER(p, "CCReprocessedTuples", TUnit::UNIT);
        _cc_pruned_groups = ADD_COUNTER(p, "CCPrunedGroups", TUnit::UNIT);
        _cc_max_radix_level = ADD_COUNTER(p, "CCMaxRadixLevel", TUnit::UNIT);
        _cc_swap_arm_chunk = ADD_COUNTER(p, "CCSwapArmChunk", TUnit::UNIT);
        _cc_swap_promotions = ADD_COUNTER(p, "CCSwapPromotions", TUnit::UNIT);
        _cc_swap_evictions = ADD_COUNTER(p, "CCSwapEvictions", TUnit::UNIT);
        _cc_swap_skipped_scattered = ADD_COUNTER(p, "CCSwapSkippedScattered", TUnit::UNIT);
        _cc_swap_declined = ADD_COUNTER(p, "CCSwapDeclined", TUnit::UNIT);
        _cc_swap_estimate_skipped = ADD_COUNTER(p, "CCSwapEstimateSkipped", TUnit::UNIT);
        _cc_swap_reaggregated_tuples = ADD_COUNTER(p, "CCSwapReaggregatedTuples", TUnit::UNIT);
        _cc_ca_spilled = ADD_COUNTER(p, "CCCaSpilled", TUnit::UNIT);
        _cc_ca_restored_rows = ADD_COUNTER(p, "CCCaRestoredRows", TUnit::UNIT);
        _cc_ca_restore_pruned_rows = ADD_COUNTER(p, "CCCaRestorePrunedRows", TUnit::UNIT);
        _cc_mcv_keys = ADD_COUNTER(p, "CCMcvKeys", TUnit::UNIT);
        COUNTER_SET(_cc_mcv_keys, static_cast<int64_t>(_cc_mcv_key_ctxs.size()));
        _cc_mcv_seeded = ADD_COUNTER(p, "CCMcvSeeded", TUnit::UNIT);
    }

    bool has_outer_join_child = _params->has_outer_join_child;

    size_t group_by_size = _group_by_expr_ctxs.size();
    _group_by_columns.resize(group_by_size);
    _group_by_types = _params->group_by_types;
    _has_nullable_key = _params->has_nullable_key;

    _tmp_agg_states.resize(_state->chunk_size());

    auto& aggregate_functions = _params->aggregate_functions;
    size_t agg_size = aggregate_functions.size();
    _agg_fn_ctxs.resize(agg_size);
    _agg_functions.resize(agg_size);
    _agg_expr_ctxs.resize(agg_size);
    _agg_input_columns.resize(agg_size);
    _agg_input_raw_columns.resize(agg_size);
    _agg_states_offsets.resize(agg_size);
    _is_merge_funcs.resize(agg_size);
    _agg_fn_types = _params->agg_fn_types;

    // Save the TFunction objects up front: close() walks _agg_functions/_agg_fn_ctxs and indexes _fns with
    // the same index, so _fns must be filled before any error return below can leave prepare half-done.
    _fns.reserve(agg_size);
    for (int i = 0; i < agg_size; ++i) {
        _fns.emplace_back(aggregate_functions[i].nodes[0].fn);
    }

    for (int i = 0; i < agg_size; ++i) {
        const TExpr& desc = aggregate_functions[i];
        const TFunction& fn = desc.nodes[0].fn;
        const auto& agg_fn_type = _agg_fn_types[i];
        _is_merge_funcs[i] = aggregate_functions[i].nodes[0].agg_expr.is_merge_agg;

        // get function
        bool is_result_nullable = _is_agg_result_nullable(desc, agg_fn_type);
        RETURN_IF_ERROR(_create_aggregate_function(state, fn, is_result_nullable, &_agg_functions[i]));
        VLOG_ROW << "agg_fn_name: " << fn.name.function_name << ", has_outer_join_child: " << has_outer_join_child
                 << ", is_result_nullable " << is_result_nullable;

        int node_idx = 0;
        for (int j = 0; j < desc.nodes[0].num_children; ++j) {
            ++node_idx;
            Expr* expr = nullptr;
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_tree_from_thrift_with_jit(_pool.get(), desc.nodes, nullptr, &node_idx, &expr,
                                                                   &ctx, state));
            _agg_expr_ctxs[i].emplace_back(ctx);
        }

        // It is very critical, because for a count(*) or count(1) aggregation function, when it first be applied to
        // input data, the agg function needs no input columns; but when it is parted into two parts when query cache
        // enabled, the latter part after cache operator must always handle intermediate types, so the agg function
        // need at least one input column to store intermediate result.
        auto num_args = std::max<size_t>(1UL, desc.nodes[0].num_children);
        _agg_input_columns[i].resize(num_args);
        _agg_input_raw_columns[i].resize(num_args);
    }

    if (!_params->intermediate_aggr_exprs.empty()) {
        auto& aggr_exprs = _params->intermediate_aggr_exprs;
        _intermediate_agg_expr_ctxs.resize(agg_size);
        for (int i = 0; i < agg_size; ++i) {
            int node_idx = 0;
            Expr* expr = nullptr;
            ExprContext* ctx = nullptr;
            RETURN_IF_ERROR(Expr::create_tree_from_thrift_with_jit(_pool.get(), aggr_exprs[i].nodes, nullptr, &node_idx,
                                                                   &expr, &ctx, state));
            _intermediate_agg_expr_ctxs[i].emplace_back(ctx);
        }
    }

    _mem_pool = std::make_unique<MemPool>();
    _is_only_group_by_columns = _agg_expr_ctxs.empty() && !_group_by_expr_ctxs.empty();

    _agg_stat = _pool->add(new AggStatistics(_runtime_profile));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _intermediate_tuple_desc = state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());

    RETURN_IF_ERROR(Expr::prepare(_group_by_expr_ctxs, state));

    for (const auto& ctx : _agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::prepare(ctx, state));
    }

    for (const auto& ctx : _intermediate_agg_expr_ctxs) {
        RETURN_IF_ERROR(Expr::prepare(ctx, state));
    }

    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state));

    // Initial for FunctionContext of every aggregate functions
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        auto& agg_fn_type = _agg_fn_types[i];
        auto& agg_func = _agg_functions[i];
        TypeDescriptor return_type = agg_fn_type.result_type;
        std::vector<TypeDescriptor> arg_types = agg_fn_type.arg_typedescs;

        const AggStateDesc* agg_state_desc = AggStateUtils::get_agg_state_desc(agg_func);
        if (agg_state_desc != nullptr) {
            return_type = agg_state_desc->get_return_type();
            arg_types = agg_state_desc->get_arg_types();
        }

        _agg_fn_ctxs[i] =
                FunctionContext::create_context(state, _mem_pool.get(), return_type, arg_types, agg_fn_type.is_distinct,
                                                agg_fn_type.is_asc_order, agg_fn_type.nulls_first);
        if (state->query_options().__isset.group_concat_max_len) {
            _agg_fn_ctxs[i]->set_group_concat_max_len(state->query_options().group_concat_max_len);
        }
        state->obj_pool()->add(_agg_fn_ctxs[i]);
        _agg_fn_ctxs[i]->set_mem_usage_counter(&_agg_state_mem_usage);
    }

    // prepare for spiller
    if (spiller()) {
        RETURN_IF_ERROR(spiller()->prepare(state));
    }

    return Status::OK();
}

bool Aggregator::_is_agg_result_nullable(const TExpr& desc, const AggFunctionTypes& agg_func_type) {
    const TFunction& fn = desc.nodes[0].fn;
    // NOTE: count and count_combine carry mocked agg_func_type values (non-nullable fast-path), so
    // their NULL-skipping choice must come from the real input nullability on the plan node.
    if (fn.name.function_name == FUNCTION_COUNT ||
        fn.name.function_name == FUNCTION_COUNT + AggStateUtils::AGG_STATE_COMBINE_SUFFIX) {
        if (fn.arg_types.empty()) {
            return false;
        }
        return _params->has_outer_join_child || desc.nodes[0].has_nullable_child;
    } else {
        return agg_func_type.use_nullable_fn(_use_intermediate_as_output());
    }
}

Status Aggregator::_create_aggregate_function(starrocks::RuntimeState* state, const TFunction& fn,
                                              bool is_result_nullable, const AggregateFunction** ret) {
    std::vector<TypeDescriptor> arg_types;
    for (auto& type : fn.arg_types) {
        arg_types.push_back(TypeDescriptor::from_thrift(type));
    }

    // check whether it's _merge/_union combinator if it contains agg state type
    auto& func_name = fn.name.function_name;
    if (fn.__isset.agg_state_desc) {
        auto agg_state_desc = AggStateDesc::from_thrift(fn.agg_state_desc);
        // Ensure agg_state_desc's nullable is compatible with the result type.
        if (!AggStateUtils::is_agg_state_if(func_name)) {
            agg_state_desc.set_is_result_nullable(is_result_nullable);
        }
        ASSIGN_OR_RETURN(const AggregateFunction* agg_state_func,
                         AggStateUtils::get_agg_state_function(agg_state_desc, func_name, arg_types));
        *ret = agg_state_func;
        _combinator_function.emplace_back(agg_state_func);
    } else {
        // get function
        if (func_name == FUNCTION_COUNT) {
            auto* func = get_aggregate_function(FUNCTION_COUNT, TYPE_BIGINT, TYPE_BIGINT, is_result_nullable);
            if (func == nullptr) {
                return Status::InternalError(strings::Substitute("Invalid agg function plan: $0 ", func_name));
            }
            *ret = func;
        } else {
            TypeDescriptor return_type = TypeDescriptor::from_thrift(fn.ret_type);
            TypeDescriptor serde_type = TypeDescriptor::from_thrift(fn.aggregate_fn.intermediate_type);
            DCHECK_LE(1, fn.arg_types.size());
            const TypeDescriptor& arg_type = arg_types[0];
            auto* func = get_aggregate_function(func_name, return_type, arg_types, is_result_nullable, fn.binary_type,
                                                state->func_version());
            if (func == nullptr) {
                return Status::InternalError(strings::Substitute(
                        "Invalid agg function plan: $0 with (arg type $1, serde type $2, result type $3, nullable $4)",
                        func_name, type_to_string(arg_type.type), type_to_string(serde_type.type),
                        type_to_string(return_type.type), is_result_nullable ? "true" : "false"));
            }
            *ret = func;
            VLOG_ROW << "get agg function " << func->get_name() << " serde_type " << serde_type << " return_type "
                     << return_type;
        }
    }
    return Status::OK();
}

Status Aggregator::reset_state(starrocks::RuntimeState* state, const std::vector<ChunkPtr>& refill_chunks,
                               pipeline::Operator* refill_op, bool reset_sink_complete) {
    RETURN_IF_ERROR(_reset_state(state, reset_sink_complete));
    // begin_pending_reset_state just tells the Aggregator, the chunks are intermediate type, it should call
    // merge method of agg functions to process these chunks.
    begin_pending_reset_state();
    for (const auto& chunk : refill_chunks) {
        if (chunk == nullptr || chunk->is_empty()) {
            continue;
        }
        RETURN_IF_ERROR(refill_op->push_chunk(state, chunk));
    }
    end_pending_reset_state();
    return Status::OK();
}

Status Aggregator::_reset_state(RuntimeState* state, bool reset_sink_complete) {
    SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
    _is_ht_eos = false;
    _num_input_rows = 0;
    _is_prepared = false;
    _is_opened = false;
    if (reset_sink_complete) {
        _is_sink_complete = false;
    }
    _cache_conscious_active = false;
    _cache_conscious_ca_spilled = false;
    _cache_conscious_ca.reset();
    _cache_conscious_fa.reset();
    _prune_session.reset();
    _cc_spill_merge.reset();
    _cache_conscious_result_chunk.reset();
    _cache_conscious_result_ready = false;
    _cc_result_offset = 0;
    _cache_conscious_result_emitted = false;
    _cache_conscious_pruned_mask.clear();
    _cc_input_counts = nullptr;
    _cc_count_deltas.reset();
    _inline_chunk = {}; // any pending fold died with the discarded input
    _it_hash.reset();
    _num_rows_processed = 0;
    _num_pass_through_rows = 0;
    _num_rows_returned = 0;

    _limited_buffer->clear();

    _tmp_agg_states.assign(_tmp_agg_states.size(), nullptr);
    _streaming_selection.assign(_streaming_selection.size(), 0);

    DCHECK(_mem_pool != nullptr);
    // Note: we must free agg_states object before _mem_pool free_all;
    if (_group_by_expr_ctxs.empty()) {
        for (int i = 0; i < _agg_functions.size(); i++) {
            _agg_functions[i]->destroy(_agg_fn_ctxs[i], _single_agg_state + _agg_states_offsets[i]);
        }
    } else if (!_is_only_group_by_columns) {
        _release_agg_memory();
    }

#ifndef __APPLE__
    for (int i = 0; i < _agg_functions.size(); i++) {
        if (_agg_fn_ctxs[i] != nullptr && _fns[i].binary_type == TFunctionBinaryType::SRJAR) {
            _agg_fn_ctxs[i]->release_mems();
        }
    }
#endif

    _mem_pool->free_all();
    _agg_state_mem_usage = 0;

    if (_group_by_expr_ctxs.empty()) {
        _single_agg_state = _mem_pool->allocate_aligned(_agg_states_total_size, _max_agg_state_align_size);
        for (int i = 0; i < _agg_functions.size(); i++) {
            _agg_functions[i]->create(_agg_fn_ctxs[i], _single_agg_state + _agg_states_offsets[i]);
        }
    } else if (_is_only_group_by_columns) {
        TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_set_variant));
    } else {
        TRY_CATCH_BAD_ALLOC(_init_agg_hash_variant(_hash_map_variant, /*want_pack=*/_inline_pack));
    }

    // _state_allocator holds the entries of the hash_map/hash_set, when iterating a hash_map/set, the _state_allocator
    // is used to access these entries, so we must reset the _state_allocator along with the hash_map/hash_set.
    _state_allocator.reset();
    // The hash map was re-created small; rebaseline the grow counter so post-reset
    // growth is counted from scratch (reserve is not re-applied after reset).
    _prev_hash_map_capacity = 0;
    return Status::OK();
}

Status Aggregator::spill_aggregate_data(RuntimeState* state, std::function<StatusOr<ChunkPtr>()> chunk_provider) {
    auto spiller = this->spiller();
    auto spill_channel = this->spill_channel();

    while (!spiller->is_full()) {
        auto chunk_with_st = chunk_provider();
        if (chunk_with_st.ok()) {
            if (!chunk_with_st.value()->is_empty()) {
                RETURN_IF_ERROR(
                        spiller->spill(state, chunk_with_st.value(), TRACKER_WITH_SPILLER_GUARD(state, spiller)));
            }
        } else if (chunk_with_st.status().is_end_of_file()) {
            // chunk_provider return eos means provider has output all data from hash_map/hash_set.
            // then we just return OK
            return Status::OK();
        } else {
            return chunk_with_st.status();
        }
    }

    spill_channel->add_spill_task(std::move(chunk_provider));

    return Status::OK();
}

void Aggregator::close(RuntimeState* state) {
    if (_is_closed) {
        return;
    }

    _is_closed = true;
    // Clear the buffer
    if (_limited_buffer != nullptr) {
        _limited_buffer->clear();
    }

    auto agg_close = [this, state]() {
        // _mem_pool is nullptr means prepare phase failed
        if (_mem_pool != nullptr) {
            // Note: we must free agg_states object before _mem_pool free_all;
            if (_single_agg_state != nullptr) {
                SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
                for (int i = 0; i < _agg_functions.size(); i++) {
                    _agg_functions[i]->destroy(_agg_fn_ctxs[i], _single_agg_state + _agg_states_offsets[i]);
                }
            } else if (!_is_only_group_by_columns) {
                _release_agg_memory();
            }

            _mem_pool->free_all();
        }

#ifndef __APPLE__
        for (int i = 0; i < _agg_functions.size(); i++) {
            if (_agg_fn_ctxs[i] != nullptr && _fns[i].binary_type == TFunctionBinaryType::SRJAR) {
                _agg_fn_ctxs[i]->release_mems();
            }
        }
#endif

        if (_is_only_group_by_columns) {
            _hash_set_variant.reset();
        } else {
            _hash_map_variant.reset();
        }

        Expr::close(_group_by_expr_ctxs, state);
        for (const auto& i : _agg_expr_ctxs) {
            Expr::close(i, state);
        }
        Expr::close(_conjunct_ctxs, state);

        for (auto* func : _combinator_function) {
            delete func;
        }
        _combinator_function.clear();

        return Status::OK();
    };
#ifdef __APPLE__
    (void)agg_close();
#else
    if (_has_udaf) {
        auto promise_st = call_function_in_pthread(state, agg_close);
        (void)promise_st->get_future().get();
    } else {
        (void)agg_close();
    }
#endif
    _spiller.reset();
}

bool Aggregator::is_chunk_buffer_empty() {
    return _limited_buffer->is_empty();
}

ChunkPtr Aggregator::poll_chunk_buffer() {
    auto notify = defer_notify_sink();
    return _limited_buffer->pull();
}

void Aggregator::offer_chunk_to_buffer(const ChunkPtr& chunk) {
    auto notify = defer_notify_source();
    _limited_buffer->push(chunk);
}

bool Aggregator::is_chunk_buffer_full() {
    return _limited_buffer->is_full();
}

bool Aggregator::should_expand_preagg_hash_tables(size_t prev_row_returned, size_t input_chunk_size, int64_t ht_mem,
                                                  int64_t ht_rows) const {
    // Need some rows in tables to have valid statistics.
    if (ht_rows == 0) {
        return true;
    }

    // Find the appropriate reduction factor in our table for the current hash table sizes.
    int cache_level = 0;
    while (cache_level + 1 < STREAMING_HT_MIN_REDUCTION_SIZE &&
           ht_mem >= STREAMING_HT_MIN_REDUCTION[cache_level + 1].min_ht_mem) {
        cache_level++;
    }

    // Compare the number of rows in the hash table with the number of input rows that
    // were aggregated into it. Exclude passed through rows from this calculation since
    // they were not in hash tables.
    const int64_t input_rows = prev_row_returned - input_chunk_size;
    const int64_t aggregated_input_rows = input_rows - _num_rows_returned;
    double current_reduction = static_cast<double>(aggregated_input_rows) / ht_rows;

    // inaccurate, which could lead to a divide by zero below.
    if (aggregated_input_rows <= 0) {
        return true;
    }
    // Extrapolate the current reduction factor (r) using the formula
    // R = 1 + (N / n) * (r - 1), where R is the reduction factor over the full input data
    // set, N is the number of input rows, excluding passed-through rows, and n is the
    // number of rows inserted or merged into the hash tables. This is a very rough
    // approximation but is good enough to be useful.
    double min_reduction = STREAMING_HT_MIN_REDUCTION[cache_level].streaming_ht_min_reduction;
    return current_reduction > min_reduction;
}

Status Aggregator::evaluate_agg_input_column(Chunk* chunk, std::vector<ExprContext*>& agg_expr_ctxs, int i) {
    SCOPED_TIMER(_agg_stat->expr_compute_timer);
    for (size_t j = 0; j < agg_expr_ctxs.size(); j++) {
        // _agg_input_raw_columns[i][j] != nullptr means this column has been evaluated
        if (_agg_input_raw_columns[i][j] != nullptr) {
            continue;
        }
        // For simplicity and don't change the overall processing flow,
        // We handle const column as normal data column
        // TODO(kks): improve const column aggregate later
        ASSIGN_OR_RETURN(auto&& col, agg_expr_ctxs[j]->evaluate(chunk));
        // if first column is const, we have to unpack it. Most agg function only has one arg, and treat it as non-const column
        if (j == 0) {
            _agg_input_columns[i][j] =
                    ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), std::move(col));
        } else {
            // if function has at least two argument, unpack const column selectively
            // for function like percentile_disc, the second args is const, do not unpack it
            // NOTE: an argument that the analyzer saw as non-constant can still be constant here,
            // because the optimizer folds constants after analysis. Every aggregate function that
            // reads an argument other than the first one must therefore cope with a const column
            // (see `GetContainer` / `ColumnHelper::get_data_column`), it cannot assume the column
            // has the concrete type of its argument.
            if (agg_expr_ctxs[j]->root()->is_constant()) {
                _agg_input_columns[i][j] = std::move(col);
            } else {
                _agg_input_columns[i][j] =
                        ColumnHelper::unpack_and_duplicate_const_column(chunk->num_rows(), std::move(col));
            }
        }
        _agg_input_raw_columns[i][j] = _agg_input_columns[i][j].get();
    }
    return Status::OK();
}

Status Aggregator::compute_single_agg_state(Chunk* chunk, size_t chunk_size) {
    SCOPED_TIMER(_agg_stat->agg_function_compute_timer);
    bool use_intermediate = _use_intermediate_as_input();
    auto& agg_expr_ctxs = use_intermediate ? _intermediate_agg_expr_ctxs : _agg_expr_ctxs;

    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        // evaluate arguments at i-th agg function
        RETURN_IF_ERROR(evaluate_agg_input_column(chunk, agg_expr_ctxs[i], i));
        SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
        // batch call update or merge for singe stage
        if (!_is_merge_funcs[i] && !use_intermediate) {
            _agg_functions[i]->update_batch_single_state_exception_safe(_agg_fn_ctxs[i], chunk_size,
                                                                        _agg_input_raw_columns[i].data(),
                                                                        _single_agg_state + _agg_states_offsets[i]);
        } else {
            DCHECK_GE(_agg_input_columns[i].size(), 1);
            _agg_functions[i]->merge_batch_single_state_exception_safe(_agg_fn_ctxs[i],
                                                                       _single_agg_state + _agg_states_offsets[i],
                                                                       _agg_input_columns[i][0].get(), 0, chunk_size);
        }
    }
    RETURN_IF_ERROR(check_has_error());
    return Status::OK();
}

template <typename Op>
static void inline_agg_fold_dispatch(AggHashMapVariant& variant, size_t chunk_size, const Columns& group_by_columns,
                                     MemPool* pool, Buffer<AggDataPtr>* agg_states,
                                     const typename Op::DeltaType* partials, const Filter* selection = nullptr);
template <typename Op>
static void inline_agg_commit(AggHashMapVariant& variant, size_t chunk_size, const Columns& group_by_columns,
                              Buffer<AggDataPtr>* scratch, const Filter* selection, typename Op::DeltaType delta);

// Typed merge fold: evaluate the intermediate partial column the same way the general merge
// path does, unwrap a possible (NULL-free) nullable wrapper and fold slot += partial for the
// kept rows. The fold lazy_emplaces every non-streamed key itself, so it serves the no-op
// build, the allocate builds (groups pre-created at identity) and the group-by-limit classify
// build alike.
template <typename Op, typename PartialColumnT>
Status Aggregator::_inline_fold_merge_chunk(Chunk* chunk, size_t chunk_size, const Filter* selection) {
    bool use_intermediate = _use_intermediate_as_input();
    auto& agg_expr_ctxs = use_intermediate ? _intermediate_agg_expr_ctxs : _agg_expr_ctxs;
    RETURN_IF_ERROR(evaluate_agg_input_column(chunk, agg_expr_ctxs[0], 0));
    const Column* input = _agg_input_columns[0][0].get();
    const Column* data = input->is_nullable() ? down_cast<const NullableColumn*>(input)->data_column().get() : input;
    const auto* partial = down_cast<const PartialColumnT*>(data);
    if (_tmp_agg_states.size() < chunk_size) {
        _tmp_agg_states.resize(chunk_size);
    }
    inline_agg_fold_dispatch<Op>(_hash_map_variant, chunk_size, _group_by_columns, _mem_pool.get(), &_tmp_agg_states,
                                 partial->get_data().data(), selection);
    return check_has_error();
}

// Typed sum update fold: the deltas are the input values themselves. A value column whose
// type already equals the accumulator folds zero-copy; a narrow type is widened into the
// delta scratch (the int64 buffer doubles as a double buffer -- same 8B/row).
template <typename Op, typename DeltaColumnT>
Status Aggregator::_inline_fold_sum_update_chunk(Chunk* chunk, size_t chunk_size, const Filter* selection) {
    using DeltaT = typename Op::DeltaType;
    const Column* input = _agg_input_columns[0][0].get();
    // The gate only admits the non-nullable-resolved sum, so a nullable wrapper here carries
    // no NULLs; unwrap it for the raw values.
    const Column* data = input->is_nullable() ? down_cast<const NullableColumn*>(input)->data_column().get() : input;
    const auto* values = down_cast<const DeltaColumnT*>(data);
    const DeltaT* deltas;
    if constexpr (std::is_same_v<typename DeltaColumnT::ValueType, DeltaT>) {
        deltas = values->get_data().data();
    } else {
        if (_inline_delta_scratch.size() < chunk_size) {
            _inline_delta_scratch.resize(chunk_size);
        }
        auto* out = reinterpret_cast<DeltaT*>(_inline_delta_scratch.data());
        const auto& in = values->get_data();
        for (size_t i = 0; i < chunk_size; ++i) {
            out[i] = static_cast<DeltaT>(in[i]);
        }
        deltas = out;
    }
    if (_tmp_agg_states.size() < chunk_size) {
        _tmp_agg_states.resize(chunk_size);
    }
    inline_agg_fold_dispatch<Op>(_hash_map_variant, chunk_size, _group_by_columns, _mem_pool.get(), &_tmp_agg_states,
                                 deltas, selection);
    return check_has_error();
}

// Dispatch a sum update chunk by the runtime input column type (the gate guarantees one of
// these): int family widens into the int64 accumulator, float widens into double.
Status Aggregator::_inline_sum_int_update(Chunk* chunk, size_t chunk_size, const Filter* selection) {
    const Column* in_col = _agg_input_columns[0][0].get();
    const Column* in = in_col->is_nullable() ? down_cast<const NullableColumn*>(in_col)->data_column().get() : in_col;
    if (typeid(*in) == typeid(Int64Column)) {
        return _inline_fold_sum_update_chunk<InlineAddOp<int64_t>, Int64Column>(chunk, chunk_size, selection);
    }
    if (typeid(*in) == typeid(Int32Column)) {
        return _inline_fold_sum_update_chunk<InlineAddOp<int64_t>, Int32Column>(chunk, chunk_size, selection);
    }
    if (typeid(*in) == typeid(Int16Column)) {
        return _inline_fold_sum_update_chunk<InlineAddOp<int64_t>, Int16Column>(chunk, chunk_size, selection);
    }
    if (typeid(*in) == typeid(Int8Column)) {
        return _inline_fold_sum_update_chunk<InlineAddOp<int64_t>, Int8Column>(chunk, chunk_size, selection);
    }
    if (typeid(*in) == typeid(BooleanColumn)) {
        return _inline_fold_sum_update_chunk<InlineAddOp<int64_t>, BooleanColumn>(chunk, chunk_size, selection);
    }
    DCHECK(false) << "unexpected inline sum(int) input column " << in->get_name();
    return Status::InternalError("inline sum: unexpected input column type");
}

Status Aggregator::_inline_sum_double_update(Chunk* chunk, size_t chunk_size, const Filter* selection) {
    const Column* in_col = _agg_input_columns[0][0].get();
    const Column* in = in_col->is_nullable() ? down_cast<const NullableColumn*>(in_col)->data_column().get() : in_col;
    if (typeid(*in) == typeid(DoubleColumn)) {
        return _inline_fold_sum_update_chunk<InlineAddOp<double>, DoubleColumn>(chunk, chunk_size, selection);
    }
    if (typeid(*in) == typeid(FloatColumn)) {
        return _inline_fold_sum_update_chunk<InlineAddOp<double>, FloatColumn>(chunk, chunk_size, selection);
    }
    DCHECK(false) << "unexpected inline sum(double) input column " << in->get_name();
    return Status::InternalError("inline sum: unexpected input column type");
}

#define INLINE_MINMAX_LT_CASES(M) \
    M(TYPE_TINYINT)               \
    M(TYPE_SMALLINT)              \
    M(TYPE_INT)                   \
    M(TYPE_BIGINT)                \
    M(TYPE_DATE)                  \
    M(TYPE_DATETIME)              \
    M(TYPE_FLOAT)                 \
    M(TYPE_DOUBLE)

// min/max update fold: the deltas are the raw input values (the slot holds T itself, so the
// typed column folds zero-copy; the gate admits only non-nullable-resolved min/max, so a
// nullable wrapper carries no NULLs).
Status Aggregator::_inline_minmax_update(Chunk* chunk, size_t chunk_size, const Filter* selection) {
    const bool is_min = _inline_op == InlineOpKind::kMin;
    switch (_inline_minmax_lt) {
#define M(LT)                                                                                            \
    case LT:                                                                                             \
        return is_min ? _inline_fold_sum_update_chunk<InlineMinMaxOp<LT, true>, RunTimeColumnType<LT>>(  \
                                chunk, chunk_size, selection)                                            \
                      : _inline_fold_sum_update_chunk<InlineMinMaxOp<LT, false>, RunTimeColumnType<LT>>( \
                                chunk, chunk_size, selection);
        INLINE_MINMAX_LT_CASES(M)
#undef M
    default:
        DCHECK(false) << "unexpected inline minmax type " << _inline_minmax_lt;
        return Status::InternalError("inline minmax: unexpected type");
    }
}

// min/max merge fold (query-cache refill of this aggregator's own NULL-free intermediates):
// the partials are raw T values of the same type.
Status Aggregator::_inline_minmax_merge(Chunk* chunk, size_t chunk_size, const Filter* selection) {
    const bool is_min = _inline_op == InlineOpKind::kMin;
    switch (_inline_minmax_lt) {
#define M(LT)                                                                                                         \
    case LT:                                                                                                          \
        return is_min ? _inline_fold_merge_chunk<InlineMinMaxOp<LT, true>, RunTimeColumnType<LT>>(chunk, chunk_size,  \
                                                                                                  selection)          \
                      : _inline_fold_merge_chunk<InlineMinMaxOp<LT, false>, RunTimeColumnType<LT>>(chunk, chunk_size, \
                                                                                                   selection);
        INLINE_MINMAX_LT_CASES(M)
#undef M
    default:
        DCHECK(false) << "unexpected inline minmax type " << _inline_minmax_lt;
        return Status::InternalError("inline minmax: unexpected type");
    }
}

// Build-entry dispatch: every creating build must store the ACTIVE op's identity image into a
// new group's slot. The additive family all share the all-zero identity, so they ride the
// count op (delta 1 fused / 0 deferred -- a zero delta is a bitwise no-op); min/max creates
// with its typed identity and a delta equal to the identity (combine(identity, identity) ==
// identity), so a creating build never disturbs the slot either.
// Pack build: one op instance per pack arity. A fused build (every field count(*)) adds the
// all-ones delta per row; a creating/classifying build folds the all-zero delta -- bitwise
// neutral over the zero identity cell, exactly like the single-op additive family.
template <typename HTBuildOp>
void Aggregator::_inline_pack_dispatch_build(size_t chunk_size, Filter* not_founds, size_t limit, bool fused) {
    InlinePackDelta delta{};
    if (fused) {
        for (uint8_t k = 0; k < _inline_pack_n; ++k) {
            delta.d[k] = 1;
        }
    }
    switch (_inline_pack_n) {
    case 2:
        inline_agg_build<InlinePackOp<2>, HTBuildOp>(_hash_map_variant, chunk_size, _group_by_columns, _mem_pool.get(),
                                                     &_tmp_agg_states, not_founds, limit, delta);
        return;
    case 3:
        inline_agg_build<InlinePackOp<3>, HTBuildOp>(_hash_map_variant, chunk_size, _group_by_columns, _mem_pool.get(),
                                                     &_tmp_agg_states, not_founds, limit, delta);
        return;
    case 4:
        inline_agg_build<InlinePackOp<4>, HTBuildOp>(_hash_map_variant, chunk_size, _group_by_columns, _mem_pool.get(),
                                                     &_tmp_agg_states, not_founds, limit, delta);
        return;
    default:
        DCHECK(false) << "unexpected inline pack arity " << static_cast<int>(_inline_pack_n);
    }
}

template <typename HTBuildOp>
void Aggregator::_inline_dispatch_build(size_t chunk_size, Filter* not_founds, size_t limit, bool fused_count) {
    if (_inline_pack) {
        _inline_pack_dispatch_build<HTBuildOp>(chunk_size, not_founds, limit, fused_count);
        return;
    }
    switch (_inline_op) {
    case InlineOpKind::kCountStar:
    case InlineOpKind::kCountCol:
    case InlineOpKind::kSumInt:
    case InlineOpKind::kSumDouble:
        inline_agg_build<InlineAddOp<int64_t>, HTBuildOp>(_hash_map_variant, chunk_size, _group_by_columns,
                                                          _mem_pool.get(), &_tmp_agg_states, not_founds, limit,
                                                          fused_count ? 1 : 0);
        return;
    case InlineOpKind::kMin:
    case InlineOpKind::kMax: {
        const bool is_min = _inline_op == InlineOpKind::kMin;
        switch (_inline_minmax_lt) {
#define M(LT)                                                                                                        \
    case LT:                                                                                                         \
        if (is_min) {                                                                                                \
            inline_agg_build<InlineMinMaxOp<LT, true>, HTBuildOp>(_hash_map_variant, chunk_size, _group_by_columns,  \
                                                                  _mem_pool.get(), &_tmp_agg_states, not_founds,     \
                                                                  limit, InlineMinMaxOp<LT, true>::identity());      \
        } else {                                                                                                     \
            inline_agg_build<InlineMinMaxOp<LT, false>, HTBuildOp>(_hash_map_variant, chunk_size, _group_by_columns, \
                                                                   _mem_pool.get(), &_tmp_agg_states, not_founds,    \
                                                                   limit, InlineMinMaxOp<LT, false>::identity());    \
        }                                                                                                            \
        return;
            INLINE_MINMAX_LT_CASES(M)
#undef M
        default:
            DCHECK(false) << "unexpected inline minmax type " << _inline_minmax_lt;
            return;
        }
    }
    }
    __builtin_unreachable();
}

// Materialize the kCountCol update deltas from the (already evaluated) input column: 1 for a
// non-null row, 0 for a null one. A non-nullable or null-free column means all-ones. The
// scratch lives until the end of the compute call.
const int64_t* Aggregator::_compute_count_col_deltas(size_t chunk_size) {
    if (_inline_delta_scratch.size() < chunk_size) {
        _inline_delta_scratch.resize(chunk_size);
    }
    const Column* input = _agg_input_columns[0][0].get();
    if (input->is_nullable() && input->has_null()) {
        const auto& null_data = down_cast<const NullableColumn*>(input)->null_column_data();
        for (size_t i = 0; i < chunk_size; ++i) {
            _inline_delta_scratch[i] = null_data[i] == 0;
        }
    } else {
        std::fill_n(_inline_delta_scratch.begin(), chunk_size, 1);
    }
    return _inline_delta_scratch.data();
}

// Materialize the chunk's per-row pack deltas (AoS, 32B per row in the delta scratch).
// On an update chunk field k's delta is aggregate k's per-row contribution (1 / !null[i] /
// the widened sum input); on a merge chunk it is aggregate k's BIGINT partial. Field
// argument evaluation happens here for every member, so an erroring argument expression
// fails identically with the pack on or off.
StatusOr<const InlinePackDelta*> Aggregator::_compute_pack_deltas(Chunk* chunk, size_t chunk_size, bool is_merge) {
    if (_inline_delta_scratch.size() < chunk_size * 4) {
        _inline_delta_scratch.resize(chunk_size * 4);
    }
    auto* out = reinterpret_cast<InlinePackDelta*>(_inline_delta_scratch.data());
    const bool use_intermediate = _use_intermediate_as_input();
    auto& expr_ctxs = (is_merge && use_intermediate) ? _intermediate_agg_expr_ctxs : _agg_expr_ctxs;
    for (uint8_t k = 0; k < _inline_pack_n; ++k) {
        RETURN_IF_ERROR(evaluate_agg_input_column(chunk, expr_ctxs[k], k));
        if (is_merge) {
            const Column* in = _agg_input_columns[k][0].get();
            const Column* data = in->is_nullable() ? down_cast<const NullableColumn*>(in)->data_column().get() : in;
            const auto& vals = down_cast<const Int64Column*>(data)->get_data();
            for (size_t i = 0; i < chunk_size; ++i) {
                out[i].d[k] = vals[i];
            }
            continue;
        }
        switch (_inline_pack_ops[k]) {
        case InlineOpKind::kCountStar:
            for (size_t i = 0; i < chunk_size; ++i) {
                out[i].d[k] = 1;
            }
            break;
        case InlineOpKind::kCountCol: {
            const Column* input = _agg_input_columns[k][0].get();
            if (input->is_nullable() && input->has_null()) {
                const auto& null_data = down_cast<const NullableColumn*>(input)->null_column_data();
                for (size_t i = 0; i < chunk_size; ++i) {
                    out[i].d[k] = null_data[i] == 0;
                }
            } else {
                for (size_t i = 0; i < chunk_size; ++i) {
                    out[i].d[k] = 1;
                }
            }
            break;
        }
        case InlineOpKind::kSumInt: {
            const Column* in_col = _agg_input_columns[k][0].get();
            const Column* in =
                    in_col->is_nullable() ? down_cast<const NullableColumn*>(in_col)->data_column().get() : in_col;
            auto widen = [&](const auto* typed) {
                const auto& vals = typed->get_data();
                for (size_t i = 0; i < chunk_size; ++i) {
                    out[i].d[k] = static_cast<int64_t>(vals[i]);
                }
            };
            if (typeid(*in) == typeid(Int64Column)) {
                widen(down_cast<const Int64Column*>(in));
            } else if (typeid(*in) == typeid(Int32Column)) {
                widen(down_cast<const Int32Column*>(in));
            } else if (typeid(*in) == typeid(Int16Column)) {
                widen(down_cast<const Int16Column*>(in));
            } else if (typeid(*in) == typeid(Int8Column)) {
                widen(down_cast<const Int8Column*>(in));
            } else if (typeid(*in) == typeid(BooleanColumn)) {
                widen(down_cast<const BooleanColumn*>(in));
            } else {
                return Status::InternalError("unexpected inline pack sum input column");
            }
            break;
        }
        default:
            return Status::InternalError("unexpected inline pack member op");
        }
    }
    return out;
}

// Pack compute: consume the captured chunk state for a 2..4-aggregate pack. The fold/commit
// arity dispatch instantiates one op per pack size; the protocol (committed / fold-all /
// fold-selection x update / merge) is exactly the single-op one.
Status Aggregator::_inline_pack_compute(Chunk* chunk, size_t chunk_size, const Filter* selection,
                                        InlineChunkState chunk_state) {
    if (!chunk_state.is_merge && _inline_pack_fused) {
        // All-count(*) pack: argument expressions still get evaluated for error parity.
        for (uint8_t k = 0; k < _inline_pack_n; ++k) {
            RETURN_IF_ERROR(evaluate_agg_input_column(chunk, _agg_expr_ctxs[k], k));
        }
        DCHECK(chunk_state.fold == InlineChunkState::kCommitted ||
               chunk_state.fold == InlineChunkState::kFoldSelection);
        if (chunk_state.fold == InlineChunkState::kFoldSelection) {
            InlinePackDelta unit{};
            for (uint8_t k = 0; k < _inline_pack_n; ++k) {
                unit.d[k] = 1;
            }
            switch (_inline_pack_n) {
            case 2:
                inline_agg_commit<InlinePackOp<2>>(_hash_map_variant, chunk_size, _group_by_columns, &_tmp_agg_states,
                                                   selection, unit);
                break;
            case 3:
                inline_agg_commit<InlinePackOp<3>>(_hash_map_variant, chunk_size, _group_by_columns, &_tmp_agg_states,
                                                   selection, unit);
                break;
            case 4:
                inline_agg_commit<InlinePackOp<4>>(_hash_map_variant, chunk_size, _group_by_columns, &_tmp_agg_states,
                                                   selection, unit);
                break;
            default:
                DCHECK(false);
            }
        }
        return check_has_error();
    }
    DCHECK(chunk_state.fold == InlineChunkState::kFoldAll || chunk_state.fold == InlineChunkState::kFoldSelection);
    ASSIGN_OR_RETURN(const InlinePackDelta* deltas, _compute_pack_deltas(chunk, chunk_size, chunk_state.is_merge));
    switch (_inline_pack_n) {
    case 2:
        inline_agg_fold_dispatch<InlinePackOp<2>>(_hash_map_variant, chunk_size, _group_by_columns, _mem_pool.get(),
                                                  &_tmp_agg_states, deltas, selection);
        break;
    case 3:
        inline_agg_fold_dispatch<InlinePackOp<3>>(_hash_map_variant, chunk_size, _group_by_columns, _mem_pool.get(),
                                                  &_tmp_agg_states, deltas, selection);
        break;
    case 4:
        inline_agg_fold_dispatch<InlinePackOp<4>>(_hash_map_variant, chunk_size, _group_by_columns, _mem_pool.get(),
                                                  &_tmp_agg_states, deltas, selection);
        break;
    default:
        DCHECK(false);
    }
    return check_has_error();
}

Status Aggregator::compute_batch_agg_states(Chunk* chunk, size_t chunk_size) {
    SCOPED_TIMER(_agg_stat->agg_function_compute_timer);
    if (_inline_agg) {
        // Consume the chunk state captured by this chunk's build entry (never re-evaluate the
        // merge predicate here -- the capture is what guarantees build/compute consistency).
        const InlineChunkState chunk_state = _inline_chunk;
        _inline_chunk.fold = InlineChunkState::kConsumed;
        if (_inline_pack) {
            return _inline_pack_compute(chunk, chunk_size, nullptr, chunk_state);
        }
        if (!chunk_state.is_merge) {
            // Update chunk. The count argument is evaluated exactly like the general path does:
            // kCountCol reads its null mask as the deltas; for kCountStar it is discarded, but an
            // erroring argument expression -- count(1/x) under a strict mode, a throwing UDF --
            // must fail the query identically with inline on or off (for count(*) the expr list
            // is empty and for a bare column it is a pass-through).
            RETURN_IF_ERROR(evaluate_agg_input_column(chunk, _agg_expr_ctxs[0], 0));
            if (!_inline_op_is_fused()) {
                // Per-row-delta op: every update chunk arrives as a deferred fold (the build
                // created/classified only); the non-selective entry is the all-kept branch, so
                // the fold runs unmasked. kCountCol folds !null[i] of the input column, the sum
                // ops fold the input values themselves.
                DCHECK(chunk_state.fold == InlineChunkState::kFoldAll ||
                       chunk_state.fold == InlineChunkState::kFoldSelection);
                switch (_inline_op) {
                case InlineOpKind::kCountCol: {
                    const int64_t* deltas = _compute_count_col_deltas(chunk_size);
                    inline_agg_fold_dispatch<InlineAddOp<int64_t>>(_hash_map_variant, chunk_size, _group_by_columns,
                                                                   _mem_pool.get(), &_tmp_agg_states, deltas);
                    RETURN_IF_ERROR(check_has_error());
                    return Status::OK();
                }
                case InlineOpKind::kSumInt:
                    return _inline_sum_int_update(chunk, chunk_size, nullptr);
                case InlineOpKind::kSumDouble:
                    return _inline_sum_double_update(chunk, chunk_size, nullptr);
                case InlineOpKind::kMin:
                case InlineOpKind::kMax:
                    return _inline_minmax_update(chunk, chunk_size, nullptr);
                default:
                    __builtin_unreachable();
                }
            }
            // kCountStar: a fused/allocate build already counted in place (kCommitted -> no-op
            // fold); a classifying build left kFoldSelection: commit the +1 now by re-probing the
            // keys -- this non-selective entry is the all-hit branch, so every row commits
            // (selection null).
            DCHECK(chunk_state.fold == InlineChunkState::kCommitted ||
                   chunk_state.fold == InlineChunkState::kFoldSelection);
            if (chunk_state.fold == InlineChunkState::kFoldSelection) {
                inline_agg_commit<InlineAddOp<int64_t>>(_hash_map_variant, chunk_size, _group_by_columns,
                                                        &_tmp_agg_states, nullptr, 1);
            }
            return Status::OK();
        }
        // Merge chunk: fold the typed NULL-free partials; on this non-selective entry the
        // operator guarantees every row is kept, so the fold runs unmasked.
        if (_inline_op == InlineOpKind::kMin || _inline_op == InlineOpKind::kMax) {
            return _inline_minmax_merge(chunk, chunk_size, nullptr);
        }
        if (_inline_op == InlineOpKind::kSumDouble) {
            return _inline_fold_merge_chunk<InlineAddOp<double>, DoubleColumn>(chunk, chunk_size, nullptr);
        }
        return _inline_fold_merge_chunk<InlineAddOp<int64_t>, Int64Column>(chunk, chunk_size, nullptr);
    }
    bool use_intermediate = _use_intermediate_as_input();
    auto& agg_expr_ctxs = use_intermediate ? _intermediate_agg_expr_ctxs : _agg_expr_ctxs;

    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        // evaluate arguments at i-th agg function
        RETURN_IF_ERROR(evaluate_agg_input_column(chunk, agg_expr_ctxs[i], i));
        SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
        // batch call update or merge
        if (!_is_merge_funcs[i] && !use_intermediate) {
            _agg_functions[i]->update_batch_exception_safe(_agg_fn_ctxs[i], chunk_size, _agg_states_offsets[i],
                                                           _agg_input_raw_columns[i].data(), _tmp_agg_states.data());
        } else {
            DCHECK_GE(_agg_input_columns[i].size(), 1);
            _agg_functions[i]->merge_batch_exception_safe(_agg_fn_ctxs[i], _agg_input_columns[i][0]->size(),
                                                          _agg_states_offsets[i], _agg_input_columns[i][0].get(),
                                                          _tmp_agg_states.data());
        }
    }
    RETURN_IF_ERROR(check_has_error());
    return Status::OK();
}

Status Aggregator::compute_batch_agg_states_with_selection(Chunk* chunk, size_t chunk_size) {
    SCOPED_TIMER(_agg_stat->agg_function_compute_timer);
    if (_inline_agg) {
        const InlineChunkState chunk_state = _inline_chunk;
        _inline_chunk.fold = InlineChunkState::kConsumed;
        if (_inline_pack) {
            return _inline_pack_compute(chunk, chunk_size, &_streaming_selection, chunk_state);
        }
        // A selective compute follows either a classify build (kFoldSelection) or a counting
        // limited build whose over-limit rows go to streaming (kCommitted, update only).
        DCHECK(chunk_state.fold == InlineChunkState::kFoldSelection ||
               (chunk_state.fold == InlineChunkState::kCommitted && !chunk_state.is_merge));
        if (chunk_state.is_merge) {
            // Merge chunk on a selective path (spill preaggregation, or a group-by-limit build
            // that streamed the over-limit keys): the kept rows (selection==0) carry typed
            // partials from the intermediate column; streamed rows (selection==1) leave
            // unchanged and are skipped by the selection mask.
            if (_inline_op == InlineOpKind::kMin || _inline_op == InlineOpKind::kMax) {
                return _inline_minmax_merge(chunk, chunk_size, &_streaming_selection);
            }
            if (_inline_op == InlineOpKind::kSumDouble) {
                return _inline_fold_merge_chunk<InlineAddOp<double>, DoubleColumn>(chunk, chunk_size,
                                                                                   &_streaming_selection);
            }
            return _inline_fold_merge_chunk<InlineAddOp<int64_t>, Int64Column>(chunk, chunk_size,
                                                                               &_streaming_selection);
        }
        // Update chunk on the selective path. Evaluate the argument for error parity with the
        // general path (the per-row-delta ops also read it as the delta source).
        RETURN_IF_ERROR(evaluate_agg_input_column(chunk, _agg_expr_ctxs[0], 0));
        if (!_inline_op_is_fused()) {
            // Fold the kept rows' deltas; streamed rows (selection==1) are skipped.
            switch (_inline_op) {
            case InlineOpKind::kCountCol: {
                const int64_t* deltas = _compute_count_col_deltas(chunk_size);
                inline_agg_fold_dispatch<InlineAddOp<int64_t>>(_hash_map_variant, chunk_size, _group_by_columns,
                                                               _mem_pool.get(), &_tmp_agg_states, deltas,
                                                               &_streaming_selection);
                RETURN_IF_ERROR(check_has_error());
                return Status::OK();
            }
            case InlineOpKind::kSumInt:
                return _inline_sum_int_update(chunk, chunk_size, &_streaming_selection);
            case InlineOpKind::kSumDouble:
                return _inline_sum_double_update(chunk, chunk_size, &_streaming_selection);
            case InlineOpKind::kMin:
            case InlineOpKind::kMax:
                return _inline_minmax_update(chunk, chunk_size, &_streaming_selection);
            default:
                __builtin_unreachable();
            }
        }
        // kCountStar: commit the classifying selective build by re-probing kept rows (+1 each);
        // a counting limited build (kCommitted) already added everything in place -- no-op fold.
        if (chunk_state.fold == InlineChunkState::kFoldSelection) {
            inline_agg_commit<InlineAddOp<int64_t>>(_hash_map_variant, chunk_size, _group_by_columns, &_tmp_agg_states,
                                                    &_streaming_selection, 1);
        }
        return Status::OK();
    }
    bool use_intermediate = _use_intermediate_as_input();
    auto& agg_expr_ctxs = use_intermediate ? _intermediate_agg_expr_ctxs : _agg_expr_ctxs;

    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        RETURN_IF_ERROR(evaluate_agg_input_column(chunk, agg_expr_ctxs[i], i));
        SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
        if (!_is_merge_funcs[i] && !use_intermediate) {
            _agg_functions[i]->update_batch_selectively_exception_safe(
                    _agg_fn_ctxs[i], chunk_size, _agg_states_offsets[i], _agg_input_raw_columns[i].data(),
                    _tmp_agg_states.data(), _streaming_selection);
        } else {
            DCHECK_GE(_agg_input_columns[i].size(), 1);
            _agg_functions[i]->merge_batch_selectively_exception_safe(
                    _agg_fn_ctxs[i], _agg_input_columns[i][0]->size(), _agg_states_offsets[i],
                    _agg_input_columns[i][0].get(), _tmp_agg_states.data(), _streaming_selection);
        }
    }
    RETURN_IF_ERROR(check_has_error());
    return Status::OK();
}

RuntimeFilter* Aggregator::build_in_filters(RuntimeState* state, RuntimeFilterBuildDescriptor* desc) {
    if (desc->type() != TRuntimeFilterBuildType::AGG_FILTER) {
        return nullptr;
    }
    int expr_order = desc->build_expr_order();
    const auto& group_type_type = _group_by_types[expr_order].result_type.type;
    AggInRuntimeFilterBuilder in_builder(desc, group_type_type);
    return in_builder.build(this, state->obj_pool());
}

RuntimeFilter* Aggregator::build_topn_filters(RuntimeState* state, RuntimeFilterBuildDescriptor* desc) {
    if (desc->type() != TRuntimeFilterBuildType::TOPN_FILTER) {
        return nullptr;
    }
    int expr_order = desc->build_expr_order();
    const auto& group_type_type = _group_by_types[expr_order].result_type.type;
    // only build when group by keys's size is less than limit
    if (size() < desc->limit()) {
        return nullptr;
    }

    if (_topn_runtime_filter_builder == nullptr) {
        // for the first time to build the topn runtime filter
        _topn_runtime_filter_builder = new AggTopNRuntimeFilterBuilder(desc, group_type_type);
        _pool->add(_topn_runtime_filter_builder);
        return _topn_runtime_filter_builder->build(this, state->obj_pool());
    } else {
        return _topn_runtime_filter_builder->runtime_filter();
    }
}

Status Aggregator::_evaluate_const_columns(int i) {
    // used for const columns.
    Columns const_columns;
    const_columns.reserve(_agg_expr_ctxs[i].size());
    for (auto& j : _agg_expr_ctxs[i]) {
        ASSIGN_OR_RETURN(auto col, j->root()->evaluate_const(j));
        const_columns.emplace_back(std::move(col));
    }
    _agg_fn_ctxs[i]->set_constant_columns(const_columns);
    return Status::OK();
}

Status Aggregator::convert_to_chunk_no_groupby(ChunkPtr* chunk) {
    SCOPED_TIMER(_agg_stat->get_results_timer);
    // TODO(kks): we should approve memory allocate here
    auto use_intermediate = _use_intermediate_as_output();
    MutableColumns agg_result_column = _create_agg_result_columns(1, use_intermediate);
    SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
    if (!use_intermediate) {
        TRY_CATCH_BAD_ALLOC(_finalize_to_chunk(_single_agg_state, agg_result_column));
    } else {
        TRY_CATCH_BAD_ALLOC(_serialize_to_chunk(_single_agg_state, agg_result_column));
    }
    RETURN_IF_ERROR(check_has_error());
    // For agg function column is non-nullable and table is empty
    // sum(zero_row) should be null, not 0.
    if (UNLIKELY(_num_input_rows == 0 && _group_by_expr_ctxs.empty() && !use_intermediate)) {
        for (size_t i = 0; i < _agg_fn_types.size(); i++) {
            if (_agg_fn_types[i].is_nullable) {
                agg_result_column[i] = ColumnHelper::create_column(_agg_fn_types[i].result_type, true);
                agg_result_column[i]->append_default();
            }
        }
    }

    TupleDescriptor* tuple_desc = use_intermediate ? _intermediate_tuple_desc : _output_tuple_desc;

    ChunkPtr result_chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < agg_result_column.size(); i++) {
        result_chunk->append_column(std::move(agg_result_column[i]), tuple_desc->slots()[i]->id());
    }
    ++_num_rows_returned;
    ++_num_rows_processed;
    *chunk = std::move(result_chunk);
    _is_ht_eos = true;

    return Status::OK();
}

void Aggregator::process_limit(ChunkPtr* chunk) {
    if (_reached_limit()) {
        int64_t num_rows_over = _num_rows_returned - _limit;
        (*chunk)->set_num_rows((*chunk)->num_rows() - num_rows_over);
        COUNTER_SET(_agg_stat->rows_returned_counter, _limit);
        _is_ht_eos = true;
        LOG(INFO) << "Aggregate Node ReachedLimit " << _limit;
    }
}

Status Aggregator::evaluate_groupby_exprs(Chunk* chunk) {
    _set_passthrough(chunk->owner_info().is_passthrough());
    _reset_exprs();
    return _evaluate_group_by_exprs(chunk);
}

Status Aggregator::output_chunk_by_streaming(Chunk* input_chunk, ChunkPtr* chunk,
                                             bool force_use_intermediate_as_output) {
    return output_chunk_by_streaming(input_chunk, chunk, input_chunk->num_rows(), false,
                                     force_use_intermediate_as_output);
}

Status Aggregator::output_chunk_by_streaming(Chunk* input_chunk, ChunkPtr* chunk, size_t num_input_rows,
                                             bool use_selection, bool force_use_intermediate_as_output) {
    // The input chunk is already intermediate-typed, so there is no need to convert it again.
    // Only when the input chunk is input-typed, we should convert it into intermediate-typed chunk.
    // is_passthrough is on indicate that the chunk is input-typed.
    auto use_intermediate_as_input = _use_intermediate_as_input();
    const auto& slots = _intermediate_tuple_desc->slots();

    DCHECK(!use_selection || !_group_by_columns.empty());
    // If using selection, then `_group_by_columns` has been filtered by `_streaming_selection`, and input_chunk has
    // not been filtered yet. `input_chunk` is filtered by `_streaming_selection` after `evaluate_agg_fn_exprs`.
    const size_t num_rows = use_selection ? _group_by_columns[0]->size() : num_input_rows;

    // build group by columns
    ChunkPtr result_chunk = std::make_shared<Chunk>();
    for (size_t i = 0; i < _group_by_columns.size(); i++) {
        DCHECK_EQ(num_rows, _group_by_columns[i]->size());
        // materialize group by const columns
        if (_group_by_columns[i]->is_constant()) {
            auto res =
                    ColumnHelper::unfold_const_column(_group_by_types[i].result_type, num_rows, _group_by_columns[i]);
            result_chunk->append_column(std::move(res), slots[i]->id());
        } else {
            result_chunk->append_column(_group_by_columns[i], slots[i]->id());
        }
    }

    // build aggregate function values
    if (!_agg_fn_ctxs.empty()) {
        DCHECK(!_group_by_columns.empty());
        RETURN_IF_ERROR(evaluate_agg_fn_exprs(input_chunk));
        if (use_selection) {
            for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
                for (auto& agg_input_column : _agg_input_columns[i]) {
                    // AggColumn and GroupColumn may be the same SharedPtr,
                    // If ColumnSize and ChunkSize are not equal,
                    // indicating that the Filter has been executed in GroupByColumn
                    // e.g.: select c1, count(distinct c1) from t1 group by c1;

                    // At present, the type of problem cannot be completely solved,
                    // and a new solution needs to be designed to solve it completely
                    if (agg_input_column != nullptr && agg_input_column->size() == num_input_rows) {
                        agg_input_column->as_mutable_raw_ptr()->filter(_streaming_selection);
                    }
                }
            }
        }

        MutableColumns agg_result_column = _create_agg_result_columns(num_rows, true);
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            size_t id = _group_by_columns.size() + i;
            auto slot_id = slots[id]->id();
            if (_is_merge_funcs[i] || use_intermediate_as_input) {
                DCHECK(i < _agg_input_columns.size() && _agg_input_columns[i].size() >= 1);
                if (force_use_intermediate_as_output) {
                    if (agg_result_column[i]->is_nullable()) {
                        _agg_input_columns[i][0] =
                                ColumnHelper::cast_to_nullable_column(std::move(_agg_input_columns[i][0]));
                    }
                }
                result_chunk->append_column(std::move(_agg_input_columns[i][0]), slot_id);
            } else {
                {
                    SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
                    // convert_to_serialize_format expects const Columns&, create a view
                    _agg_functions[i]->convert_to_serialize_format(_agg_fn_ctxs[i], _agg_input_columns[i],
                                                                   result_chunk->num_rows(), agg_result_column[i]);
                }
                result_chunk->append_column(std::move(agg_result_column[i]), slot_id);
            }
        }
        RETURN_IF_ERROR(check_has_error());
    }

    _num_pass_through_rows += result_chunk->num_rows();
    _num_rows_returned += result_chunk->num_rows();
    _num_rows_processed += result_chunk->num_rows();
    COUNTER_UPDATE(_agg_stat->pass_through_row_count, result_chunk->num_rows());
    *chunk = std::move(result_chunk);
    return Status::OK();
}

Status Aggregator::convert_to_spill_format(Chunk* input_chunk, ChunkPtr* chunk) {
    auto use_intermediate_as_input = _use_intermediate_as_input();
    size_t num_rows = input_chunk->num_rows();
    ChunkPtr result_chunk = std::make_shared<Chunk>();
    const auto& slots = _intermediate_tuple_desc->slots();
    // build group by column
    for (size_t i = 0; i < _group_by_columns.size(); i++) {
        DCHECK_EQ(num_rows, _group_by_columns[i]->size());
        // materialize group by const columns
        if (_group_by_columns[i]->is_constant()) {
            auto res =
                    ColumnHelper::unfold_const_column(_group_by_types[i].result_type, num_rows, _group_by_columns[i]);
            result_chunk->append_column(std::move(res), slots[i]->id());
        } else {
            result_chunk->append_column(_group_by_columns[i], slots[i]->id());
        }
    }

    if (!_agg_fn_ctxs.empty()) {
        DCHECK(!_group_by_columns.empty());

        RETURN_IF_ERROR(evaluate_agg_fn_exprs(input_chunk));

        const auto num_rows = _group_by_columns[0]->size();
        MutableColumns agg_result_column = _create_agg_result_columns(num_rows, true);
        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
            size_t id = _group_by_columns.size() + i;
            auto slot_id = slots[id]->id();
            // If it is AGG stage 3/4, the input of AGG is the intermediate result type (merge/serilaze stage and merge/finalize stage),
            // and it can be directly converted to intermediate result type at this time
            if (_is_merge_funcs[i] || use_intermediate_as_input) {
                DCHECK(i < _agg_input_columns.size() && _agg_input_columns[i].size() >= 1);
                result_chunk->append_column(std::move(_agg_input_columns[i][0]), slot_id);
            } else {
                _agg_functions[i]->convert_to_serialize_format(_agg_fn_ctxs[i], _agg_input_columns[i],
                                                               result_chunk->num_rows(), agg_result_column[i]);
                result_chunk->append_column(std::move(agg_result_column[i]), slot_id);
            }
        }
        RETURN_IF_ERROR(check_has_error());
    }
    _num_rows_processed += result_chunk->num_rows();
    *chunk = std::move(result_chunk);

    return Status::OK();
}

Status Aggregator::output_chunk_by_streaming_with_selection(Chunk* input_chunk, ChunkPtr* chunk,
                                                            bool force_use_intermediate_as_output) {
    // Streaming aggregate at least has one group by column
    const size_t num_input_rows = _group_by_columns[0]->size();
    for (auto& _group_by_column : _group_by_columns) {
        // Multi GroupColumn may be have the same SharedPtr
        // If ColumnSize and ChunkSize are not equal,
        // indicating that the Filter has been executed in previous GroupByColumn
        // e.g.: select c1, cast(c1 as int) from t1 group by c1, cast(c1 as int);

        // At present, the type of problem cannot be completely solved,
        // and a new solution needs to be designed to solve it completely
        if (_group_by_column->size() == num_input_rows) {
            _group_by_column->as_mutable_raw_ptr()->filter(_streaming_selection);
        }
    }

    RETURN_IF_ERROR(
            output_chunk_by_streaming(input_chunk, chunk, num_input_rows, true, force_use_intermediate_as_output));
    return Status::OK();
}

// Blocking/spillable sinks call this between an inline-agg build (classify) and its compute
// fold. That is address-safe (the fold re-probes keys, it never replays cell pointers), and
// it stays safe for the convertible variants the inline gate admits (the single-string maps):
// the conversion migrates the {key, slot} pairs by value into the two-level backing, whose
// class -- and therefore inline support -- is the same. The threshold below reads a map that
// is still missing the chunk's pending new groups; that only delays a conversion by a chunk.
void Aggregator::try_convert_to_two_level_map() {
    auto current_size = _hash_map_variant.reserved_memory_usage(mem_pool());
    if (current_size > get_two_level_threahold()) {
        _hash_map_variant.convert_to_two_level(_state);
    }
}

void Aggregator::try_convert_to_two_level_set() {
    auto current_size = _hash_set_variant.reserved_memory_usage(mem_pool());
    if (current_size > get_two_level_threahold()) {
        _hash_set_variant.convert_to_two_level(_state);
    }
}

Status Aggregator::check_has_error() {
    for (const auto* ctx : _agg_fn_ctxs) {
        if (ctx->has_error()) {
            return Status::RuntimeError(ctx->error_msg());
        }
    }
    return Status::OK();
}

// When need finalize, create column by result type
// otherwise, create column by serde type
MutableColumns Aggregator::_create_agg_result_columns(size_t num_rows, bool use_intermediate) {
    MutableColumns agg_result_columns(_agg_fn_types.size());

    if (!use_intermediate) {
        for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
            // For count, count distinct, bitmap_union_int such as never return null function,
            // we need to create a not-nullable column.
            agg_result_columns[i] = ColumnHelper::create_column(_agg_fn_types[i].result_type,
                                                                _agg_fn_types[i].is_result_nullable<false>());
            agg_result_columns[i]->reserve(num_rows);
        }
    } else {
        for (size_t i = 0; i < _agg_fn_types.size(); ++i) {
            agg_result_columns[i] = ColumnHelper::create_column(_agg_fn_types[i].serde_type,
                                                                _agg_fn_types[i].is_result_nullable<true>());
            agg_result_columns[i]->reserve(num_rows);
        }
    }
    return agg_result_columns;
}

MutableColumns Aggregator::_create_group_by_columns(size_t num_rows) const {
    MutableColumns group_by_columns(_group_by_types.size());
    for (size_t i = 0; i < _group_by_types.size(); ++i) {
        group_by_columns[i] =
                ColumnHelper::create_column(_group_by_types[i].result_type, _group_by_types[i].is_nullable);
        group_by_columns[i]->reserve(num_rows);
    }
    return group_by_columns;
}

void Aggregator::_serialize_to_chunk(ConstAggDataPtr __restrict state, MutableColumns& agg_result_columns) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->serialize_to_column(_agg_fn_ctxs[i], state + _agg_states_offsets[i],
                                               agg_result_columns[i].get());
    }
}

void Aggregator::_finalize_to_chunk(ConstAggDataPtr __restrict state, MutableColumns& agg_result_columns) {
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->finalize_to_column(_agg_fn_ctxs[i], state + _agg_states_offsets[i],
                                              agg_result_columns[i].get());
    }
}

void Aggregator::_destroy_state(AggDataPtr __restrict state) {
    SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        _agg_functions[i]->destroy(_agg_fn_ctxs[i], state + _agg_states_offsets[i]);
    }
}

ChunkPtr Aggregator::_build_output_chunk(const Columns& group_by_columns, const Columns& agg_result_columns,
                                         bool use_intermediate_as_output) {
    ChunkPtr result_chunk = std::make_shared<Chunk>();
    // For different agg phase, we should use different TupleDescriptor
    if (!use_intermediate_as_output) {
        for (size_t i = 0; i < group_by_columns.size(); i++) {
            result_chunk->append_column(group_by_columns[i], _output_tuple_desc->slots()[i]->id());
        }
        for (size_t i = 0; i < agg_result_columns.size(); i++) {
            size_t id = group_by_columns.size() + i;
            result_chunk->append_column(agg_result_columns[i], _output_tuple_desc->slots()[id]->id());
        }
    } else {
        for (size_t i = 0; i < group_by_columns.size(); i++) {
            result_chunk->append_column(group_by_columns[i], _intermediate_tuple_desc->slots()[i]->id());
        }
        for (size_t i = 0; i < agg_result_columns.size(); i++) {
            size_t id = group_by_columns.size() + i;
            result_chunk->append_column(agg_result_columns[i], _intermediate_tuple_desc->slots()[id]->id());
        }
    }
    return result_chunk;
}

ChunkPtr Aggregator::_build_output_chunk(MutableColumns&& group_by_columns, MutableColumns&& agg_result_columns,
                                         bool use_intermediate_as_output) {
    ChunkPtr result_chunk = std::make_shared<Chunk>();
    // For different agg phase, we should use different TupleDescriptor
    if (!use_intermediate_as_output) {
        for (size_t i = 0; i < group_by_columns.size(); i++) {
            result_chunk->append_column(std::move(group_by_columns[i]), _output_tuple_desc->slots()[i]->id());
        }
        for (size_t i = 0; i < agg_result_columns.size(); i++) {
            size_t id = group_by_columns.size() + i;
            result_chunk->append_column(std::move(agg_result_columns[i]), _output_tuple_desc->slots()[id]->id());
        }
    } else {
        for (size_t i = 0; i < group_by_columns.size(); i++) {
            result_chunk->append_column(std::move(group_by_columns[i]), _intermediate_tuple_desc->slots()[i]->id());
        }
        for (size_t i = 0; i < agg_result_columns.size(); i++) {
            size_t id = group_by_columns.size() + i;
            result_chunk->append_column(std::move(agg_result_columns[i]), _intermediate_tuple_desc->slots()[id]->id());
        }
    }
    return result_chunk;
}

void Aggregator::_reset_exprs() {
    SCOPED_TIMER(_agg_stat->expr_release_timer);
    for (auto& _group_by_column : _group_by_columns) {
        _group_by_column = nullptr;
    }

    for (size_t i = 0; i < _agg_input_columns.size(); i++) {
        for (size_t j = 0; j < _agg_input_columns[i].size(); j++) {
            _agg_input_columns[i][j] = nullptr;
            _agg_input_raw_columns[i][j] = nullptr;
        }
    }
}

Status Aggregator::_evaluate_group_by_exprs(Chunk* chunk) {
    SCOPED_TIMER(_agg_stat->expr_compute_timer);
    // Compute group by columns
    for (size_t i = 0; i < _group_by_expr_ctxs.size(); i++) {
        ASSIGN_OR_RETURN(_group_by_columns[i], _group_by_expr_ctxs[i]->evaluate(chunk));
        DCHECK(_group_by_columns[i] != nullptr);
        if (_group_by_columns[i]->is_constant()) {
            // All hash table could handle only null, and we don't know the real data
            // type for only null column, so we don't unpack it.
            if (!_group_by_columns[i]->only_null()) {
                auto* const_column = static_cast<const ConstColumn*>(_group_by_columns[i].get());
                const_column->data_column()->as_mutable_raw_ptr()->assign(chunk->num_rows(), 0);
                _group_by_columns[i] = const_column->data_column();
            }
        }
        // Scalar function compute will return non-nullable column
        // for nullable column when the real whole chunk data all not-null.
        if (_group_by_types[i].is_nullable && !_group_by_columns[i]->is_nullable()) {
            // TODO: optimized the memory usage
            _group_by_columns[i] =
                    NullableColumn::create(_group_by_columns[i], NullColumn::create(_group_by_columns[i]->size(), 0));
        } else if (!_group_by_types[i].is_nullable && _group_by_columns[i]->is_nullable()) {
            return Status::InternalError(fmt::format("error nullablel column, index: {}, slot: {}", i,
                                                     _group_by_expr_ctxs[i]->root()->debug_string()));
        }
    }

    return Status::OK();
}

Status Aggregator::evaluate_agg_fn_exprs(Chunk* chunk) {
    bool use_intermediate = _use_intermediate_as_input();
    return evaluate_agg_fn_exprs(chunk, use_intermediate);
}

Status Aggregator::evaluate_agg_fn_exprs(Chunk* chunk, bool use_intermediate) {
    auto& agg_expr_ctxs = use_intermediate ? _intermediate_agg_expr_ctxs : _agg_expr_ctxs;
    for (size_t i = 0; i < agg_expr_ctxs.size(); i++) {
        RETURN_IF_ERROR(evaluate_agg_input_column(chunk, agg_expr_ctxs[i], i));
    }
    return Status::OK();
}

bool could_apply_bitcompress_opt(
        const std::vector<ColumnType>& group_by_types,
        const std::vector<std::optional<std::pair<VectorizedLiteral*, VectorizedLiteral*>>>& ranges,
        std::vector<std::any>& base, std::vector<int>& used_bytes, size_t* max_size, bool* has_null) {
    size_t accumulated = 0;
    size_t accumulated_fixed_length_bits = 0;
    for (size_t i = 0; i < group_by_types.size(); i++) {
        size_t size = 0;
        // 1 bytes for null flag.
        if (group_by_types[i].is_nullable) {
            *has_null = true;
            size += 1;
        }
        if (group_by_types[i].result_type.is_complex_type()) {
            return false;
        }
        LogicalType ltype = group_by_types[i].result_type.type;

        size_t fixed_base_size = get_size_of_fixed_length_type(ltype);
        if (fixed_base_size == 0) return false;
        accumulated_fixed_length_bits += fixed_base_size * 8;

        if (!ranges[i].has_value()) {
            return false;
        }
        auto used_bits = get_used_bits(ltype, *ranges[i]->first, *ranges[i]->second, base[i]);
        if (!used_bits.has_value()) {
            return false;
        }
        size += used_bits.value();

        accumulated += size;
        used_bytes[i] = accumulated;
    }
    auto get_level = [](size_t used_bits) {
        if (used_bits <= sizeof(uint8_t) * 8)
            return 1;
        else if (used_bits <= sizeof(uint16_t) * 8)
            return 2;
        else if (used_bits <= sizeof(uint32_t) * 8)
            return 3;
        else if (used_bits <= sizeof(uint64_t) * 8)
            return 4;
        else if (used_bits <= sizeof(int128_t) * 8)
            return 5;
        else
            return 6;
    };
    // If they are at the same level, grouping by compressed key will not optimize performance, so we disable it.
    // eg: For example, two int32 values both have a threshold of 0-2^32, so they need to use group by int64.
    // In this case, there will be no optimization effect. We disable this situation.
    if (get_level(accumulated_fixed_length_bits) > get_level(accumulated)) {
        *max_size = accumulated;
        return true;
    }
    // Single-INT keys with a value range <= 16 bits get a dedicated direct-array map
    // (uint8/uint16, keyed on value - min) downstream. That path optimizes even when the
    // null flag bumps the packed width into int32's storage class -- e.g. a nullable INT
    // spanning [0, 65535] is 17 packed bits, so the level check above sees 32 -> 32 and
    // bails, yet the value range alone (16 bits) fits uint16. Gate that case on value bits
    // only; the routing in _try_to_apply_compressed_key_opt re-subtracts the null flag to
    // pick the uint8/uint16 bucket.
    if (group_by_types.size() == 1 && group_by_types[0].result_type.type == TYPE_INT) {
        const size_t null_bits = group_by_types[0].is_nullable ? 1 : 0;
        if (accumulated - null_bits <= 16) {
            *max_size = accumulated;
            return true;
        }
    }
    return false;
}

bool is_group_columns_fixed_size(std::vector<ColumnType>& group_by_types, size_t* max_size, bool* has_null) {
    size_t size = 0;
    *has_null = false;

    for (size_t i = 0; i < group_by_types.size(); i++) {
        // 1 bytes for null flag.
        if (group_by_types[i].is_nullable) {
            *has_null = true;
            size += 1;
        }
        LogicalType ltype = group_by_types[i].result_type.type;
        if (group_by_types[i].result_type.is_complex_type()) {
            return false;
        }
        size_t byte_size = get_size_of_fixed_length_type(ltype);
        if (byte_size == 0) return false;
        size += byte_size;
    }
    *max_size = size;
    return true;
}

template <typename HashVariantType>
typename HashVariantType::Type Aggregator::_get_hash_table_type() {
    auto type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice : HashVariantType::Type::phase2_slice;
    if (_group_by_types.empty()) {
        return type;
    }
    // using one key hash table
    if (_group_by_types.size() == 1) {
        bool nullable = _group_by_types[0].is_nullable;
        LogicalType type = _group_by_types[0].result_type.type;
        return HashVariantResolver<HashVariantType>::instance().get_unary_type(_aggr_phase, type, nullable);
    }
    return type;
}

template <typename HashVariantType>
typename HashVariantType::Type Aggregator::_try_to_apply_fixed_size_opt(typename HashVariantType::Type type,
                                                                        bool* has_null, int* fixed_size) {
    bool has_null_column = false;
    int fixed_byte_size = 0;
    // this optimization don't need to be limited to multi-column group by.
    // single column like float/double/decimal/largeint could also be applied to.
    if (type == HashVariantType::Type::phase1_slice || type == HashVariantType::Type::phase2_slice) {
        size_t max_size = 0;
        if (is_group_columns_fixed_size(_group_by_types, &max_size, &has_null_column)) {
            // we need reserve a byte for serialization length for nullable columns
            if (max_size < 4 || (!has_null_column && max_size == 4)) {
                type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_fx4
                                                 : HashVariantType::Type::phase2_slice_fx4;
            } else if (max_size < 8 || (!has_null_column && max_size == 8)) {
                type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_fx8
                                                 : HashVariantType::Type::phase2_slice_fx8;
            } else if (max_size < 16 || (!has_null_column && max_size == 16)) {
                type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_fx16
                                                 : HashVariantType::Type::phase2_slice_fx16;
            }
            if (!has_null_column) {
                fixed_byte_size = max_size;
            }
        }
    }
    *has_null = has_null_column;
    *fixed_size = fixed_byte_size;
    return type;
}

template <typename HashVariantType>
typename HashVariantType::Type Aggregator::_try_to_apply_compressed_key_opt(typename HashVariantType::Type input_type,
                                                                            CompressKeyContext* ctx) {
    typename HashVariantType::Type type = input_type;
    if (_group_by_types.empty()) {
        return type;
    }
    // Don't shadow direct-array variants with the slice_cx1 rewrite.
    // TINYINT / BOOL / SMALLINT route to SmallFixedSizeHashMap-backed
    // direct arrays; the slice_cx1 path sits on the same direct array
    // under int8 but adds a per-row bitcompress_serialize step, so any
    // query that supplies range stats via `group_by_min_max` would
    // otherwise silently regress to the slower slice path.
    if (_group_by_types.size() == 1) {
        switch (_group_by_types[0].result_type.type) {
        case TYPE_TINYINT:
        case TYPE_BOOLEAN:
        case TYPE_SMALLINT:
            return type;
        default:
            break;
        }
    }
    for (size_t i = 0; i < _ranges.size(); ++i) {
        if (!_ranges[i].has_value()) {
            return type;
        }
    }

    // check apply bit compress opt
    {
        bool has_null_column;
        size_t new_max_bit_size = 0;
        std::vector<int>& offsets = ctx->offsets;
        std::vector<int>& used_bits = ctx->used_bits;
        std::vector<std::any>& bases = ctx->bases;

        size_t group_by_keys = _group_by_types.size();
        used_bits.resize(group_by_keys);
        offsets.resize(group_by_keys);
        bases.resize(group_by_keys);

        if (could_apply_bitcompress_opt(_group_by_types, _ranges, bases, used_bits, &new_max_bit_size,
                                        &has_null_column)) {
            if (_group_by_types.size() > 0) {
                // Single-INT GROUP BY with FE-supplied range that fits in
                // 16 bits: skip the slice_cx4 path (phmap<SliceKey4> with
                // per-row bitcompress_serialize) and route to a 65 536-cell
                // direct-array map keyed by (value - min) -> uint16.
                // int32_range_uint{8,16} only exist in AggHashMapVariant
                // (GROUP BY); DISTINCT-only Sets fall through to slice_cx*.
                // if-constexpr keeps the Set template instantiation
                // compilable.
                bool routed_int32_range = false;
                if constexpr (std::is_same_v<HashVariantType, AggHashMapVariant>) {
                    const bool single_int_col = group_by_keys == 1 && _group_by_types[0].result_type.type == TYPE_INT;
                    // could_apply_bitcompress_opt folds a 1-bit null flag
                    // into new_max_bit_size, so for nullable keys with a
                    // value range that already saturates uintN the sum is
                    // N+1 and would miss the uintN bucket. Compare against
                    // the value-bits-only width so a nullable INT with
                    // [0, 65535] still reaches the uint16 direct map.
                    const bool is_nullable = _group_by_types[0].is_nullable;
                    const size_t value_bits = new_max_bit_size - (is_nullable ? 1 : 0);
                    if (single_int_col && value_bits > 8 && value_bits <= 16) {
                        if (is_nullable) {
                            type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_null_int32_range_uint16
                                                             : HashVariantType::Type::phase2_null_int32_range_uint16;
                        } else {
                            type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_int32_range_uint16
                                                             : HashVariantType::Type::phase2_int32_range_uint16;
                        }
                        routed_int32_range = true;
                    } else if (single_int_col && value_bits <= 8) {
                        // ≤8-bit range with INT column -> 256-cell
                        // direct-array, skipping the slice_cx1 phmap detour.
                        if (is_nullable) {
                            type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_null_int32_range_uint8
                                                             : HashVariantType::Type::phase2_null_int32_range_uint8;
                        } else {
                            type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_int32_range_uint8
                                                             : HashVariantType::Type::phase2_int32_range_uint8;
                        }
                        routed_int32_range = true;
                    }
                }
                if (routed_int32_range) {
                    // already routed
                } else if (new_max_bit_size <= 8) {
                    type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_cx1
                                                     : HashVariantType::Type::phase2_slice_cx1;
                } else if (new_max_bit_size <= 4 * 8) {
                    type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_cx4
                                                     : HashVariantType::Type::phase2_slice_cx4;
                } else if (new_max_bit_size <= 8 * 8) {
                    type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_cx8
                                                     : HashVariantType::Type::phase2_slice_cx8;
                } else if (new_max_bit_size <= 16 * 8) {
                    type = _aggr_phase == AggrPhase1 ? HashVariantType::Type::phase1_slice_cx16
                                                     : HashVariantType::Type::phase2_slice_cx16;
                }
            }
        }

        offsets[0] = 0;
        for (size_t i = 1; i < group_by_keys; ++i) {
            offsets[i] = used_bits[i - 1];
        }
    }
    return type;
}

template <typename HashVariantType>
void Aggregator::_build_hash_variant(HashVariantType& hash_variant, typename HashVariantType::Type type,
                                     CompressKeyContext&& context) {
    hash_variant.init(_state, type, _agg_stat);
    hash_variant.visit([&](auto& variant) {
        if constexpr (is_compressed_fixed_size_key<std::decay_t<decltype(*variant)>>) {
            variant->offsets = std::move(context.offsets);
            variant->used_bits = std::move(context.used_bits);
            variant->bases = std::move(context.bases);
        } else if constexpr (is_compressible_int_key<std::decay_t<decltype(*variant)>>) {
            // The compressible-int wrapper only needs the min offset from
            // bases[0] (single-column gate); offsets / used_bits are
            // slice-shape state it does not consume.
            DCHECK(!context.bases.empty());
            variant->set_min(std::any_cast<int32_t>(context.bases[0]));
        }
    });
}

namespace {
// Flat slice/string map variants that grow into a partitioned (two-level) map once
// they pass the two-level memory threshold. Only these can be preselected as
// two-level from the FE estimate; fx/cx-compressed and numeric variants cannot.
bool is_flat_slice_string_map_type(AggHashMapVariant::Type type) {
    switch (type) {
    case AggHashMapVariant::Type::phase1_slice:
    case AggHashMapVariant::Type::phase2_slice:
    case AggHashMapVariant::Type::phase1_string:
    case AggHashMapVariant::Type::phase2_string:
    case AggHashMapVariant::Type::phase1_null_string:
    case AggHashMapVariant::Type::phase2_null_string:
        return true;
    default:
        return false;
    }
}

AggHashMapVariant::Type to_two_level_map_type(AggHashMapVariant::Type type) {
    switch (type) {
    case AggHashMapVariant::Type::phase1_slice:
        return AggHashMapVariant::Type::phase1_slice_two_level;
    case AggHashMapVariant::Type::phase2_slice:
        return AggHashMapVariant::Type::phase2_slice_two_level;
    case AggHashMapVariant::Type::phase1_string:
        return AggHashMapVariant::Type::phase1_string_two_level;
    case AggHashMapVariant::Type::phase2_string:
        return AggHashMapVariant::Type::phase2_string_two_level;
    case AggHashMapVariant::Type::phase1_null_string:
        return AggHashMapVariant::Type::phase1_null_string_two_level;
    case AggHashMapVariant::Type::phase2_null_string:
        return AggHashMapVariant::Type::phase2_null_string_two_level;
    default:
        return type;
    }
}
} // namespace

template <typename HashVariantType>
void Aggregator::_init_agg_hash_variant(HashVariantType& hash_variant, bool want_pack) {
    auto type = _get_hash_table_type<HashVariantType>();

    CompressKeyContext compress_key_ctx;
    bool apply_compress_key_opt = false;
    typename HashVariantType::Type prev_type = type;
    type = _try_to_apply_compressed_key_opt<HashVariantType>(type, &compress_key_ctx);
    apply_compress_key_opt = prev_type != type;
    if (apply_compress_key_opt) {
        // build with compressed key (no pack twin exists for the compressed flavors; a pack
        // re-init lands back on this same type and the caller sees is_inline_pack() false)
        VLOG_ROW << "apply compressed key";
        _build_hash_variant<HashVariantType>(hash_variant, type, std::move(compress_key_ctx));
        return;
    }

    bool has_null_column = false;
    int fixed_byte_size = 0;

    if (_group_by_types.size() > 1) {
        type = _try_to_apply_fixed_size_opt<HashVariantType>(type, &has_null_column, &fixed_byte_size);
    }

    if constexpr (std::is_same_v<HashVariantType, AggHashMapVariant>) {
        if (want_pack) {
            type = AggHashMapVariant::pack_type_for(type);
        }
        // If the FE estimate already implies a slice/string map larger than the two-level
        // threshold, build the partitioned (two-level) map directly so the later reserve
        // sizes it, skipping the grow-then-convert path. Gated by the same master switch as
        // the reserve (agg_hashtable_reserve_max_bytes > 0) so 0 fully restores the original
        // build. Blocking aggregators only: the blocking factory sets DOP before open(),
        // streaming leaves it 0. MAP only (not the distinct set).
        if (config::agg_hashtable_reserve_max_bytes > 0 && _degree_of_parallelism > 0 &&
            _params->estimated_cardinality > 0 && is_flat_slice_string_map_type(type)) {
            const int64_t per_driver =
                    std::min<int64_t>(_params->estimated_cardinality / _degree_of_parallelism, int64_t{1} << 30);
            // Slice slot = Slice key (16B) + AggDataPtr (8B) + 1 control byte; worst-case
            // phmap capacity is ~16/7x the requested count (power-of-two rounding).
            constexpr int64_t kSliceSlotBytes = 16 + 8 + 1;
            const int64_t est_bytes = (per_driver * 16 / 7 + 1) * kSliceSlotBytes;
            if (est_bytes > get_two_level_threahold()) {
                type = to_two_level_map_type(type);
            }
        }
    }

    VLOG_ROW << "hash type is "
             << static_cast<typename std::underlying_type<typename HashVariantType::Type>::type>(type);
    hash_variant.init(_state, type, _agg_stat);

    hash_variant.visit([&](auto& variant) {
        if constexpr (is_combined_fixed_size_key<std::decay_t<decltype(*variant)>>) {
            variant->has_null_column = has_null_column;
            variant->fixed_byte_size = fixed_byte_size;
        }
    });

    // Session-gated activation. Default ON; FE may turn off per query for
    // diagnosis or to bisect a regression.
    const bool cache_enabled = _state == nullptr || _state->enable_agg_consecutive_keys_cache();
    if (!cache_enabled) {
        hash_variant.visit([](auto& variant) {
            if constexpr (requires { variant->_consecutive_key_cache.force_disable(); }) {
                variant->_consecutive_key_cache.force_disable();
            }
        });
    }
}

void Aggregator::_update_hash_table_grow_count() {
    const size_t cap = _hash_map_variant.capacity();
    if (cap <= _prev_hash_map_capacity) {
        return;
    }
    // phmap doubles capacity (2^k-1) on each rehash; count the doublings via the
    // bit-length delta (highest set bit). A table reserved to its final size does not
    // grow here, so the counter stays ~0 -- a direct profile signal that reserve worked.
    auto bit_len = [](size_t v) { return v == 0 ? 0 : 64 - __builtin_clzll(static_cast<unsigned long long>(v)); };
    const int delta = bit_len(cap) - bit_len(_prev_hash_map_capacity);
    if (delta > 0) {
        COUNTER_UPDATE(_agg_stat->hash_table_grow_count, delta);
    }
    _prev_hash_map_capacity = cap;
}

// Inline-count build: drive the in-slot counter path for a supported numeric
// variant. `not_founds`/`limit` carry the selection/limit semantics (ignored for
// the plain HTBuildOp). `delta` is what an emplaced/found row adds to its slot:
// 1 on an update chunk; 0 on a merge chunk routed through a creating build
// (group-by limit / allocate paths), where the build only classifies-and-creates
// and the partial counts are folded later by selection in compute. DCHECKs on
// unsupported variants -- _inline_agg is only set when supports_inline_agg()
// is true, so the supported branch always runs.
template <typename Op, typename HTBuildOp>
static void inline_agg_build(AggHashMapVariant& variant, size_t chunk_size, const Columns& group_by_columns,
                             MemPool* pool, Buffer<AggDataPtr>* tmp_states, Filter* not_founds, size_t limit,
                             typename Op::DeltaType delta) {
    variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (agg_inline_op_for_map<Op, MapType>) {
            if (not_founds != nullptr) not_founds->assign(chunk_size, 0);
            ExtraAggParam extra;
            extra.not_founds = not_founds;
            extra.limits = limit;
            hash_map_with_key->template build_inline_agg<Op, HTBuildOp>(chunk_size, group_by_columns, pool, tmp_states,
                                                                        &extra, delta);
        } else {
            DCHECK(false) << "inline_agg enabled on unsupported variant";
        }
    });
}

// Dispatch the merge fold to the active map's typed build_inline_agg_fold. Only the supported
// variants reach here (gated by supports_inline_agg() at open()); the rest DCHECK.
template <typename Op>
static void inline_agg_fold_dispatch(AggHashMapVariant& variant, size_t chunk_size, const Columns& group_by_columns,
                                     MemPool* pool, Buffer<AggDataPtr>* agg_states,
                                     const typename Op::DeltaType* partials, const Filter* selection) {
    variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (agg_inline_op_for_map<Op, MapType>) {
            hash_map_with_key->template build_inline_agg_fold<Op>(chunk_size, group_by_columns, pool, agg_states,
                                                                  partials, selection);
        } else {
            DCHECK(false) << "inline_agg fold on unsupported variant";
        }
    });
}

// Commit the deferred +1 for the selective inline-agg count build: re-probe each row the operator chose
// to aggregate locally (selection[i] == 0, or all rows when selection is null) and add to its slot.
template <typename Op>
static void inline_agg_commit(AggHashMapVariant& variant, size_t chunk_size, const Columns& group_by_columns,
                              Buffer<AggDataPtr>* scratch, const Filter* selection, typename Op::DeltaType delta) {
    variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (agg_inline_op_for_map<Op, MapType>) {
            hash_map_with_key->template commit_inline_agg<Op>(chunk_size, group_by_columns, scratch, selection, delta);
        } else {
            DCHECK(false) << "inline_agg commit on unsupported variant";
        }
    });
}

void Aggregator::build_hash_map(size_t chunk_size, bool agg_group_by_with_limit) {
    if (agg_group_by_with_limit) {
        if (_hash_map_variant.size() >= _limit) {
            build_hash_map_with_selection(chunk_size);
            return;
        } else {
            _streaming_selection.assign(chunk_size, 0);
        }
    }

    if (_inline_agg) {
        // A merge chunk folds partial counts in compute_batch_agg_states (it needs the chunk to
        // evaluate the intermediate column; the fold emplaces keys itself); nothing to build here.
        DCHECK(_inline_chunk.fold == InlineChunkState::kConsumed ||
               _inline_chunk.fold == InlineChunkState::kFoldSelection);
        const bool merge_chunk = _inline_agg_merge_chunk();
        // A per-row-delta op (kCountCol) defers its update fold to compute exactly like a merge
        // chunk: the deltas need the input column, which only compute evaluates.
        const bool deferred_fold = merge_chunk || !_inline_op_is_fused();
        if (!deferred_fold) {
            _inline_dispatch_build<HTBuildOp<true, false, false>>(chunk_size, nullptr, 0, true);
        }
        _inline_chunk = {deferred_fold ? InlineChunkState::kFoldAll : InlineChunkState::kCommitted, merge_chunk};
        return;
    }

    _hash_map_variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (agg_inline_pack<MapType>) {
            DCHECK(false) << "general build on a pack variant";
        } else {
            hash_map_with_key->build_hash_map(chunk_size, _group_by_columns, _mem_pool.get(),
                                              AllocateState<MapType>(this), &_tmp_agg_states);
        }
    });
    _update_hash_table_grow_count();
}

void Aggregator::build_hash_map(size_t chunk_size, std::atomic<int64_t>& shared_limit_countdown,
                                bool agg_group_by_with_limit) {
    if (agg_group_by_with_limit && _params->enable_pipeline_share_limit) {
        _build_hash_map_with_shared_limit(chunk_size, shared_limit_countdown);
        return;
    }
    build_hash_map(chunk_size, agg_group_by_with_limit);
}

void Aggregator::_build_hash_map_with_shared_limit(size_t chunk_size, std::atomic<int64_t>& shared_limit_countdown) {
    auto start_size = _hash_map_variant.size();
    if (_hash_map_variant.size() >= _limit || shared_limit_countdown.load(std::memory_order_relaxed) <= 0) {
        build_hash_map_with_selection(chunk_size);
        return;
    } else {
        _streaming_selection.resize(chunk_size);
    }
    if (_inline_agg) {
        // Update chunk: count in place (delta 1) -> kCommitted; over-limit new keys are streamed
        // misses the selective compute must not touch. Merge chunk: the same limited build only
        // classifies -- admitted keys are created with a zero counter (delta 0) -- and the partial
        // counts are folded by selection in compute (kFoldSelection).
        DCHECK(_inline_chunk.fold == InlineChunkState::kConsumed ||
               _inline_chunk.fold == InlineChunkState::kFoldSelection);
        const bool merge_chunk = _inline_agg_merge_chunk();
        // A per-row-delta op (kCountCol) defers its update fold to compute exactly like a merge
        // chunk: the deltas need the input column, which only compute evaluates.
        const bool deferred_fold = merge_chunk || !_inline_op_is_fused();
        _inline_dispatch_build<HTBuildOp<false, true, true>>(chunk_size, &_streaming_selection, _limit, !deferred_fold);
        shared_limit_countdown.fetch_sub(_hash_map_variant.size() - start_size, std::memory_order_relaxed);
        _inline_chunk = {deferred_fold ? InlineChunkState::kFoldSelection : InlineChunkState::kCommitted, merge_chunk};
        return;
    }
    _hash_map_variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (agg_inline_pack<MapType>) {
            DCHECK(false) << "general build on a pack variant";
        } else {
            hash_map_with_key->build_hash_map_with_limit(chunk_size, _group_by_columns, _mem_pool.get(),
                                                         AllocateState<MapType>(this), &_tmp_agg_states,
                                                         &_streaming_selection, _limit);
        }
    });
    shared_limit_countdown.fetch_sub(_hash_map_variant.size() - start_size, std::memory_order_relaxed);
    _update_hash_table_grow_count();
}

void Aggregator::build_hash_map_with_selection(size_t chunk_size) {
    if (_inline_agg) {
        // Classify-only: probe each row to fill the streaming selection, but count nothing and stash
        // nothing, so a chunk the operator later streams out whole is not counted (its
        // kFoldSelection state is legally overwritten by the next build). The matching
        // compute_batch_agg_states* re-probes the kept rows and applies the +1 (commit_inline_agg),
        // or folds the kept partials when the chunk carries merge input. The delta argument is
        // irrelevant here -- the probe branch neither creates nor counts.
        DCHECK(_inline_chunk.fold == InlineChunkState::kConsumed ||
               _inline_chunk.fold == InlineChunkState::kFoldSelection);
        const bool merge_chunk = _inline_agg_merge_chunk();
        _inline_dispatch_build<HTBuildOp<false, true, false>>(chunk_size, &_streaming_selection, 0, false);
        _inline_chunk = {InlineChunkState::kFoldSelection, merge_chunk};
        return;
    }
    _hash_map_variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (agg_inline_pack<MapType>) {
            DCHECK(false) << "general build on a pack variant";
        } else {
            hash_map_with_key->build_hash_map_with_selection(chunk_size, _group_by_columns, _mem_pool.get(),
                                                             AllocateState<MapType>(this), &_tmp_agg_states,
                                                             &_streaming_selection);
        }
    });
    _update_hash_table_grow_count();
}

void Aggregator::build_hash_map_with_topn_runtime_filter(size_t chunk_size) {
    _streaming_selection.resize(chunk_size);
    if (_inline_agg) {
        // Allocate path: groups are created during the build (the runtime filter needs the
        // new-key selection). An update chunk counts in place (kCommitted); a merge chunk creates
        // with a zero counter and the partials are folded over every row in compute (kFoldAll --
        // this path's compute entry is the non-selective one).
        DCHECK(_inline_chunk.fold == InlineChunkState::kConsumed ||
               _inline_chunk.fold == InlineChunkState::kFoldSelection);
        const bool merge_chunk = _inline_agg_merge_chunk();
        // A per-row-delta op (kCountCol) defers its update fold to compute exactly like a merge
        // chunk: the deltas need the input column, which only compute evaluates.
        const bool deferred_fold = merge_chunk || !_inline_op_is_fused();
        _inline_dispatch_build<HTBuildOp<true, true, false>>(chunk_size, &_streaming_selection, 0, !deferred_fold);
        _inline_chunk = {deferred_fold ? InlineChunkState::kFoldAll : InlineChunkState::kCommitted, merge_chunk};
    } else {
        _hash_map_variant.visit([&](auto& hash_map_with_key) {
            using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
            if constexpr (agg_inline_pack<MapType>) {
                DCHECK(false) << "general build on a pack variant";
            } else {
                hash_map_with_key->build_hash_map_with_selection_and_allocation(
                        chunk_size, _group_by_columns, _mem_pool.get(), AllocateState<MapType>(this), &_tmp_agg_states,
                        &_streaming_selection);
            }
        });
    }
    // if _streaming_selection is not all 0, means there are new group by keys,
    // we need to build the topn runtime filter
    if (_topn_runtime_filter_builder != nullptr &&
        SIMD::count_zero(_streaming_selection.data(), chunk_size) != chunk_size) {
        _topn_runtime_filter_builder->update(_group_by_columns, _streaming_selection);
    }
    _update_hash_table_grow_count();
}

// When meets not found group keys, mark the first pos into `_streaming_selection` and insert into the hashmap
// so the following group keys(same as the first not found group keys) are not marked as non-founded.
// This can be used for stream mv so no need to find multi times for the same non-found group keys.
void Aggregator::build_hash_map_with_selection_and_allocation(size_t chunk_size, bool agg_group_by_with_limit) {
    if (_inline_agg) {
        // Same allocate semantics as the topn-runtime-filter build above.
        DCHECK(_inline_chunk.fold == InlineChunkState::kConsumed ||
               _inline_chunk.fold == InlineChunkState::kFoldSelection);
        const bool merge_chunk = _inline_agg_merge_chunk();
        // A per-row-delta op (kCountCol) defers its update fold to compute exactly like a merge
        // chunk: the deltas need the input column, which only compute evaluates.
        const bool deferred_fold = merge_chunk || !_inline_op_is_fused();
        _inline_dispatch_build<HTBuildOp<true, true, false>>(chunk_size, &_streaming_selection, 0, !deferred_fold);
        _inline_chunk = {deferred_fold ? InlineChunkState::kFoldAll : InlineChunkState::kCommitted, merge_chunk};
        return;
    }
    _hash_map_variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (agg_inline_pack<MapType>) {
            DCHECK(false) << "general build on a pack variant";
        } else {
            hash_map_with_key->build_hash_map_with_selection_and_allocation(
                    chunk_size, _group_by_columns, _mem_pool.get(), AllocateState<MapType>(this), &_tmp_agg_states,
                    &_streaming_selection);
        }
    });
    _update_hash_table_grow_count();
}

void Aggregator::collect_cache_conscious_topn_counts(std::vector<int64_t>* counts) {
    counts->clear();
    counts->reserve(_hash_map_variant.size());
    if (_inline_agg && !_inline_pack) {
        // The inline-accumulator path keeps the running count in the AggDataPtr slot itself and
        // allocates no per-group arena state, so read the counts straight off the map slots.
        _hash_map_variant.visit([&](auto& variant_value) {
            using HashMapWithKey = std::remove_reference_t<decltype(*variant_value)>;
            if constexpr (agg_inline_supported<HashMapWithKey> && !agg_inline_pack<HashMapWithKey>) {
                for (const auto& kv : variant_value->hash_map) {
                    counts->push_back(agg_inline_slot_load<int64_t>(kv.second));
                }
            }
        });
        return;
    }
    // The group state blob lays the aggregate states out at _agg_states_offsets; for a single
    // count(*) the first one is the int64 running count.
    const size_t count_offset = _agg_states_offsets.empty() ? 0 : _agg_states_offsets[0];
    auto it = _state_allocator.begin();
    const auto end = _state_allocator.end();
    while (it != end) {
        const uint8_t* value = it.value();
        counts->push_back(*reinterpret_cast<const int64_t*>(value + count_offset));
        it.next();
    }
}

bool Aggregator::collect_cache_conscious_topn_groups(std::vector<std::pair<uint64_t, int64_t>>* groups) {
    // Post-flip the phmap-backed FA owns the live counts; the hash map below is dormant and its
    // count states hold only the stale pre-flip snapshot. Every finalize path reads the FA; the
    // hash-map decode below runs only once, at the flip, to seed it.
    if (_cache_conscious_fa != nullptr) {
        _cache_conscious_fa->collect(groups);
        return true;
    }
    const size_t count_offset = _agg_states_offsets.empty() ? 0 : _agg_states_offsets[0];
    const LogicalType key_lt = _group_by_types[0].result_type.type;
    bool supported = true;
    // The group state blob lays the hash-map's internal key out at its front and the aggregate
    // states at _agg_states_offsets. The stored key is NOT necessarily the real group value: the
    // min-max / bit-compress variants pack it as (value - base) in the low bits and leave the
    // rest of the slot untouched. Read the raw keys + counts, then decode the keys through the
    // same insert_keys_to_columns path the normal output uses (bitcompress_deserialize for the
    // compressed variants, a plain copy otherwise). That yields real group values -- matching the
    // real keys route_cache_conscious_cold_rows puts into CA from the un-encoded group-by column.
    auto st = _hash_map_variant.visit([&](auto& variant_value) {
        using HashMapWithKey = std::remove_reference_t<decltype(*variant_value)>;
        using KeyType = typename HashMapWithKey::KeyType;
        if constexpr (std::is_integral_v<KeyType> || is_compressed_fixed_size_key<HashMapWithKey>) {
            const size_t n = _hash_map_variant.size();
            typename HashMapWithKey::ResultVector raw_keys;
            raw_keys.reserve(n);
            std::vector<int64_t> counts;
            counts.reserve(n);
            if (_inline_agg && !_inline_pack) {
                if constexpr (agg_inline_supported<HashMapWithKey> && !agg_inline_pack<HashMapWithKey>) {
                    // Inline-accumulator keeps the count in the AggDataPtr slot and has no
                    // arena states; the map slot holds the same internal key the blob would.
                    for (const auto& [key, slot] : variant_value->hash_map) {
                        raw_keys.push_back(key);
                        counts.push_back(agg_inline_slot_load<int64_t>(slot));
                    }
                } else {
                    supported = false;
                    return Status::OK();
                }
            } else {
                auto it = _state_allocator.begin();
                const auto end = _state_allocator.end();
                while (it != end) {
                    const uint8_t* value = it.value();
                    KeyType key;
                    memcpy(&key, value, sizeof(key));
                    raw_keys.push_back(key);
                    counts.push_back(*reinterpret_cast<const int64_t*>(value + count_offset));
                    it.next();
                }
            }
            MutableColumns key_columns = _create_group_by_columns(counts.size());
            variant_value->insert_keys_to_columns(raw_keys, key_columns, counts.size());
            const Column* kc = ColumnHelper::get_data_column(key_columns[0].get());
            groups->reserve(counts.size());
            for (size_t i = 0; i < counts.size(); ++i) {
                uint64_t key;
                switch (key_lt) {
                case TYPE_BOOLEAN:
                    key = down_cast<const UInt8Column*>(kc)->get_data()[i];
                    break;
                case TYPE_TINYINT:
                    key = static_cast<uint64_t>(static_cast<int64_t>(down_cast<const Int8Column*>(kc)->get_data()[i]));
                    break;
                case TYPE_SMALLINT:
                    key = static_cast<uint64_t>(static_cast<int64_t>(down_cast<const Int16Column*>(kc)->get_data()[i]));
                    break;
                case TYPE_INT:
                    key = static_cast<uint64_t>(static_cast<int64_t>(down_cast<const Int32Column*>(kc)->get_data()[i]));
                    break;
                case TYPE_BIGINT:
                    key = static_cast<uint64_t>(down_cast<const Int64Column*>(kc)->get_data()[i]);
                    break;
                default:
                    supported = false;
                    return Status::OK();
                }
                groups->emplace_back(key, counts[i]);
            }
        } else {
            supported = false;
        }
        return Status::OK();
    });
    return supported && st.ok();
}

bool Aggregator::cache_conscious_group_key_supported() const {
    // A nullable key has its NULL group stored outside the state allocator, so FA extraction
    // would silently drop it; require a single non-nullable integral key.
    // TODO: widen group-key support (each step is contained, no algorithm change):
    //  - int-backed fixed types (date/datetime, decimal32/64): cheap, add to the switch +
    //    a to-uint64 conversion;
    //  - LARGEINT/decimal128 and string/Slice/multi-column keys: need a templated key in the
    //    engine (identity by the real key, radix by its hash) and copying keys out of the
    //    hash-table arena;
    //  - nullable keys: handle the NULL group explicitly instead of excluding it here.
    if (_group_by_types.size() != 1 || _has_nullable_key) {
        return false;
    }
    switch (_group_by_types[0].result_type.type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        return true; // exact in a uint64
    default:
        return false; // LARGEINT (int128), decimals, strings, multi-column: unsupported for now
    }
}

namespace {
template <typename KeyColumn>
void route_cold_rows(CacheConsciousCa* ca, const Column* key_col, const Int64Column* cnt_col, const Filter& selection,
                     size_t n) {
    const auto& keys = down_cast<const KeyColumn*>(key_col)->get_data();
    // 2-phase routes the partial count carried in the input column; 1-phase has no input column
    // (every miss row contributes 1). Both go through the inlinable batched router on the engine
    // -- one function call per chunk instead of one per row.
    const int64_t* partials = (cnt_col != nullptr) ? cnt_col->get_data().data() : nullptr;
    ca->route_batch(keys.data(), partials, selection.data(), n);
}

// Post-flip FA probe + count, the counterpart of route_cold_rows: same key extraction (raw
// integral column -> uint64) and same partial-count handling (2-phase merges the upstream count,
// 1-phase weighs 1), so FA and CA stay in one key space. Writes the miss mask into `sel`.
template <typename KeyColumn>
size_t probe_cc_fa(CacheConsciousFa* fa, const Column* key_col, const Int64Column* cnt_col, uint8_t* sel, size_t n) {
    const auto& keys = down_cast<const KeyColumn*>(key_col)->get_data();
    const int64_t* partials = (cnt_col != nullptr) ? cnt_col->get_data().data() : nullptr;
    return fa->probe_and_count(keys.data(), partials, sel, n);
}

// Merge the sorted cold stream without reconstructing its tuple arenas. The partition
// bitmap drops only whole groups, so surviving rows of a key remain contiguous.
template <typename KeyColumn>
size_t restore_cold_tuples(CacheConsciousCa* ca, CacheConsciousTopN::SpillMerge* merge, const Column* key_col,
                           const Int64Column* cnt_col, size_t n, const uint8_t* pruned_mask) {
    const auto& keys = down_cast<const KeyColumn*>(key_col)->get_data();
    const auto& counts = cnt_col->get_data();
    size_t pruned = 0;
    if (pruned_mask != nullptr) {
        for (size_t i = 0; i < n; ++i) {
            const uint64_t k = static_cast<uint64_t>(keys[i]);
            if (pruned_mask[ca->bucket(k)]) {
                ++pruned;
                continue;
            }
            merge->merge_sorted(k, counts[i]);
        }
    } else {
        for (size_t i = 0; i < n; ++i) {
            merge->merge_sorted(static_cast<uint64_t>(keys[i]), counts[i]);
        }
    }
    return pruned;
}

// Append an integral group key into its fixed-length column by logical type. The key was gated
// to a single non-nullable integral type, so it round-trips through a uint64 exactly.
Status append_group_key(Column* key_col, LogicalType key_lt, uint64_t key) {
    switch (key_lt) {
    case TYPE_BOOLEAN:
        down_cast<UInt8Column*>(key_col)->get_data().push_back(static_cast<uint8_t>(key));
        break;
    case TYPE_TINYINT:
        down_cast<Int8Column*>(key_col)->get_data().push_back(static_cast<int8_t>(key));
        break;
    case TYPE_SMALLINT:
        down_cast<Int16Column*>(key_col)->get_data().push_back(static_cast<int16_t>(key));
        break;
    case TYPE_INT:
        down_cast<Int32Column*>(key_col)->get_data().push_back(static_cast<int32_t>(key));
        break;
    case TYPE_BIGINT:
        down_cast<Int64Column*>(key_col)->get_data().push_back(static_cast<int64_t>(key));
        break;
    default:
        return Status::InternalError("cache-conscious top-n: unexpected group key type");
    }
    return Status::OK();
}
} // namespace

void Aggregator::activate_cache_conscious_topn(size_t fa_capacity) {
    // Snapshot the live map's (real key, count) FA before anything is frozen. The dense FA is not
    // set yet, so collect reads the hash map (decoding any bit-compress key encoding). If the key
    // is unsupported -- the FE gate should already preclude this -- stay on the normal path.
    std::vector<std::pair<uint64_t, int64_t>> fa_seed;
    if (!collect_cache_conscious_topn_groups(&fa_seed)) {
        return;
    }
    _cache_conscious_active = true;
    _cache_conscious_ca_spilled = false;
    // CA partition fanout. Paper: one staging cache line per partition, all resident in the CA half
    // of the cache, so fanout ~ budget / cache_line. But our arena also holds the routed tuples in
    // 64 KiB blocks (one per non-empty partition), so a high fanout multiplies the cold-tail RAM
    // footprint -- the auto value is power-of-two and capped well below the staging-only bound;
    // config::cache_conscious_topn_ca_fanout (0 = auto) overrides it for tuning sweeps.
    const int64_t cfg_fanout = config::cache_conscious_topn_ca_fanout;
    const int64_t fanout_target = cfg_fanout > 0 ? cfg_fanout : (config::cache_conscious_topn_l2_budget_bytes / 64);
    const size_t fanout_cap = (cfg_fanout > 0) ? CacheConsciousCa::kMaxFanout : 1024;
    size_t fanout = 64;
    while (fanout * 2 <= static_cast<size_t>(std::max<int64_t>(fanout_target, 64)) && fanout < fanout_cap) {
        fanout *= 2;
    }
    _cache_conscious_ca = std::make_unique<CacheConsciousCa>(cache_conscious_topn_limit(), fa_capacity, fanout,
                                                             config::cache_conscious_topn_swap_cooldown_chunks);
    // Pack the snapshot into the phmap-backed FA. From here it is the source of truth for FA
    // counts: the post-flip probe + count runs through it and the hash map is left dormant.
    _cache_conscious_fa = std::make_unique<CacheConsciousFa>();
    _cache_conscious_fa->build(fa_seed);
    // Prime the swap watermark with the k-th largest seed count (topKBound at flip). Skipped if the
    // seed holds fewer than k keys -- there is no bound to claim and the swap stays dormant.
    const int64_t topk_bound_flip = _cache_conscious_fa->kth_largest_count(cache_conscious_topn_limit());
    if (topk_bound_flip != INT64_MIN) {
        _cache_conscious_ca->prime_swap(topk_bound_flip);
    }
    // Pin the FE-supplied MCV (known-hot) group-by keys into FA -- before the bloom is built, so it
    // covers them. A key that first appears only after the flip then still lands in FA (its rows hit
    // FA instead of routing to CA) and the swap never evicts it. The carried counts are estimates and
    // unused; only the key list matters. Decoded from the per-key literal exprs built in open().
    for (auto* ctx : _cc_mcv_key_ctxs) {
        auto* lit = down_cast<VectorizedLiteral*>(ctx->root());
        const ColumnPtr& v = lit->value();
        if (v == nullptr || v->size() == 0 || v->is_null(0)) {
            continue;
        }
        int64_t key;
        switch (ctx->root()->type().type) {
        case TYPE_TINYINT:
            key = v->get(0).get_int8();
            break;
        case TYPE_SMALLINT:
            key = v->get(0).get_int16();
            break;
        case TYPE_INT:
            key = v->get(0).get_int32();
            break;
        case TYPE_BIGINT:
            key = v->get(0).get_int64();
            break;
        default:
            continue; // only integral keys are supported by the cc probe today
        }
        _cache_conscious_fa->seed_pinned(static_cast<uint64_t>(key));
    }
    // Bloom pre-filter decision. A non-positive threshold forces it on at the flip; otherwise the
    // post-flip miss rate over the first window of rows decides (see probe_cache_conscious_fa).
    _cc_postflip_rows = 0;
    _cc_postflip_hits = 0;
    _cc_bloom_decided = false;
    if (config::cache_conscious_topn_bloom_miss_threshold <= 0.0) {
        _cache_conscious_fa->build_bloom();
        _cache_conscious_fa->set_bloom_active(true);
        _cc_bloom_decided = true;
    }
    if (_cc_flipped != nullptr) {
        int64_t preflip_rows = 0;
        for (const auto& kv : fa_seed) preflip_rows += kv.second;
        COUNTER_SET(_cc_flipped, static_cast<int64_t>(1));
        COUNTER_SET(_cc_preflip_rows, preflip_rows);
        COUNTER_SET(_cc_fa_keys, static_cast<int64_t>(_cache_conscious_fa->size()));
        COUNTER_SET(_cc_mcv_seeded, static_cast<int64_t>(_cache_conscious_fa->size() - fa_seed.size()));
        COUNTER_SET(_cc_ca_partitions, static_cast<int64_t>(_cache_conscious_ca->fanout()));
    }
}

Status Aggregator::probe_cache_conscious_fa(Chunk* chunk, size_t chunk_size) {
    // Post-flip per-row FA work, mirroring how route_cache_conscious_cold_rows reads input: probe
    // the dense FA for each row's raw key, bump the inline counter on a hit, and mark misses in
    // _streaming_selection (== 1) so route_cache_conscious_cold_rows sends them to CA. A 2-phase
    // plan merges the upstream partial count on a hit; a 1-phase colocate count(*) has no input
    // column, so each hit weighs 1.
    //
    // Evaluate the agg input columns first: the replaced compute_batch_agg_states_with_selection
    // did this, and the 2-phase route + this probe read the partial-count column out of
    // _agg_input_columns. For argument-less count(*) the expr list is empty, so this is a no-op.
    const bool use_intermediate = _use_intermediate_as_input();
    auto& agg_expr_ctxs = use_intermediate ? _intermediate_agg_expr_ctxs : _agg_expr_ctxs;
    for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
        RETURN_IF_ERROR(evaluate_agg_input_column(chunk, agg_expr_ctxs[i], i));
    }
    const LogicalType key_lt = _group_by_types[0].result_type.type;
    const bool merge_input = _is_merge_funcs[0] || use_intermediate;
    _cc_input_counts = nullptr;
    if (merge_input) {
        _cc_input_counts = down_cast<const Int64Column*>(ColumnHelper::get_data_column(_agg_input_columns[0][0].get()));
    } else if (_agg_input_columns[0][0] != nullptr && _agg_input_columns[0][0]->has_null()) {
        // COUNT(expr) preserves groups with a zero count, but NULL rows add no weight.
        const Column* input = _agg_input_columns[0][0].get();
        if (_cc_count_deltas == nullptr) {
            _cc_count_deltas = Int64Column::create();
        }
        auto& deltas = _cc_count_deltas->get_data();
        deltas.resize(chunk_size);
        for (size_t i = 0; i < chunk_size; ++i) {
            deltas[i] = input->is_null(i) ? 0 : 1;
        }
        _cc_input_counts = _cc_count_deltas.get();
    }
    const Int64Column* cnt_col = _cc_input_counts;
    const Column* key_col = ColumnHelper::get_data_column(_group_by_columns[0].get());
    _streaming_selection.assign(chunk_size, 0);
    CacheConsciousFa* fa = _cache_conscious_fa.get();
    uint8_t* sel = _streaming_selection.data();
    size_t fa_hits = 0;
    switch (key_lt) {
    case TYPE_BOOLEAN:
        fa_hits = probe_cc_fa<UInt8Column>(fa, key_col, cnt_col, sel, chunk_size);
        break;
    case TYPE_TINYINT:
        fa_hits = probe_cc_fa<Int8Column>(fa, key_col, cnt_col, sel, chunk_size);
        break;
    case TYPE_SMALLINT:
        fa_hits = probe_cc_fa<Int16Column>(fa, key_col, cnt_col, sel, chunk_size);
        break;
    case TYPE_INT:
        fa_hits = probe_cc_fa<Int32Column>(fa, key_col, cnt_col, sel, chunk_size);
        break;
    case TYPE_BIGINT:
        fa_hits = probe_cc_fa<Int64Column>(fa, key_col, cnt_col, sel, chunk_size);
        break;
    default:
        break; // unsupported key gated out at flip time
    }
    if (_cc_fa_hit_rows != nullptr) {
        COUNTER_UPDATE(_cc_fa_hit_rows, static_cast<int64_t>(fa_hits));
        COUNTER_UPDATE(_cc_ca_routed_rows, static_cast<int64_t>(chunk_size - fa_hits));
    }
    // Decide the bloom pre-filter once, after a short observation window: turn it on iff the
    // post-flip miss rate cleared the threshold. A cold-tail stream (most rows miss) activates it;
    // a hit-heavy stream leaves it off, so the probe pays nothing extra there.
    _cc_postflip_rows += static_cast<int64_t>(chunk_size);
    _cc_postflip_hits += static_cast<int64_t>(fa_hits);
    if (!_cc_bloom_decided && _cc_postflip_rows >= (1 << 18)) {
        _cc_bloom_decided = true;
        const double miss_rate = 1.0 - static_cast<double>(_cc_postflip_hits) / static_cast<double>(_cc_postflip_rows);
        if (miss_rate >= config::cache_conscious_topn_bloom_miss_threshold) {
            _cache_conscious_fa->build_bloom();
            _cache_conscious_fa->set_bloom_active(true);
        }
    }
    return Status::OK();
}

void Aggregator::route_cache_conscious_cold_rows(size_t chunk_size) {
    // Called on push after build_hash_map_with_selection marked misses (selection == 1) and the
    // live group-by / aggregate-input columns were evaluated. Route each miss row to its CA
    // partition, mirroring how the live path reads input (compute_batch_agg_states): a 2-phase
    // plan merges the partial count from the first input column, a 1-phase colocate count(*) has
    // no input column so each row counts as 1.
    const LogicalType key_lt = _group_by_types[0].result_type.type;
    const Int64Column* cnt_col = _cc_input_counts;
    const Column* key_col = ColumnHelper::get_data_column(_group_by_columns[0].get());
    CacheConsciousCa* ca = _cache_conscious_ca.get();
    switch (key_lt) {
    case TYPE_BOOLEAN:
        route_cold_rows<UInt8Column>(ca, key_col, cnt_col, _streaming_selection, chunk_size);
        break;
    case TYPE_TINYINT:
        route_cold_rows<Int8Column>(ca, key_col, cnt_col, _streaming_selection, chunk_size);
        break;
    case TYPE_SMALLINT:
        route_cold_rows<Int16Column>(ca, key_col, cnt_col, _streaming_selection, chunk_size);
        break;
    case TYPE_INT:
        route_cold_rows<Int32Column>(ca, key_col, cnt_col, _streaming_selection, chunk_size);
        break;
    case TYPE_BIGINT:
        route_cold_rows<Int64Column>(ca, key_col, cnt_col, _streaming_selection, chunk_size);
        break;
    default:
        break; // unsupported key gated out at flip time
    }
#if defined(__x86_64__)
    // A driver or spill task may resume on another thread after this chunk.
    // Publish all non-temporal arena writes before handing off the state.
    _mm_sfence();
#endif
}

void Aggregator::maybe_swap_cache_conscious() {
    // Disabled once the CA has spilled: the swap rebuilds a partition off its in-RAM rows only, but a
    // spilled partition's mass is on disk with its upper bound retained, and restore re-routes those
    // rows without bumping the stat. Promoting then would double-count the on-disk rows and corrupt
    // the bound, so after a spill the late hot keys are left to the end-of-input prune.
    if (config::cache_conscious_topn_enable_swap && _cache_conscious_active && !_cache_conscious_ca_spilled &&
        _cache_conscious_ca != nullptr && _cache_conscious_fa != nullptr) {
        _cache_conscious_ca->swap_pass(_cache_conscious_fa.get());
    }
}

Status Aggregator::finalize_cache_conscious_topn(RuntimeState* state) {
    if (!_cache_conscious_active) {
        return Status::OK();
    }
    // Freeze the swap telemetry now -- the sink is done, the CA is still alive on both the spill and
    // the in-memory path, and no further chunk will run the swap.
    if (_cc_swap_arm_chunk != nullptr && _cache_conscious_ca != nullptr) {
        COUNTER_SET(_cc_swap_arm_chunk, static_cast<int64_t>(_cache_conscious_ca->swap_arm_chunk()));
        COUNTER_SET(_cc_swap_promotions, static_cast<int64_t>(_cache_conscious_ca->swap_promotions()));
        COUNTER_SET(_cc_swap_evictions, static_cast<int64_t>(_cache_conscious_ca->swap_evictions()));
        COUNTER_SET(_cc_swap_skipped_scattered, static_cast<int64_t>(_cache_conscious_ca->swap_skipped_scattered()));
        COUNTER_SET(_cc_swap_declined, static_cast<int64_t>(_cache_conscious_ca->swap_declined()));
        COUNTER_SET(_cc_swap_estimate_skipped, static_cast<int64_t>(_cache_conscious_ca->swap_estimate_skipped()));
        COUNTER_SET(_cc_swap_reaggregated_tuples,
                    static_cast<int64_t>(_cache_conscious_ca->swap_reaggregated_tuples()));
    }
    if (_cc_bloom_active != nullptr && _cache_conscious_fa != nullptr) {
        COUNTER_SET(_cc_bloom_active, _cache_conscious_fa->bloom_active() ? int64_t{1} : int64_t{0});
    }
    // If the CA spilled, its tuples are on disk and can only be read back on the source side
    // (the spiller restores after the sink is complete). The source drives restore + finalize
    // pull-driven; keep the CA alive (do not reset here).
    if (_cache_conscious_ca_spilled) {
        return Status::OK();
    }
    // Non-spill: collect FA exactly once, hand FA + the live CA partitions to a multi-step prune
    // session, and let the source advance it. Each pull resolves / re-partitions one partition
    // and yields, so a large surviving CA cannot monopolize the driver thread. Once the session
    // drains, the source's advance call builds the result chunk and the next pull emits it.
    std::vector<std::pair<uint64_t, int64_t>> fa_pairs;
    if (!collect_cache_conscious_topn_groups(&fa_pairs)) {
        return Status::OK(); // unsupported key slipped through; the normal convert still emits FA
    }
    std::vector<CacheConsciousTopN::Group> fa;
    fa.reserve(fa_pairs.size());
    for (const auto& [key, count] : fa_pairs) {
        fa.push_back({key, count});
    }
    _prune_session =
            std::make_unique<CacheConsciousTopN::PruneSession>(_cache_conscious_ca->begin_finalize(std::move(fa)));
    return Status::OK();
}

Status Aggregator::advance_cache_conscious_prune() {
    if (_prune_session == nullptr) {
        return Status::OK();
    }
    // One driver visit takes a small bounded number of partition steps. Each step is one PQ pop
    // (resolve or re-partition); 16 is enough that small CAs finish in a single pull and large
    // ones still yield often enough to keep the pipeline scheduler honest. The exact number is
    // not load-bearing -- the yield property is what M1e cares about.
    constexpr int kStepsPerCall = 16;
    for (int i = 0; i < kStepsPerCall && !_prune_session->done(); ++i) {
        _prune_session->step();
    }
    if (!_prune_session->done()) {
        return Status::OK();
    }
    std::vector<CacheConsciousTopN::Group> top = _prune_session->finish();
    if (_cc_partitions_resolved != nullptr) {
        COUNTER_SET(_cc_partitions_resolved, static_cast<int64_t>(_prune_session->resolved_partitions()));
        COUNTER_SET(_cc_partitions_repartitioned, static_cast<int64_t>(_prune_session->repartitioned_partitions()));
        COUNTER_SET(_cc_partitions_pruned, static_cast<int64_t>(_prune_session->pruned_partitions()));
        COUNTER_SET(_cc_reprocessed_tuples, static_cast<int64_t>(_prune_session->reprocessed_tuples()));
        COUNTER_SET(_cc_pruned_groups, static_cast<int64_t>(_prune_session->pruned_groups()));
        COUNTER_SET(_cc_max_radix_level, static_cast<int64_t>(_prune_session->max_radix_level()));
    }
    _prune_session.reset();
    // Free the CA tuples once the result is ready; the source's has_output stays on the cc
    // branch until every result chunk has been pulled.
    _cache_conscious_ca.reset();
    std::vector<std::pair<uint64_t, int64_t>> result;
    result.reserve(top.size());
    for (const auto& g : top) {
        result.emplace_back(g.key, g.count);
    }
    return _build_cache_conscious_result_chunk(result);
}

Status Aggregator::_build_cache_conscious_result_chunk(const std::vector<std::pair<uint64_t, int64_t>>& result) {
    _cc_result_offset = 0;
    const size_t n = result.size();
    MutableColumns group_by_columns = _create_group_by_columns(n);
    // The flip is gated to a finalizing, non-pre-cache operator (see the sink's flip guard), so
    // it always emits the final result layout; build the count column finalized, not serialized.
    MutableColumns agg_result_columns = _create_agg_result_columns(n, /*use_intermediate=*/false);

    // count(*) result column is a non-nullable int64.
    auto* count_col = down_cast<Int64Column*>(ColumnHelper::get_data_column(agg_result_columns[0].get()));
    // The group key was gated to a single non-nullable integral type, so append it as the
    // exact value of that fixed-length column.
    Column* key_col = ColumnHelper::get_data_column(group_by_columns[0].get());
    const LogicalType key_lt = _group_by_types[0].result_type.type;
    for (const auto& [key, count] : result) {
        RETURN_IF_ERROR(append_group_key(key_col, key_lt, key));
        count_col->get_data().push_back(count);
    }

    _cache_conscious_result_chunk = _build_output_chunk(std::move(group_by_columns), std::move(agg_result_columns),
                                                        /*use_intermediate_as_output=*/false);
    _cache_conscious_result_ready = true;
    return Status::OK();
}

std::function<StatusOr<ChunkPtr>()> Aggregator::_build_cache_conscious_ca_spill_task(RuntimeState* state) {
    // Resumable drain cursor over the CA partitions. take_partition_tuples empties a partition and
    // leaves its logical stat behind (so prune still works and the partition is routable again);
    // `pid` is the next partition to drain, `drained`/`pos` the tuples taken from the current one
    // not yet emitted. Each call yields one intermediate (key, partial) chunk; the pid is not
    // preserved — restore re-buckets by key — so partitions share a chunk, avoiding tiny chunks.
    const LogicalType key_lt = _group_by_types[0].result_type.type;
    const size_t fanout = _cache_conscious_ca->fanout();
    const size_t batch_rows = state->chunk_size();
    return [this, key_lt, fanout, batch_rows, pid = size_t{0}, drained = std::vector<CacheConsciousTopN::Group>{},
            pos = size_t{0}]() mutable -> StatusOr<ChunkPtr> {
        std::vector<CacheConsciousTopN::Group> batch;
        batch.reserve(batch_rows);
        while (batch.size() < batch_rows) {
            if (pos >= drained.size()) {
                drained.clear();
                pos = 0;
                while (pid < fanout && drained.empty()) {
                    drained = _cache_conscious_ca->take_partition_tuples(pid);
                    ++pid;
                }
                if (drained.empty()) {
                    break; // all partitions consumed
                }
            }
            while (pos < drained.size() && batch.size() < batch_rows) {
                batch.push_back(drained[pos++]);
            }
        }
        if (batch.empty()) {
            return Status::EndOfFile("cache-conscious CA drained");
        }
        const size_t n = batch.size();
        MutableColumns group_by_columns = _create_group_by_columns(n);
        MutableColumns agg_result_columns = _create_agg_result_columns(n, /*use_intermediate=*/true);
        auto* count_col = down_cast<Int64Column*>(ColumnHelper::get_data_column(agg_result_columns[0].get()));
        Column* key_col = ColumnHelper::get_data_column(group_by_columns[0].get());
        for (const auto& g : batch) {
            RETURN_IF_ERROR(append_group_key(key_col, key_lt, g.key));
            count_col->get_data().push_back(g.count);
        }
        return _build_output_chunk(std::move(group_by_columns), std::move(agg_result_columns),
                                   /*use_intermediate_as_output=*/true);
    };
}

Status Aggregator::spill_cache_conscious_ca(RuntimeState* state) {
    if (!_cache_conscious_active || _cache_conscious_ca == nullptr) {
        return Status::OK();
    }
    // Mark spilled synchronously: set_finishing and the source key off this, and it must hold even
    // when the remainder is still draining in the channel.
    _cache_conscious_ca_spilled = true;
    if (_cc_ca_spilled != nullptr) {
        COUNTER_SET(_cc_ca_spilled, static_cast<int64_t>(1));
    }
    auto& spiller = _spiller;
    auto task = _build_cache_conscious_ca_spill_task(state);
    // Spill inline while the spiller has room (honors the spill() !is_full contract); the moment it
    // fills, hand the same generator to the spill channel so the SpillProcessOperator drains the
    // rest with yield/backpressure. need_input gates on is_full / has_task, so push pauses while the
    // channel drains and never races it for the CA partitions.
    while (!spiller->is_full()) {
        auto chunk_st = task();
        if (chunk_st.ok()) {
            RETURN_IF_ERROR(spiller->spill(state, chunk_st.value(), TRACKER_WITH_SPILLER_GUARD(state, spiller)));
        } else if (chunk_st.status().is_end_of_file()) {
            return Status::OK();
        } else {
            return chunk_st.status();
        }
    }
    _spill_channel->add_spill_task({std::move(task)});
    return Status::OK();
}

void Aggregator::queue_cache_conscious_ca_tail(RuntimeState* state) {
    _spill_channel->add_spill_task({_build_cache_conscious_ca_spill_task(state)});
}

Status Aggregator::restore_cache_conscious_chunk(RuntimeState* state) {
    // Runs are merged by group key. Aggregate one cold group across chunk boundaries and
    // retain only its top-k candidate; no cold tuples are materialized back into the arenas.
    auto& spiller = _spiller;
    ASSIGN_OR_RETURN(ChunkPtr chunk, spiller->restore(state, TRACKER_WITH_SPILLER_READER_GUARD(state, spiller)));
    if (chunk == nullptr || chunk->is_empty()) {
        return Status::OK();
    }
    // FA is final before restore. Its k-th count is a safe lower bound for the result,
    // so partitions below it may be skipped without entering the sorted group merger.
    if (_cc_spill_merge == nullptr) {
        _cc_spill_merge = std::make_unique<CacheConsciousTopN::SpillMerge>(cache_conscious_topn_limit());
        std::vector<std::pair<uint64_t, int64_t>> fa_pairs;
        if (collect_cache_conscious_topn_groups(&fa_pairs)) {
            std::vector<CacheConsciousTopN::Group> fa;
            fa.reserve(fa_pairs.size());
            for (const auto& [key, count] : fa_pairs) {
                fa.push_back({key, count});
                _cc_spill_merge->add_exact({key, count});
            }
            const int64_t threshold = _cache_conscious_ca->topk_threshold(fa);
            _cache_conscious_pruned_mask = _cache_conscious_ca->pruned_mask(threshold);
        }
    }
    const uint8_t* pruned = _cache_conscious_pruned_mask.empty() ? nullptr : _cache_conscious_pruned_mask.data();
    // Read the spilled intermediate chunk directly by column position ([key, count]) instead of
    // evaluate_agg_fn_exprs: a 1-phase colocate count(*) has no intermediate agg ctx, so the
    // evaluate path would not resolve. The spiller preserves column order on restore.
    const LogicalType key_lt = _group_by_types[0].result_type.type;
    const size_t n = chunk->num_rows();
    const Column* key_col = ColumnHelper::get_data_column(chunk->get_column_by_index(0).get());
    const auto* cnt_col =
            down_cast<const Int64Column*>(ColumnHelper::get_data_column(chunk->get_column_by_index(1).get()));
    size_t pruned_rows = 0;
    switch (key_lt) {
    case TYPE_BOOLEAN:
        pruned_rows = restore_cold_tuples<UInt8Column>(_cache_conscious_ca.get(), _cc_spill_merge.get(), key_col,
                                                       cnt_col, n, pruned);
        break;
    case TYPE_TINYINT:
        pruned_rows = restore_cold_tuples<Int8Column>(_cache_conscious_ca.get(), _cc_spill_merge.get(), key_col,
                                                      cnt_col, n, pruned);
        break;
    case TYPE_SMALLINT:
        pruned_rows = restore_cold_tuples<Int16Column>(_cache_conscious_ca.get(), _cc_spill_merge.get(), key_col,
                                                       cnt_col, n, pruned);
        break;
    case TYPE_INT:
        pruned_rows = restore_cold_tuples<Int32Column>(_cache_conscious_ca.get(), _cc_spill_merge.get(), key_col,
                                                       cnt_col, n, pruned);
        break;
    case TYPE_BIGINT:
        pruned_rows = restore_cold_tuples<Int64Column>(_cache_conscious_ca.get(), _cc_spill_merge.get(), key_col,
                                                       cnt_col, n, pruned);
        break;
    default:
        return Status::InternalError("cache-conscious top-n: unexpected group key type");
    }
    if (_cc_ca_restored_rows != nullptr) {
        COUNTER_UPDATE(_cc_ca_restored_rows, static_cast<int64_t>(n - pruned_rows));
        COUNTER_UPDATE(_cc_ca_restore_pruned_rows, static_cast<int64_t>(pruned_rows));
    }
    return Status::OK();
}

Status Aggregator::finalize_cache_conscious_ca(RuntimeState* state) {
    // An empty cold stream never entered restore_cache_conscious_chunk; seed from FA here.
    if (_cc_spill_merge == nullptr) {
        _cc_spill_merge = std::make_unique<CacheConsciousTopN::SpillMerge>(cache_conscious_topn_limit());
        std::vector<std::pair<uint64_t, int64_t>> fa_pairs;
        if (!collect_cache_conscious_topn_groups(&fa_pairs)) {
            return Status::InternalError("cache-conscious top-n: cannot collect final groups");
        }
        for (const auto& [key, count] : fa_pairs) _cc_spill_merge->add_exact({key, count});
    }
    const auto top = _cc_spill_merge->finish();
    _cc_spill_merge.reset();
    _cache_conscious_ca.reset();
    std::vector<std::pair<uint64_t, int64_t>> result;
    result.reserve(top.size());
    for (const auto& g : top) result.emplace_back(g.key, g.count);
    return _build_cache_conscious_result_chunk(result);
}

Status Aggregator::convert_hash_map_to_chunk(int32_t chunk_size, ChunkPtr* chunk,
                                             bool force_use_intermediate_as_output) {
    SCOPED_TIMER(_agg_stat->get_results_timer);

    // Keep the visit call outside RETURN_IF_ERROR: the lambda body carries #define/#undef
    // blocks (the min/max LT dispatch), and preprocessor directives inside a macro argument
    // are undefined behavior -- gcc silently skips them, leaving M(...) unexpanded.
    Status visit_status = _hash_map_variant.visit([&, this](auto& variant_value) {
        auto& hash_map_with_key = *variant_value;
        using HashMapWithKey = std::remove_reference_t<decltype(hash_map_with_key)>;

        // Pack finalize: iterate the phmap directly, draining field k of every cell into
        // aggregate k's result column. The intermediate type of every pack member equals its
        // result type (all BIGINT), so the same drain serves the final output and the
        // spill/intermediate drain; only the count members' outputs are non-nullable.
        if constexpr (agg_inline_pack<HashMapWithKey>) {
            if (_inline_pack) {
                using MapIter = typename HashMapWithKey::HashMapType::iterator;
                MapIter it = (_it_hash.type() == typeid(RawHashTableIterator)) ? hash_map_with_key.hash_map.begin()
                                                                               : std::any_cast<MapIter>(_it_hash);
                auto end = hash_map_with_key.hash_map.end();
                const auto hash_map_size = _hash_map_variant.size();
                auto num_rows = std::min<size_t>(hash_map_size - _num_rows_processed, chunk_size);
                auto use_intermediate = force_use_intermediate_as_output || _use_intermediate_as_output();
                MutableColumns group_by_columns = _create_group_by_columns(num_rows);
                MutableColumns agg_result_columns = _create_agg_result_columns(num_rows, use_intermediate);

                Int64Column* field_cols[4] = {};
                for (uint8_t k = 0; k < _inline_pack_n; ++k) {
                    Column* acc_col = agg_result_columns[k].get();
                    Column* data_col = acc_col->is_nullable()
                                               ? down_cast<NullableColumn*>(acc_col)->data_column_raw_ptr()
                                               : acc_col;
                    field_cols[k] = down_cast<Int64Column*>(data_col);
                }
                int32_t read_index = 0;
                hash_map_with_key.results.resize(chunk_size);
                {
                    SCOPED_TIMER(_agg_stat->iter_timer);
                    while ((it != end) & (read_index < chunk_size)) {
                        hash_map_with_key.results[read_index] = it->first;
                        const InlinePackCell& cell = it->second;
                        for (uint8_t k = 0; k < _inline_pack_n; ++k) {
                            field_cols[k]->get_data().emplace_back(cell.f[k]);
                        }
                        ++read_index;
                        ++it;
                    }
                }
                for (uint8_t k = 0; k < _inline_pack_n; ++k) {
                    Column* acc_col = agg_result_columns[k].get();
                    if (acc_col->is_nullable()) {
                        auto* nullable = down_cast<NullableColumn*>(acc_col);
                        nullable->null_column_data().resize(read_index, 0);
                        nullable->set_has_null(false);
                    }
                }
                if (read_index > 0) {
                    SCOPED_TIMER(_agg_stat->group_by_append_timer);
                    hash_map_with_key.insert_keys_to_columns(hash_map_with_key.results, group_by_columns, read_index);
                }
                _is_ht_eos = (it == end);
                // The NULL-key group rides in null_key_data (the whole pack cell) + the
                // existence bit; emit it as one extra row exactly like the single-op drain.
                if constexpr (HashMapWithKey::has_single_null_key) {
                    if (_is_ht_eos && hash_map_with_key.has_null_key()) {
                        if (read_index < chunk_size) {
                            DCHECK(group_by_columns.size() == 1);
                            DCHECK(group_by_columns[0]->is_nullable());
                            group_by_columns[0]->append_default();
                            const InlinePackCell& cell = hash_map_with_key.null_key_data;
                            for (uint8_t k = 0; k < _inline_pack_n; ++k) {
                                field_cols[k]->get_data().emplace_back(cell.f[k]);
                                Column* acc_col = agg_result_columns[k].get();
                                if (acc_col->is_nullable()) {
                                    down_cast<NullableColumn*>(acc_col)->null_column_data().emplace_back(0);
                                }
                            }
                            ++read_index;
                        } else {
                            _is_ht_eos = false;
                        }
                    }
                }
                _it_hash = it;
                auto result_chunk = _build_output_chunk(std::move(group_by_columns), std::move(agg_result_columns),
                                                        use_intermediate);
                _num_rows_returned += read_index;
                _num_rows_processed += read_index;
                *chunk = std::move(result_chunk);
                return Status::OK();
            }
            DCHECK(false) << "pack variant active without the pack gate";
            return Status::InternalError("pack variant active without the pack gate");
        } else {
            // Inline-agg finalize: iterate the phmap directly (key from the cell,
            // count from the in-slot int64), instead of the arena walk. The operators
            // seed _it_hash with a RawHashTableIterator (arena begin); on the first
            // inline round we replace it with the phmap begin() and resume from there.
            if constexpr (agg_inline_supported<HashMapWithKey>) {
                if (_inline_agg) {
                    using MapIter = typename HashMapWithKey::HashMapType::iterator;
                    MapIter it = (_it_hash.type() == typeid(RawHashTableIterator)) ? hash_map_with_key.hash_map.begin()
                                                                                   : std::any_cast<MapIter>(_it_hash);
                    auto end = hash_map_with_key.hash_map.end();
                    const auto hash_map_size = _hash_map_variant.size();
                    auto num_rows = std::min<size_t>(hash_map_size - _num_rows_processed, chunk_size);
                    auto use_intermediate = force_use_intermediate_as_output || _use_intermediate_as_output();
                    MutableColumns group_by_columns = _create_group_by_columns(num_rows);
                    MutableColumns agg_result_columns = _create_agg_result_columns(num_rows, use_intermediate);

                    // Drain the slots into the result column with the op's slot type: int64 ->
                    // Int64Column for the counts and sum(int), double -> DoubleColumn for
                    // sum(double). The intermediate type equals the result type for every inline op
                    // (the contract: intermediate type == result type), so this also serves the
                    // spill/intermediate drain.
                    Column* acc_col = agg_result_columns[0].get();
                    Column* acc_data_col = acc_col->is_nullable()
                                                   ? down_cast<NullableColumn*>(acc_col)->data_column_raw_ptr()
                                                   : acc_col;
                    int32_t read_index = 0;
                    hash_map_with_key.results.resize(chunk_size);
                    auto drain = [&](auto* typed_col, auto slot_type_tag) {
                        using SlotT = decltype(slot_type_tag);
                        SCOPED_TIMER(_agg_stat->iter_timer);
                        while ((it != end) & (read_index < chunk_size)) {
                            hash_map_with_key.results[read_index] = it->first;
                            typed_col->get_data().emplace_back(agg_inline_slot_load<SlotT>(it->second));
                            ++read_index;
                            ++it;
                        }
                    };
                    if (_inline_op == InlineOpKind::kMin || _inline_op == InlineOpKind::kMax) {
                        switch (_inline_minmax_lt) {
#define M(LT)                                                                         \
    case LT:                                                                          \
        drain(down_cast<RunTimeColumnType<LT>*>(acc_data_col), RunTimeCppType<LT>{}); \
        break;
                            INLINE_MINMAX_LT_CASES(M)
#undef M
                        default:
                            DCHECK(false) << "unexpected inline minmax type " << _inline_minmax_lt;
                        }
                    } else if (_inline_op == InlineOpKind::kSumDouble) {
                        drain(down_cast<DoubleColumn*>(acc_data_col), double{});
                    } else {
                        drain(down_cast<Int64Column*>(acc_data_col), int64_t{});
                    }
                    if (acc_col->is_nullable()) {
                        auto* nullable = down_cast<NullableColumn*>(acc_col);
                        nullable->null_column_data().resize(read_index, 0);
                        nullable->set_has_null(false);
                    }
                    if (read_index > 0) {
                        SCOPED_TIMER(_agg_stat->group_by_append_timer);
                        hash_map_with_key.insert_keys_to_columns(hash_map_with_key.results, group_by_columns,
                                                                 read_index);
                    }
                    _is_ht_eos = (it == end);
                    // The NULL group is kept out of the hash map in null_key_data (reused as a slot).
                    // Emit it as one extra row in the final chunk: NULL key + its accumulator. If it
                    // does not fit this chunk, hold eos so it lands in the next round.
                    if constexpr (HashMapWithKey::has_single_null_key) {
                        if (_is_ht_eos && hash_map_with_key.has_null_key()) {
                            if (read_index < chunk_size) {
                                DCHECK(group_by_columns.size() == 1);
                                DCHECK(group_by_columns[0]->is_nullable());
                                group_by_columns[0]->append_default();
                                auto drain_null = [&](auto* typed_col, auto slot_type_tag) {
                                    using SlotT = decltype(slot_type_tag);
                                    typed_col->get_data().emplace_back(
                                            agg_inline_slot_load<SlotT>(hash_map_with_key.null_key_data));
                                };
                                if (_inline_op == InlineOpKind::kMin || _inline_op == InlineOpKind::kMax) {
                                    switch (_inline_minmax_lt) {
#define M(LT)                                                                              \
    case LT:                                                                               \
        drain_null(down_cast<RunTimeColumnType<LT>*>(acc_data_col), RunTimeCppType<LT>{}); \
        break;
                                        INLINE_MINMAX_LT_CASES(M)
#undef M
                                    default:
                                        DCHECK(false) << "unexpected inline minmax type " << _inline_minmax_lt;
                                    }
                                } else if (_inline_op == InlineOpKind::kSumDouble) {
                                    drain_null(down_cast<DoubleColumn*>(acc_data_col), double{});
                                } else {
                                    drain_null(down_cast<Int64Column*>(acc_data_col), int64_t{});
                                }
                                if (acc_col->is_nullable()) {
                                    down_cast<NullableColumn*>(acc_col)->null_column_data().emplace_back(0);
                                }
                                ++read_index;
                            } else {
                                _is_ht_eos = false;
                            }
                        }
                    }
                    _it_hash = it;
                    auto result_chunk = _build_output_chunk(std::move(group_by_columns), std::move(agg_result_columns),
                                                            use_intermediate);
                    _num_rows_returned += read_index;
                    _num_rows_processed += read_index;
                    *chunk = std::move(result_chunk);
                    return Status::OK();
                }
            }

            auto it = std::any_cast<RawHashTableIterator>(_it_hash);
            auto end = _state_allocator.end();

            const auto hash_map_size = _hash_map_variant.size();
            auto num_rows = std::min<size_t>(hash_map_size - _num_rows_processed, chunk_size);
            auto use_intermediate = force_use_intermediate_as_output || _use_intermediate_as_output();
            MutableColumns group_by_columns = _create_group_by_columns(num_rows);
            MutableColumns agg_result_columns = _create_agg_result_columns(num_rows, use_intermediate);

            int32_t read_index = 0;
            {
                SCOPED_TIMER(_agg_stat->iter_timer);
                hash_map_with_key.results.resize(chunk_size);
                // get key/value from hashtable
                while ((it != end) & (read_index < chunk_size)) {
                    auto* value = it.value();
                    hash_map_with_key.results[read_index] = *reinterpret_cast<typename HashMapWithKey::KeyType*>(value);
                    _tmp_agg_states[read_index] = value;
                    ++read_index;
                    it.next();
                }
            }

            if (read_index > 0) {
                {
                    SCOPED_TIMER(_agg_stat->group_by_append_timer);
                    // Pass MutableColumns directly to hashtable interface
                    hash_map_with_key.insert_keys_to_columns(hash_map_with_key.results, group_by_columns, read_index);
                }

                {
                    SCOPED_TIMER(_agg_stat->agg_append_timer);
                    SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
                    if (!use_intermediate) {
                        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
                            TRY_CATCH_BAD_ALLOC(_agg_functions[i]->batch_finalize(
                                    _agg_fn_ctxs[i], read_index, _tmp_agg_states, _agg_states_offsets[i],
                                    agg_result_columns[i].get()));
                        }
                    } else {
                        for (size_t i = 0; i < _agg_fn_ctxs.size(); i++) {
                            TRY_CATCH_BAD_ALLOC(_agg_functions[i]->batch_serialize(
                                    _agg_fn_ctxs[i], read_index, _tmp_agg_states, _agg_states_offsets[i],
                                    agg_result_columns[i].get()));
                        }
                    }
                }
            }

            RETURN_IF_ERROR(check_has_error());
            _is_ht_eos = (it == end);

            // If there is null key, output it last
            if constexpr (HashMapWithKey::has_single_null_key) {
                if (_is_ht_eos && hash_map_with_key.has_null_key()) {
                    // The output chunk size couldn't larger than _state->chunk_size()
                    if (read_index < _state->chunk_size()) {
                        // For multi group by key, we don't need to special handle null key
                        DCHECK(group_by_columns.size() == 1);
                        DCHECK(group_by_columns[0]->is_nullable());
                        group_by_columns[0]->append_default();
                        SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
                        if (!use_intermediate) {
                            TRY_CATCH_BAD_ALLOC(
                                    _finalize_to_chunk(hash_map_with_key.null_key_data, agg_result_columns));
                        } else {
                            TRY_CATCH_BAD_ALLOC(
                                    _serialize_to_chunk(hash_map_with_key.null_key_data, agg_result_columns));
                        }
                        RETURN_IF_ERROR(check_has_error());
                        ++read_index;
                    } else {
                        // Output null key in next round
                        _is_ht_eos = false;
                    }
                }
            }

            _it_hash = it;
            auto result_chunk =
                    _build_output_chunk(std::move(group_by_columns), std::move(agg_result_columns), use_intermediate);
            _num_rows_returned += read_index;
            _num_rows_processed += read_index;
            *chunk = std::move(result_chunk);

            return Status::OK();
        }
    });
    RETURN_IF_ERROR(visit_status);

    return Status::OK();
}

void Aggregator::build_hash_set(size_t chunk_size) {
    _hash_set_variant.visit(
            [&](auto& hash_set) { hash_set->build_hash_set(chunk_size, _group_by_columns, _mem_pool.get()); });
}

void Aggregator::build_hash_set_with_selection(size_t chunk_size) {
    _hash_set_variant.visit([&](auto& hash_set) {
        hash_set->build_hash_set_with_selection(chunk_size, _group_by_columns, _mem_pool.get(), &_streaming_selection);
    });
}

void Aggregator::convert_hash_set_to_chunk(int32_t chunk_size, ChunkPtr* chunk) {
    SCOPED_TIMER(_agg_stat->get_results_timer);

    _hash_set_variant.visit([&, this](auto& variant_value) {
        auto& hash_set = *variant_value;
        using HashSetWithKey = std::remove_reference_t<decltype(hash_set)>;
        using Iterator = typename HashSetWithKey::Iterator;
        auto it = std::any_cast<Iterator>(_it_hash);
        auto end = hash_set.hash_set.end();
        const auto hash_set_size = _hash_set_variant.size();
        auto num_rows = std::min<size_t>(hash_set_size - _num_rows_processed, chunk_size);
        MutableColumns group_by_columns = _create_group_by_columns(num_rows);

        // Computer group by columns and aggregate result column
        int32_t read_index = 0;
        hash_set.results.resize(chunk_size);
        while (it != end && read_index < chunk_size) {
            // hash_set.insert_key_to_columns(*it, group_by_columns);
            hash_set.results[read_index] = *it;
            ++read_index;
            ++it;
        }

        {
            SCOPED_TIMER(_agg_stat->group_by_append_timer);
            hash_set.insert_keys_to_columns(hash_set.results, group_by_columns, read_index);
        }

        _is_ht_eos = (it == end);

        // IF there is null key, output it last
        if constexpr (HashSetWithKey::has_single_null_key) {
            if (_is_ht_eos && hash_set.has_null_key) {
                // The output chunk size couldn't larger than _state->chunk_size()
                if (read_index < _state->chunk_size()) {
                    // For multi group by key, we don't need to special handle null key
                    DCHECK(group_by_columns.size() == 1);
                    DCHECK(group_by_columns[0]->is_nullable());
                    group_by_columns[0]->append_default();
                    ++read_index;
                } else {
                    // Output null key in next round
                    _is_ht_eos = false;
                }
            }
        }

        _it_hash = it;

        ChunkPtr result_chunk = std::make_shared<Chunk>();
        // For different agg phase, we should use different TupleDescriptor
        auto use_intermediate = _use_intermediate_as_output();
        if (!use_intermediate) {
            for (size_t i = 0; i < group_by_columns.size(); i++) {
                result_chunk->append_column(std::move(group_by_columns[i]), _output_tuple_desc->slots()[i]->id());
            }
        } else {
            for (size_t i = 0; i < group_by_columns.size(); i++) {
                result_chunk->append_column(std::move(group_by_columns[i]), _intermediate_tuple_desc->slots()[i]->id());
            }
        }
        _num_rows_returned += read_index;
        _num_rows_processed += read_index;
        *chunk = std::move(result_chunk);
    });
}

void Aggregator::_release_agg_memory() {
    // If all function states are of POD type,
    // then we don't have to traverse the hash table to call destroy method.
    //
    SCOPED_THREAD_LOCAL_STATE_ALLOCATOR_SETTER(_allocator.get());
    _hash_map_variant.visit([&](auto& hash_map_with_key) {
        using MapType = std::remove_reference_t<decltype(*hash_map_with_key)>;
        if constexpr (agg_inline_pack<MapType>) {
            // Pack accumulators are POD cells inside the map value; there are no arena
            // states (and no state pointers) to destroy.
            return;
        } else {
            bool skip_destroy = std::all_of(_agg_functions.begin(), _agg_functions.end(),
                                            [](auto* func) { return func->is_pod_state(); });
            if (hash_map_with_key != nullptr && !skip_destroy) {
                auto null_data_ptr = hash_map_with_key->get_null_key_data();
                if (null_data_ptr != nullptr) {
                    for (int i = 0; i < _agg_functions.size(); i++) {
                        _agg_functions[i]->destroy(_agg_fn_ctxs[i], null_data_ptr + _agg_states_offsets[i]);
                    }
                }
                auto it = _state_allocator.begin();
                auto end = _state_allocator.end();

                while (it != end) {
                    for (int i = 0; i < _agg_functions.size(); i++) {
                        _agg_functions[i]->destroy(_agg_fn_ctxs[i], it.value() + _agg_states_offsets[i]);
                    }
                    it.next();
                }
            }
        }
    });
}

} // namespace starrocks
