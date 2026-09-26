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

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"

#include "exec/hash_joiner.h"
#include "exec/pipeline/chunk_accumulate_operator.h"
#include "exec/pipeline/hashjoin/hash_joiner_factory.h"
#include "exec/pipeline/hashjoin/local_runtime_filter_feedback.h"
#include "exec/pipeline/project_operator.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "runtime/current_thread.h"

namespace starrocks::pipeline {

void HashJoinProbeOperator::configure_local_runtime_filter_feedback(const Operators& operators) {
    if (operators.empty() || dynamic_cast<ScanOperator*>(operators.front().get()) == nullptr) return;
    auto* source = operators.front().get();
    const auto* filters = source->runtime_bloom_filters();
    if (filters == nullptr || filters->empty()) return;
    for (size_t i = 1; i < operators.size(); ++i) {
        auto* op = operators[i].get();
        if (typeid(*op) == typeid(HashJoinProbeOperator)) {
            auto* join = down_cast<HashJoinProbeOperator*>(op);
            const auto type = join->_join_prober->join_type();
            if (type != TJoinOp::INNER_JOIN && type != TJoinOp::LEFT_SEMI_JOIN) return;
            for (const auto& [id, desc] : filters->descriptors()) {
                if (!desc->is_local() || desc->is_stream_build_filter() ||
                    desc->build_plan_node_id() != join->get_plan_node_id())
                    return;
            }
            std::vector<RuntimeProfile::Counter*> intermediate_timers;
            for (size_t j = 1; j < i; ++j) {
                for (const char* name : {"PushTotalTime", "PullTotalTime"}) {
                    auto* timer = operators[j]->common_metrics()->get_counter(name);
                    if (timer == nullptr) return;
                    intermediate_timers.push_back(timer);
                }
            }
            auto feedback = std::make_shared<LocalRuntimeFilterFeedback>();
            auto* profile = join->unique_metrics();
            for (int on = 0; on < 2; ++on) {
                const std::string prefix = on ? "LocalRfOn" : "LocalRfOff";
                join->_feedback_input_rows[on] = ADD_COUNTER(profile, prefix + "InputRows", TUnit::UNIT);
                join->_feedback_filter_time[on] = ADD_TIMER(profile, prefix + "FilterTime");
                join->_feedback_lookup_time[on] = ADD_TIMER(profile, prefix + "LookupTime");
                join->_feedback_intermediate_time[on] = ADD_TIMER(profile, prefix + "IntermediateTime");
            }
            join->_feedback_decisions = ADD_COUNTER(profile, "LocalRfDecisions", TUnit::UNIT);
            join->_feedback_switches = ADD_COUNTER(profile, "LocalRfSwitches", TUnit::UNIT);
            profile->add_info_string("LocalRfFeedbackSource", std::to_string(source->get_plan_node_id()));
            // Publish shared state only after all profile counters are ready.
            join->_feedback_intermediate_timers = std::move(intermediate_timers);
            source->set_local_runtime_filter_feedback(feedback);
            for (size_t j = 1; j < i; ++j) {
                if (typeid(*operators[j]) == typeid(ChunkAccumulateOperator)) {
                    operators[j]->set_local_runtime_filter_feedback(feedback);
                }
            }
            join->_probe_rf_feedback = std::move(feedback);
            return;
        }
        const auto* intervening_filters = op->runtime_bloom_filters();
        if ((typeid(*op) != typeid(ChunkAccumulateOperator) && typeid(*op) != typeid(ProjectOperator)) ||
            (intervening_filters != nullptr && !intervening_filters->empty()) || !op->rf_waiting_set().empty() ||
            !op->filter_null_value_columns().empty())
            return;
    }
}

Status HashJoinProbeOperator::drain_local_runtime_filter_input(RuntimeState* state, Operator* op) {
    if (typeid(*op) == typeid(ChunkAccumulateOperator)) {
        down_cast<ChunkAccumulateOperator*>(op)->drain_input();
    } else if (typeid(*op) == typeid(HashJoinProbeOperator)) {
        auto* join = down_cast<HashJoinProbeOperator*>(op);
        if (join->_probe_rf_feedback && join->_join_prober->has_referenced_hash_table()) {
            RETURN_IF_ERROR(join->_join_prober->drain_probe_input(state));
            join->_observe_local_rf_lookup();
        }
    }
    return Status::OK();
}

void HashJoinProbeOperator::_observe_local_rf_lookup() {
    if (!_probe_rf_feedback || !_probe_rf_feedback->enabled()) return;
    const auto& metrics = _join_prober->probe_metrics();
    const auto completed = _join_prober->completed_probe_rows();
    const auto lookup = COUNTER_VALUE(metrics.search_ht_timer);
    const auto keys = COUNTER_VALUE(metrics.probe_conjunct_evaluate_timer);
    const auto partition = COUNTER_VALUE(metrics.partition_probe_overhead);
    const auto decisions = _probe_rf_feedback->totals().decisions;
    int64_t intermediate = 0;
    for (auto* timer : _feedback_intermediate_timers) intermediate += COUNTER_VALUE(timer);
    // Partition dispatch includes key preparation for newly activated partitions.
    const auto ns =
            lookup - _feedback_lookup_ns + std::max(keys - _feedback_key_ns, partition - _feedback_partition_ns);
    _probe_rf_feedback->observe_lookup(completed - _feedback_completed_rows, ns,
                                       intermediate - _feedback_intermediate_ns);
    if (!_probe_rf_feedback->enabled()) {
        _unique_metrics->add_info_string(
                "LocalRfFallbackReason",
                strings::Substitute("lookup accounting: completed=$0 previous=$1 outstanding=$2 ns=$3 intermediate=$4",
                                    completed, _feedback_completed_rows, _probe_rf_feedback->outstanding_rows(), ns,
                                    intermediate - _feedback_intermediate_ns));
    }
    _feedback_completed_rows = completed;
    _feedback_lookup_ns = lookup;
    _feedback_key_ns = keys;
    _feedback_partition_ns = partition;
    _feedback_intermediate_ns = intermediate;
    if (decisions != _probe_rf_feedback->totals().decisions) _publish_local_rf_feedback();
}

void HashJoinProbeOperator::_publish_local_rf_feedback() {
    if (!_probe_rf_feedback) return;
    const auto& totals = _probe_rf_feedback->totals();
    for (int on = 0; on < 2; ++on) {
        COUNTER_SET(_feedback_input_rows[on], totals.input_rows[on]);
        COUNTER_SET(_feedback_filter_time[on], totals.filter_ns[on]);
        COUNTER_SET(_feedback_lookup_time[on], totals.lookup_ns[on]);
        COUNTER_SET(_feedback_intermediate_time[on], totals.intermediate_ns[on]);
    }
    COUNTER_SET(_feedback_decisions, totals.decisions);
    COUNTER_SET(_feedback_switches, totals.switches);
    _unique_metrics->add_info_string("LocalRfFeedbackState", _probe_rf_feedback->enabled() ? "active" : "fallback");
}

HashJoinProbeOperator::HashJoinProbeOperator(OperatorFactory* factory, int32_t id, const string& name,
                                             int32_t plan_node_id, int32_t driver_sequence, HashJoinerPtr join_prober,
                                             HashJoinerPtr join_builder)
        : OperatorWithDependency(factory, id, name, plan_node_id, false, driver_sequence),
          _join_prober(std::move(join_prober)),
          _join_builder(std::move(join_builder)) {}

void HashJoinProbeOperator::close(RuntimeState* state) {
    _publish_local_rf_feedback();
    if (_probe_rf_feedback) _probe_rf_feedback->disable();
    if (_join_prober != _join_builder) {
        _join_prober->unref(state);
    }

    _join_builder->decr_prober(state);

    OperatorWithDependency::close(state);
}

Status HashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorWithDependency::prepare(state));

    _join_builder->incr_prober();

    if (_join_builder != _join_prober) {
        _join_prober->ref();
    }

    RETURN_IF_ERROR(_join_prober->prepare_prober(state, _unique_metrics.get()));
    _join_builder->attach_probe_observer(state, observer());

    return Status::OK();
}

bool HashJoinProbeOperator::has_output() const {
    return _join_prober->has_output();
}

bool HashJoinProbeOperator::need_input() const {
    if (_join_prober->need_input()) {
        return true;
    }

    if (is_ready()) {
        // If hasn't referenced hash table, return true to reference hash table in push_chunk.
        return !_join_prober->has_referenced_hash_table();
    }
    return false;
}

bool HashJoinProbeOperator::is_finished() const {
    return _join_prober->is_done() || _join_builder->is_done();
}

bool HashJoinProbeOperator::is_ready() const {
    return _join_builder->is_build_done();
}

Status HashJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    RETURN_IF_ERROR(_reference_builder_hash_table_once());
    RETURN_IF_ERROR(_join_prober->push_chunk(state, std::move(const_cast<ChunkPtr&>(chunk))));
    if (_probe_rf_feedback && _probe_rf_feedback->needs_drain()) {
        RETURN_IF_ERROR(_join_prober->drain_probe_input(state));
    }
    _observe_local_rf_lookup();
    return Status::OK();
}

StatusOr<ChunkPtr> HashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_reference_builder_hash_table_once());
    auto result = _join_prober->pull_chunk(state);
    if (result.ok()) _observe_local_rf_lookup();
    return result;
}

Status HashJoinProbeOperator::set_finishing(RuntimeState* state) {
    // TODO: notify one will be ok
    auto notify = _join_builder->defer_notify_build();
    RETURN_IF_ERROR(_join_prober->probe_input_finished(state));
    _observe_local_rf_lookup();
    _join_prober->enter_post_probe_phase();
    return Status::OK();
}

Status HashJoinProbeOperator::set_finished(RuntimeState* state) {
    _join_prober->enter_eos_phase();
    _join_builder->set_prober_finished();
    return Status::OK();
}

Status HashJoinProbeOperator::_reference_builder_hash_table_once() {
    if (!is_ready()) {
        return Status::OK();
    }

    if (_join_prober->has_referenced_hash_table()) {
        return Status::OK();
    }

    TRY_CATCH_ALLOC_SCOPE_START()
    _join_prober->reference_hash_table(_join_builder.get());
    if (_probe_rf_feedback) {
        _join_prober->track_completed_probe_rows();
    }
    TRY_CATCH_ALLOC_SCOPE_END()
    return Status::OK();
}

Status HashJoinProbeOperator::reset_state(RuntimeState* state, const vector<ChunkPtr>& refill_chunks) {
    RETURN_IF_ERROR(_reference_builder_hash_table_once());
    // Reset probe state only when it has valid state after referencing the build hash table.
    if (_join_prober->has_referenced_hash_table()) {
        RETURN_IF_ERROR(_join_prober->reset_probe(state));
    }
    return Status::OK();
}

void HashJoinProbeOperator::update_exec_stats(RuntimeState* state) {
    auto ctx = state->query_ctx();
    if (ctx != nullptr) {
        ctx->update_pull_rows_stats(_plan_node_id, COUNTER_VALUE(_pull_row_num_counter));
        if (_conjuncts_input_counter != nullptr && _conjuncts_output_counter != nullptr) {
            ctx->update_pred_filter_stats(
                    _plan_node_id, COUNTER_VALUE(_conjuncts_input_counter) - COUNTER_VALUE(_conjuncts_output_counter));
        }
        if (_bloom_filter_eval_context.join_runtime_filter_input_counter != nullptr) {
            int64_t input_rows = COUNTER_VALUE(_bloom_filter_eval_context.join_runtime_filter_input_counter);
            int64_t output_rows = COUNTER_VALUE(_bloom_filter_eval_context.join_runtime_filter_output_counter);
            ctx->update_rf_filter_stats(_plan_node_id, input_rows - output_rows);
        }
    }
}

HashJoinProbeOperatorFactory::HashJoinProbeOperatorFactory(int32_t id, int32_t plan_node_id,
                                                           HashJoinerFactoryPtr hash_joiner_factory)
        : OperatorFactory(id, "hash_join_probe", plan_node_id), _hash_joiner_factory(std::move(hash_joiner_factory)) {}

Status HashJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    return OperatorFactory::prepare(state);
}
void HashJoinProbeOperatorFactory::close(RuntimeState* state) {
    OperatorFactory::close(state);
}

OperatorPtr HashJoinProbeOperatorFactory::create(int32_t dop, int32_t driver_sequence) {
    return std::make_shared<HashJoinProbeOperator>(this, _id, _name, _plan_node_id, driver_sequence,
                                                   _hash_joiner_factory->create_prober(dop, driver_sequence),
                                                   _hash_joiner_factory->get_builder(dop, driver_sequence));
}

} // namespace starrocks::pipeline
