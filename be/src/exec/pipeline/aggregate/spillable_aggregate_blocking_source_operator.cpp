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

#include "exec/pipeline/aggregate/spillable_aggregate_blocking_source_operator.h"

#include <algorithm>

#include "common/status.h"
#include "exec/pipeline/aggregate/aggregate_blocking_source_operator.h"

namespace starrocks::pipeline {
Status SpillableAggregateBlockingSourceOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(AggregateBlockingSourceOperator::prepare(state));
    RETURN_IF_ERROR(_stream_aggregator->prepare(state, _unique_metrics.get()));
    RETURN_IF_ERROR(_stream_aggregator->open(state));
    _accumulator.set_max_size(state->chunk_size());
    return Status::OK();
}

void SpillableAggregateBlockingSourceOperator::close(RuntimeState* state) {
    AggregateBlockingSourceOperator::close(state);
    _stream_aggregator->close(state);
    DCHECK(is_finished());
    DCHECK(!has_output());
}

bool SpillableAggregateBlockingSourceOperator::has_output() const {
    if (_is_finished) {
        return false;
    }
    // Cache-conscious with a spilled CA: restore the CA pull-driven, then emit one result chunk.
    if (_aggregator->cache_conscious_topn_active() && _aggregator->cache_conscious_ca_spilled()) {
        if (!_aggregator->is_sink_complete()) {
            return false;
        }
        // Still restoring: only runnable once the reader has a buffered chunk. has_output_data is
        // null-safe, so this also blocks until the flush callback has acquired the reader stream
        // (which happens after sink_complete) — never restore against a not-yet-acquired stream.
        if (!_aggregator->is_spilled_eos()) {
            return _aggregator->spiller()->has_output_data();
        }
        // Restored: one finalize + emit left.
        return !_aggregator->cache_conscious_result_emitted();
    }
    bool has_spilled = _aggregator->spiller()->spilled();

    if (!has_spilled && AggregateBlockingSourceOperator::has_output()) {
        return true;
    }

    if (!has_spilled) {
        return false;
    }
    if (_accumulator.has_output()) {
        return true;
    }
    // has output data from spiller.
    if (_aggregator->spiller()->has_output_data()) {
        return true;
    }
    RETURN_TRUE_IF_SPILL_TASK_ERROR(_aggregator->spiller());
    // has eos chunk
    if (_aggregator->is_spilled_eos() && _has_last_chunk) {
        return true;
    }
    return false;
}

bool SpillableAggregateBlockingSourceOperator::is_finished() const {
    if (_is_finished) {
        return true;
    }
    if (_aggregator->cache_conscious_topn_active() && _aggregator->cache_conscious_ca_spilled()) {
        return _aggregator->cache_conscious_result_emitted();
    }
    if (!_aggregator->spiller()->spilled()) {
        return AggregateBlockingSourceOperator::is_finished();
    }
    if (_accumulator.has_output()) {
        return false;
    }
    if (_aggregator->spiller()->is_cancel()) {
        return true;
    }
    return _aggregator->is_spilled_eos() && !_has_last_chunk;
}

Status SpillableAggregateBlockingSourceOperator::set_finishing(RuntimeState* state) {
    if (state->is_cancelled()) {
        _aggregator->spiller()->cancel();
    }
    return Status::OK();
}

Status SpillableAggregateBlockingSourceOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    RETURN_IF_ERROR(AggregateBlockingSourceOperator::set_finished(state));
    return Status::OK();
}

StatusOr<ChunkPtr> SpillableAggregateBlockingSourceOperator::pull_chunk(RuntimeState* state) {
    RETURN_IF_ERROR(_aggregator->spiller()->task_status());
    // Cache-conscious with a spilled CA: restore one chunk per pull (re-routing it into the CA)
    // until every spilled chunk is read back, then prune against FA and emit the local top-n once,
    // then EOS. has_output gates each restore on the reader being ready, so this never spins on an
    // empty restore nor touches a not-yet-acquired stream. (A non-spilled CA is finalized in the
    // sink and emitted by the base pull_chunk's cache_conscious_result_ready path.)
    if (_aggregator->cache_conscious_topn_active() && _aggregator->cache_conscious_ca_spilled()) {
        if (!_aggregator->is_spilled_eos()) {
            RETURN_IF_ERROR(_aggregator->restore_cache_conscious_chunk(state));
            return std::make_shared<Chunk>(); // not done restoring; yield an empty chunk
        }
        if (!_aggregator->cache_conscious_result_ready()) {
            RETURN_IF_ERROR(_aggregator->finalize_cache_conscious_ca(state));
        }
        return _aggregator->pull_cache_conscious_result_chunk();
    }
    if (!_aggregator->spiller()->spilled()) {
        return AggregateBlockingSourceOperator::pull_chunk(state);
    }
    ASSIGN_OR_RETURN(auto res, _pull_spilled_chunk(state));

    if (res != nullptr) {
        const int64_t old_size = res->num_rows();
        RETURN_IF_ERROR(eval_conjuncts_and_in_filters(_stream_aggregator->conjunct_ctxs(), res.get()));
        _stream_aggregator->update_num_rows_returned(-(old_size - static_cast<int64_t>(res->num_rows())));
    }

    return res;
}

Status SpillableAggregateBlockingSourceOperator::reset_state(RuntimeState* state,
                                                             const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    _has_last_chunk = true;
    _accumulator.reset_state();
    return Status::OK();
}

StatusOr<ChunkPtr> SpillableAggregateBlockingSourceOperator::_pull_spilled_chunk(RuntimeState* state) {
    ChunkPtr res;

    if (_accumulator.has_output()) {
        auto accumulated = std::move(_accumulator.pull());
        return accumulated;
    }

    auto& spiller = _aggregator->spiller();

    if (!_aggregator->is_spilled_eos()) {
        DCHECK(_accumulator.need_input());
        ASSIGN_OR_RETURN(auto chunk, spiller->restore(state, TRACKER_WITH_SPILLER_READER_GUARD(state, spiller)));
        if (chunk->is_empty()) {
            return chunk;
        }
        RETURN_IF_ERROR(_stream_aggregator->evaluate_groupby_exprs(chunk.get()));
        RETURN_IF_ERROR(_stream_aggregator->evaluate_agg_fn_exprs(chunk.get(), true));
        ASSIGN_OR_RETURN(res, _stream_aggregator->streaming_compute_agg_state(chunk->num_rows(), false));
        _accumulator.push(res);

    } else if (_has_last_chunk) {
        DCHECK(_accumulator.need_input());
        _has_last_chunk = false;
        ASSIGN_OR_RETURN(res, _stream_aggregator->pull_eos_chunk());
        if (res != nullptr && !res->is_empty()) {
            _accumulator.push(res);
        }
        _accumulator.finalize();
    }

    if (_accumulator.has_output()) {
        auto accumulated = std::move(_accumulator.pull());
        return accumulated;
    }

    return nullptr;
}

Status SpillableAggregateBlockingSourceOperatorFactory::prepare(RuntimeState* state) {
    _stream_aggregator_factory = std::make_shared<StreamingAggregatorFactory>(_hash_aggregator_factory->t_node());
    _stream_aggregator_factory->set_aggr_mode(_hash_aggregator_factory->aggr_mode());
    return Status::OK();
}

OperatorPtr SpillableAggregateBlockingSourceOperatorFactory::create(int32_t degree_of_parallelism,
                                                                    int32_t driver_sequence) {
    return std::make_shared<SpillableAggregateBlockingSourceOperator>(
            _hash_aggregator_factory->get_or_create(driver_sequence),
            _stream_aggregator_factory->get_or_create(driver_sequence), this, _id, _plan_node_id, driver_sequence);
}
} // namespace starrocks::pipeline
