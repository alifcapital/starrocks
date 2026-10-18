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

#include "formats/parquet/group_reader.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <unordered_set>
#include <utility>

#include "agent/master_info.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exec/exec_node.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_materializer.h"
#include "formats/parquet/column_reader_factory.h"
#include "formats/parquet/iceberg_row_id_reader.h"
#include "formats/parquet/lazy_materialization_context.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/parquet_pos_reader.h"
#include "formats/parquet/predicate_filter_evaluator.h"
#include "formats/parquet/read_range_planner.h"
#include "formats/parquet/row_source_reader.h"
#include "formats/parquet/scalar_column_reader.h"
#include "formats/parquet/schema.h"
#include "formats/reserved_columns.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "runtime/current_thread.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "util/defer_op.h"
#include "util/raw_container.h"
#include "utils.h"

namespace starrocks::parquet {

// ── GroupReader: construction / destruction ─────────────────────────────────

GroupReader::GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                         int64_t row_group_first_row)
        : _row_group_first_row(row_group_first_row), _skip_rows_ctx(std::move(skip_rows_ctx)), _param(param) {
    _row_group_metadata = &_param.file_metadata->t_metadata().row_groups[row_group_number];
    _column_materializer = std::make_unique<ColumnMaterializer>(_param, &_column_readers);
}

GroupReader::~GroupReader() {
    if (_param.sb_stream) {
        _param.sb_stream->release_to_offset(_end_offset);
    }
    // If GroupReader is filtered by statistics, it's _has_prepared = false
    if (_has_prepared) {
        // We want the share of row groups that need their lazy columns. A row group without lazy columns
        // says nothing about it, so we do not count it.
        if (_param.lazy_coalesce_row_groups_finished != nullptr && _param.lazy_coalesce_row_groups_needed != nullptr &&
            !_column_materializer->lazy_column_indices().empty()) {
            if (_column_materializer->lazy_column_needed()) {
                _param.lazy_coalesce_row_groups_needed->fetch_add(1, std::memory_order_relaxed);
            }
            _param.lazy_coalesce_row_groups_finished->fetch_add(1, std::memory_order_relaxed);
        }
        // Lazy-materialization diagnostic: count row groups where every lazy slot
        // was triggered during predicate evaluation.  A consistently high count
        // suggests the active/lazy classification (Phase 3b) would help.
        if (_param.stats) {
            size_t total_lazy = _column_materializer->lazy_slot_ids().size();
            size_t triggered = _column_materializer->lazy_triggered_count();
            if (total_lazy > 0 && static_cast<size_t>(triggered) >= total_lazy) {
                _param.stats->parquet_lazy_full_trigger_count++;
            }
        }
        _param.stats->group_min_round_cost =
                _param.stats->group_min_round_cost == 0
                        ? _column_materializer->min_round_cost()
                        : std::min(_param.stats->group_min_round_cost, int64_t(_column_materializer->min_round_cost()));
    }
}

Status GroupReader::init() {
    // Create column readers and bind ParquetField & ColumnChunkMetaData(except complex type) to each ColumnReader
    RETURN_IF_ERROR(_create_column_readers());
    _process_columns_and_conjunct_ctxs();
    _range = SparseRange<uint64_t>(_row_group_first_row, _row_group_first_row + _row_group_metadata->num_rows);
    return Status::OK();
}

void GroupReader::select_page_ranges() {
    if (_range.span_size() != get_row_group_metadata()->num_rows) {
        for (const auto& pair : _column_readers) {
            pair.second->select_offset_index(_range, _row_group_first_row);
        }
    }
}

Status GroupReader::prepare() {
    RETURN_IF_ERROR(_prepare_column_readers());
    // we need deal with page index first, so that it can work on collect_io_range,
    // and pageindex's io has been collected in FileReader

    select_page_ranges();

    // if coalesce read enabled, we have to
    // 1. allocate shared buffered input stream and
    // 2. collect io ranges of every row group reader.
    // 3. set io ranges to the stream.
    if (config::parquet_coalesce_read_enable && _param.sb_stream != nullptr) {
        std::vector<starrocks::io::SharedBufferedInputStream::IORange> ranges;
        int64_t end_offset = 0;
        collect_io_ranges(&ranges, &end_offset, ColumnIOType::PAGES);
        auto* planner = _column_materializer->read_range_planner();
        const int64_t lazy_bytes = planner->coalescable_lazy_bytes();
        const bool coalesce_lazy = planner->should_coalesce_active_lazy(lazy_bytes);
        // Without lazy ranges nothing was decided, so we count nothing.
        if (lazy_bytes > 0) {
            if (coalesce_lazy) {
                _param.stats->active_lazy_coalesce_together += 1;
                _param.stats->active_lazy_coalesce_together_bytes += lazy_bytes;
            } else {
                _param.stats->active_lazy_coalesce_seperately += 1;
            }
        }
        _set_end_offset(end_offset);
        RETURN_IF_ERROR(_param.sb_stream->set_io_ranges(ranges, coalesce_lazy));
    }

    RETURN_IF_ERROR(_column_materializer->rewrite_dict_conjuncts_to_predicate(&_is_group_filtered));
    RETURN_IF_ERROR(_column_materializer->init_read_chunk());

    if (!_is_group_filtered) {
        _range_iter = _range.new_iterator();
    }

    _has_prepared = true;
    return Status::OK();
}

const tparquet::ColumnChunk* GroupReader::get_chunk_metadata(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second->get_chunk_metadata();
}

ColumnReader* GroupReader::get_column_reader(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second.get();
}

const ParquetField* GroupReader::get_column_parquet_field(SlotId slot_id) {
    const auto& it = _column_readers.find(slot_id);
    if (it == _column_readers.end()) {
        return nullptr;
    }
    return it->second->get_column_parquet_field();
}

const tparquet::RowGroup* GroupReader::get_row_group_metadata() const {
    return _row_group_metadata;
}

Status GroupReader::get_next(ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_chunk_read_ns);
    if (_is_group_filtered) {
        *row_count = 0;
        return Status::EndOfFile("");
    }

    // *row_count is also the output, and a pass whose rows the deferred compound conjuncts all drop sets
    // it to 0 before the next pass. We fear that the next pass then asks the range iterator for 0 rows,
    // which does not move the iterator, so every pass reads up to the count that the caller asked for.
    const size_t max_rows = *row_count;
    while (true) {
        if (!_range_iter.has_more()) {
            *row_count = 0;
            return Status::EndOfFile("");
        }

        auto r = _range_iter.next(max_rows);
        _param.stats->raw_rows_read += r.span_size();

        _column_materializer->reset_read_chunk();

        RowGroupScanState state;
        state.active_chunk = _column_materializer->create_active_chunk();
        state.row_count = r.span_size();
        state.chunk_filter = Filter(state.row_count, 1);

        LazyMaterializationContext lazy_ctx(_column_materializer.get(), r, nullptr, state.active_chunk);

        // 1. Prune deleted rows
        ASSIGN_OR_RETURN(bool rows_survive, _prune_deleted_rows(r, state));
        if (!rows_survive) continue;

        // 2. Read & filter active columns
        state.active_chunk->set_missing_column_provider(&lazy_ctx);
        ASSIGN_OR_RETURN(rows_survive, _read_and_filter_active_columns(r, state, &lazy_ctx));
        if (!rows_survive) {
            state.active_chunk->set_missing_column_provider(nullptr);
            continue;
        }

        // A join runtime filter that has arrived costs a hash probe per row and often drops most
        // rows, while a compound conjunct can cost much more per row. So we probe the filter before
        // stage 3. The rows that it drops are zeros in chunk_filter, and stage 3 then evaluates the
        // conjuncts on the rows that are left only.
        const bool runtime_filters_first = _should_probe_runtime_filters_first();
        if (runtime_filters_first) {
            ASSIGN_OR_RETURN(rows_survive, _evaluate_runtime_filters(r, state));
            if (!rows_survive) {
                state.active_chunk->set_missing_column_provider(nullptr);
                continue;
            }
        }

        // 3. Evaluate compound predicates
        ASSIGN_OR_RETURN(rows_survive, _evaluate_compound_predicates(r, state, &lazy_ctx));
        state.active_chunk->set_missing_column_provider(nullptr);
        if (!rows_survive) continue;

        // 3.1 Probe join runtime filters
        if (!runtime_filters_first) {
            ASSIGN_OR_RETURN(rows_survive, _evaluate_runtime_filters(r, state));
            if (!rows_survive) continue;
        }

        // 4. Apply combined filter and backfill lazy columns
        ASSIGN_OR_RETURN(rows_survive, _filter_and_backfill_lazy(r, state));
        if (!rows_survive) continue;

        // 5. Emit output
        RETURN_IF_ERROR(_emit_output_columns(state, chunk, row_count));

        // 6. Append output side columns AFTER emit.
        if ((*row_count) > 0) {
            RETURN_IF_ERROR(_param.scanner_ctx->append_side_columns_to_chunk(chunk, (*row_count)));
        }

        // 7. Evaluate the compound conjuncts that stage 3 deferred. The output chunk holds only the
        //    rows that are left, with the values of every column that these conjuncts read.
        if (!state.deferred_conjuncts.empty() && (*row_count) > 0) {
            RETURN_IF_ERROR(_evaluate_deferred_compound_predicates(state, chunk, row_count));
            if ((*row_count) == 0) continue;
        }
        break;
    }

    return _range_iter.has_more() ? Status::OK() : Status::EndOfFile("");
}

// ── 1. Prune deleted rows ──────

StatusOr<bool> GroupReader::_prune_deleted_rows(const Range<uint64_t>& r, RowGroupScanState& state) {
    if (nullptr == _skip_rows_ctx || !_skip_rows_ctx->has_skip_rows()) {
        return true;
    }
    SCOPED_RAW_TIMER(&_param.stats->build_rowid_filter_ns);
    ASSIGN_OR_RETURN(state.has_filter,
                     _skip_rows_ctx->deletion_bitmap->fill_filter(r.begin(), r.end(), state.chunk_filter));
    if (SIMD::count_nonzero(state.chunk_filter.data(), state.row_count) == 0) {
        return false;
    }
    return true;
}

// ── 2. Read & filter active columns ──────

StatusOr<bool> GroupReader::_read_and_filter_active_columns(const Range<uint64_t>& r, RowGroupScanState& state,
                                                            LazyMaterializationContext* lazy_ctx) {
    if (_column_materializer->has_predicate_filter()) {
        state.has_filter = true;
        ASSIGN_OR_RETURN(size_t hit_count, _column_materializer->read_active_range_round_by_round(
                                                   r, &state.chunk_filter, &state.active_chunk, lazy_ctx));
        if (hit_count == 0) {
            _param.stats->late_materialize_skip_rows += state.row_count;
            return false;
        }
    } else if (state.has_filter) {
        RETURN_IF_ERROR(_column_materializer->read_active_range(r, &state.chunk_filter, &state.active_chunk));
    } else {
        RETURN_IF_ERROR(_column_materializer->read_active_range(r, nullptr, &state.active_chunk));
    }
    return true;
}

// ── 3. Evaluate compound predicates ──────

namespace {

// A dictionary mapping reads its column of codes through slot_id(), while get_slot_ids() reports
// the slots of the expression that it maps. We add the code slots of every dictionary mapping in
// the tree to slots, and set *found when there is one. do_for_each_child() also visits the common
// expressions that a lambda host keeps outside its children.
void collect_dict_mapping_slots(Expr* expr, std::vector<SlotId>* slots, bool* found) {
    if (expr->is_dictmapping_expr()) {
        slots->push_back(down_cast<DictMappingExpr*>(expr)->slot_id());
        *found = true;
    }
    (void)expr->do_for_each_child([slots, found](Expr* child) {
        collect_dict_mapping_slots(child, slots, found);
        return Status::OK();
    });
}

// A chunk with only the columns for slots (the slots that a conjunct reads), sharing the columns of
// active_chunk without a copy. Expr::evaluate_selected() copies the inputs of the expression for
// the selected rows, and it copies every column of the chunk when none of the inputs is in it. We
// expect a compound conjunct to read few columns, often only lazy ones, so we give it this narrow
// chunk. A chunk keeps its row count only through a column, so when no input is present we add the
// smallest column of active_chunk; it is copied but never read.
//
// A lambda host and a dictionary mapping read their input columns from the chunk with
// Chunk::get_column_by_slot_id(), and only a ColumnRef asks the missing column provider for a
// lazy column. We fear that such a conjunct (has_indirect_inputs) meets a lazy column that is not
// in the chunk, so we read the lazy columns that it needs before the evaluation and put them in
// the narrow chunk. The read goes through lazy_ctx, so it honors the filter that lazy_ctx holds
// and the later backfill of the lazy columns reuses the result. lazy_ctx is null for the output
// chunk, which holds every column that the conjunct reads.
StatusOr<ChunkPtr> compound_conjunct_input(const std::vector<SlotId>& slots, bool has_indirect_inputs,
                                           const ChunkPtr& active_chunk, LazyMaterializationContext* lazy_ctx) {
    auto input = std::make_shared<Chunk>();
    for (SlotId slot : slots) {
        if (active_chunk->is_slot_exist(slot)) {
            input->append_column(active_chunk->get_column_by_slot_id(slot), slot);
        } else if (has_indirect_inputs && lazy_ctx != nullptr && lazy_ctx->can_materialize(slot)) {
            ASSIGN_OR_RETURN(ColumnPtr column, lazy_ctx->provide(slot));
            input->append_column(std::move(column), slot);
        }
    }
    if (input->num_columns() == 0) {
        SlotId carrier = 0;
        size_t carrier_bytes = std::numeric_limits<size_t>::max();
        for (const auto& [slot, index] : active_chunk->get_slot_id_to_index_map()) {
            const size_t bytes = active_chunk->get_column_by_index(index)->byte_size();
            if (bytes < carrier_bytes) {
                carrier = slot;
                carrier_bytes = bytes;
            }
        }
        input->append_column(active_chunk->get_column_by_slot_id(carrier), carrier);
    }
    input->set_missing_column_provider(active_chunk->missing_column_provider());
    return input;
}

// 0, 1, 2, ... for at least num_rows elements. We want to select the positions of the nonzero bytes of a
// filter with filter_range() over this array, since it has a SIMD path. The array lives as long as the
// thread and serves every query that runs on it, and we do not want the query that first grows it to
// carry its memory, so we charge the growth to the process.
const uint32_t* row_positions(size_t num_rows) {
    thread_local std::vector<uint32_t> positions;
    if (positions.size() < num_rows) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
        const size_t old_size = positions.size();
        positions.resize(num_rows);
        std::iota(positions.begin() + old_size, positions.end(), static_cast<uint32_t>(old_size));
    }
    return positions.data();
}

// The positions of the nonzero bytes of the first num_rows bytes of filter, in ascending order.
void selected_rows_of(const Filter& filter, size_t num_rows, std::vector<uint32_t>* rows) {
    raw::stl_vector_resize_uninitialized(rows, num_rows);
    rows->resize(ColumnHelper::filter_range<uint32_t>(filter, rows->data(), row_positions(num_rows), 0, num_rows));
}

// ANDs the result of a conjunct evaluated on rows into filter, with NULL as false and the same byte
// arithmetic as ColumnHelper::merge_two_filters(), and keeps in rows only the rows that passed.
// We expect every row in rows to have its filter byte at 1, so only a row that failed changes the
// filter. We also expect most conjuncts to keep most of the rows that they see, so we look for the failed
// rows with SIMD and touch only those.
Status apply_selected_result(const ColumnPtr& result, std::vector<uint32_t>* rows, Filter* filter) {
    const size_t num_rows = rows->size();
    uint8_t* __restrict out = filter->data();
    uint32_t* __restrict idx = rows->data();
    if (result->is_constant()) {
        ColumnViewer<TYPE_BOOLEAN> viewer(result);
        if (!viewer.is_null(0) && (viewer.value(0) & 1)) {
            return Status::OK();
        }
        for (size_t i = 0; i < num_rows; ++i) {
            out[idx[i]] = 0;
        }
        rows->clear();
        return Status::OK();
    }
    if (result->size() != num_rows) {
        return Status::InternalError(
                fmt::format("compound predicate returned {} rows for {} selected rows", result->size(), num_rows));
    }

    Filter keep;
    raw::stl_vector_resize_uninitialized(&keep, num_rows);
    uint8_t* __restrict keep_data = keep.data();
    const uint8_t* __restrict values =
            down_cast<const UInt8Column*>(ColumnHelper::get_data_column(result.get()))->immutable_data().data();
    if (result->is_nullable()) {
        const uint8_t* __restrict nulls =
                down_cast<const NullableColumn*>(result.get())->immutable_null_column_data().data();
        for (size_t i = 0; i < num_rows; ++i) {
            keep_data[i] = static_cast<uint8_t>(1 & (!nulls[i]) & values[i]);
        }
    } else {
        for (size_t i = 0; i < num_rows; ++i) {
            keep_data[i] = static_cast<uint8_t>(1 & values[i]);
        }
    }

    const size_t failed = SIMD::count_zero(keep);
    if (failed == 0) {
        return Status::OK();
    }
    if (failed * 8 < num_rows) {
        for (size_t i = SIMD::find_zero(keep, 0); i < num_rows; i = SIMD::find_zero(keep, i + 1)) {
            out[idx[i]] = 0;
        }
    } else {
        for (size_t i = 0; i < num_rows; ++i) {
            out[idx[i]] = keep_data[i];
        }
    }
    rows->resize(ColumnHelper::filter_range<uint32_t>(keep, idx, 0, num_rows));
    return Status::OK();
}

// Evaluates the conjunct ctx on the given rows of chunk with Expr::evaluate_selected(), ANDs the result
// into filter and keeps in rows the rows that passed. slots and has_indirect_inputs are the facts about
// the conjunct that compound_conjunct_input() needs.
Status evaluate_conjunct_on_rows(ExprContext* ctx, const std::vector<SlotId>& slots, bool has_indirect_inputs,
                                 const ChunkPtr& chunk, LazyMaterializationContext* lazy_ctx,
                                 std::vector<uint32_t>* rows, Filter* filter) {
    ASSIGN_OR_RETURN(ChunkPtr input, compound_conjunct_input(slots, has_indirect_inputs, chunk, lazy_ctx));
    StatusOr<ColumnPtr> result;
    // Same error conversion as ExprContext::evaluate().
    try {
        result = ctx->root()->evaluate_selected(ctx, input.get(), *rows);
    } catch (std::runtime_error& e) {
        return Status::RuntimeError(fmt::format("Expr evaluate meet error: {}", e.what()));
    }
    RETURN_IF_ERROR(result);
    return apply_selected_result(result.value(), rows, filter);
}

} // namespace

void GroupReader::_collect_compound_conjunct_info() {
    _compound_conjuncts.clear();
    _has_compound_conjunct_with_indirect_inputs = false;
    if (_param.scanner_ctx == nullptr) {
        return;
    }
    for (ExprContext* ctx : _param.scanner_ctx->conjuncts.scanner_ctxs) {
        Expr* root = ctx->root();
        CompoundConjunctInfo info;
        info.has_indirect_inputs = root->has_indirect_inputs();
        root->get_slot_ids(&info.slots);
        if (info.has_indirect_inputs) {
            collect_dict_mapping_slots(root, &info.slots, &info.has_dict_mapping);
        }
        std::sort(info.slots.begin(), info.slots.end());
        info.slots.erase(std::unique(info.slots.begin(), info.slots.end()), info.slots.end());
        _has_compound_conjunct_with_indirect_inputs |= info.has_indirect_inputs;
        _compound_conjuncts.push_back(std::move(info));
    }

    // We want to defer a conjunct whatever it keeps when that makes the reader read no lazy column for the
    // rows that the conjunct drops. The lazy columns that the reader reads for something are the output ones
    // and the ones that a compound conjunct reads (a lazy column that nothing needs is filled with defaults).
    // A conjunct that reads all of them reads them for its surviving rows either way, so we mark it free.
    // We fear that a later conjunct that stays in stage 3 runs on the rows that this one would have dropped,
    // so a conjunct is free only if every later one is free and is deferred with it.
    std::vector<SlotId> lazy_needed;
    for (int col_idx : _column_materializer->lazy_column_indices()) {
        const auto& column = _param.read_cols[col_idx];
        const SlotId slot_id = column.slot_id();
        const bool read_by_conjunct = std::any_of(
                _compound_conjuncts.begin(), _compound_conjuncts.end(), [slot_id](const CompoundConjunctInfo& info) {
                    return std::binary_search(info.slots.begin(), info.slots.end(), slot_id);
                });
        if (column.slot_desc->is_output_column() || read_by_conjunct) {
            lazy_needed.push_back(slot_id);
        }
    }
    std::sort(lazy_needed.begin(), lazy_needed.end());
    bool suffix_is_free = true;
    for (size_t i = _compound_conjuncts.size(); i-- > 0;) {
        CompoundConjunctInfo& info = _compound_conjuncts[i];
        suffix_is_free = suffix_is_free && !info.has_dict_mapping &&
                         std::includes(info.slots.begin(), info.slots.end(), lazy_needed.begin(), lazy_needed.end());
        info.defer_is_free = suffix_is_free;
    }
}

StatusOr<bool> GroupReader::_evaluate_compound_predicates(const Range<uint64_t>& r, RowGroupScanState& state,
                                                          LazyMaterializationContext* lazy_ctx) {
    const auto& conjuncts = _param.scanner_ctx->conjuncts.scanner_ctxs;
    if (conjuncts.empty()) {
        return true;
    }
    DCHECK_EQ(conjuncts.size(), _compound_conjuncts.size());

    // Append side columns to active_chunk so compound conjuncts referencing
    // partition / not-existed / extended slots can be evaluated correctly.
    if (state.active_chunk->num_rows() > 0) {
        RETURN_IF_ERROR(
                _param.scanner_ctx->append_side_columns_to_chunk(&state.active_chunk, state.active_chunk->num_rows()));
    }

    // Finalize all active columns to logical form before compound conjunct eval.
    for (int col_idx : _column_materializer->active_column_indices()) {
        SlotId slot_id = _param.read_cols[col_idx].slot_id();
        RETURN_IF_ERROR(_column_materializer->finalize_active_slot(slot_id, state.active_chunk));
    }

    SCOPED_RAW_TIMER(&_param.stats->compound_pred_eval_ns);
    // We fear two things about a row that the earlier stages already dropped. An expensive conjunct
    // (a JSON parse, for example) costs time on it, since most expressions ignore the filter passed
    // to them and evaluate every row. A lazy column read under the filter holds a placeholder in it,
    // and a function may raise an error on that value. So when at least one row is dropped we
    // evaluate the conjuncts on the rows that are left only. When no row is dropped every row is
    // left, every column holds real values, and we evaluate the conjuncts on the whole chunk.
    // chunk_filter keeps its full-range positions, so the later stages see it as before.
    const size_t rows_left = SIMD::count_nonzero(state.chunk_filter.data(), state.row_count);
    const bool on_selected_rows = state.active_chunk->num_rows() > 0 && rows_left < state.row_count;
    auto& conjunct_stats = _param.scanner_ctx->compound_conjunct_stats;
    if (conjunct_stats.size() != conjuncts.size()) {
        conjunct_stats.assign(conjuncts.size(), {});
    }
    size_t compound_hit = 0;
    if (on_selected_rows) {
        // At least one row is dropped, so a conjunct may be left to stage 7, which evaluates it on the output chunk.
        if (config::parquet_compound_predicate_defer_enable) {
            for (size_t i = 0; i < conjuncts.size(); ++i) {
                if (_can_defer_compound_conjunct(i)) {
                    state.deferred_conjuncts.push_back(i);
                }
            }
        }
        compound_hit = rows_left;
        if (state.deferred_conjuncts.size() < conjuncts.size()) {
            // A row counts once, even when stage 7 evaluates the other conjuncts on it later.
            _param.stats->compound_pred_input_rows += rows_left;
            _param.stats->compound_pred_selected_rows += rows_left;
            ASSIGN_OR_RETURN(compound_hit, _evaluate_compound_predicates_on_selected_rows(state, lazy_ctx));
        }
    } else {
        _param.stats->compound_pred_input_rows += state.row_count;
        ASSIGN_OR_RETURN(compound_hit, _evaluate_compound_predicates_on_whole_chunk(state, lazy_ctx));
    }
    // When stage 7 evaluates conjuncts, the rows that pass all of them are known after it.
    if (state.deferred_conjuncts.empty()) {
        _param.stats->compound_pred_output_rows += compound_hit;
    }
    if (compound_hit == 0) {
        _param.stats->late_materialize_skip_rows += state.row_count;
        return false;
    }
    state.has_filter = true;
    return true;
}

StatusOr<size_t> GroupReader::_evaluate_compound_predicates_on_whole_chunk(RowGroupScanState& state,
                                                                           LazyMaterializationContext* lazy_ctx) {
    const auto& conjuncts = _param.scanner_ctx->conjuncts.scanner_ctxs;
    // When deferring is on, we want to know the rows that each conjunct sees and keeps, so we evaluate
    // the conjuncts one by one. Otherwise one call evaluates them all, which lets them share the casts
    // of a column.
    const bool track_stats = config::parquet_compound_predicate_defer_enable;
    // We fear that a narrow chunk made from a chunk without rows has no column to carry the row
    // count, so we pass such a chunk on as it is; eval_conjuncts_into_filter() returns at once for it.
    if ((!_has_compound_conjunct_with_indirect_inputs && !track_stats) || state.active_chunk->num_rows() == 0) {
        return ExecNode::eval_conjuncts_into_filter(conjuncts, state.active_chunk.get(), &state.chunk_filter);
    }

    // A conjunct with indirect inputs gets its own chunk, with the lazy columns that it needs read
    // for the whole range, since no row is dropped. The other conjuncts read active_chunk.
    auto& conjunct_stats = _param.scanner_ctx->compound_conjunct_stats;
    size_t hit = state.row_count;
    for (size_t i = 0; i < conjuncts.size(); ++i) {
        ExprContext* ctx = conjuncts[i];
        const CompoundConjunctInfo& info = _compound_conjuncts[i];
        ChunkPtr input = state.active_chunk;
        if (info.has_indirect_inputs) {
            ASSIGN_OR_RETURN(
                    input, compound_conjunct_input(info.slots, info.has_indirect_inputs, state.active_chunk, lazy_ctx));
        }
        const size_t input_rows = hit;
        ASSIGN_OR_RETURN(hit, ExecNode::eval_conjuncts_into_filter({ctx}, input.get(), &state.chunk_filter));
        conjunct_stats[i].input_rows += input_rows;
        conjunct_stats[i].output_rows += hit;
        if (hit == 0) {
            return 0;
        }
    }
    return hit;
}

StatusOr<size_t> GroupReader::_evaluate_compound_predicates_on_selected_rows(RowGroupScanState& state,
                                                                             LazyMaterializationContext* lazy_ctx) {
    // Here an expression touches only selected rows, and they stay inside chunk_filter, so a lazy
    // column read for it may skip the rows outside chunk_filter. Stage 4 keeps only rows inside it.
    lazy_ctx->set_filter(&state.chunk_filter);
    DeferOp reset_filter([lazy_ctx] { lazy_ctx->set_filter(nullptr); });

    // rows holds the positions where chunk_filter is 1. Each conjunct sees only the rows the conjuncts
    // before it kept, so apply_selected_result() drops from rows the rows that a conjunct rejects, and
    // rows stays in step with chunk_filter.
    std::vector<uint32_t> rows;
    selected_rows_of(state.chunk_filter, state.row_count, &rows);
    const auto& conjuncts = _param.scanner_ctx->conjuncts.scanner_ctxs;
    auto& conjunct_stats = _param.scanner_ctx->compound_conjunct_stats;
    // state.deferred_conjuncts is in ascending order, so we step through it together with the conjuncts.
    size_t next_deferred = 0;
    for (size_t conjunct_idx = 0; conjunct_idx < conjuncts.size(); ++conjunct_idx) {
        if (next_deferred < state.deferred_conjuncts.size() &&
            state.deferred_conjuncts[next_deferred] == conjunct_idx) {
            ++next_deferred;
            continue;
        }
        if (rows.empty()) {
            return 0;
        }
        const CompoundConjunctInfo& info = _compound_conjuncts[conjunct_idx];
        const size_t input_rows = rows.size();
        RETURN_IF_ERROR(evaluate_conjunct_on_rows(conjuncts[conjunct_idx], info.slots, info.has_indirect_inputs,
                                                  state.active_chunk, lazy_ctx, &rows, &state.chunk_filter));
        conjunct_stats[conjunct_idx].input_rows += input_rows;
        conjunct_stats[conjunct_idx].output_rows += rows.size();
    }
    return rows.size();
}

bool GroupReader::_can_defer_compound_conjunct(size_t i) const {
    const CompoundConjunctInfo& info = _compound_conjuncts[i];
    // We fear that a dictionary mapping reads its column of dictionary codes from the chunk, and the
    // output chunk holds that column in another form than the active chunk does. So we never defer a
    // conjunct with a dictionary mapping.
    if (info.has_dict_mapping) {
        return false;
    }
    // We expect that deferring a free conjunct saves the copy of its inputs and reads nothing extra, so we do
    // not wait for its statistics.
    if (info.defer_is_free) {
        return true;
    }
    // Deferring any other conjunct makes the reader read lazy columns for the rows that it drops, so we
    // defer it only if it drops few rows. We expect a conjunct to keep a similar part of the rows in the
    // rest of the scan range, so we decide by what it kept so far, once it has seen enough rows to tell.
    const auto& stats = _param.scanner_ctx->compound_conjunct_stats[i];
    if (stats.input_rows < config::parquet_compound_predicate_defer_min_observed_rows) {
        return false;
    }
    return static_cast<double>(stats.output_rows) >=
           config::parquet_compound_predicate_defer_min_keep_ratio * static_cast<double>(stats.input_rows);
}

Status GroupReader::_evaluate_deferred_compound_predicates(const RowGroupScanState& state, ChunkPtr* chunk,
                                                           size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->compound_pred_eval_ns);
    const auto& conjuncts = _param.scanner_ctx->conjuncts.scanner_ctxs;
    auto& conjunct_stats = _param.scanner_ctx->compound_conjunct_stats;
    const size_t num_rows = (*chunk)->num_rows();
    _param.stats->compound_pred_deferred_rows += num_rows;
    // Stage 3 counted the input rows when it evaluated a conjunct. When every conjunct is deferred, no
    // stage counted them, and they are the rows of the output chunk.
    if (state.deferred_conjuncts.size() == conjuncts.size()) {
        _param.stats->compound_pred_input_rows += num_rows;
    }

    // Every row of the output chunk is a surviving row, so the first conjunct runs on the whole chunk. We
    // fear that a later conjunct sees a row that an earlier one dropped, so once a row is dropped the
    // later conjuncts run only on the rows that are left, in the same way as in stage 3. The chunk is
    // compacted once, at the end.
    Filter filter(num_rows, 1);
    std::vector<uint32_t> rows;
    bool on_selected_rows = false;
    size_t rows_left = num_rows;
    for (size_t i : state.deferred_conjuncts) {
        const size_t input_rows = rows_left;
        if (!on_selected_rows) {
            ASSIGN_OR_RETURN(rows_left, ExecNode::eval_conjuncts_into_filter({conjuncts[i]}, chunk->get(), &filter));
            if (rows_left > 0 && rows_left < num_rows) {
                selected_rows_of(filter, num_rows, &rows);
                on_selected_rows = true;
            }
        } else {
            const CompoundConjunctInfo& info = _compound_conjuncts[i];
            RETURN_IF_ERROR(evaluate_conjunct_on_rows(conjuncts[i], info.slots, info.has_indirect_inputs, *chunk,
                                                      nullptr, &rows, &filter));
            rows_left = rows.size();
        }
        conjunct_stats[i].input_rows += input_rows;
        conjunct_stats[i].output_rows += rows_left;
        if (rows_left == 0) {
            break;
        }
    }
    if (rows_left == 0) {
        (*chunk)->set_num_rows(0);
    } else if (rows_left < num_rows) {
        (*chunk)->filter(filter);
    }
    _param.stats->compound_pred_output_rows += rows_left;
    *row_count = (*chunk)->num_rows();
    return Status::OK();
}

// ── 3.1 Evaluate join runtime filters ──────
//
// Runs before stage 4 so the filter is applied where it pays off: it narrows the
// lazy-column read range and shrinks the emitted chunk. Once a filter has arrived it
// runs before stage 3 instead (see _should_probe_runtime_filters_first()), so the
// compound predicates see fewer rows too. Like stages 1-3 this only ANDs into
// state.chunk_filter, so it can never change query results --
// worst case it is a no-op. The scan operator still probes the same filters
// afterwards (has_push_down_to_storage stays false for connector scans), so a slot we
// cannot serve here stays correct, just not accelerated.
//
// ── Why every probe column is finalized to its logical form first ──
//
// A column sitting in a slot after read_range() may be PHYSICAL rather than logical:
// Int32 dictionary codes, or decoded-but-not-yet-converted parquet values (see the
// swap machinery documented in scalar_column_reader.h). Three properties make this
// impossible to reason about from here, so we do not try:
//
//   1. It cannot be inferred from the column. Dictionary codes are an Int32Column,
//      indistinguishable from a genuine INT column. Only the owning ColumnReader
//      knows, via pointer identity against its own _code_column, and that check is
//      private to the reader -- ColumnReader deliberately exposes only
//      finalize_lazy_state() ("make it logical"), never "what is it right now".
//   2. Active does not imply decoded. A column whose conjuncts all became dict
//      filters is evaluated directly on the codes and is never finalized in stage 2
//      (see ColumnMaterializer::read_active_range_round_by_round), and stage 3
//      early-returns when there are no compound conjuncts.
//   3. It varies per chunk. ScalarColumnReader::read_range() picks DICT_CODE only
//      when the incoming filter is selective enough, so the same column in the same
//      row group can hand back codes for one chunk and values for the next.
//
// Getting this wrong yields silently wrong rows, not a crash: the filter would probe
// dictionary codes against a filter built over the decoded values. So we pay an
// unconditional finalize_active_slot(), which is idempotent and costs a null check
// when the column is already logical.
//
// The cost of that choice: for a dict-encoded string join key we materialize every
// surviving string and hash it once per row. Probing the dictionary instead (D
// distinct values, D << rows) and reducing each row to a byte lookup -- what
// DictColumnRuntimeFilterPredicate already does for OLAP -- would be considerably
// cheaper. It is deliberately not done here, because a "detect the state, then take
// the fast path" implementation would be racing property 3 above. Doing it safely
// means making the state deterministic instead of detecting it: request dict codes
// up front so read_range() must produce them, and expose the fast path through an
// API that cannot be misread (e.g. one returning the code column or nullptr, never
// leaving the caller to guess). Until a string join key actually shows up as a
// bottleneck, the simpler always-finalize path is the better trade -- note the
// motivating TPC-DS q27 case joins exclusively on integer surrogate keys, which this
// optimization would not help at all.
StatusOr<bool> GroupReader::_evaluate_runtime_filters(const Range<uint64_t>& r, RowGroupScanState& state) {
    if (_rf_probe_columns.empty() || state.row_count == 0) {
        return true;
    }
    // RuntimeFilterPredicates::evaluate() takes uint16_t row bounds. FileReader caps
    // requests at _chunk_size, so this is defensive only.
    if (state.row_count > std::numeric_limits<uint16_t>::max()) {
        return true;
    }
    // Nothing has arrived yet: skip before materializing any probe column, so an RF
    // that never shows up costs no extra IO at all.
    if (!_rf_predicates.any_filter_ready()) {
        return true;
    }

    const size_t input_rows = SIMD::count_nonzero(state.chunk_filter.data(), state.row_count);
    if (input_rows == 0) {
        return false;
    }

    SCOPED_RAW_TIMER(&_param.stats->rf_cond_evaluate_ns);

    // RuntimeFilterPredicate looks columns up by ColumnId (Chunk::_cid_to_index), but
    // the parquet active chunk is keyed by SlotId only. Build a throwaway cid-keyed
    // view over the same columns, the way SegmentIterator does for its per-column
    // runtime filters. It must be built *after* the columns are finalized, because
    // finalizing replaces the ColumnPtr rather than mutating it in place.
    Chunk rf_chunk;
    for (const auto& probe : _rf_probe_columns) {
        if (probe.is_active) {
            // Unconditional and idempotent -- see the note above on why the physical
            // vs logical state cannot be inferred here.
            RETURN_IF_ERROR(_column_materializer->finalize_active_slot(probe.slot_id, state.active_chunk));
            rf_chunk.append_column(state.active_chunk->get_column_by_slot_id(probe.slot_id),
                                   static_cast<ColumnId>(probe.slot_id), true);
        } else {
            // Lazy column: read just this slot on demand. materialize_slot() honors the
            // current filter (so rows already dropped by stages 1-4 are skipped),
            // finalizes to logical form, and caches the column so stage 4's backfill
            // picks it up instead of re-reading it.
            RETURN_IF_ERROR(_column_materializer->materialize_slot(probe.slot_id, r, &state.chunk_filter));
            const auto* cached = _column_materializer->get_slot_cache(probe.slot_id);
            RETURN_IF_ERROR(cached != nullptr ? Status::OK()
                                              : Status::InternalError("runtime filter probe column not materialized"));
            rf_chunk.append_column(cached->values, static_cast<ColumnId>(probe.slot_id), true);
        }
    }

    RETURN_IF_ERROR(
            _rf_predicates.evaluate(&rf_chunk, state.chunk_filter.data(), 0, static_cast<uint16_t>(state.row_count)));

    const size_t output_rows = SIMD::count_nonzero(state.chunk_filter.data(), state.row_count);
    _param.stats->rf_cond_input_rows += input_rows;
    _param.stats->rf_cond_output_rows += output_rows;
    if (output_rows != input_rows) {
        state.has_filter = true;
    }
    if (output_rows == 0) {
        _param.stats->late_materialize_skip_rows += state.row_count;
        return false;
    }
    return true;
}

// We expect the join filter to cost a hash probe per row and to drop many rows, while a compound
// predicate can cost much more per row (a JSON parse, for example). So when a join filter has
// arrived and there are compound predicates, the join filter goes first and the compound
// predicates see only the rows that it kept. Both stages only AND into chunk_filter, and stage 3
// reads the rows left from chunk_filter, so the order does not change the result. Without compound
// predicates there is nothing to save, and before a filter arrives stage 3.1 does nothing, so then
// stage 3 stays first. Stage 3 runs for every chunk, so this reads members only.
bool GroupReader::_should_probe_runtime_filters_first() const {
    return !_param.scanner_ctx->conjuncts.scanner_ctxs.empty() && !_rf_probe_columns.empty() &&
           _rf_predicates.any_filter_ready();
}

// ── 5. Filter & backfill lazy ──────

StatusOr<bool> GroupReader::_filter_and_backfill_lazy(const Range<uint64_t>& r, RowGroupScanState& state) {
    Range<uint64_t> post_filter_range;
    Filter post_filter;

    if (state.has_filter) {
        state.active_chunk->filter(state.chunk_filter);
        if (state.active_chunk->num_rows() == 0) {
            return false;
        }

        post_filter_range = r.filter(&state.chunk_filter);
        DCHECK(post_filter_range.span_size() > 0);
        post_filter = {state.chunk_filter.begin() + post_filter_range.begin() - r.begin(),
                       state.chunk_filter.begin() + post_filter_range.end() - r.begin()};
    }

    bool has_any_lazy = !_column_materializer->lazy_column_indices().empty();
    if (has_any_lazy) {
        _param.stats->parquet_lazy_col_skip_rows += state.row_count - state.active_chunk->num_rows();
    }
    if (!_column_materializer->lazy_column_indices().empty()) {
        // A deferred conjunct runs on the output chunk and reads its lazy columns there. We fear that it sees
        // the default value of a lazy column that no output needs, so we read the columns that it reads.
        std::vector<SlotId> force_backfill_slots;
        for (size_t i : state.deferred_conjuncts) {
            const auto& slots = _compound_conjuncts[i].slots;
            force_backfill_slots.insert(force_backfill_slots.end(), slots.begin(), slots.end());
        }
        _column_materializer->set_force_backfill_slots(std::move(force_backfill_slots));
        DeferOp clear_force_backfill_slots([this] { _column_materializer->set_force_backfill_slots({}); });
        RETURN_IF_ERROR(_column_materializer->read_lazy_columns(r, post_filter_range, post_filter, state.chunk_filter,
                                                                state.has_filter, state.active_chunk));
    }

    return true;
}

// ── 7. Emit output ──────

Status GroupReader::_emit_output_columns(RowGroupScanState& state, ChunkPtr* chunk, size_t* row_count) {
    SCOPED_RAW_TIMER(&_param.stats->group_dict_decode_ns);
    *row_count = state.active_chunk->num_rows();
    RETURN_IF_ERROR(_column_materializer->emit_physical_columns(state.active_chunk, chunk));
    return Status::OK();
}

// ── Column reader creation ─────────────────────────────────────────────────

StatusOr<ColumnReaderPtr> GroupReader::_create_reserved_iceberg_column_reader(const SlotDescriptor* slot,
                                                                              int32_t field_id) {
    // Try to find the physical column in the Parquet file by Iceberg spec field ID first (canonical),
    // then fall back to column name lookup for compatibility.
    int32_t field_idx = _param.file_metadata->schema().get_field_idx_by_field_id(field_id);
    if (field_idx < 0) {
        field_idx = _param.file_metadata->schema().get_field_idx_by_column_name(slot->col_name());
    }
    if (field_idx < 0) {
        return ColumnReaderPtr(nullptr);
    }

    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_field_idx(field_idx);
    GroupReaderParam::Column column{};
    column.idx_in_parquet = field_idx;
    column.type_in_parquet = schema_node->physical_type;
    column.slot_desc = const_cast<SlotDescriptor*>(slot);
    column.t_lake_schema_field = nullptr;
    column.decode_needed = true;
    return _create_column_reader(column);
}

StatusOr<Datum> GroupReader::_get_extended_bigint_value(SlotId slot_id) const {
    if (_param.scan_range == nullptr || !_param.scan_range->__isset.extended_columns) {
        return Status::NotFound(strings::Substitute("Cannot find extended column for slot $0", slot_id));
    }

    const auto& extended_columns = _param.scan_range->extended_columns;
    auto it = extended_columns.find(slot_id);
    if (it == extended_columns.end()) {
        return Status::NotFound(strings::Substitute("Cannot find extended column value for slot $0", slot_id));
    }

    const auto& expr = it->second;
    if (expr.nodes.empty()) {
        return Status::InvalidArgument(strings::Substitute("Invalid extended column expression for slot $0", slot_id));
    }

    const auto& node = expr.nodes[0];
    if (node.node_type == TExprNodeType::NULL_LITERAL) {
        return kNullDatum;
    }
    if (node.node_type != TExprNodeType::INT_LITERAL || !node.__isset.int_literal) {
        return Status::InvalidArgument(
                strings::Substitute("Unsupported extended column expression for slot $0", slot_id));
    }

    return Datum(node.int_literal.value);
}

Status GroupReader::_create_column_readers() {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    _global_dict_applied_in_group = false;
    // ColumnReaderOptions is used by all column readers in one row group
    ColumnReaderOptions& opts = _column_reader_opts;
    opts.file_meta_data = _param.file_metadata;
    if (_param.scanner_ctx == nullptr) {
        return Status::InternalError("GroupReader: scanner_ctx must not be null");
    }
    opts.timezone = _param.scanner_ctx->timezone;
    opts.case_sensitive = _param.scanner_ctx->options.case_sensitive;
    opts.use_file_pagecache = _param.scanner_ctx->options.use_file_pagecache;
    opts.chunk_size = _param.chunk_size;
    opts.stats = _param.stats;
    opts.file = _param.file;
    opts.row_group_meta = _row_group_metadata;
    opts.first_row_index = _row_group_first_row;
    opts.modification_time = _param.modification_time;
    opts.file_size = _param.file_size;
    opts.datacache_options = _param.datacache_options;
    for (const auto& column : _param.read_cols) {
        ASSIGN_OR_RETURN(ColumnReaderPtr column_reader, _create_column_reader(column));
        _column_readers[column.slot_id()] = std::move(column_reader);
    }

    // create for partition values
    const auto& partition_columns = _param.scanner_ctx->partition_columns;
    const auto& partition_values = _param.scanner_ctx->partition_values;
    for (size_t i = 0; i < partition_columns.size(); i++) {
        const auto& column = partition_columns[i];
        const auto* slot_desc = column.slot_desc;
        const auto value = partition_values[i];
        _column_readers.emplace(slot_desc->id(), std::make_unique<FixedValueColumnReader>(value->get(0)));
    }

    // create for not existed column
    for (const auto* slot : _param.scanner_ctx->not_existed_slots) {
        _column_readers.emplace(slot->id(), std::make_unique<FixedValueColumnReader>(kNullDatum));
    }

    const auto& reserved_slots = _param.scanner_ctx->reserved_field_slots;
    if (!reserved_slots.empty()) {
        bool use_legacy_lookup_row_id =
                std::any_of(reserved_slots.begin(), reserved_slots.end(), [](const SlotDescriptor* slot) {
                    return slot->col_name() == "_row_source_id" || slot->col_name() == "_scan_range_id";
                });
        for (const auto* slot : reserved_slots) {
            if (slot->col_name() == HdfsScanner::ICEBERG_ROW_ID) {
                // Iceberg v3 row lineage: try physical column first (post-compaction files),
                // fall back to computed row_id (firstRowId + position) for non-compacted files.
                ASSIGN_OR_RETURN(auto reader,
                                 _create_reserved_iceberg_column_reader(slot, formats::kIcebergRowIdColumnId));
                std::optional<int64_t> first_row_id = std::nullopt;
                if (_param.scan_range != nullptr && _param.scan_range->__isset.first_row_id) {
                    // IcebergRowIdReader emits `first_row_id + i` where `i` is a file-local row
                    // index (it already includes the row-group start), so the base must be the
                    // file-level first_row_id. A row-group-level base double-counts the row-group
                    // start and shifts the emitted _row_id for every row group after the first,
                    // diverging from the physical _row_id column of compacted files. The file-level
                    // base also keeps the reader's zone-map filters (base + rg_first_row) correct.
                    first_row_id = std::optional<int64_t>(_param.scan_range->first_row_id);
                } else if (use_legacy_lookup_row_id) {
                    // Legacy (non-lineage) GLM keys rows by file-local position: the base is 0.
                    first_row_id = std::optional<int64_t>(0);
                }
                ColumnReaderPtr row_id_reader =
                        reader != nullptr ? std::make_unique<IcebergRowIdReader>(std::move(reader), first_row_id)
                                          : std::make_unique<IcebergRowIdReader>(first_row_id);
                _column_readers.emplace(slot->id(), std::move(row_id_reader));
            } else if (slot->col_name() == HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER) {
                // Iceberg v3 row lineage: try physical column first (post-compaction files),
                // fall back to file-level dataSequenceNumber passed via extended_columns from FE.
                ASSIGN_OR_RETURN(auto reader,
                                 _create_reserved_iceberg_column_reader(
                                         slot, HdfsScanner::ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COLUMN_ID));
                Datum sequence_number = kNullDatum;
                bool can_use_fallback = false;
                auto sequence_number_or = _get_extended_bigint_value(slot->id());
                if (sequence_number_or.ok()) {
                    sequence_number = sequence_number_or.value();
                    can_use_fallback = true;
                } else if (!sequence_number_or.status().is_not_found()) {
                    return sequence_number_or.status();
                }
                ColumnReaderPtr seq_reader =
                        reader != nullptr ? std::make_unique<IcebergLastUpdatedSequenceNumberReader>(
                                                    std::move(reader), can_use_fallback, sequence_number)
                                          : std::make_unique<IcebergLastUpdatedSequenceNumberReader>(sequence_number);
                _column_readers.emplace(slot->id(), std::move(seq_reader));
            } else if (slot->col_name() == "_row_source_id") {
                if (auto opt = get_backend_id(); opt.has_value()) {
                    _column_readers.emplace(slot->id(), std::make_unique<RowSourceReader>(opt.value()));
                } else {
                    return Status::InternalError("get_backend_id failed");
                }
            } else if (slot->col_name() == "_scan_range_id") {
                _column_readers.emplace(slot->id(), std::make_unique<FixedValueColumnReader>(_param.scan_range_id));
            } else if (slot->col_name() == HdfsScanner::ICEBERG_ROW_POSITION) {
                _column_readers.emplace(slot->id(), std::make_unique<ParquetPosReader>());
            }
        }
    }
    if (_param.stats != nullptr) {
        _param.stats->global_dict_total_row_groups++;
        if (_global_dict_applied_in_group) {
            _param.stats->global_dict_applied_row_groups++;
        }
    }

    return Status::OK();
}

StatusOr<ColumnReaderPtr> GroupReader::_create_column_reader(const GroupReaderParam::Column& column) {
    std::unique_ptr<ColumnReader> column_reader = nullptr;
    const auto* schema_node = _param.file_metadata->schema().get_stored_column_by_field_idx(column.idx_in_parquet);
    {
        if (column.t_lake_schema_field == nullptr) {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type()));
        } else {
            ASSIGN_OR_RETURN(column_reader,
                             ColumnReaderFactory::create(_column_reader_opts, schema_node, column.slot_type(),
                                                         column.t_lake_schema_field));
        }
        auto* global_dictmaps = _param.scanner_ctx->global_dictmaps;
        if (global_dictmaps->contains(column.slot_id())) {
            GlobalDictReaderKind kind = GlobalDictReaderKind::kNone;
            ASSIGN_OR_RETURN(column_reader, ColumnReaderFactory::create(
                                                    std::move(column_reader), global_dictmaps->at(column.slot_id()),
                                                    column.slot_id(), _row_group_metadata->num_rows, &kind));
            if (_param.stats != nullptr && kind != GlobalDictReaderKind::kNone) {
                _param.stats->global_dict_applied_slots++;
                if (kind == GlobalDictReaderKind::kDictCode) {
                    _param.stats->global_dict_dict_code_reader_slots++;
                } else if (kind == GlobalDictReaderKind::kLowRowsEncode) {
                    _param.stats->global_dict_encode_reader_slots++;
                }
                _global_dict_applied_in_group = true;
            }
        }
        if (column_reader == nullptr) {
            // this shouldn't happen but guard
            return Status::InternalError("No valid column reader.");
        }
    }
    return column_reader;
}

Status GroupReader::_prepare_column_readers() const {
    SCOPED_RAW_TIMER(&_param.stats->column_reader_init_ns);
    for (const auto& [slot_id, column_reader] : _column_readers) {
        RETURN_IF_ERROR(column_reader->prepare());
        if (column_reader->get_column_parquet_field() != nullptr &&
            column_reader->get_column_parquet_field()->is_complex_type()) {
            // For complex type columns, we need parse def & rep levels.
            // For OptionalColumnReader, by default, we will not parse it's def level for performance. But if
            // column is a complex type, we have to parse def level to calculate nullability.
            column_reader->set_need_parse_levels(true);
        }
    }
    return Status::OK();
}

void GroupReader::_process_columns_and_conjunct_ctxs() {
    // ── Classify physical columns and conjuncts ───────────────────────────────
    bool has_reserved_field_filter = false;
    std::unordered_set<SlotId> deferred_slots;
    _column_materializer->classify_columns(deferred_slots, &has_reserved_field_filter);

    if (!config::parquet_late_materialization_enable) {
        _column_materializer->promote_lazy_to_active();
    }

    // ── Promote lazy to active when no active columns exist ───────────────────
    if (_column_materializer->active_slot_ids().empty() && !has_reserved_field_filter) {
        _column_materializer->promote_lazy_to_active();
    }

    // ── Compound conjuncts ────────────────────────────────────────────────────
    _collect_compound_conjunct_info();

    // ── Join runtime filter pushdown ──────────────────────────────────────────
    // Must run last: it needs the final active/lazy split.
    _setup_runtime_filter_predicates();
}

void GroupReader::_setup_runtime_filter_predicates() {
    _rf_predicates = RuntimeFilterPredicates();
    _rf_probe_columns.clear();

    auto* src = _param.scanner_ctx == nullptr ? nullptr : _param.scanner_ctx->runtime_filter_preds;
    if (src == nullptr || src->empty()) {
        return;
    }

    // Membership must be tested against exactly the sets the two probe paths can
    // serve, and nothing else. active_slot_ids() is precisely what
    // create_active_chunk() materializes, and lazy_slot_ids() is precisely what
    // materialize_slot() can read. A runtime filter may target a slot in neither --
    // ConnectorPredicateParser::can_pushdown() accepts every slot, so partition,
    // not-existed and extended slots all reach us, as do columns pruned from this
    // file by schema evolution. Those must be dropped: deriving "active" as "not
    // lazy" would wrongly admit them and then fail looking them up.
    const auto& active_ids = _column_materializer->active_slot_ids();
    const auto& lazy_ids = _column_materializer->lazy_slot_ids();
    const std::unordered_set<SlotId> active_slots(active_ids.begin(), active_ids.end());
    const std::unordered_set<SlotId> lazy_slots(lazy_ids.begin(), lazy_ids.end());

    RuntimeFilterPredicates preds(src->driver_sequence());
    std::unordered_set<SlotId> probe_columns_seen;
    for (auto* pred : src->rf_predicates()) {
        // Group-colocate filters hold one sub-filter per driver and are only valid for the
        // driver that built them. Every other pushdown consumer excludes them via
        // can_push_down_runtime_filter() -- the operator-level collector, project and
        // aggregate nodes -- but get_runtime_filter_predicates() does not, so do it here.
        // A lake fragment is not expected to produce one (FE adds every connector scan to
        // its exec group with disableColocateGroup=true), which is why driver_sequence is
        // never consulted below; this is the cheap guard that keeps that from mattering.
        if (!pred->get_rf_desc()->can_push_down_runtime_filter()) continue;
        // The check above also covers what the storage-layer probe needs:
        // RuntimeFilterPredicate::evaluate() derives the sub-filter index from the single
        // probe column, which only matches the operator-level probe while the filter has no
        // partition-by exprs -- with them, compute_hash_values() evaluates those into a
        // separate column list and indexes off that instead. Both predicates read
        // _partition_by_exprs_contexts, so this cannot fail today; state it so the coupling
        // is visible if can_push_down_runtime_filter() is ever narrowed.
        DCHECK_EQ(pred->get_rf_desc()->num_partition_by_exprs(), 0);
        // ConnectorPredicateParser::column_id() returns SlotDescriptor::id(), so the
        // predicate's ColumnId is the probe slot id.
        const auto slot_id = static_cast<SlotId>(pred->get_column_id());
        const bool is_active = active_slots.count(slot_id) > 0;
        if (!is_active && lazy_slots.count(slot_id) == 0) continue;
        if (_column_readers.find(slot_id) == _column_readers.end()) continue;
        preds.add_predicate(pred);
        // Several filters may probe the same column. _rf_probe_columns drives building
        // the probe chunk, which keys columns by id and rejects duplicates, so it must
        // hold each column once even though every predicate is kept.
        if (probe_columns_seen.insert(slot_id).second) {
            _rf_probe_columns.push_back({slot_id, is_active});
        }
    }
    if (_rf_probe_columns.empty()) {
        return;
    }
    _rf_predicates = std::move(preds);
}

// ── IO range collection ─────────────────────────────────────────────────────

void GroupReader::collect_io_ranges(std::vector<starrocks::io::SharedBufferedInputStream::IORange>* ranges,
                                    int64_t* end_offset, ColumnIOTypeFlags types) {
    int64_t end = 0;
    _column_materializer->collect_io_ranges(ranges, &end, types);
    ReadRangePlanner::deduplicate(ranges);
    *end_offset = end;
}

} // namespace starrocks::parquet
