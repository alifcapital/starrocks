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

#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/expr_context.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/narrow_io_planner.h"
#include "formats/parquet/page_cache_io_planner.h"
#include "formats/parquet/utils.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/range.h"
#include "storage/runtime_filter_predicate.h"

namespace starrocks {
class RandomAccessFile;
struct HdfsScannerStats;
struct HdfsScannerContext;
class ExprContext;
class TIcebergSchemaField;
class THdfsScanRange;

namespace parquet {
class ColumnMaterializer;
class FileMetaData;
class LazyMaterializationContext;
} // namespace parquet
struct TypeDescriptor;
} // namespace starrocks

namespace starrocks::parquet {

struct GroupReaderParam {
    struct Column {
        // parquet field index in root node's children
        int32_t idx_in_parquet;

        // column type in parquet file
        tparquet::Type::type type_in_parquet;

        SlotDescriptor* slot_desc = nullptr;

        const TIcebergSchemaField* t_lake_schema_field = nullptr;

        bool decode_needed;

        const TypeDescriptor& slot_type() const { return slot_desc->type(); }
        const SlotId slot_id() const { return slot_desc->id(); }
    };

    // Non-owning pointer to the scanner context. Provides access to all
    // context-derived fields (partition columns, not-existed slots, options,
    // global dicts, etc.) without copying them into every GroupReaderParam.
    // Always non-null when used from FileReader; may be null in unit tests.
    // Non-const because unit tests need to populate the context fields after
    // construction; GroupReader treats it as read-only by convention.
    HdfsScannerContext* scanner_ctx = nullptr;

    // conjunct_ctxs that column is materialized in group reader
    // Mutable per-group-reader shallow copy of the scanner context's by_slot map;
    // update_with_none_existed_slot() erases entries for absent columns.
    std::unordered_map<SlotId, std::vector<ExprContext*>> conjunct_ctxs_by_slot;

    // columns
    std::vector<Column> read_cols;

    HdfsScannerStats* stats = nullptr;

    io::SharedBufferedInputStream* sb_stream = nullptr;

    int chunk_size = 0;

    RandomAccessFile* file = nullptr;

    const FileMetaData* file_metadata = nullptr;

    int64_t modification_time = 0;
    uint64_t file_size = 0;
    const DataCacheOptions* datacache_options;

    // Used to decide io coalescing of lazy columns. Both are shared by all row groups of one scan: the number
    // of row groups that finished reading, and how many of them needed their lazy columns. A null pointer
    // means we always coalesce.
    std::atomic<int64_t>* lazy_coalesce_row_groups_finished = nullptr;
    std::atomic<int64_t>* lazy_coalesce_row_groups_needed = nullptr;

    // Kept directly in GroupReaderParam for test-compatibility; also used by
    // _get_extended_bigint_value() to read extended_columns from the scan range.

    int32_t scan_range_id = -1;
    const THdfsScanRange* scan_range = nullptr;
};

class GroupReader {
public:
    GroupReader(GroupReaderParam& param, int row_group_number, SkipRowsContextPtr skip_rows_ctx,
                int64_t row_group_first_row);
    ~GroupReader();

    // init used to init column reader, and devide active/lazy
    // then we can use inited column collect io range.
    Status init();
    Status prepare();
    void select_page_ranges();
    const tparquet::ColumnChunk* get_chunk_metadata(SlotId slot_id);
    const ParquetField* get_column_parquet_field(SlotId slot_id);
    ColumnReader* get_column_reader(SlotId slot_id);
    uint64_t get_row_group_first_row() const { return _row_group_first_row; }
    const tparquet::RowGroup* get_row_group_metadata() const;
    Status get_next(ChunkPtr* chunk, size_t* row_count);
    void collect_io_ranges(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                           ColumnIOTypeFlags types = ColumnIOType::PAGES);

    SparseRange<uint64_t> get_range() const { return _range; }
    SparseRange<uint64_t>& get_range() { return _range; }
    const bool get_is_group_filtered() const { return _is_group_filtered; }
    bool& get_is_group_filtered() { return _is_group_filtered; }

private:
    void _set_end_offset(int64_t value) { _end_offset = value; }

    Status _create_column_readers();
    StatusOr<ColumnReaderPtr> _create_reserved_iceberg_column_reader(const SlotDescriptor* slot, int32_t field_id);
    StatusOr<Datum> _get_extended_bigint_value(SlotId slot_id) const;
    StatusOr<ColumnReaderPtr> _create_column_reader(const GroupReaderParam::Column& column);
    Status _prepare_column_readers() const;
    // We expect the page cache to hold the pages that an earlier scan of the file read, and we fear that the shared
    // buffers of the stream load these pages again with the pages around them. So we give the stream a planner that
    // names the cached pages of this row group before it loads a buffer (see PageCacheIoPlanner).
    void _setup_page_cache_io();
    // Gives the stream the providers of this row group that name bytes it should not load: the page cache planner and
    // the planner of the narrowing (see NarrowIoPlanner).
    void _install_skip_ranges_providers();
    // Extract dict filter columns and conjuncts
    void _process_columns_and_conjunct_ctxs();

    // Fills _compound_conjuncts and _has_compound_conjunct_with_indirect_inputs. It needs the final
    // split of the columns into active and lazy ones, since defer_is_free depends on the lazy columns.
    void _collect_compound_conjunct_info();

    // ── get_next() pipeline phases ───────────────────────────────────────────

    // Bundles the per-range mutable state that flows through the filter pipeline.
    struct RowGroupScanState {
        ChunkPtr active_chunk;
        Filter chunk_filter;
        bool has_filter = false;
        size_t row_count = 0;
        // The compound conjuncts (indexes into scanner_ctxs, in ascending order) that stage 3 left to
        // stage 7, which evaluates them on the output chunk.
        std::vector<size_t> deferred_conjuncts;
    };

    // 1. Prune deleted rows: applies deletion bitmap to produce chunk_filter.
    //    Returns true if rows survive; false to skip this range entirely.
    StatusOr<bool> _prune_deleted_rows(const Range<uint64_t>& r, RowGroupScanState& state);

    // 2. Read & filter active columns: reads active physical columns and
    //    evaluates dict / expression filters.  Populates chunk_filter and
    //    fills active_chunk.  Returns true if rows survive.
    StatusOr<bool> _read_and_filter_active_columns(const Range<uint64_t>& r, RowGroupScanState& state,
                                                   LazyMaterializationContext* lazy_ctx);

    // 3. Evaluate compound (multi-slot) conjuncts via scanner_ctxs.
    //    Side columns are appended to active_chunk for predicate evaluation.
    //    lazy_ctx must still be attached.  Returns true if rows survive. Conjuncts that
    //    state.deferred_conjuncts holds are left to stage 7.
    StatusOr<bool> _evaluate_compound_predicates(const Range<uint64_t>& r, RowGroupScanState& state,
                                                 LazyMaterializationContext* lazy_ctx);

    // Stage 3 when at least one row is dropped: each conjunct that state.deferred_conjuncts does not
    // hold is evaluated on the rows that chunk_filter keeps, and its result is ANDed back into
    // chunk_filter. A conjunct that reads its inputs from the chunk directly (lambda, dictionary
    // mapping) gets the lazy columns it needs read under chunk_filter. Returns the surviving row count.
    StatusOr<size_t> _evaluate_compound_predicates_on_selected_rows(RowGroupScanState& state,
                                                                    LazyMaterializationContext* lazy_ctx);

    // Stage 3 when no row is dropped: the conjuncts are evaluated on the whole chunk, and the lazy
    // columns that a conjunct reading its inputs directly needs are read for the whole range.
    // Returns the surviving row count.
    StatusOr<size_t> _evaluate_compound_predicates_on_whole_chunk(RowGroupScanState& state,
                                                                  LazyMaterializationContext* lazy_ctx);

    // Whether stage 3 may leave compound conjunct i to stage 7 in this pass. The caller has checked that
    // the config is on and that at least one row of the pass is dropped. A conjunct with a dictionary
    // mapping stays in stage 3, and a conjunct that is free to defer (CompoundConjunctInfo::defer_is_free)
    // is always left to stage 7. The statistics of any other conjunct in this scan range must allow it.
    bool _can_defer_compound_conjunct(size_t i) const;

    // Stage 7: evaluates the deferred compound conjuncts on the output chunk, which holds only the rows
    // that stages 1 to 6 kept, and drops the rows that they reject from the chunk. Sets row_count to
    // the rows left.
    Status _evaluate_deferred_compound_predicates(const RowGroupScanState& state, ChunkPtr* chunk, size_t* row_count);

    // 3.1 Probe join runtime filters against decoded rows, ANDing into chunk_filter
    //     so non-matching rows are dropped before lazy columns are materialized.
    //     A lazy probe column is read under chunk_filter and cached, so stages 3 and 4
    //     reuse it. Returns true if rows survive.
    StatusOr<bool> _evaluate_runtime_filters(const Range<uint64_t>& r, RowGroupScanState& state);

    // True when stage 3.1 should run before stage 3 for the current chunk: there are compound
    // conjuncts and a join runtime filter has arrived. Stage 3 then evaluates the conjuncts on
    // the rows that the filter kept.
    bool _should_probe_runtime_filters_first() const;

    // 4. Apply combined chunk_filter, compute post-filter range (internal),
    //    and backfill lazy physical columns.
    //    Returns true if rows survive filtering; false to skip this range.
    StatusOr<bool> _filter_and_backfill_lazy(const Range<uint64_t>& r, RowGroupScanState& state);

    // Build the subset of scanner_ctx->runtime_filter_preds that this row group can
    // actually serve. Called once per row group after column classification.
    void _setup_runtime_filter_predicates();

    // 5. Emit output: physical columns into destination chunk.
    Status _emit_output_columns(RowGroupScanState& state, ChunkPtr* chunk, size_t* row_count);

    // ── Member variables ─────────────────────────────────────────────────────

    // row group meta
    const tparquet::RowGroup* _row_group_metadata = nullptr;
    int64_t _row_group_first_row = 0;
    SkipRowsContextPtr _skip_rows_ctx;

    // column readers for column chunk in row group
    std::unordered_map<SlotId, std::unique_ptr<ColumnReader>> _column_readers;
    // Own the initial-default values the FixedValueColumnReaders of not existed columns point into.
    std::vector<ColumnPtr> _not_existed_default_columns;

    // The provider of the stream that names the cached pages of this row group. It holds the readers of the chunks, so
    // the destructor takes it out of the stream first. Null when the page cache is not used.
    std::unique_ptr<PageCacheIoPlanner> _page_cache_io_planner;

    // The columns of this row group that skip pages through their offset index tell this planner the pages they need,
    // and it names the pages in front of their ranges to the stream. Null when the shared stream is not used.
    std::unique_ptr<NarrowIoPlanner> _narrow_io_planner;

    // The provider of the stream: it asks the planners above. The destructor takes it out of the stream first.
    SkipRangesProviderChain _skip_ranges_chain;

    // Column materialization layer over ColumnReaders. Predicate classification
    // still lives in GroupReader until the next refactor steps.
    std::unique_ptr<ColumnMaterializer> _column_materializer;

    // dict value is empty after conjunct eval, file group can be skipped
    bool _is_group_filtered = false;

    // ── Join runtime filter pushdown ───────────────────────────────────────
    // Per-row-group subset of scan_ctx->runtime_filter_preds, holding only the
    // predicates whose probe column this row group can supply. The subset is
    // required for correctness -- RuntimeFilterPredicates::evaluate() looks up every
    // predicate's column, so a predicate we cannot serve must not be in the list, and
    // which columns exist differs per file after schema evolution. The predicate
    // objects themselves are shared with the scanner context; only this container
    // (which carries the adaptive-sampling state) is per-GroupReader.
    RuntimeFilterPredicates _rf_predicates;
    // Probe columns, in predicate order. `is_active` selects where the decoded column
    // comes from: the active chunk, or an on-demand materialize_slot() lazy read.
    struct RuntimeFilterProbeColumn {
        SlotId slot_id;
        bool is_active;
    };
    std::vector<RuntimeFilterProbeColumn> _rf_probe_columns;
    // The descriptors of _rf_predicates. A filter that the build side has not built yet may have a sink there, where
    // this reader reports what the lazy columns of a row cost.
    std::vector<RuntimeFilterProbeDescriptor*> _rf_descs;

    // ── Compound conjuncts ─────────────────────────────────────────────────
    // The conjuncts in scanner_ctx->conjuncts.scanner_ctxs are prepared before the group reader is
    // created and do not change while it scans, and stage 3 runs for every chunk. So we walk the
    // expression trees once, in _process_columns_and_conjunct_ctxs(), and keep the result here.
    struct CompoundConjunctInfo {
        // True when the conjunct may read slots that get_slot_ids() does not report (a lambda
        // host, a dictionary mapping), so it needs the lazy columns read for it.
        bool has_indirect_inputs = false;
        // True when a dictionary mapping is in the conjunct. Such a conjunct is never deferred.
        bool has_dict_mapping = false;
        // The slots that the conjunct reads, sorted and without duplicates: get_slot_ids(), and
        // when has_indirect_inputs is true also the code slot of every dictionary mapping.
        std::vector<SlotId> slots;
        // True when we expect that deferring the conjunct costs no lazy read, so we defer it whatever it
        // keeps. A deferred conjunct leaves the rows that it would drop in the chunk until stage 7, and we
        // fear that the reader then reads a lazy column, or runs another conjunct, for these rows. So the
        // flag is true only when
        // - the conjunct reads every lazy column that the reader reads for an output or for a compound
        //   conjunct: it reads them for the surviving rows either way, and no other lazy column is left to
        //   read for the extra rows,
        // - the conjunct has no dictionary mapping, and
        // - every later conjunct has the flag too, so it is deferred as well and does not run in stage 3 on
        //   the extra rows.
        bool defer_is_free = false;
    };
    // One entry per conjunct, in the order of scanner_ctxs.
    std::vector<CompoundConjunctInfo> _compound_conjuncts;
    // True when at least one entry of _compound_conjuncts has has_indirect_inputs.
    bool _has_compound_conjunct_with_indirect_inputs = false;

    // param for read row group
    const GroupReaderParam& _param;

    ObjectPool _obj_pool;

    ColumnReaderOptions _column_reader_opts;

    int64_t _end_offset = 0;

    bool _global_dict_applied_in_group = false;

    SparseRange<uint64_t> _range;
    SparseRangeIterator<uint64_t> _range_iter;

    // a flag to reflect prepare() is called
    bool _has_prepared = false;
};

using GroupReaderPtr = std::shared_ptr<GroupReader>;

} // namespace starrocks::parquet
