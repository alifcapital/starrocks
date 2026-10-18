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

#include "connector/partition_chunk_writer.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "column/field.h"
#include "column/schema.h"
#include "connector/async_flush_stream_poller.h"
#include "connector/connector_sink_executor.h"
#include "connector/sink_memory_manager.h"
#include "exec/pipeline/fragment_context.h"
#include "formats/file_writer.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/chunk_helper.h"
#include "storage/convert_helper.h"
#include "storage/load_spill_block_manager.h"
#include "storage/storage_engine.h"
#include "storage/types.h"
#include "util/monotime.h"
#include "util/time.h"

namespace starrocks::connector {

DEFINE_FAIL_POINT(parquet_chunk_writer_init_failed);

namespace {

// TIME has no storage field type, but a TIME value is a double in memory and its column is a DoubleColumn. So the
// storage field of a TIME slot says DOUBLE: we expect the sort, the spill and the merge to treat the column as any
// DOUBLE column. The file writer reads the type from the slot expression, so the values keep their TIME meaning.
void map_time_to_double(const TypeDescriptor& type, Field* field) {
    if (type.type == TYPE_TIME) {
        *field = *field->convert_to(TYPE_DOUBLE);
        return;
    }
    // The sub fields follow the children of the type: the element, the key and the value, or the struct fields.
    if (!field->has_sub_fields()) {
        return;
    }
    auto& sub_fields = field->sub_fields();
    for (size_t i = 0; i < sub_fields.size() && i < type.children.size(); ++i) {
        map_time_to_double(type.children[i], &sub_fields[i]);
    }
}

FieldPtr make_storage_field(const SlotDescriptor& slot) {
    FieldPtr field = Field::convert_from_slot_desc(slot);
    map_time_to_double(slot.type(), field.get());
    return field;
}

// A schema that comes with the chunk does not know the slot type, so the message names the storage type of the field.
Status check_schema_supported(const Schema& schema) {
    for (size_t i = 0; i < schema.num_fields(); ++i) {
        const FieldPtr& field = schema.field(i);
        if (!ChunkHelper::is_column_creatable(*field)) {
            std::string type_name = field->type() != nullptr ? logical_type_to_string(field->type()->type()) : "null";
            return Status::NotSupported(fmt::format("the spill writer does not support column {} of storage type {}",
                                                    field->name(), type_name));
        }
    }
    return Status::OK();
}

} // namespace

PartitionChunkWriter::PartitionChunkWriter(std::string partition, std::vector<int8_t> partition_field_null_list,
                                           const std::shared_ptr<PartitionChunkWriterContext>& ctx)
        : _partition(std::move(partition)),
          _partition_field_null_list(std::move(partition_field_null_list)),
          _file_writer_factory(ctx->file_writer_factory),
          _location_provider(ctx->location_provider),
          _max_file_size(ctx->max_file_size),
          _is_default_partition(ctx->is_default_partition) {
    _commit_extra_data.resize(_partition_field_null_list.size(), '0');
    std::transform(_partition_field_null_list.begin(), _partition_field_null_list.end(), _commit_extra_data.begin(),
                   [](int8_t b) { return b + '0'; });
}

Status PartitionChunkWriter::create_file_writer_if_needed() {
    if (!_file_writer) {
        std::string path = _is_default_partition ? _location_provider->get() : _location_provider->get(_partition);
        ASSIGN_OR_RETURN(auto new_writer_and_stream, _file_writer_factory->create(path));
        _file_writer = std::move(new_writer_and_stream.writer);
        _out_stream = std::move(new_writer_and_stream.stream);
        RETURN_IF_ERROR(_file_writer->init());

        FAIL_POINT_TRIGGER_EXECUTE(parquet_chunk_writer_init_failed,
                                   { return Status::InternalError("Create file writer failed due to fail point"); });
        _io_poller->enqueue(_out_stream);
    }
    return Status::OK();
}

Status PartitionChunkWriter::commit_file() {
    if (!_file_writer) {
        return Status::OK();
    }
    SCOPED_TIMER(_sink_profile ? _sink_profile->commit_file_timer : nullptr);
    auto result = _file_writer->close();
    _commit_callback(result.set_extra_data(_commit_extra_data));
    _file_writer = nullptr;
    VLOG(3) << "commit to remote file, filename: " << _out_stream->filename()
            << ", size: " << result.file_statistics.file_size;
    _out_stream = nullptr;
    return result.io_status;
}

Status BufferPartitionChunkWriter::init() {
    RETURN_IF_ERROR(create_file_writer_if_needed());
    return Status::OK();
}

Status BufferPartitionChunkWriter::write(const ChunkPtr& chunk) {
    if (_file_writer && _file_writer->get_written_bytes() >= _max_file_size) {
        RETURN_IF_ERROR(commit_file());
    }
    RETURN_IF_ERROR(create_file_writer_if_needed());
    SCOPED_TIMER(_sink_profile ? _sink_profile->write_file_timer : nullptr);
    return _file_writer->write(chunk.get());
}

Status BufferPartitionChunkWriter::flush() {
    return commit_file();
}

Status BufferPartitionChunkWriter::wait_flush() {
    return Status::OK();
}

Status BufferPartitionChunkWriter::finish() {
    return commit_file();
}

SpillPartitionChunkWriter::SpillPartitionChunkWriter(std::string partition,
                                                     std::vector<int8_t> partition_field_null_list,
                                                     const std::shared_ptr<SpillPartitionChunkWriterContext>& ctx)
        : PartitionChunkWriter(std::move(partition), std::move(partition_field_null_list), ctx),
          _fs(ctx->fs),
          _fragment_context(ctx->fragment_context),
          _column_evaluators(ctx->column_evaluators),
          _sort_ordering(ctx->sort_ordering) {
    _chunk_spill_token = ExecEnv::GetInstance()->connector_sink_spill_executor()->create_token();
    _block_merge_token = StorageEngine::instance()->load_spill_block_merge_executor()->create_token();
    _tuple_desc = ctx->tuple_desc;
    // The sink columns are the first slots of the tuple, one for each evaluator.
    if (_column_evaluators != nullptr) {
        _num_columns = _column_evaluators->size();
    } else if (_tuple_desc != nullptr) {
        _num_columns = _tuple_desc->slots().size();
    }
    _writer_id = generate_uuid();
    _spill_mode = _sort_ordering != nullptr;
}

Status SpillPartitionChunkWriter::check_tuple_supported(const TupleDescriptor* tuple_desc, size_t num_columns) {
    if (tuple_desc == nullptr) {
        return Status::NotSupported("the tuple descriptor of the sink is missing");
    }
    if (tuple_desc->slots().size() < num_columns) {
        return Status::InternalError(fmt::format("the sink tuple has {} slots, fewer than the {} sink columns",
                                                 tuple_desc->slots().size(), num_columns));
    }
    for (size_t i = 0; i < num_columns; ++i) {
        const SlotDescriptor* slot = tuple_desc->slots()[i];
        if (!ChunkHelper::is_column_creatable(*make_storage_field(*slot))) {
            return Status::NotSupported(fmt::format("the spill writer does not support column {} of type {}",
                                                    slot->col_name(), slot->type().debug_string()));
        }
    }
    return Status::OK();
}

SpillPartitionChunkWriter::~SpillPartitionChunkWriter() {
    if (_chunk_spill_token) {
        _chunk_spill_token->shutdown();
    }
    if (_block_merge_token) {
        _block_merge_token->shutdown();
    }
    if (_load_spill_block_mgr != nullptr) {
        (void)_load_spill_block_mgr->clear_parent_path();
    }
}

Status SpillPartitionChunkWriter::init() {
    std::string root_location = _location_provider->root_location();
    _load_spill_block_mgr =
            std::make_unique<LoadSpillBlockManager>(_fragment_context->query_id(), _writer_id, root_location, _fs);
    RETURN_IF_ERROR(_load_spill_block_mgr->init());
    _load_chunk_spiller = std::make_unique<LoadChunkSpiller>(_load_spill_block_mgr.get(),
                                                             _fragment_context->runtime_state()->runtime_profile());
    RETURN_IF_ERROR(create_file_writer_if_needed());
    return Status::OK();
}

Status SpillPartitionChunkWriter::write(const ChunkPtr& chunk) {
    RETURN_IF_ERROR(create_file_writer_if_needed());
    if (!_spill_mode) {
        return _write_chunk(chunk.get());
    }

    _chunks.push_back(chunk);
    _chunk_bytes_usage += chunk->bytes_usage();
    if (!_base_chunk) {
        _base_chunk = _chunks.back();
    }

    int64_t max_flush_batch_size = _file_writer->get_flush_batch_size();
    if (_sort_ordering || max_flush_batch_size == 0) {
        max_flush_batch_size = _max_file_size;
    }
    if (_chunk_bytes_usage >= max_flush_batch_size) {
        return _flush_to_file();
    } else if (_mem_insufficent()) {
        return _spill();
    }
    return Status::OK();
}

Status SpillPartitionChunkWriter::flush() {
    RETURN_IF(!_file_writer, Status::OK());
    // The sink memory manager calls flush() to release memory and only logs the result. A failed flush keeps the rows
    // in memory, so we fear that the manager chooses this writer again and again and nobody sees the error. So we
    // report the error to the sink too, which fails the fragment.
    Status st;
    // Change to spill mode if memory is insufficent.
    if (!_spill_mode) {
        _spill_mode = true;
        st = commit_file();
    } else {
        st = _spill();
    }
    _handle_err(st);
    return st;
}

Status SpillPartitionChunkWriter::wait_flush() {
    _chunk_spill_token->wait();
    return Status::OK();
}

Status SpillPartitionChunkWriter::finish() {
    // If no chunks have been spilled, flush data to remote file directly.
    if (_load_chunk_spiller->empty()) {
        VLOG(2) << "flush to remote directly when finish, query_id: " << print_id(_fragment_context->query_id())
                << ", writer_id: " << print_id(_writer_id);
        RETURN_IF_ERROR(_flush_to_file());
        return commit_file();
    }

    auto cb = [this](const Status& st) {
        LOG_IF(ERROR, !st.ok()) << "fail to merge spill blocks, query_id: " << print_id(_fragment_context->query_id())
                                << ", writer_id: " << print_id(_writer_id);
        // Currently, should always call commit_file; otherwise the output stream can't be closed.
        auto commit_st = commit_file();
        _handle_err(st);
        if (st.ok()) {
            _handle_err(commit_st);
        }
    };
    auto merge_task = std::make_shared<MergeBlockTask>(this, _fragment_context->runtime_state()->instance_mem_tracker(),
                                                       std::move(cb));
    return _block_merge_token->submit(merge_task);
}

const int64_t SpillPartitionChunkWriter::kWaitMilliseconds = 10;

bool SpillPartitionChunkWriter::is_finished() {
    bool finished = _chunk_spill_token->wait_for(MonoDelta::FromMilliseconds(kWaitMilliseconds)) &&
                    _block_merge_token->wait_for(MonoDelta::FromMilliseconds(kWaitMilliseconds));
    return finished;
}

Status SpillPartitionChunkWriter::merge_blocks() {
    SCOPED_TIMER(_sink_profile ? _sink_profile->merge_blocks_timer : nullptr);
    _chunk_spill_token->wait();
    auto write_func = [this](Chunk* chunk) { return _flush_chunk(chunk, false); };
    auto flush_func = [this]() {
        // Commit file after each merge function to ensure the data written to one file is ordered,
        // because data generated by different merge function may be unordered.
        if (_sort_ordering) {
            RETURN_IF_ERROR(commit_file());
        }
        return Status::OK();
    };
    Status st = _load_chunk_spiller->merge_write(_max_file_size, _max_file_size, _sort_ordering != nullptr,
                                                 false /* do_agg */, write_func, flush_func);
    VLOG(2) << "finish merge blocks, query_id: " << _fragment_context->query_id() << ", status: " << st.message();
    return st;
}

Status SpillPartitionChunkWriter::_sort() {
    SCOPED_TIMER(_sink_profile ? _sink_profile->sort_timer : nullptr);
    RETURN_IF(!_result_chunk, Status::OK());

    auto chunk = _result_chunk->clone_empty_with_schema(0);
    _result_chunk->swap_chunk(*chunk);
    SmallPermutation perm = create_small_permutation(static_cast<uint32_t>(chunk->num_rows()));
    Columns columns;
    for (auto sort_key_idx : _sort_ordering->sort_key_idxes) {
        columns.push_back(chunk->get_column_by_index(sort_key_idx));
    }

    RETURN_IF_ERROR(stable_sort_and_tie_columns(false, columns, _sort_ordering->sort_descs, &perm));
    std::vector<uint32_t> selective;
    permutate_to_selective(perm, &selective);
    _result_chunk->rolling_append_selective(*chunk, selective.data(), 0, chunk->num_rows());
    return Status::OK();
}

Status SpillPartitionChunkWriter::_spill() {
    RETURN_IF(_chunks.empty(), Status::OK());

    RETURN_IF_ERROR(_merge_chunks());
    if (_sort_ordering) {
        RETURN_IF_ERROR(_sort());
    }

    // Record the time when the async spill task is submitted. Note that the
    // corresponding timer below will measure the duration from this point
    // until the callback is executed, which includes both the actual spill
    // work and any queuing or scheduling delays in the thread pool.
    int64_t spill_start_ns = MonotonicNanos();
    RuntimeProfile::Counter* spill_timer = _sink_profile ? _sink_profile->spill_chunk_timer : nullptr;
    auto callback = [this, spill_start_ns, spill_timer](const ChunkPtr& chunk, const StatusOr<size_t>& res) {
        // Update spill timer with complete async execution time
        if (spill_timer) {
            COUNTER_UPDATE(spill_timer, MonotonicNanos() - spill_start_ns);
        }
        if (!res.ok()) {
            LOG(ERROR) << "fail to spill connector partition chunk sink, write it to remote file directly. msg: "
                       << res.status().message();
            Status st = _flush_chunk(chunk.get(), true);
            _handle_err(st);
        } else {
            VLOG(3) << "spill chunk data, filename: " << out_stream()->filename() << ", size: " << chunk->bytes_usage()
                    << ", rows: " << chunk->num_rows() << ", partition: " << _partition
                    << ", writer_id: " << _writer_id;
        }
        _spilling_bytes_usage.fetch_sub(chunk->bytes_usage(), std::memory_order_relaxed);
    };
    auto spill_task = std::make_shared<ChunkSpillTask>(_load_chunk_spiller.get(), _result_chunk,
                                                       _fragment_context->runtime_state()->instance_mem_tracker(),
                                                       std::move(callback));
    RETURN_IF_ERROR(_chunk_spill_token->submit(spill_task));
    _spilling_bytes_usage.fetch_add(_result_chunk->bytes_usage(), std::memory_order_relaxed);
    if (_sink_profile) {
        COUNTER_SET(_sink_profile->spilling_bytes_usage_peak, _spilling_bytes_usage.load(std::memory_order_relaxed));
    }

    _chunk_bytes_usage = 0;
    _result_chunk.reset();
    return Status::OK();
}

Status SpillPartitionChunkWriter::_flush_to_file() {
    RETURN_IF(_chunks.empty(), Status::OK());

    if (!_sort_ordering) {
        for (auto& chunk : _chunks) {
            RETURN_IF_ERROR(_flush_chunk(chunk.get(), false));
        }
    } else {
        RETURN_IF_ERROR(_merge_chunks());
        RETURN_IF_ERROR(_sort());
        RETURN_IF_ERROR(_flush_chunk(_result_chunk.get(), true));
        RETURN_IF_ERROR(commit_file());
    }
    _chunks.clear();
    _chunk_bytes_usage = 0;

    return Status::OK();
};

Status SpillPartitionChunkWriter::_flush_chunk(Chunk* chunk, bool split) {
    if (chunk->get_slot_id_to_index_map().empty()) {
        auto& slot_map = _base_chunk->get_slot_id_to_index_map();
        for (auto& it : slot_map) {
            chunk->set_slot_id_to_index(it.first, _col_index_map[it.second]);
        }
    }

    if (!split) {
        return _write_chunk(chunk);
    }
    size_t chunk_size = config::vector_chunk_size;
    for (size_t offset = 0; offset < chunk->num_rows(); offset += chunk_size) {
        auto sub_chunk = chunk->clone_empty(chunk_size);
        size_t num_rows = std::min(chunk_size, chunk->num_rows() - offset);
        sub_chunk->append(*chunk, offset, num_rows);
        RETURN_IF_ERROR(_write_chunk(sub_chunk.get()));
    }
    return Status::OK();
}

Status SpillPartitionChunkWriter::_write_chunk(Chunk* chunk) {
    if (!_sort_ordering && _file_writer->get_written_bytes() >= _max_file_size) {
        RETURN_IF_ERROR(commit_file());
    }
    RETURN_IF_ERROR(create_file_writer_if_needed());
    SCOPED_TIMER(_sink_profile ? _sink_profile->write_file_timer : nullptr);
    RETURN_IF_ERROR(_file_writer->write(chunk));
    return Status::OK();
}

Status SpillPartitionChunkWriter::_merge_chunks() {
    if (_chunks.empty()) {
        return Status::OK();
    }

    // Create a target chunk with schema to make it can use some
    // module functions of native table directly.
    size_t num_rows = std::accumulate(_chunks.begin(), _chunks.end(), 0,
                                      [](int sum, const ChunkPtr& chunk) { return sum + chunk->num_rows(); });
    ASSIGN_OR_RETURN(ChunkPtr result_chunk, _create_schema_chunk(_chunks.front(), num_rows));

    // We fear an error after the first input chunk is released: the released rows are lost, and a later call (a flush
    // again, finish, the memory manager) would find them missing in _chunks. So we read and check the sink columns of
    // all the chunks first. The loop that appends and releases them has no error path, so on an error _chunks,
    // _result_chunk and _col_index_map stay as they were.
    const size_t num_columns = result_chunk->num_columns();
    if (_column_evaluators != nullptr) {
        if (_column_evaluators->size() < num_columns) {
            return Status::InternalError(fmt::format("the spill schema has {} columns, more than the {} evaluators",
                                                     num_columns, _column_evaluators->size()));
        }
    } else {
        for (const auto& chunk : _chunks) {
            if (chunk->num_columns() < num_columns) {
                return Status::InternalError(fmt::format("the spill schema has {} columns, more than the {} of a chunk",
                                                         num_columns, chunk->num_columns()));
            }
        }
    }

    std::vector<Columns> src_columns;
    src_columns.reserve(_chunks.size());
    for (const auto& chunk : _chunks) {
        Columns& columns = src_columns.emplace_back();
        columns.reserve(num_columns);
        for (size_t i = 0; i < num_columns; ++i) {
            if (_column_evaluators) {
                ASSIGN_OR_RETURN(auto src_col, (*_column_evaluators)[i]->evaluate(chunk.get()));
                columns.push_back(std::move(src_col));
            } else {
                columns.push_back(chunk->get_column_by_index(i));
            }
        }
    }

    // The writer of the file reads the columns of the result chunk through the slot ids of the first chunk, so every
    // sink column must be a column of the first chunk.
    std::unordered_map<const Column*, size_t> col_ptr_index_map;
    auto& first_columns = _chunks.front()->columns();
    for (size_t i = 0; i < first_columns.size(); ++i) {
        col_ptr_index_map[first_columns[i]->get_ptr()] = i;
    }
    std::vector<std::pair<int, int>> col_index_pairs;
    col_index_pairs.reserve(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        auto it = col_ptr_index_map.find(src_columns.front()[i].get());
        if (it == col_ptr_index_map.end()) {
            return Status::InternalError("unknown column index: " + std::to_string(i));
        }
        col_index_pairs.emplace_back(it->second, i);
    }
    for (const auto& [chunk_idx, result_idx] : col_index_pairs) {
        _col_index_map[chunk_idx] = result_idx;
    }

    // We release each input chunk as soon as it is appended, to keep the memory of the merge low. A chunk leaves
    // _chunks together with its release, so _chunks never holds a released chunk.
    for (auto& columns : src_columns) {
        for (size_t i = 0; i < num_columns; ++i) {
            result_chunk->get_column_raw_ptr_by_index(i)->append(*columns[i]);
        }
        columns.clear();
        _chunks.pop_front();
    }

    _result_chunk = std::move(result_chunk);
    return Status::OK();
}

bool SpillPartitionChunkWriter::_mem_insufficent() {
    // Return false because we will trigger spill by sink memory manager.
    return false;
}

void SpillPartitionChunkWriter::_handle_err(const Status& st) {
    if (!st.ok() && _error_handler) {
        _error_handler(st);
    }
}

SchemaPtr SpillPartitionChunkWriter::_make_schema() {
    Fields fields;
    fields.reserve(_num_columns);
    for (size_t i = 0; i < _num_columns; ++i) {
        fields.push_back(make_storage_field(*_tuple_desc->slots()[i]));
    }

    SchemaPtr schema;
    if (_sort_ordering) {
        schema = std::make_shared<Schema>(std::move(fields), KeysType::DUP_KEYS, _sort_ordering->sort_key_idxes,
                                          std::make_shared<SortDescs>(_sort_ordering->sort_descs));
    } else {
        schema = std::make_shared<Schema>(std::move(fields), KeysType::DUP_KEYS, std::vector<uint32_t>(), nullptr);
    }
    return schema;
}

Status SpillPartitionChunkWriter::_check_sort_key_idxes(size_t num_columns) const {
    if (_sort_ordering == nullptr) {
        return Status::OK();
    }
    for (uint32_t idx : _sort_ordering->sort_key_idxes) {
        if (idx >= num_columns) {
            return Status::InternalError(
                    fmt::format("the sort key index {} is out of the {} sink columns", idx, num_columns));
        }
    }
    return Status::OK();
}

StatusOr<ChunkPtr> SpillPartitionChunkWriter::_create_schema_chunk(const ChunkPtr& base_chunk, size_t num_rows) {
    if (!_schema) {
        SchemaPtr schema = base_chunk->schema();
        if (schema) {
            RETURN_IF_ERROR(check_schema_supported(*schema));
            RETURN_IF_ERROR(_check_sort_key_idxes(schema->num_fields()));
            if (_sort_ordering) {
                schema->set_sort_key_idxes(_sort_ordering->sort_key_idxes);
                schema->set_sort_descs(std::make_shared<SortDescs>(_sort_ordering->sort_descs));
            }
        } else {
            // The sink checks the tuple before it chooses this writer. We check it again here because creating a
            // chunk for a field without a storage field type aborts the process, and an error only fails the query.
            RETURN_IF_ERROR(check_tuple_supported(_tuple_desc, _num_columns));
            RETURN_IF_ERROR(_check_sort_key_idxes(_num_columns));
            schema = _make_schema();
        }
        _schema = std::move(schema);
    }
    ChunkPtr chunk = ChunkHelper::new_chunk(*_schema, num_rows);
    return chunk;
}

} // namespace starrocks::connector
