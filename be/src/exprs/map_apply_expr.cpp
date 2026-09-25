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

#include "exprs/map_apply_expr.h"

#include <fmt/format.h>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/map_column.h"
#include "column/vectorized_fwd.h"
#include "exprs/expr_context.h"
#include "exprs/function_helper.h"
#include "exprs/lambda_function.h"
#include "exprs/map_expr.h"
#include "exprs/selected_collection.h"
#include "glog/logging.h"
#include "runtime/user_function_cache.h"
#include "storage/chunk_helper.h"

namespace starrocks {

MapApplyExpr::MapApplyExpr(const TExprNode& node) : Expr(node, false) {}

// for tests
MapApplyExpr::MapApplyExpr(TypeDescriptor type) : Expr(std::move(type), false), _maybe_duplicated_keys(true) {}

Status MapApplyExpr::prepare(starrocks::RuntimeState* state, starrocks::ExprContext* context) {
    RETURN_IF_ERROR(Expr::prepare(state, context));
    if (_is_prepared) {
        return Status::OK();
    }
    _is_prepared = true;
    if (_children.size() < 2) {
        return Status::InternalError("map expression's children size should not less than 2");
    }
    auto lambda_func = down_cast<LambdaFunction*>(_children[0]);
    auto map_expr = down_cast<MapExpr*>(lambda_func->get_lambda_expr());
    _maybe_duplicated_keys = map_expr->maybe_duplicated_keys();
    lambda_func->get_lambda_arguments_ids(&_arguments_ids);
    return Status::OK();
}

StatusOr<ColumnPtr> MapApplyExpr::evaluate_checked(ExprContext* context, Chunk* chunk) {
    Columns input_columns;
    NullColumn::MutablePtr input_null_map = nullptr;
    MapColumn* input_map = nullptr;
    ColumnPtr input_map_ptr_ref = nullptr; // hold shared_ptr to avoid early deleted.
    // step 1: get input columns from map(key_col, value_col)
    for (int i = 1; i < _children.size(); ++i) { // currently only 2 children, may be more in the future
        ASSIGN_OR_RETURN(auto child_col, context->evaluate(_children[i], chunk));
        // the column is a null literal.
        if (child_col->only_null()) {
            return ColumnHelper::align_return_type(std::move(child_col), type(), chunk->num_rows(), true);
        }
        // no optimization for const columns.
        child_col = ColumnHelper::unpack_and_duplicate_const_column(child_col->size(), child_col);
        auto data_column = child_col;
        if (child_col->is_nullable()) {
            auto nullable = down_cast<const NullableColumn*>(child_col.get());
            DCHECK(nullable != nullptr);
            data_column = nullable->data_column();
            // empty null map with non-empty elements
            auto data_mut = std::move(*data_column).mutate();
            data_mut->empty_null_in_complex_column(
                    nullable->null_column()->immutable_data(),
                    down_cast<MapColumn*>(data_mut.get())->offsets_column()->immutable_data());
            data_column = std::move(data_mut);
            if (input_null_map) {
                input_null_map = FunctionHelper::union_null_column(nullable->null_column(),
                                                                   std::move(input_null_map)); // merge null
            } else {
                input_null_map = NullColumn::static_pointer_cast(Column::mutate(nullable->null_column()));
            }
        }
        DCHECK(data_column->is_map());
        auto* cur_map = down_cast<MapColumn*>(data_column->as_mutable_raw_ptr());

        if (input_map_ptr_ref == nullptr) {
            input_map_ptr_ref = data_column;
            input_map = cur_map;
        } else {
            if (UNLIKELY(!ColumnHelper::offsets_equal(cur_map->offsets_column(), input_map->offsets_column()))) {
                return Status::InternalError("Input map element's size are not equal in map_apply().");
            }
        }
        input_columns.push_back(cur_map->keys_column());
        input_columns.push_back(cur_map->values_column());
    }
    // step 2: construct a new chunk to evaluate the lambda expression, output a map column without warping null info.
    MutableColumnPtr column = nullptr;
    if (input_map->keys_column()->empty()) { // map is empty
        column = ColumnHelper::create_column(type(), false);
    } else {
        auto cur_chunk = std::make_shared<Chunk>();
        // put all arguments into the new chunk
        int argument_num = _arguments_ids.size();
        DCHECK(argument_num == input_columns.size())
                << "arg num << " << argument_num << " != input size " << input_columns.size();
        for (int i = 0; i < argument_num; ++i) {
            cur_chunk->append_column(input_columns[i], _arguments_ids[i]); // column ref
        }
        // put captured columns into the new chunk aligning with the first map's offsets
        auto lambda_func = dynamic_cast<LambdaFunction*>(_children[0]);
        std::vector<SlotId> slot_ids;
        lambda_func->get_captured_slot_ids(&slot_ids);
        for (auto id : slot_ids) {
            DCHECK(id > 0);
            auto* captured = chunk->get_column_raw_ptr_by_slot_id(id);
            if (UNLIKELY(captured->size() < input_map->size())) {
                return Status::InternalError(fmt::format("The size of the captured column {} is less than map's size.",
                                                         captured->get_name()));
            }

            ASSIGN_OR_RETURN(auto replicated_col, captured->replicate(input_map->offsets_column_raw_ptr()->get_data()));
            cur_chunk->append_column(std::move(replicated_col), id);
        }
        // evaluate the lambda expression
        if (cur_chunk->num_rows() <= chunk->num_rows() * 8) {
            ASSIGN_OR_RETURN(auto tmp_column, context->evaluate(_children[0], cur_chunk.get()));
            column = ColumnHelper::align_return_type(std::move(*tmp_column).mutate(), type(), cur_chunk->num_rows(),
                                                     false);
        } else { // split large chunks into small ones to avoid too large or various batch_size
            ChunkAccumulator accumulator(DEFAULT_CHUNK_SIZE);
            RETURN_IF_ERROR(accumulator.push(std::move(cur_chunk)));
            accumulator.finalize();
            while (auto tmp_chunk = accumulator.pull()) {
                ASSIGN_OR_RETURN(auto tmp_col, context->evaluate(_children[0], tmp_chunk.get()));
                tmp_col = ColumnHelper::align_return_type(std::move(tmp_col), type(), tmp_chunk->num_rows(), false);
                if (column == nullptr) {
                    column = std::move(*tmp_col).mutate();
                } else {
                    column->append(*tmp_col);
                }
            }
        }
    }
    // attach offsets
    auto map_col = down_cast<MapColumn*>(column.get());
    if (UNLIKELY(input_map->offsets_column()->immutable_data().back() < map_col->keys_column()->size())) {
        return Status::InternalError(fmt::format("The max index of offsets {} < map->key column's size {}",
                                                 input_map->offsets_column()->immutable_data().back(),
                                                 map_col->keys_column()->size()));
    }

    auto res_map = MapColumn::create(
            std::move(*map_col->keys_column()).mutate(), std::move(*map_col->values_column()).mutate(),
            ColumnHelper::as_column<UInt32Column>(std::move(*input_map->offsets_column()).mutate()));

    if (_maybe_duplicated_keys && res_map->size() > 0) {
        down_cast<MapColumn*>(res_map->as_mutable_raw_ptr())->remove_duplicated_keys();
    }
    // attach null info
    if (input_null_map != nullptr) {
        return NullableColumn::create(std::move(res_map), std::move(input_null_map));
    }
    return res_map;
}

StatusOr<ColumnPtr> MapApplyExpr::evaluate_selected(ExprContext* context, Chunk* chunk,
                                                    const std::vector<uint32_t>& rows) {
    if (rows.empty()) return ColumnHelper::create_column(type(), true);
    SelectedColumns arguments;
    std::vector<const MapColumn*> maps;
    for (size_t i = 1; i < _children.size(); ++i) {
        ASSIGN_OR_RETURN(auto input, selected_expression_argument(_children[i], context, chunk, rows));
        if (input.column->only_null()) return ColumnHelper::create_const_null_column(rows.size());
        maps.push_back(down_cast<const MapColumn*>(ColumnHelper::get_data_column(input.column.get())));
        arguments.emplace_back(std::move(input));
    }
    auto nulls = NullColumn::create(rows.size(), 0);
    auto offsets = UInt32Column::create();
    offsets->append(0);
    std::vector<std::vector<uint32_t>> entries(arguments.size());
    for (size_t row = 0; row < rows.size(); ++row) {
        bool null = false;
        for (const auto& input : arguments) null |= input.column->is_null(input_row(input, row));
        nulls->get_data()[row] = null;
        if (!null) {
            size_t first = input_row(arguments[0], row);
            size_t count = maps[0]->offsets().get_data()[first + 1] - maps[0]->offsets().get_data()[first];
            for (size_t arg = 0; arg < arguments.size(); ++arg) {
                size_t src = input_row(arguments[arg], row);
                const auto& source_offsets = maps[arg]->offsets().get_data();
                if (count != source_offsets[src + 1] - source_offsets[src])
                    return Status::InternalError("Input map element's size are not equal in map_apply().");
                for (uint32_t i = source_offsets[src]; i < source_offsets[src + 1]; ++i) entries[arg].push_back(i);
            }
        }
        offsets->append(entries[0].size());
    }
    size_t count = offsets->get_data().back();
    if (count == 0) {
        auto empty = ColumnHelper::create_column(type(), false);
        empty->append_default(rows.size());
        return NullableColumn::create(std::move(empty), std::move(nulls));
    }
    auto* lambda = down_cast<LambdaFunction*>(_children[0]);
    std::vector<SlotId> captured;
    lambda->get_captured_slot_ids(&captured);
    MutableColumnPtr values;
    if (maps.size() == 1 && captured.empty() && lambda->get_common_sub_expr_ids().empty()) {
        Chunk input;
        input.append_column(maps[0]->keys_column(), _arguments_ids[0]);
        input.append_column(maps[0]->values_column(), _arguments_ids[1]);
        for (size_t begin = 0; begin < count; begin += DEFAULT_CHUNK_SIZE) {
            size_t end = std::min(count, begin + DEFAULT_CHUNK_SIZE);
            std::vector<uint32_t> selected(entries[0].begin() + begin, entries[0].begin() + end);
            ASSIGN_OR_RETURN(auto output, lambda->get_lambda_expr()->evaluate_selected(context, &input, selected));
            output = ColumnHelper::align_return_type(std::move(output), type(), end - begin, false);
            if (values == nullptr)
                values = std::move(*output).mutate();
            else
                values->append(*output);
        }
    } else {
        auto input = std::make_shared<Chunk>();
        for (size_t arg = 0; arg < arguments.size(); ++arg) {
            auto keys = maps[arg]->keys_column()->clone_empty();
            auto vals = maps[arg]->values_column()->clone_empty();
            keys->append_selective(maps[arg]->keys(), entries[arg]);
            vals->append_selective(maps[arg]->values(), entries[arg]);
            input->append_column(std::move(keys), _arguments_ids[arg * 2]);
            input->append_column(std::move(vals), _arguments_ids[arg * 2 + 1]);
        }
        for (SlotId slot : captured) {
            const auto& source = chunk->get_column_by_slot_id(slot);
            auto repeated = source->clone_empty();
            for (size_t row = 0; row < rows.size(); ++row)
                repeated->append_value_multiple_times(*source, rows[row],
                                                      offsets->get_data()[row + 1] - offsets->get_data()[row]);
            input->append_column(std::move(repeated), slot);
        }
        ChunkAccumulator accumulator(DEFAULT_CHUNK_SIZE);
        RETURN_IF_ERROR(accumulator.push(std::move(input)));
        accumulator.finalize();
        while (auto part = accumulator.pull()) {
            ASSIGN_OR_RETURN(auto output, context->evaluate(lambda, part.get()));
            output = ColumnHelper::align_return_type(std::move(output), type(), part->num_rows(), false);
            if (values == nullptr)
                values = std::move(*output).mutate();
            else
                values->append(*output);
        }
    }
    auto* mapped = down_cast<MapColumn*>(values.get());
    if (mapped->keys().size() != count)
        return Status::InternalError("map_apply lambda must return one key/value pair per input entry");
    // The lambda output already owns the final keys/values; only regroup its offsets.
    mapped->offsets_column() = std::move(offsets);
    if (_maybe_duplicated_keys) mapped->remove_duplicated_keys();
    return NullableColumn::create(std::move(values), std::move(nulls));
}

} // namespace starrocks
