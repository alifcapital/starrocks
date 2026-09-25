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

#include "column/column_builder.h"
#include "column/column_viewer.h"
#include "column/map_column.h"
#include "exprs/cast_expr.h"
#include "exprs/selected_expr.h"
#include "util/variant.h"

namespace starrocks {

StatusOr<ColumnPtr> CastJsonToMap::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ASSIGN_OR_RETURN(auto column, _children[0]->evaluate_checked(context, ptr));
    return evaluate_impl(context, Columns{std::move(column)});
}
StatusOr<ColumnPtr> CastJsonToMap::evaluate_selected(ExprContext* context, Chunk* chunk,
                                                     const std::vector<uint32_t>& rows) {
    if (rows.empty()) return ColumnHelper::create_column(type(), true);
    ASSIGN_OR_RETURN(auto input, selected_expression_argument(_children[0], context, chunk, rows));
    return evaluate_impl(context, SelectedColumns{std::move(input)});
}
template <typename Inputs>
StatusOr<ColumnPtr> CastJsonToMap::evaluate_impl(ExprContext* context, const Inputs& inputs) {
    const auto& src_column = input_column(inputs[0]);
    const size_t work_rows = src_column->is_constant() ? 1 : input_num_rows(inputs);
    if (src_column->only_null()) {
        return ColumnHelper::create_const_null_column(input_num_rows(inputs));
    }

    FunctionColumnViewer<TYPE_JSON, Inputs> src_viewer(inputs[0]);
    NullColumn::MutablePtr null_column = NullColumn::create();
    UInt32Column::MutablePtr offsets_column = UInt32Column::create();
    ColumnBuilder<TYPE_VARCHAR> keys_builder(work_rows);
    ColumnBuilder<TYPE_JSON> values_builder(work_rows);

    // 1. Cast JsonObject to MAP<VARCHAR,JSON>
    uint32_t offset = 0;
    for (size_t i = 0; i < work_rows; i++) {
        offsets_column->append(offset);
        if (src_viewer.is_null(i)) {
            null_column->append(1);
            continue;
        }
        const JsonValue* json_value = src_viewer.value(i);
        if (json_value && json_value->get_type() == JsonType::JSON_OBJECT) {
            vpack::Slice json_slice = json_value->to_vslice();
            DCHECK(json_slice.isObject());
            for (const auto& pair : vpack::ObjectIterator(json_slice)) {
                keys_builder.append(pair.key.copyString());
                JsonValue value(pair.value);
                values_builder.append(std::move(value));
            }
            offset += json_slice.length();
            null_column->append(0);
        } else {
            null_column->append(1);
        }
    }
    offsets_column->append(offset);
    auto keys_column = keys_builder.build_nullable_column();
    auto values_column = values_builder.build_nullable_column();

    // 2. Cast key and value if needed
    if (_key_cast_expr != nullptr) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_key_cast_expr->get_child(0))->slot_id();
        chunk->append_column(std::move(keys_column), slot_id);
        ASSIGN_OR_RETURN(auto result, _key_cast_expr->evaluate_checked(context, chunk.get()));
        keys_column = ColumnHelper::cast_to_nullable_column(std::move(result));
    }
    if (_value_cast_expr != nullptr) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_value_cast_expr->get_child(0))->slot_id();
        chunk->append_column(std::move(values_column), slot_id);
        ASSIGN_OR_RETURN(auto result, _value_cast_expr->evaluate_checked(context, chunk.get()));
        values_column = ColumnHelper::cast_to_nullable_column(std::move(result));
    }

    auto map_column = MapColumn::create(std::move(keys_column), std::move(values_column), std::move(offsets_column));
    map_column->remove_duplicated_keys();
    RETURN_IF_ERROR(map_column->unfold_const_children(_type));
    ColumnPtr result = NullableColumn::create(std::move(map_column), std::move(null_column));
    if (src_column->is_constant()) return ConstColumn::create(std::move(result), input_num_rows(inputs));
    return result;
}

StatusOr<ColumnPtr> CastVariantToMap::evaluate_checked(ExprContext* context, Chunk* ptr) {
    ASSIGN_OR_RETURN(auto column, _children[0]->evaluate_checked(context, ptr));
    return evaluate_impl(context, Columns{std::move(column)});
}
StatusOr<ColumnPtr> CastVariantToMap::evaluate_selected(ExprContext* context, Chunk* chunk,
                                                        const std::vector<uint32_t>& rows) {
    if (rows.empty()) return ColumnHelper::create_column(type(), true);
    ASSIGN_OR_RETURN(auto input, selected_expression_argument(_children[0], context, chunk, rows));
    return evaluate_impl(context, SelectedColumns{std::move(input)});
}
template <typename Inputs>
StatusOr<ColumnPtr> CastVariantToMap::evaluate_impl(ExprContext* context, const Inputs& inputs) {
    const auto& src_column = input_column(inputs[0]);
    const size_t work_rows = src_column->is_constant() ? 1 : input_num_rows(inputs);
    if (src_column->only_null()) {
        return ColumnHelper::create_const_null_column(input_num_rows(inputs));
    }

    FunctionColumnViewer<TYPE_VARIANT, Inputs> variant_viewer(inputs[0]);
    NullColumn::MutablePtr null_column = NullColumn::create();
    UInt32Column::MutablePtr offsets_column = UInt32Column::create();
    ColumnBuilder<TYPE_VARCHAR> keys_builder(work_rows);
    ColumnBuilder<TYPE_VARIANT> values_builder(work_rows);

    // 1. Cast Variant(type=MAP) to MAP<VARCHAR,VARIANT>
    uint32_t offset = 0;
    for (size_t i = 0; i < work_rows; i++) {
        offsets_column->append(offset);
        if (variant_viewer.is_null(i)) {
            null_column->append(1);
            continue;
        }

        const VariantRowValue* variant = variant_viewer.value(i);
        const VariantValue& value = variant->get_value();
        const VariantMetadata& metadata = variant->get_metadata();
        // Only OBJECT type can be cast to MAP, other types are set to null
        if (value.type() == VariantType::OBJECT) {
            ASSIGN_OR_RETURN(const auto object_info, value.get_object_info());
            const uint32_t map_size = object_info.num_elements;
            for (uint32_t idx = 0; idx < map_size; idx++) {
                // read field id from the value
                uint32_t filed_id = VariantUtil::read_little_endian_unsigned32(
                        value.raw().data() + object_info.id_start_offset + idx * object_info.id_size,
                        object_info.id_size);
                uint32_t offset_pos = VariantUtil::read_little_endian_unsigned32(
                        value.raw().data() + object_info.offset_start_offset + idx * object_info.offset_size,
                        object_info.offset_size);
                ASSIGN_OR_RETURN(std::string_view key, metadata.get_key(filed_id));
                uint32_t next_pos = object_info.data_start_offset + offset_pos;
                if (next_pos >= value.raw().size()) {
                    return Status::InternalError(
                            fmt::format("Cannot get variant object field value by key: {}, offset out of bounds", key));
                }

                keys_builder.append(Slice(key));
                auto sub_value = value.raw().substr(next_pos, value.raw().size() - next_pos);
                ASSIGN_OR_RETURN(VariantRowValue result, VariantRowValue::create(metadata.raw(), sub_value));
                values_builder.append(std::move(result));
            }

            offset += map_size;
            null_column->append(0);
        } else {
            null_column->append(1);
        }
    }

    offsets_column->append(offset);
    auto keys_column = keys_builder.build_nullable_column();
    auto values_column = values_builder.build_nullable_column();

    // 2. Try to cast keys if required
    if (keys_need_cast()) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_key_cast_expr->get_child(0))->slot_id();
        chunk->append_column(keys_column, slot_id);
        ASSIGN_OR_RETURN(auto result, _key_cast_expr->evaluate_checked(context, chunk.get()));
        keys_column = ColumnHelper::cast_to_nullable_column(std::move(result));
    }
    // 3. Try to cast values if required
    if (values_need_cast()) {
        ChunkPtr chunk = std::make_shared<Chunk>();
        SlotId slot_id = down_cast<ColumnRef*>(_value_cast_expr->get_child(0))->slot_id();
        chunk->append_column(values_column, slot_id);
        ASSIGN_OR_RETURN(auto result, _value_cast_expr->evaluate_checked(context, chunk.get()));
        values_column = ColumnHelper::cast_to_nullable_column(std::move(result));
    }

    // 4. Move to MapColumn
    auto map_column = MapColumn::create(std::move(keys_column), std::move(values_column), std::move(offsets_column));
    map_column->remove_duplicated_keys();
    RETURN_IF_ERROR(map_column->unfold_const_children(_type));
    ColumnPtr result = NullableColumn::create(std::move(map_column), std::move(null_column));
    if (src_column->is_constant()) return ConstColumn::create(std::move(result), input_num_rows(inputs));
    return result;
}

} // namespace starrocks