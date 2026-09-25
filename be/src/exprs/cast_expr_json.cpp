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

#include <velocypack/Exception.h>

#include <unordered_map>

#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/column_visitor_adapter.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "column/struct_column.h"
#include "column/type_traits.h"
#include "exprs/cast_expr.h"
#include "exprs/decimal_cast_expr.h"
#include "exprs/selected_column.h"

namespace starrocks {

template <typename, typename = void>
constexpr bool is_type_complete_v = false;

template <typename T>
constexpr bool is_type_complete_v<T, std::void_t<decltype(sizeof(T))>> = true;

// Reproduce validation of the selected nested column without copying its payload. A map
// visitor historically rejects every row of that map column if any gathered key is NULL.
using SelectedMapNulls = std::unordered_map<const MapColumn*, bool>;
static bool has_nested_null_map_key(const ColumnPtr& column) {
    if (column->is_constant())
        return has_nested_null_map_key(down_cast<const ConstColumn*>(column.get())->data_column());
    if (column->is_nullable())
        return has_nested_null_map_key(down_cast<const NullableColumn*>(column.get())->data_column());
    if (column->is_map()) {
        const auto* map = down_cast<const MapColumn*>(column.get());
        return map->keys().has_null() || has_nested_null_map_key(map->values_column());
    }
    if (column->is_array())
        return has_nested_null_map_key(down_cast<const ArrayColumn*>(column.get())->elements_column());
    if (column->is_struct()) {
        for (const auto& field : down_cast<const StructColumn*>(column.get())->fields())
            if (has_nested_null_map_key(field)) return true;
    }
    return false;
}
static void collect_selected_map_nulls(const ColumnPtr& column, const std::vector<uint32_t>& rows,
                                       SelectedMapNulls* nulls) {
    if (!has_nested_null_map_key(column)) return;
    if (column->is_constant()) {
        std::vector<uint32_t> physical(rows.size(), 0);
        collect_selected_map_nulls(down_cast<const ConstColumn*>(column.get())->data_column(), physical, nulls);
        return;
    }
    if (column->is_nullable()) {
        collect_selected_map_nulls(down_cast<const NullableColumn*>(column.get())->data_column(), rows, nulls);
        return;
    }
    if (column->is_struct()) {
        for (const auto& field : down_cast<const StructColumn*>(column.get())->fields())
            collect_selected_map_nulls(field, rows, nulls);
    } else if (column->is_array() || column->is_map()) {
        const UInt32Column* offsets;
        ColumnPtr child;
        const MapColumn* map = nullptr;
        if (column->is_array()) {
            auto* array = down_cast<const ArrayColumn*>(column.get());
            offsets = array->offsets_column().get();
            child = array->elements_column();
        } else {
            map = down_cast<const MapColumn*>(column.get());
            offsets = map->offsets_column().get();
            child = map->values_column();
            nulls->try_emplace(map, false);
        }
        std::vector<uint32_t> elements;
        for (uint32_t row : rows) {
            for (uint32_t i = offsets->get_data()[row]; i < offsets->get_data()[row + 1]; ++i) {
                if (map != nullptr && map->keys().is_null(i)) (*nulls)[map] = true;
                elements.push_back(i);
            }
        }
        collect_selected_map_nulls(child, elements, nulls);
    }
}

// Cast item in column
// NOTE: cast in rowwise is not efficent but intuitive
class CastColumnItemVisitor final : public ColumnVisitorAdapter<CastColumnItemVisitor> {
public:
    CastColumnItemVisitor(int row, const std::string& field_name, vpack::Builder* builder, bool unindexed_struct,
                          const SelectedMapNulls* selected_map_nulls = nullptr)
            : ColumnVisitorAdapter(this),
              _row(row),
              _field_name(field_name),
              _builder(builder),
              _unindexed_struct(unindexed_struct),
              _selected_map_nulls(selected_map_nulls) {}

    static Status cast_datum_to_json(const ColumnPtr& col, int row, const std::string& name, vpack::Builder* builder,
                                     bool unindexed_struct = false,
                                     const SelectedMapNulls* selected_map_nulls = nullptr) {
        CastColumnItemVisitor visitor(row, name, builder, unindexed_struct, selected_map_nulls);
        try {
            return col->accept(&visitor);
        } catch (const arangodb::velocypack::Exception& e) {
            std::string data = col->debug_item(row);
            LOG(WARNING) << "cast to json failed: error_code=" << e.errorCode() << ", error_msg=" << e.what()
                         << ", data=" << data;
            return Status::DataQualityError("cast to json failed: " + data);
        }
    }

    template <class T>
    void _add_element(T&& value) {
        if (_field_name.empty()) {
            _builder->add(vpack::Value(value));
        } else {
            _builder->add(_field_name, vpack::Value(value));
        }
    }

    template <class T, typename = std::enable_if<is_type_complete_v<typename ColumnTraits<T>::ColumnType>, void>>
    Status do_visit(const FixedLengthColumn<T>& col) {
        if constexpr (CastToString::extend_type<T>()) {
            // Cast extended type to string in JSON
            auto value = col.get(_row).template get<T>();
            std::string str = CastToString::apply<T, std::string>(value);
            _add_element(std::move(str));
        } else if constexpr (std::is_integral_v<T> || std::is_floating_point_v<T>) {
            auto value = col.get(_row).template get<T>();
            _add_element(std::move(value));
        } else {
            return Status::NotSupported("not supported");
        }
        return {};
    }

    template <class T>
    Status do_visit(const DecimalV3Column<T>& col) {
        int precision = col.precision();
        int scale = col.scale();
        auto value = col.get(_row).template get<T>();
        auto str = DecimalV3Cast::to_string<T>(value, precision, scale);
        _add_element(std::move(str));
        return {};
    }

    Status do_visit(const JsonColumn& col) {
        JsonValue* json = col.get_object(_row);
        if (_field_name.empty()) {
            _builder->add(json->to_vslice());
        } else {
            _builder->add(_field_name, json->to_vslice());
        }
        return {};
    }

    Status do_visit(const BinaryColumn& col) {
        Slice slice = col.get_slice(_row);
#ifdef __APPLE__
        // On macOS, velocypack's template overload resolution may not correctly
        // match std::string_view constructor, causing "Must give a string or char const*" error.
        // Use std::string explicitly to avoid type ambiguity.
        _add_element(std::string(slice.data, slice.size));
#else
        _add_element(std::string_view(slice.data, slice.size));
#endif
        return {};
    }

    Status do_visit(const StructColumn& col) {
        // Use indexed or unindexed object based on _unindexed_struct flag
        // unindexed=true preserves field insertion order (crucial for default values)
        if (_field_name.empty()) {
            _builder->openObject(_unindexed_struct);
        } else {
            _builder->add(_field_name, vpack::Value(vpack::ValueType::Object, _unindexed_struct));
        }
        const auto& names = col.field_names();
        const auto columns = col.fields();
        for (int i = 0; i < columns.size(); i++) {
            auto name = names.size() > i ? names[i] : fmt::format("k{}", i);
            auto& field_column = columns[i];
            RETURN_IF_ERROR(
                    cast_datum_to_json(field_column, _row, name, _builder, _unindexed_struct, _selected_map_nulls));
        }
        if (!_builder->isClosed()) {
            _builder->close();
        }

        return {};
    }

    Status do_visit(const MapColumn& col) {
        if (_field_name.empty()) {
            _builder->openObject(_unindexed_struct);
        } else {
            _builder->add(_field_name, vpack::Value(vpack::ValueType::Object, _unindexed_struct));
        }
        auto [map_start, map_size] = col.get_map_offset_size(_row);
        const auto& val_col = col.values_column();

        auto key_col = col.keys_column();
        if (_selected_map_nulls == nullptr ? key_col->has_null()
                                           : (_selected_map_nulls->contains(&col) && _selected_map_nulls->at(&col))) {
            return Status::NotSupported("key of Map should not be null");
        }
        if (key_col->is_nullable()) {
            key_col = ColumnHelper::as_column<NullableColumn>(key_col)->data_column();
        }

        for (int i = map_start; i < map_start + map_size; i++) {
            std::string name;
            if (key_col->is_binary()) {
                auto binary_col = ColumnHelper::as_column<BinaryColumn>(key_col);
                name = binary_col->get_slice(i);
            } else if (key_col->is_large_binary()) {
                auto binary_col = ColumnHelper::as_column<LargeBinaryColumn>(key_col);
                name = binary_col->get_slice(i);
            } else {
                // TODO(murphy) cast to string instead of debug
                name = key_col->debug_item(i);
            }

            // JSON doesn't support empty key, so just skip it
            if (name.empty()) {
                continue;
            }
            // VLOG(2) << "map key " << i << ": " << key_col->debug_item(i) << " , name=" << name;
            RETURN_IF_ERROR(cast_datum_to_json(val_col, i, name, _builder, _unindexed_struct, _selected_map_nulls));
        }

        if (!_builder->isClosed()) {
            _builder->close();
        }
        return {};
    }

    Status do_visit(const ArrayColumn& col) {
        if (_field_name.empty()) {
            _builder->openArray();
        } else {
            _builder->add(_field_name, vpack::Value(vpack::ValueType::Array));
        }

        auto [offset, size] = col.get_element_offset_size(_row);
        const auto& elements = col.elements_column();
        for (int i = offset; i < offset + size; i++) {
            RETURN_IF_ERROR(cast_datum_to_json(elements, i, "", _builder, _unindexed_struct, _selected_map_nulls));
        }

        if (!_builder->isClosed()) {
            _builder->close();
        }

        return {};
    }

    Status do_visit(const NullableColumn& col) {
        if (col.is_null(_row)) {
            _add_element(vpack::ValueType::Null);
        } else {
            RETURN_IF_ERROR(cast_datum_to_json(col.data_column(), _row, _field_name, _builder, _unindexed_struct,
                                               _selected_map_nulls));
        }
        return {};
    }

    // for type like hll and bitmap, right now only output NULL
    template <class T>
    Status do_visit(const ObjectColumn<T>& col) {
        _add_element(vpack::ValueType::Null);
        return {};
    }

    template <class ColumnType>
    Status do_visit(const ColumnType& _) {
        return Status::NotSupported("not supported");
    }

private:
    int _row;
    const std::string& _field_name;
    vpack::Builder* _builder;
    bool _unindexed_struct;
    const SelectedMapNulls* _selected_map_nulls;
};

// Cast nested type(including struct/map/* to json)
// TODO(murphy): optimize the performance with columnwise-casting
StatusOr<ColumnPtr> cast_nested_to_json(const ColumnPtr& column, bool allow_throw_exception) {
    ColumnBuilder<TYPE_JSON> column_builder(column->size());
    vpack::Builder json_builder;
    if (allow_throw_exception) {
        for (int row = 0; row < column->size(); row++) {
            if (column->is_null(row)) {
                column_builder.append_null();
                continue;
            }
            json_builder.clear();
            RETURN_IF_ERROR(CastColumnItemVisitor::cast_datum_to_json(column, row, "", &json_builder));
            JsonValue json(json_builder.slice());
            column_builder.append(std::move(json));
        }
    } else {
        for (int row = 0; row < column->size(); row++) {
            if (column->is_null(row)) {
                column_builder.append_null();
                continue;
            }
            json_builder.clear();
            auto st = CastColumnItemVisitor::cast_datum_to_json(column, row, "", &json_builder);
            if (!st.ok()) {
                column_builder.append_null();
                continue;
            }

            JsonValue json(json_builder.slice());
            column_builder.append(std::move(json));
        }
    }

    return column_builder.build(false);
}

StatusOr<ColumnPtr> cast_nested_to_json_selected(const SelectedColumn& input, size_t rows, bool allow_throw_exception) {
    std::vector<uint32_t> selected(rows);
    for (size_t i = 0; i < rows; ++i) selected[i] = input_row(input, i);
    SelectedMapNulls map_nulls;
    const ColumnPtr source = input.column->is_constant()
                                     ? down_cast<const ConstColumn*>(input.column.get())->data_column()
                                     : input.column;
    collect_selected_map_nulls(source, selected, &map_nulls);
    ColumnBuilder<TYPE_JSON> result(rows);
    vpack::Builder builder;
    for (uint32_t row : selected) {
        if (source->is_null(row)) {
            result.append_null();
            continue;
        }
        builder.clear();
        auto status = CastColumnItemVisitor::cast_datum_to_json(source, row, "", &builder, false, &map_nulls);
        if (!status.ok()) {
            if (allow_throw_exception) return status;
            result.append_null();
        } else {
            result.append(JsonValue(builder.slice()));
        }
    }
    return result.build(false);
}

StatusOr<std::string> cast_type_to_json_str(const ColumnPtr& column, int idx, bool unindexed_struct) {
    vpack::Builder json_builder;
    json_builder.clear();
    RETURN_IF_ERROR(CastColumnItemVisitor::cast_datum_to_json(column, idx, "", &json_builder, unindexed_struct));

    auto slice = json_builder.slice();
    JsonValue json(slice);
    auto result = json.to_string();
    return result;
}

} // namespace starrocks