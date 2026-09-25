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

#include <cstdint>
#include <type_traits>
#include <vector>

#include "column/column_helper.h"
#include "column/column_viewer.h"

namespace starrocks {

// Input ownership stays with the call. A null selection denotes an already compact argument.
struct SelectedColumn {
    ColumnPtr column;
    const std::vector<uint32_t>* rows = nullptr;
};
using SelectedColumns = std::vector<SelectedColumn>;

template <LogicalType Type>
class SelectedColumnViewer {
public:
    explicit SelectedColumnViewer(const SelectedColumn& input)
            : _viewer(input.column),
              _rows(input.rows),
              _size(input.rows == nullptr ? input.column->size() : input.rows->size()) {}
    size_t size() const { return _size; }
    auto value(size_t row) const { return _viewer.value(source_row(row)); }
    bool is_null(size_t row) const { return _viewer.is_null(source_row(row)); }

private:
    size_t source_row(size_t row) const { return _rows == nullptr ? row : (*_rows)[row]; }
    ColumnViewer<Type> _viewer;
    const std::vector<uint32_t>* _rows;
    size_t _size;
};

inline bool selected_columns_are_constant(const SelectedColumns& columns) {
    for (const auto& input : columns) {
        if (!input.column->is_constant()) return false;
    }
    return true;
}

inline size_t input_row(const ColumnPtr& input, size_t row) {
    return input->is_constant() ? 0 : row;
}
inline size_t input_row(const SelectedColumn& input, size_t row) {
    return input.column->is_constant() ? 0 : (input.rows == nullptr ? row : (*input.rows)[row]);
}
inline NullColumn::MutablePtr input_null_flags(const ColumnPtr& input, size_t rows) {
    if (!input->is_constant() && input->is_nullable()) {
        return NullColumn::static_pointer_cast(down_cast<const NullableColumn*>(input.get())->null_column()->clone());
    }
    return NullColumn::create(rows, rows != 0 && input->is_null(0));
}
inline NullColumn::MutablePtr input_null_flags(const SelectedColumn& input, size_t rows) {
    auto result = NullColumn::create(rows, 0);
    for (size_t row = 0; row < rows; ++row) result->get_data()[row] = input.column->is_null(input_row(input, row));
    return result;
}

// Compile ordinary kernels with their original viewer; only selected calls pay for indirection.
template <LogicalType Type, typename Inputs>
using FunctionColumnViewer =
        std::conditional_t<std::is_same_v<Inputs, Columns>, ColumnViewer<Type>, SelectedColumnViewer<Type>>;

inline const ColumnPtr& input_column(const ColumnPtr& input) {
    return input;
}
inline const ColumnPtr& input_column(const SelectedColumn& input) {
    return input.column;
}
inline size_t input_num_rows(const Columns& inputs) {
    return inputs[0]->size();
}
inline size_t input_num_rows(const SelectedColumns& inputs) {
    return inputs[0].rows == nullptr ? inputs[0].column->size() : inputs[0].rows->size();
}
inline bool input_columns_are_constant(const Columns& inputs) {
    return ColumnHelper::is_all_const(inputs);
}
inline bool input_columns_are_constant(const SelectedColumns& inputs) {
    return selected_columns_are_constant(inputs);
}

inline auto input_num_packed_rows(const Columns& inputs) {
    return ColumnHelper::num_packed_rows(inputs);
}
inline auto input_num_packed_rows(const SelectedColumns& inputs) {
    return std::pair(selected_columns_are_constant(inputs), input_num_rows(inputs));
}

template <typename Inputs>
inline typename Inputs::value_type compact_input(ColumnPtr column) {
    if constexpr (std::is_same_v<Inputs, Columns>)
        return column;
    else
        return {std::move(column), nullptr};
}

// For kernels that require contiguous storage, gather arguments without reevaluating children.
inline Columns materialize_selected_inputs(const SelectedColumns& inputs) {
    Columns result;
    result.reserve(inputs.size());
    for (const auto& input : inputs) {
        if (input.rows == nullptr) {
            result.emplace_back(input.column);
        } else {
            auto compact = input.column->clone_empty();
            compact->append_selective(*input.column, input.rows->data(), 0, input.rows->size());
            result.emplace_back(std::move(compact));
        }
    }
    return result;
}

} // namespace starrocks
