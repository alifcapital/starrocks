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

#include "column/array_column.h"
#include "column/array_view_column.h"
#include "column/map_column.h"
#include "exprs/selected_column.h"
#include "exprs/selected_expr.h"

namespace starrocks {

// Hash/equality retain each element's original row coordinate, including nested values.
struct CollectionElementHash {
    const Column* column;
    size_t operator()(uint32_t row) const {
        uint32_t hash = 0;
        column->fnv_hash_at(&hash, row);
        return hash;
    }
};
struct CollectionElementEqual {
    const Column* column;
    bool operator()(uint32_t lhs, uint32_t rhs) const { return column->equals(lhs, *column, rhs); }
};

// Filter collection elements directly from the original storage. Only the final result is copied.
// Null filter rows produce empty collections; collection nulls are preserved.
template <typename Collection>
ColumnPtr filter_selected_collection(const SelectedColumns& inputs, size_t rows) {
    const auto& input = inputs[0];
    const auto& filter = inputs[1];
    if (input.column->only_null()) return ColumnHelper::create_const_null_column(rows);
    const auto* source = down_cast<const Collection*>(ColumnHelper::get_data_column(input.column.get()));
    auto result = Collection::static_pointer_cast(source->clone_empty());
    auto nulls = input_null_flags(input, rows);
    const ArrayColumn* predicates =
            filter.column->only_null()
                    ? nullptr
                    : down_cast<const ArrayColumn*>(ColumnHelper::get_data_column(filter.column.get()));
    std::vector<uint32_t> selected;
    const auto& offsets = source->offsets().get_data();
    for (size_t row = 0; row < rows; ++row) {
        size_t src = input_row(input, row);
        size_t pred = input_row(filter, row);
        if (!nulls->get_data()[row] && predicates != nullptr && !filter.column->is_null(pred)) {
            const auto& filter_offsets = predicates->offsets().get_data();
            size_t end = std::min(offsets[src + 1] - offsets[src], filter_offsets[pred + 1] - filter_offsets[pred]);
            ColumnViewer<TYPE_BOOLEAN> values(predicates->elements_column());
            for (size_t i = 0; i < end; ++i) {
                size_t index = filter_offsets[pred] + i;
                if (!values.is_null(index) && values.value(index)) selected.push_back(offsets[src] + i);
            }
        }
        result->offsets_column_raw_ptr()->append(selected.size());
    }
    if constexpr (std::is_same_v<Collection, ArrayColumn>) {
        result->elements_column_raw_ptr()->append_selective(source->elements(), selected);
    } else {
        result->keys_column_raw_ptr()->append_selective(source->keys(), selected);
        result->values_column_raw_ptr()->append_selective(source->values(), selected);
    }
    if (input.column->is_nullable()) return NullableColumn::create(std::move(result), std::move(nulls));
    return result;
}

} // namespace starrocks
