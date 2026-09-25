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

#include "column/column_builder.h"
#include "exprs/selected_column.h"

namespace starrocks {

// Share the existing strict scalar operation; only input addressing differs from the dense loop.
template <LogicalType InputType, LogicalType ResultType, typename Op>
ColumnPtr evaluate_selected_strict_unary(const SelectedColumns& inputs, size_t rows) {
    SelectedColumnViewer<InputType> input(inputs[0]);
    ColumnBuilder<ResultType> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (input.is_null(row)) {
            result.append_null();
        } else {
            auto value = Op::template apply<RunTimeCppType<InputType>, RunTimeCppType<ResultType>>(input.value(row));
            if constexpr (lt_is_string<ResultType>)
                result.append(Slice(value));
            else
                result.append(value);
        }
    }
    return result.build(selected_columns_are_constant(inputs));
}

template <LogicalType LeftType, LogicalType RightType, LogicalType ResultType, typename Op>
ColumnPtr evaluate_selected_strict_binary(const SelectedColumns& inputs, size_t rows) {
    SelectedColumnViewer<LeftType> left(inputs[0]);
    SelectedColumnViewer<RightType> right(inputs[1]);
    ColumnBuilder<ResultType> result(rows);
    for (size_t row = 0; row < rows; ++row) {
        if (left.is_null(row) || right.is_null(row))
            result.append_null();
        else
            result.append(
                    Op::template apply<RunTimeCppType<LeftType>, RunTimeCppType<RightType>, RunTimeCppType<ResultType>>(
                            left.value(row), right.value(row)));
    }
    return result.build(selected_columns_are_constant(inputs));
}

} // namespace starrocks
