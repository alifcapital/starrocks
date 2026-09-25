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

#include "column/array_view_column.h"
#include "exprs/column_ref.h"
#include "exprs/selected_column.h"

namespace starrocks {

inline StatusOr<SelectedColumn> selected_expression_argument(Expr* expression, ExprContext* context, Chunk* chunk,
                                                             const std::vector<uint32_t>& rows) {
    if (dynamic_cast<const ColumnRef*>(expression) != nullptr) {
        ASSIGN_OR_RETURN(auto column, expression->evaluate_checked(context, chunk));
        if (!column->is_view() && !column->is_array_view()) return SelectedColumn{std::move(column), &rows};
    }
    ASSIGN_OR_RETURN(auto column, expression->evaluate_selected(context, chunk, rows));
    if (column->is_array_view()) column = ArrayViewColumn::to_array_column(column);
    return SelectedColumn{std::move(column), nullptr};
}

} // namespace starrocks
