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

#include "column/column.h"
#include "common/statusor.h"
#include "exprs/selected_column.h"
#include "function_helper.h"
#include "types/logical_type.h"

namespace starrocks {

class VariantFunctions {
public:
    /**
     * @param: [variant, path]
     * @paramType: [VariantColumn, BinaryColumn]
     * @return: VariantColumn
     */
    DEFINE_VECTORIZED_FN(variant_query);
    static StatusOr<ColumnPtr> variant_query_selected(FunctionContext*, const SelectedColumns&, size_t);

    /**
     *
     * @param [variant, json_path]
     * @paramType: [VariantColumn, BinaryColumn]
     * @return : ResultTypeColumn
     */
    DEFINE_VECTORIZED_FN(get_variant_bool);
    static StatusOr<ColumnPtr> get_variant_bool_selected(FunctionContext*, const SelectedColumns&, size_t);
    // return bigint to unify all integer types
    DEFINE_VECTORIZED_FN(get_variant_int);
    static StatusOr<ColumnPtr> get_variant_int_selected(FunctionContext*, const SelectedColumns&, size_t);
    DEFINE_VECTORIZED_FN(get_variant_double);
    static StatusOr<ColumnPtr> get_variant_double_selected(FunctionContext*, const SelectedColumns&, size_t);
    DEFINE_VECTORIZED_FN(get_variant_string);
    static StatusOr<ColumnPtr> get_variant_string_selected(FunctionContext*, const SelectedColumns&, size_t);
    DEFINE_VECTORIZED_FN(get_variant_date);
    static StatusOr<ColumnPtr> get_variant_date_selected(FunctionContext*, const SelectedColumns&, size_t);
    DEFINE_VECTORIZED_FN(get_variant_datetime);
    static StatusOr<ColumnPtr> get_variant_datetime_selected(FunctionContext*, const SelectedColumns&, size_t);
    DEFINE_VECTORIZED_FN(get_variant_time);
    static StatusOr<ColumnPtr> get_variant_time_selected(FunctionContext*, const SelectedColumns&, size_t);

    /**
     * @param: [variant, path]
     * @paramType: [VariantColumn, BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(variant_typeof);
    static StatusOr<ColumnPtr> variant_typeof_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> variant_typeof_impl(FunctionContext*, const Inputs&);

    // Preload the variant segments if necessary.
    // This function is called once per query execution
    // The scope indicates whether the state is shared across the plan fragment
    // (FRAGMENT_LOCAL) or local to the execution thread (THREAD_LOCAL).
    // Returns Status::OK() on success, or an error status if initialization fails.
    static Status variant_segments_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // Clear the variant segments state.
    static Status variant_segments_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

private:
    template <LogicalType ResultType, typename Inputs>
    static StatusOr<ColumnPtr> _do_variant_query(FunctionContext* context, const Inputs& vector);
};

} // namespace starrocks
