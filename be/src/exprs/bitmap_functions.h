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

#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "exprs/selected_column.h"

namespace starrocks {
class BitmapFunctions {
public:
    /**
     * @param: 
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    template <LogicalType LT>
    static StatusOr<ColumnPtr> to_bitmap(FunctionContext* context, const starrocks::Columns& columns);
    template <LogicalType LT>
    static StatusOr<ColumnPtr> to_bitmap_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <LogicalType LT, typename Inputs>
    static StatusOr<ColumnPtr> to_bitmap_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_hash);
    static StatusOr<ColumnPtr> bitmap_hash_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_hash_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_hash64);
    static StatusOr<ColumnPtr> bitmap_hash64_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_hash64_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_BIGINT
     */
    DEFINE_VECTORIZED_FN(bitmap_count);
    static StatusOr<ColumnPtr> bitmap_count_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_count_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: []
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_empty);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_or);
    static StatusOr<ColumnPtr> bitmap_or_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_or_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_and);
    static StatusOr<ColumnPtr> bitmap_and_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_and_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_VARCHAR
     */
    DEFINE_VECTORIZED_FN(bitmap_to_string);
    static StatusOr<ColumnPtr> bitmap_to_string_selected(FunctionContext*, const SelectedColumns&, size_t);

    /**
     * @param: 
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_from_string);
    static StatusOr<ColumnPtr> bitmap_from_string_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_from_string_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT]
     * @return TYPE_BOOLEAN
     */
    DEFINE_VECTORIZED_FN(bitmap_contains);
    static StatusOr<ColumnPtr> bitmap_contains_selected(FunctionContext*, const SelectedColumns&, size_t);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_BOOLEAN
     */
    DEFINE_VECTORIZED_FN(bitmap_has_any);
    static StatusOr<ColumnPtr> bitmap_has_any_selected(FunctionContext*, const SelectedColumns&, size_t);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_andnot);
    static StatusOr<ColumnPtr> bitmap_andnot_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_andnot_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_xor);
    static StatusOr<ColumnPtr> bitmap_xor_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_xor_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT]
     * @return TYPE_BOOLEAN
     */
    DEFINE_VECTORIZED_FN(bitmap_remove);
    static StatusOr<ColumnPtr> bitmap_remove_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_remove_impl(FunctionContext*, const Inputs&);

    /**
     * @param: 
     * @paramType columns: [TYPE_OBJECT]
     * @return ARRAY_BIGINT
     */
    DEFINE_VECTORIZED_FN(bitmap_to_array);
    static StatusOr<ColumnPtr> bitmap_to_array_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_to_array_impl(FunctionContext*, const Inputs&);
    static void detect_bitmap_cardinality(size_t* data_size, const int64_t cardinality);

    /**
     * @param:
     * @paramType columns: [ARRAY_BIGINT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(array_to_bitmap);
    static StatusOr<ColumnPtr> array_to_bitmap_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> array_to_bitmap_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_LARGEINT
     */
    DEFINE_VECTORIZED_FN(bitmap_max);
    static StatusOr<ColumnPtr> bitmap_max_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_max_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_LARGEINT
     */
    DEFINE_VECTORIZED_FN(bitmap_min);
    static StatusOr<ColumnPtr> bitmap_min_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_min_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(base64_to_bitmap);
    static StatusOr<ColumnPtr> base64_to_bitmap_const(FunctionContext* context, const Columns& columns);
    static Status base64_to_bitmap_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status base64_to_bitmap_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static StatusOr<ColumnPtr> base64_to_bitmap_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> base64_to_bitmap_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT, TYPE_BIGINT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(sub_bitmap);
    static StatusOr<ColumnPtr> sub_bitmap_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> sub_bitmap_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_to_base64);
    static StatusOr<ColumnPtr> bitmap_to_base64_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_to_base64_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT, TYPE_BIGINT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_subset_in_range);
    static StatusOr<ColumnPtr> bitmap_subset_in_range_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_subset_in_range_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_OBJECT, TYPE_BIGINT, TYPE_BIGINT]
     * @return TYPE_OBJECT
     */
    DEFINE_VECTORIZED_FN(bitmap_subset_limit);
    static StatusOr<ColumnPtr> bitmap_subset_limit_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_subset_limit_impl(FunctionContext*, const Inputs&);

    /**
     * @param:
     * @paramType columns: [TYPE_BITMAP]
     * @return TYPE_VARCHAR
     */
    DEFINE_VECTORIZED_FN(bitmap_to_binary);
    static StatusOr<ColumnPtr> bitmap_to_binary_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_to_binary_impl(FunctionContext*, const Inputs&);

    /**
     * @param
     * @paramType columns: [TYPE_VARCHAR]
     * @return TYPE_BITMAP
     */
    DEFINE_VECTORIZED_FN(bitmap_from_binary);
    static StatusOr<ColumnPtr> bitmap_from_binary_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> bitmap_from_binary_impl(FunctionContext*, const Inputs&);
};

} // namespace starrocks
