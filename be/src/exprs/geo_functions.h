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

#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "exprs/selected_column.h"
#include "geo/geo_common.h"

namespace starrocks {

class GeoFunctions {
public:
    /**
     * @param: [double_value, double_value]
     * @paramType: [DoubleColumn, DoubleColumn]
     * @return: BinaryColumn
     * Through the given X coordinate value, y coordinate value returns the corresponding point.
     */
    DEFINE_VECTORIZED_FN(st_point);
    static StatusOr<ColumnPtr> st_point_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_point_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: DoubleColumn
     * Returns the corresponding X coordinate value
     */
    DEFINE_VECTORIZED_FN(st_x);
    static StatusOr<ColumnPtr> st_x_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_x_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: DoubleColumn
     * Returns the corresponding y coordinate value
     */
    DEFINE_VECTORIZED_FN(st_y);
    static StatusOr<ColumnPtr> st_y_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_y_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [double_value, double_value, double_value, double_value]
     * @paramType: [DoubleColumn, DoubleColumn, DoubleColumn, DoubleColumn]
     * @return: DoubleColumn
     * Calculate the spherical distance between two points on the earth, in meters.
     */
    DEFINE_VECTORIZED_FN(st_distance_sphere);
    static StatusOr<ColumnPtr> st_distance_sphere_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_distance_sphere_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     * Transfer this Text to Well Known Text.
     */
    DEFINE_VECTORIZED_FN(st_as_wkt);
    static StatusOr<ColumnPtr> st_as_wkt_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_as_wkt_impl(FunctionContext*, const Inputs&);

    // from wkt
    static Status st_from_wkt_prepare_common(FunctionContext*, FunctionContext::FunctionStateScope,
                                             GeoShapeType shape_type);

    static Status st_from_wkt_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope)
            __attribute__((used));

    static Status st_line_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope)
            __attribute__((used));

    static Status st_polygon_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope)
            __attribute__((used));

    static Status st_from_wkt_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    template <typename Inputs>
    static StatusOr<ColumnPtr> st_from_wkt_common(FunctionContext* ctx, const Inputs& columns, GeoShapeType shape_type);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     * Convert a wkt (well known text) into the corresponding memory geometry
     */
    DEFINE_VECTORIZED_FN(st_from_wkt);
    static StatusOr<ColumnPtr> st_from_wkt_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_from_wkt_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     * Convert a wkt (well known text) into a memory representation of line form
     */
    DEFINE_VECTORIZED_FN(st_line);
    static StatusOr<ColumnPtr> st_line_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_line_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [string_value]
     * @paramType: [BinaryColumn]
     * @return: BinaryColumn
     * Convert a wkt (well known text) to the corresponding polygon memory form
     */
    DEFINE_VECTORIZED_FN(st_polygon);
    static StatusOr<ColumnPtr> st_polygon_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_polygon_impl(FunctionContext*, const Inputs&);

    static Status st_circle_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

    /**
     * @param: [double_value, double_value, double_value]
     * @paramType: [DoubleColumn, DoubleColumn, DoubleColumn]
     * @return: BinaryColumn
     * Transfer this WKT to a circle on earth's surface.
     */
    DEFINE_VECTORIZED_FN(st_circle);
    static StatusOr<ColumnPtr> st_circle_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_circle_impl(FunctionContext*, const Inputs&);

    static Status st_contains_close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope);

    static Status st_contains_prepare(FunctionContext*, FunctionContext::FunctionStateScope);

    /**
     * @param: [string_value, string_value]
     * @paramType: [DoubleColumn, DoubleColumn]
     * @return: BinaryColumn
     * Judge whether shape1(columns[0]) can completely contain shape2(columns2)
     */
    DEFINE_VECTORIZED_FN(st_contains);
    static StatusOr<ColumnPtr> st_contains_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> st_contains_impl(FunctionContext*, const Inputs&);
};

} // namespace starrocks
