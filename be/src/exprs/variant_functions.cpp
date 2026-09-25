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

#include "exprs/variant_functions.h"

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "exprs/selected_column.h"
#include "runtime/runtime_state.h"
#include "util/variant_converter.h"
#include "variant_path_parser.h"

namespace starrocks {

StatusOr<ColumnPtr> VariantFunctions::variant_query(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_VARIANT>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_string(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_VARCHAR>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_int(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_BIGINT>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_bool(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_BOOLEAN>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_double(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DOUBLE>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_date(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DATE>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_datetime(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_DATETIME>(context, columns);
}

StatusOr<ColumnPtr> VariantFunctions::get_variant_time(FunctionContext* context, const Columns& columns) {
    return _do_variant_query<TYPE_TIME>(context, columns);
}

Status VariantFunctions::variant_segments_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope != FunctionContext::FRAGMENT_LOCAL) {
        return Status::OK();
    }

    // Don't parse if the path is not constant
    if (!context->is_notnull_constant_column(1)) {
        return Status::OK();
    }

    const auto path_col = context->get_constant_column(1);
    const Slice variant_path = ColumnHelper::get_const_value<TYPE_VARCHAR>(path_col);

    std::string path_string = variant_path.to_string();
    auto variant_path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!variant_path_status.ok(), variant_path_status.status());
    auto* path_state = new VariantState();
    path_state->variant_path.reset(std::move(variant_path_status.value()));
    context->set_function_state(scope, path_state);
    VLOG(10) << "Preloaded variant path: " << path_string;

    return Status::OK();
}

static StatusOr<VariantPath*> get_or_parse_variant_segments(FunctionContext* context, const Slice path_slice,
                                                            VariantPath* variant_path) {
    auto* cached = reinterpret_cast<VariantState*>(context->get_function_state(FunctionContext::FRAGMENT_LOCAL));

    if (cached != nullptr) {
        // If we already have parsed segments, return them
        return &cached->variant_path;
    }

    std::string path_string = path_slice.to_string();
    auto path_status = VariantPathParser::parse(path_string);
    RETURN_IF(!path_status.ok(), path_status.status());
    variant_path->reset(std::move(path_status.value()));

    return variant_path;
}

Status VariantFunctions::variant_segments_close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        auto* variant_state = reinterpret_cast<VariantState*>(context->get_function_state(scope));
        delete variant_state;
    }

    return Status::OK();
}

template <LogicalType ResultType, typename Inputs>
StatusOr<ColumnPtr> VariantFunctions::_do_variant_query(FunctionContext* context, const Inputs& columns) {
    for (const auto& input : columns) {
        if (input_column(input)->only_null()) return ColumnHelper::create_const_null_column(input_num_rows(columns));
    }
    if (columns.size() != 2) {
        return Status::InvalidArgument("Variant query functions requires 2 arguments");
    }

    size_t num_rows = input_num_rows(columns);

    auto variant_viewer = FunctionColumnViewer<TYPE_VARIANT, Inputs>(columns[0]);
    auto json_path_viewer = FunctionColumnViewer<TYPE_VARCHAR, Inputs>(columns[1]);

    ColumnBuilder<ResultType> result(num_rows);
    VariantPath stored_path;
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row) || json_path_viewer.is_null(row)) {
            result.append_null();
            continue;
        }

        auto path_slice = json_path_viewer.value(row);
        auto variant_segments_status = get_or_parse_variant_segments(context, path_slice, &stored_path);
        if (!variant_segments_status.ok()) {
            result.append_null();
            continue;
        }

        const VariantRowValue* variant = variant_viewer.value(row);
        if (variant == nullptr) {
            result.append_null();
            continue;
        }

        auto field = VariantPath::seek(variant, variant_segments_status.value());
        if (!field.ok()) {
            // If seek fails (e.g., path not found), append null
            result.append_null();
            continue;
        }

        if constexpr (ResultType == TYPE_VARIANT) {
            result.append(std::move(field.value()));
        } else {
            const RuntimeState* state = context->state();
            cctz::time_zone zone = (state == nullptr) ? cctz::local_time_zone() : context->state()->timezone_obj();
            Status casted = cast_variant_value_to<ResultType, true>(field.value(), zone, result);
            // Append null if casting fails
            if (!casted.ok()) {
                result.append_null();
            }
        }
    }

    return result.build(input_columns_are_constant(columns));
}

template <typename Inputs>
StatusOr<ColumnPtr> VariantFunctions::variant_typeof_impl(FunctionContext* context, const Inputs& columns) {
    const auto& variant_column = input_column(columns[0]);
    auto variant_viewer = FunctionColumnViewer<TYPE_VARIANT, Inputs>(columns[0]);
    size_t num_rows = input_num_rows(columns);

    ColumnBuilder<TYPE_VARCHAR> result(num_rows);
    for (size_t row = 0; row < num_rows; ++row) {
        if (variant_viewer.is_null(row)) {
            result.append_null();
            continue;
        }
        const VariantRowValue* variant = variant_viewer.value(row);
        if (variant == nullptr) {
            result.append_null();
            continue;
        }
        result.append(VariantUtil::variant_type_to_string(variant->get_value().type()));
    }
    return result.build(input_columns_are_constant(columns));
}

StatusOr<ColumnPtr> VariantFunctions::variant_query_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return _do_variant_query<TYPE_VARIANT>(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::get_variant_string_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return _do_variant_query<TYPE_VARCHAR>(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::get_variant_int_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return _do_variant_query<TYPE_BIGINT>(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::get_variant_bool_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return _do_variant_query<TYPE_BOOLEAN>(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::get_variant_double_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return _do_variant_query<TYPE_DOUBLE>(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::get_variant_date_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return _do_variant_query<TYPE_DATE>(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::get_variant_datetime_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return _do_variant_query<TYPE_DATETIME>(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::get_variant_time_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return _do_variant_query<TYPE_TIME>(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::variant_typeof(FunctionContext* context, const Columns& columns) {
    return variant_typeof_impl(context, columns);
}
StatusOr<ColumnPtr> VariantFunctions::variant_typeof_selected(FunctionContext* context, const SelectedColumns& columns, size_t) {
    return variant_typeof_impl(context, columns);
}

} // namespace starrocks

#include "gen_cpp/opcode/VariantFunctions.inc"
