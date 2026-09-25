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

#include <gtest/gtest.h>

#include "util/defer_op.h"
#include "column/array_column.h"
#include "column/column_builder.h"
#include "column/json_column.h"
#include "column/map_column.h"
#include "common/config.h"
#include "exprs/array_functions.h"
#include "exprs/bitmap_functions.h"
#include "exprs/encryption_functions.h"
#include "exprs/hyperloglog_functions.h"
#include "exprs/json_functions.h"
#include "exprs/like_predicate.h"
#include "exprs/map_functions.h"
#include "exprs/percentile_functions.h"
#include "exprs/selected_column.h"
#include "exprs/string_functions.h"
#include "exprs/time_functions.h"
#include "runtime/runtime_state.h"
#include "util/percentile_value.h"
#include "util/json_flattener.h"

namespace starrocks {
namespace {
ColumnPtr strings(std::initializer_list<const char*> values) {
    ColumnBuilder<TYPE_VARCHAR> result(values.size());
    for (const char* value : values) {
        if (value == nullptr)
            result.append_null();
        else
            result.append(Slice(value));
    }
    return result.build(false);
}

using DenseFn = StatusOr<ColumnPtr> (*)(FunctionContext*, const Columns&);
using SelectedFn = StatusOr<ColumnPtr> (*)(FunctionContext*, const SelectedColumns&, size_t);

// Compare with the original kernel on an independently gathered subset. Include repeated and
// out-of-order rows: selection is addressing, not permission to reorder or mutate the input.
void compare(FunctionContext* ctx, DenseFn dense, SelectedFn selected, const Columns& source) {
    std::vector<uint32_t> rows{4, 1, 0, 4};
    SelectedColumns mapped;
    Columns gathered;
    std::vector<std::string> before;
    for (const auto& column : source) {
        mapped.push_back({column, &rows});
        auto copy = column->clone_empty();
        copy->append_selective(*column, rows.data(), 0, rows.size());
        gathered.emplace_back(std::move(copy));
        before.emplace_back(column->debug_string());
    }
    auto reference = dense(ctx, gathered);
    ASSERT_TRUE(reference.ok()) << reference.status();
    auto actual = selected(ctx, mapped, rows.size());
    ASSERT_TRUE(actual.ok()) << actual.status();
    ASSERT_EQ(rows.size(), actual.value()->size());
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(reference.value()->debug_item(i), actual.value()->debug_item(i)) << i;
    }
    // A computed child is compact while a slot child still addresses the original column.
    if (source.size() > 1) {
        mapped.back() = {gathered.back(), nullptr};
        auto mixed = selected(ctx, mapped, rows.size());
        ASSERT_TRUE(mixed.ok()) << mixed.status();
        for (size_t i = 0; i < rows.size(); ++i)
            EXPECT_EQ(reference.value()->debug_item(i), mixed.value()->debug_item(i)) << i;
    }
    for (size_t i = 0; i < source.size(); ++i) EXPECT_EQ(before[i], source[i]->debug_string());
}
} // namespace

class SelectedCollectionFunctionsTest : public ::testing::Test {
protected:
    void SetUp() override {
        _base64_limit = config::max_length_for_to_base64;
        _bitmap_limit = config::max_length_for_bitmap_function;
        config::max_length_for_to_base64 = 200000;
        config::max_length_for_bitmap_function = 1000000;
    }
    void TearDown() override {
        config::max_length_for_to_base64 = _base64_limit;
        config::max_length_for_bitmap_function = _bitmap_limit;
    }

private:
    int64_t _base64_limit;
    int64_t _bitmap_limit;
};

namespace {
ColumnPtr collection_arrays(const std::vector<std::vector<int32_t>>& values, bool null_second = true) {
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    offsets->append(0);
    for (const auto& row : values) {
        for (auto value : row) {
            if (value == INT32_MIN)
                elements->append_nulls(1);
            else
                elements->append_datum(Datum(value));
        }
        offsets->append(elements->size());
    }
    auto nulls = NullColumn::create(values.size(), 0);
    if (null_second) nulls->get_data()[1] = 1;
    return NullableColumn::create(ArrayColumn::create(std::move(elements), std::move(offsets)), std::move(nulls));
}
} // namespace

TEST_F(SelectedCollectionFunctionsTest, CollectionSortingAndIntersection) {
    RuntimeState runtime;
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ctx->set_runtime_state(&runtime);
    auto input = collection_arrays({{3, 1, 3, INT32_MIN}, {8, 7}, {6}, {}, {9, 2, 8}});
    auto keys = collection_arrays({{2, 1, 2, 0}, {1, 2}, {4, 3, 2}, {}, {1, 1, 0}}, false);
    auto keys2 = collection_arrays({{8, 7, 6, 5}, {0, 0}, {0}, {}, {7, 3, 1}}, false);
    compare(ctx.get(), ArrayFunctions::array_sort<TYPE_INT>, ArrayFunctions::array_sort_selected<TYPE_INT>, {input});
    compare(ctx.get(), ArrayFunctions::array_sortby<TYPE_INT>, ArrayFunctions::array_sortby_selected<TYPE_INT>,
            {input, keys});
    compare(ctx.get(), ArrayFunctions::array_sortby_multi, ArrayFunctions::array_sortby_multi_selected,
            {input, keys, keys2});
    compare(ctx.get(), ArrayFunctions::array_intersect<TYPE_INT>, ArrayFunctions::array_intersect_selected<TYPE_INT>,
            {input, keys});
    compare(ctx.get(), ArrayFunctions::array_distinct_any_type, ArrayFunctions::array_distinct_any_type_selected,
            {input});
    auto null_key = collection_arrays({{2, 1, 2, 0}, {1, 2}, {4}, {}, {1, 1, 0}});
    compare(ctx.get(), ArrayFunctions::array_sortby_multi, ArrayFunctions::array_sortby_multi_selected,
            {keys2, null_key, input});
}

TEST_F(SelectedCollectionFunctionsTest, PercentileReadPreservesSource) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto values = PercentileColumn::create();
    for (int row = 0; row < 5; ++row) {
        PercentileValue value;
        for (int i = 0; i < row * 100; ++i) value.add(i, i % 3 + 1);
        if (row == 4) value.quantile(0.5);
        values->append(&value);
    }
    auto rate = ColumnHelper::create_const_column<TYPE_DOUBLE>(0.7, 5);
    auto bytes = [&]() {
        std::vector<std::vector<uint8_t>> result;
        for (size_t row = 0; row < values->size(); ++row) {
            const auto* value = values->get_object(row);
            result.emplace_back(value->serialize_size());
            value->serialize(result.back().data());
        }
        return result;
    };
    auto before = bytes();
    compare(ctx.get(), PercentileFunctions::percentile_approx_raw, PercentileFunctions::percentile_approx_raw_selected,
            {values, rate});
    EXPECT_EQ(before, bytes());
}

TEST_F(SelectedCollectionFunctionsTest, FlatJsonSelectedFields) {
    bool old_lazy = config::enable_lazy_dynamic_flat_json;
    DeferOp restore([&] { config::enable_lazy_dynamic_flat_json = old_lazy; });
    for (bool lazy : {false, true}) {
        config::enable_lazy_dynamic_flat_json = lazy;
        auto source = JsonColumn::create();
        for (const char* text : {R"({"a":{"v":1},"b":"large unused field"})", "{}", R"({"a":{"v":9}})", R"({"a":null})",
                                 R"({"a":{"v":5}})"}) {
            auto json = JsonValue::parse(text);
            ASSERT_TRUE(json.ok());
            source->append(&json.value());
        }
        JsonFlattener flattener({"a", "b"}, {TYPE_JSON, TYPE_VARCHAR}, false);
        flattener.flatten(source.get());
        auto flat = JsonColumn::create();
        flat->set_flat_columns({"a", "b"}, {TYPE_JSON, TYPE_VARCHAR}, flattener.mutable_result());
        for (auto [dense, selected] : std::vector<std::pair<DenseFn, SelectedFn>>{
                     {JsonFunctions::json_query, JsonFunctions::json_query_selected},
                     {JsonFunctions::json_exists, JsonFunctions::json_exists_selected},
                     {JsonFunctions::json_length, JsonFunctions::json_length_selected},
                     {JsonFunctions::json_keys, JsonFunctions::json_keys_selected}}) {
            std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
            Columns input{flat, ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("$.a.v"), 5)};
            ctx->set_constant_columns(input);
            ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
            compare(ctx.get(), dense, selected, input);
            ASSERT_TRUE(JsonFunctions::native_json_path_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
        }
    }
}

TEST_F(SelectedCollectionFunctionsTest, HyperscanScatteredBuffers) {
    RuntimeState runtime;
    auto& options = const_cast<TQueryOptions&>(runtime.query_options());
    options.__set_enable_hyperscan_vec(true);
    for (const char* pattern : {"ab", "-"}) {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ctx->set_runtime_state(&runtime);
        // Selected order includes both a boundary-spanning match and within-row matches.
        Columns input{strings({"bab", nullptr, "unselected", "", "a"}),
                      ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(pattern), 5),
                      ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("#"), 5)};
        ctx->set_constant_columns(input);
        ASSERT_TRUE(StringFunctions::regexp_replace_prepare(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
        compare(ctx.get(), StringFunctions::regexp_replace, StringFunctions::regexp_replace_selected, input);
        ASSERT_TRUE(StringFunctions::regexp_close(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
    }
}

TEST_F(SelectedCollectionFunctionsTest, MapFiltersAndJsonNullKeyScope) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    for (bool selected_null_key : {false, true}) {
        auto keys = strings({"a", "b", nullptr, "d", selected_null_key ? nullptr : "e"});
        auto vals = strings({"v0", "v1", "bad-unselected", "v3", "v4"});
        auto offsets = UInt32Column::create();
        for (uint32_t i = 0; i <= 5; ++i) offsets->append(i);
        auto maps = MapColumn::create(std::move(*keys).mutate(), std::move(*vals).mutate(), std::move(offsets));
        compare(ctx.get(), JsonFunctions::to_json, JsonFunctions::to_json_selected, {maps});
        compare(ctx.get(), MapFunctions::distinct_map_keys, MapFunctions::distinct_map_keys_selected, {maps});
        auto flags = BooleanColumn::create();
        auto filter_offsets = UInt32Column::create();
        filter_offsets->append(0);
        for (uint32_t i = 0; i < 5; ++i) {
            flags->append(i % 2);
            filter_offsets->append(i + 1);
        }
        auto filter = ArrayColumn::create(NullableColumn::create(std::move(flags), NullColumn::create(5, 0)),
                                          std::move(filter_offsets));
        compare(ctx.get(), MapFunctions::map_filter, MapFunctions::map_filter_selected, {maps, filter});
        auto arrays = collection_arrays({{3, 1}, {8}, {6}, {}, {9, 2}});
        compare(ctx.get(), ArrayFunctions::array_filter, ArrayFunctions::array_filter_selected, {arrays, filter});
    }
}

TEST_F(SelectedCollectionFunctionsTest, ConstantPercentileVariableQuantile) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    PercentileValue value;
    for (int i = 0; i < 100; ++i) value.add(i);
    auto digest = ColumnHelper::create_const_column<TYPE_PERCENTILE>(&value, 5);
    ColumnBuilder<TYPE_DOUBLE> rates(5);
    rates.append(0.0);
    rates.append_null();
    rates.append(0.25);
    rates.append(0.5);
    rates.append(1.0);
    auto quantiles = rates.build(false);
    auto source_bytes = [&]() {
        const auto* stored = ColumnHelper::get_const_value<TYPE_PERCENTILE>(digest);
        std::vector<uint8_t> bytes(stored->serialize_size());
        stored->serialize(bytes.data());
        return bytes;
    };
    auto before = source_bytes();
    auto selected = PercentileFunctions::percentile_approx_raw_selected(
            ctx.get(), {{digest, nullptr}, {quantiles, nullptr}}, 5);
    ASSERT_TRUE(selected.ok()) << selected.status();
    EXPECT_FALSE(selected.value()->is_constant());
    EXPECT_DOUBLE_EQ(0.0, selected.value()->get(0).get_double());
    EXPECT_TRUE(selected.value()->is_null(1));
    EXPECT_DOUBLE_EQ(99.0, selected.value()->get(4).get_double());
    auto rate = ColumnHelper::create_const_column<TYPE_DOUBLE>(1.0, 5);
    auto constant = PercentileFunctions::percentile_approx_raw_selected(
            ctx.get(), {{digest, nullptr}, {rate, nullptr}}, 5);
    ASSERT_TRUE(constant.ok()) << constant.status();
    EXPECT_TRUE(constant.value()->is_constant());
    EXPECT_EQ(5, constant.value()->size());
    EXPECT_DOUBLE_EQ(99.0, constant.value()->get(4).get_double());
    EXPECT_EQ(before, source_bytes());
    auto result = PercentileFunctions::percentile_approx_raw(ctx.get(), {digest, quantiles});
    ASSERT_TRUE(result.ok());
    ASSERT_FALSE(result.value()->is_constant());
    EXPECT_TRUE(result.value()->is_null(1));
    EXPECT_DOUBLE_EQ(0.0, result.value()->get(0).get_double());
    EXPECT_DOUBLE_EQ(99.0, result.value()->get(4).get_double());
    compare(ctx.get(), PercentileFunctions::percentile_approx_raw, PercentileFunctions::percentile_approx_raw_selected,
            {digest, quantiles});
}

TEST_F(SelectedCollectionFunctionsTest, HyperscanCrossRowMatchCannotHideValidMatch) {
    RuntimeState runtime;
    auto& options = const_cast<TQueryOptions&>(runtime.query_options());
    options.__set_enable_hyperscan_vec(true);
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ctx->set_runtime_state(&runtime);
    Columns input{strings({"a", "baba"}), ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("aba"), 2),
                  ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("#"), 2)};
    ctx->set_constant_columns(input);
    ASSERT_TRUE(StringFunctions::regexp_replace_prepare(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
    auto dense = StringFunctions::regexp_replace(ctx.get(), input);
    ASSERT_TRUE(dense.ok());
    EXPECT_EQ("a", dense.value()->get(0).get_slice().to_string());
    EXPECT_EQ("b#", dense.value()->get(1).get_slice().to_string());
    std::vector<uint32_t> rows{0, 1, 0, 1};
    auto selected = StringFunctions::regexp_replace_selected(
            ctx.get(), {{input[0], &rows}, {input[1], &rows}, {input[2], &rows}}, rows.size());
    ASSERT_TRUE(selected.ok());
    for (size_t i = 0; i < rows.size(); ++i)
        EXPECT_EQ(dense.value()->debug_item(rows[i]), selected.value()->debug_item(i));
    ASSERT_TRUE(StringFunctions::regexp_close(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
}

} // namespace starrocks
