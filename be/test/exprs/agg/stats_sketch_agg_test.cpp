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

#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate_factory.h"
#include "exprs/agg/aggregate_state_allocator.h"
#include "exprs/agg/base_aggregate_test.h"
#include "exprs/function_context.h"
#include "types/date_value.h"

namespace starrocks {

class StatsSketchAggTest : public testing::Test {
public:
    void SetUp() override {
        _allocator = std::make_unique<CountingAllocatorWithHook>();
        tls_agg_state_allocator = _allocator.get();
    }

    void TearDown() override {
        tls_agg_state_allocator = nullptr;
        _allocator.reset();
    }

protected:
    static TypeDescriptor type(LogicalType lt) { return TypeDescriptor::from_logical_type(lt); }

    static std::unique_ptr<FunctionContext> make_ctx(std::vector<TypeDescriptor> arg_types, const Columns& consts) {
        std::unique_ptr<FunctionContext> ctx(
                FunctionContext::create_test_context(std::move(arg_types), type(TYPE_VARCHAR)));
        ctx->set_constant_columns(consts);
        return ctx;
    }

    static void update(FunctionContext* ctx, const AggregateFunction* func, AggDataPtr state, const Columns& columns) {
        std::vector<const Column*> raw;
        for (const auto& c : columns) {
            raw.push_back(c.get());
        }
        func->update_batch_single_state(ctx, columns[0]->size(), raw.data(), state);
    }

    static void merge_into(FunctionContext* ctx, const AggregateFunction* func, AggDataPtr from, AggDataPtr to) {
        auto serde = BinaryColumn::create();
        func->serialize_to_column(ctx, from, serde.get());
        func->merge(ctx, serde.get(), to, 0);
    }

    static std::string finalize(FunctionContext* ctx, const AggregateFunction* func, AggDataPtr state) {
        auto result = BinaryColumn::create();
        func->finalize_to_column(ctx, state, result.get());
        return result->get_slice(0).to_string();
    }

    // Every quoted string of a JSON document, in order.
    static std::vector<std::string> json_strings(const std::string& json) {
        std::vector<std::string> out;
        for (size_t i = 0; i < json.size(); ++i) {
            if (json[i] != '"') {
                continue;
            }
            std::string s;
            for (++i; i < json.size() && json[i] != '"'; ++i) {
                if (json[i] == '\\') {
                    ++i;
                }
                s.push_back(json[i]);
            }
            out.push_back(s);
        }
        return out;
    }

    static ColumnPtr int_const(int32_t v, size_t n) { return ColumnHelper::create_const_column<TYPE_INT>(v, n); }
    static ColumnPtr str_const(const std::string& v, size_t n) {
        return ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(v), n);
    }

    template <typename T>
    static MutableColumnPtr column_of(const std::vector<T>& values) {
        auto column = ColumnTraits<T>::ColumnType::create();
        for (const T& v : values) {
            column->append(v);
        }
        return column;
    }

    static MutableColumnPtr strings_of(const std::vector<std::string>& values) {
        auto column = BinaryColumn::create();
        for (const auto& v : values) {
            column->append(v);
        }
        return column;
    }

    static std::vector<int64_t> skewed_int64() {
        std::vector<int64_t> v;
        for (int64_t i = 0; i < 1000; ++i) {
            v.push_back(i);
        }
        v.insert(v.end(), 500, 5);
        v.insert(v.end(), 300, 7);
        return v;
    }

    std::unique_ptr<CountingAllocatorWithHook> _allocator;
};

// ---------------------------------------------------------------- ds_frequent_items

TEST_F(StatsSketchAggTest, frequent_items_exact_when_map_is_large_enough) {
    const auto* func = get_aggregate_function("ds_frequent_items", TYPE_BIGINT, TYPE_VARCHAR, false);
    ASSERT_NE(nullptr, func);
    auto data = column_of<int64_t>(skewed_int64());
    Columns columns{data, int_const(2, data->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT)}, columns);

    auto state = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state->state(), columns);
    EXPECT_EQ(R"([["5","501"],["7","301"]])", finalize(ctx.get(), func, state->state()));

    Columns top1{data, int_const(1, data->size())};
    auto ctx1 = make_ctx({type(TYPE_BIGINT), type(TYPE_INT)}, top1);
    EXPECT_EQ(R"([["5","501"]])", finalize(ctx1.get(), func, state->state()));
}

TEST_F(StatsSketchAggTest, frequent_items_merge_adds_counts_and_ignores_empty_state) {
    const auto* func = get_aggregate_function("ds_frequent_items", TYPE_BIGINT, TYPE_VARCHAR, false);
    std::vector<int64_t> a;
    for (int64_t i = 0; i < 1000; ++i) {
        a.push_back(i);
    }
    a.insert(a.end(), 500, 5);
    std::vector<int64_t> b(300, 7);
    b.insert(b.end(), 200, 5);

    auto col_a = column_of<int64_t>(a);
    auto col_b = column_of<int64_t>(b);
    Columns columns_a{col_a, int_const(2, col_a->size())};
    Columns columns_b{col_b, int_const(2, col_b->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT)}, columns_a);

    auto state_a = ManagedAggrState::create(ctx.get(), func);
    auto state_b = ManagedAggrState::create(ctx.get(), func);
    auto state_empty = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state_a->state(), columns_a);
    update(ctx.get(), func, state_b->state(), columns_b);

    merge_into(ctx.get(), func, state_a->state(), state_b->state());
    merge_into(ctx.get(), func, state_empty->state(), state_b->state());
    EXPECT_EQ(R"([["5","701"],["7","301"]])", finalize(ctx.get(), func, state_b->state()));

    // Merging into a state that never saw a row adopts the incoming sketch.
    auto state_fresh = ManagedAggrState::create(ctx.get(), func);
    merge_into(ctx.get(), func, state_b->state(), state_fresh->state());
    EXPECT_EQ(R"([["5","701"],["7","301"]])", finalize(ctx.get(), func, state_fresh->state()));

    EXPECT_EQ("[]", finalize(ctx.get(), func, state_empty->state()));
}

TEST_F(StatsSketchAggTest, frequent_items_strings_are_json_escaped) {
    const auto* func = get_aggregate_function("ds_frequent_items", TYPE_VARCHAR, TYPE_VARCHAR, false);
    std::vector<std::string> values;
    values.insert(values.end(), 70, "approved");
    values.insert(values.end(), 20, "declined");
    values.insert(values.end(), 10, "pending");
    values.insert(values.end(), 5, "he said \"hi\"\\");
    for (int i = 0; i < 50; ++i) {
        values.push_back("unique-" + std::to_string(i));
    }
    auto data = strings_of(values);
    Columns columns{data, int_const(4, data->size())};
    auto ctx = make_ctx({type(TYPE_VARCHAR), type(TYPE_INT)}, columns);

    auto state = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state->state(), columns);
    EXPECT_EQ(R"([["approved","70"],["declined","20"],["pending","10"],["he said \"hi\"\\","5"]])",
              finalize(ctx.get(), func, state->state()));
}

TEST_F(StatsSketchAggTest, frequent_items_memory_is_bounded_and_heavy_hitter_survives) {
    const auto* func = get_aggregate_function("ds_frequent_items", TYPE_BIGINT, TYPE_VARCHAR, false);
    std::vector<int64_t> values;
    for (int64_t i = 0; i < 100000; ++i) {
        values.push_back(i + 10);
        if (i % 5 == 0) {
            values.push_back(1);
        }
    }
    // 120000 rows, value 1 holds 20000 of them; a map of 2^6 entries has epsilon 3.5/64.
    auto data = column_of<int64_t>(values);
    Columns columns{data, int_const(1, data->size()), int_const(6, data->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT), type(TYPE_INT)}, columns);

    auto state = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state->state(), columns);
    std::vector<std::string> fields = json_strings(finalize(ctx.get(), func, state->state()));
    ASSERT_EQ(2, fields.size());
    EXPECT_EQ("1", fields[0]);
    int64_t estimate = std::stoll(fields[1]);
    EXPECT_GE(estimate, 20000);
    EXPECT_LE(estimate, 20000 + int64_t(120000 * 3.5 / 64));
}

TEST_F(StatsSketchAggTest, frequent_items_formats_decimal_and_date_like_literals) {
    {
        const auto* func = get_aggregate_function("ds_frequent_items", TYPE_DECIMAL64, TYPE_VARCHAR, false);
        auto data = RunTimeColumnType<TYPE_DECIMAL64>::create(10, 2);
        data->append(150);
        data->append(150);
        data->append(150);
        data->append(275);
        Columns columns{data, int_const(2, data->size())};
        auto ctx = make_ctx({UTRawType{.type = TYPE_DECIMAL64, .precision = 10, .scale = 2}, type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"([["1.50","3"],["2.75","1"]])", finalize(ctx.get(), func, state->state()));
    }
    {
        const auto* func = get_aggregate_function("ds_frequent_items", TYPE_DATE, TYPE_VARCHAR, false);
        auto data = column_of<DateValue>(
                {DateValue::create(2024, 1, 1), DateValue::create(2024, 1, 2), DateValue::create(2024, 1, 1)});
        Columns columns{data, int_const(1, data->size())};
        auto ctx = make_ctx({type(TYPE_DATE), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"([["2024-01-01","2"]])", finalize(ctx.get(), func, state->state()));
    }
}

TEST_F(StatsSketchAggTest, frequent_items_rejects_bad_parameters) {
    const auto* func = get_aggregate_function("ds_frequent_items", TYPE_BIGINT, TYPE_VARCHAR, false);
    auto data = column_of<int64_t>({1, 2, 3});
    {
        Columns columns{data, int_const(0, data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_THROW(finalize(ctx.get(), func, state->state()), std::runtime_error);
    }
    {
        Columns columns{data, int_const(1, data->size()), int_const(25, data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        EXPECT_THROW(update(ctx.get(), func, state->state(), columns), std::runtime_error);
    }
}

// ---------------------------------------------------------------- ds_kll_quantiles

TEST_F(StatsSketchAggTest, kll_quantiles_bound_min_max_exactly_and_median_approximately) {
    const auto* func = get_aggregate_function("ds_kll_quantiles", TYPE_BIGINT, TYPE_VARCHAR, false);
    ASSERT_NE(nullptr, func);
    std::vector<int64_t> values;
    for (int64_t i = 0; i < 10000; ++i) {
        values.push_back(i);
    }
    auto data = column_of<int64_t>(values);
    Columns columns{data, int_const(4, data->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT)}, columns);

    auto state = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state->state(), columns);
    std::vector<std::string> bounds = json_strings(finalize(ctx.get(), func, state->state()));
    ASSERT_EQ(5, bounds.size());
    EXPECT_EQ("0", bounds[0]);
    EXPECT_EQ("9999", bounds[4]);
    for (int i = 1; i <= 3; ++i) {
        int64_t q = std::stoll(bounds[i]);
        EXPECT_NEAR(i * 2500, q, 400) << "quantile " << i;
        EXPECT_LT(std::stoll(bounds[i - 1]), q);
    }
}

TEST_F(StatsSketchAggTest, kll_quantiles_collapse_repeated_boundaries) {
    const auto* func = get_aggregate_function("ds_kll_quantiles", TYPE_BIGINT, TYPE_VARCHAR, false);
    {
        auto data = column_of<int64_t>(std::vector<int64_t>(100, 42));
        Columns columns{data, int_const(8, data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"(["42"])", finalize(ctx.get(), func, state->state()));
    }
    {
        std::vector<int64_t> values(50, 1);
        values.insert(values.end(), 50, 100);
        auto data = column_of<int64_t>(values);
        Columns columns{data, int_const(4, data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"(["1","100"])", finalize(ctx.get(), func, state->state()));
    }
}

TEST_F(StatsSketchAggTest, kll_quantiles_merge_and_empty_state) {
    const auto* func = get_aggregate_function("ds_kll_quantiles", TYPE_BIGINT, TYPE_VARCHAR, false);
    std::vector<int64_t> a, b;
    for (int64_t i = 0; i < 5000; ++i) {
        a.push_back(i);
        b.push_back(5000 + i);
    }
    auto col_a = column_of<int64_t>(a);
    auto col_b = column_of<int64_t>(b);
    Columns columns_a{col_a, int_const(2, col_a->size()), int_const(400, col_a->size())};
    Columns columns_b{col_b, int_const(2, col_b->size()), int_const(400, col_b->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT), type(TYPE_INT)}, columns_a);

    auto state_a = ManagedAggrState::create(ctx.get(), func);
    auto state_b = ManagedAggrState::create(ctx.get(), func);
    auto state_empty = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state_a->state(), columns_a);
    update(ctx.get(), func, state_b->state(), columns_b);
    EXPECT_EQ("[]", finalize(ctx.get(), func, state_empty->state()));

    merge_into(ctx.get(), func, state_a->state(), state_b->state());
    merge_into(ctx.get(), func, state_empty->state(), state_b->state());
    std::vector<std::string> bounds = json_strings(finalize(ctx.get(), func, state_b->state()));
    ASSERT_EQ(3, bounds.size());
    EXPECT_EQ("0", bounds[0]);
    EXPECT_NEAR(5000, std::stoll(bounds[1]), 300);
    EXPECT_EQ("9999", bounds[2]);

    auto state_fresh = ManagedAggrState::create(ctx.get(), func);
    merge_into(ctx.get(), func, state_b->state(), state_fresh->state());
    EXPECT_EQ(bounds, json_strings(finalize(ctx.get(), func, state_fresh->state())));
}

TEST_F(StatsSketchAggTest, kll_quantiles_formats_dates_and_skips_nan) {
    {
        const auto* func = get_aggregate_function("ds_kll_quantiles", TYPE_DATE, TYPE_VARCHAR, false);
        std::vector<DateValue> dates;
        for (int d = 1; d <= 10; ++d) {
            dates.push_back(DateValue::create(2024, 1, d));
        }
        auto data = column_of<DateValue>(dates);
        Columns columns{data, int_const(1, data->size())};
        auto ctx = make_ctx({type(TYPE_DATE), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"(["2024-01-01","2024-01-10"])", finalize(ctx.get(), func, state->state()));
    }
    {
        const auto* func = get_aggregate_function("ds_kll_quantiles", TYPE_DOUBLE, TYPE_VARCHAR, false);
        auto data = column_of<double>({1.5, std::nan(""), 3.5});
        Columns columns{data, int_const(1, data->size())};
        auto ctx = make_ctx({type(TYPE_DOUBLE), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"(["1.5","3.5"])", finalize(ctx.get(), func, state->state()));
    }
}

TEST_F(StatsSketchAggTest, kll_quantiles_rejects_bad_parameters) {
    const auto* func = get_aggregate_function("ds_kll_quantiles", TYPE_BIGINT, TYPE_VARCHAR, false);
    auto data = column_of<int64_t>({1, 2, 3});
    {
        Columns columns{data, int_const(0, data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_THROW(finalize(ctx.get(), func, state->state()), std::runtime_error);
    }
    {
        Columns columns{data, int_const(4, data->size()), int_const(2, data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_INT), type(TYPE_INT)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        EXPECT_THROW(update(ctx.get(), func, state->state(), columns), std::runtime_error);
    }
}

// ---------------------------------------------------------------- histogram_by_bounds

TEST_F(StatsSketchAggTest, histogram_by_bounds_counts_mcv_exactly_and_fills_buckets) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_BIGINT, TYPE_VARCHAR, false);
    ASSERT_NE(nullptr, func);
    std::vector<int64_t> values;
    for (int64_t i = 0; i < 100; ++i) {
        values.push_back(i);
    }
    values.insert(values.end(), 50, 5);
    values.insert(values.end(), 10, 60);
    auto data = column_of<int64_t>(values);
    Columns columns{data, str_const(R"(["5"])", data->size()), str_const(R"(["0","50","99"])", data->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);

    auto state = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state->state(), columns);
    // [0, 50] holds 0..50 without the MCV 5; (50, 99] holds 51..99 plus ten more 60s.
    EXPECT_EQ(R"({"mcv":[["5","51"]],"buckets":[["0","50","50","1","50"],["51","99","59","1","49"]]})",
              finalize(ctx.get(), func, state->state()));
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_boundary_rows_and_out_of_range_rows) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_BIGINT, TYPE_VARCHAR, false);
    // 5 is the upper bound of the first bucket, 6 opens the second; -3 and 120 lie outside all bounds.
    auto data = column_of<int64_t>({5, 5, 6, -3, 120, 10, 10, 10});
    Columns columns{data, str_const("[]", data->size()), str_const(R"(["0","5","10"])", data->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);

    auto state = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state->state(), columns);
    EXPECT_EQ(R"({"mcv":[],"buckets":[["-3","5","3","2","2"],["6","120","5","1","3"]]})",
              finalize(ctx.get(), func, state->state()));
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_single_bound_and_unsorted_duplicate_bounds) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_BIGINT, TYPE_VARCHAR, false);
    auto data = column_of<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
    {
        Columns columns{data, str_const("[]", data->size()), str_const(R"(["7"])", data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"({"mcv":[],"buckets":[["1","10","10","1","10"]]})", finalize(ctx.get(), func, state->state()));
    }
    {
        Columns columns{data, str_const("[]", data->size()), str_const(R"(["5","1","5","10"])", data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"({"mcv":[],"buckets":[["1","5","5","1","5"],["6","10","5","1","5"]]})",
                  finalize(ctx.get(), func, state->state()));
    }
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_merge_equals_single_pass) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_BIGINT, TYPE_VARCHAR, false);
    std::vector<int64_t> all;
    for (int64_t i = 0; i < 100; ++i) {
        all.push_back(i);
    }
    all.insert(all.end(), 50, 5);
    all.insert(all.end(), 10, 60);
    all.push_back(-3);
    all.push_back(120);
    all.insert(all.end(), 4, 99);
    std::vector<int64_t> odd, even;
    for (size_t i = 0; i < all.size(); ++i) {
        (i % 2 == 0 ? even : odd).push_back(all[i]);
    }
    const std::string mcv = R"(["5","60"])";
    const std::string bounds = R"(["0","25","50","75","99"])";

    auto col_all = column_of<int64_t>(all);
    auto col_odd = column_of<int64_t>(odd);
    auto col_even = column_of<int64_t>(even);
    Columns columns_all{col_all, str_const(mcv, col_all->size()), str_const(bounds, col_all->size())};
    Columns columns_odd{col_odd, str_const(mcv, col_odd->size()), str_const(bounds, col_odd->size())};
    Columns columns_even{col_even, str_const(mcv, col_even->size()), str_const(bounds, col_even->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns_all);

    auto state_all = ManagedAggrState::create(ctx.get(), func);
    auto state_odd = ManagedAggrState::create(ctx.get(), func);
    auto state_even = ManagedAggrState::create(ctx.get(), func);
    auto state_empty = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state_all->state(), columns_all);
    update(ctx.get(), func, state_odd->state(), columns_odd);
    update(ctx.get(), func, state_even->state(), columns_even);

    std::string expected = finalize(ctx.get(), func, state_all->state());
    EXPECT_EQ(R"({"mcv":[["5","51"],["60","11"]],"buckets":[["-3","25","26","1","26"],["26","50","25","1","25"],)"
              R"(["51","75","24","1","24"],["76","120","29","1","25"]]})",
              expected);

    merge_into(ctx.get(), func, state_odd->state(), state_even->state());
    merge_into(ctx.get(), func, state_empty->state(), state_even->state());
    EXPECT_EQ(expected, finalize(ctx.get(), func, state_even->state()));

    auto state_fresh = ManagedAggrState::create(ctx.get(), func);
    merge_into(ctx.get(), func, state_even->state(), state_fresh->state());
    EXPECT_EQ(expected, finalize(ctx.get(), func, state_fresh->state()));
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_merge_combines_upper_repeats) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_BIGINT, TYPE_VARCHAR, false);
    auto col_a = column_of<int64_t>({3, 3, 1});
    auto col_b = column_of<int64_t>({3, 2});
    auto col_c = column_of<int64_t>({9, 9});
    const std::string bounds = R"(["1","9"])";
    Columns columns_a{col_a, str_const("[]", 3), str_const(bounds, 3)};
    Columns columns_b{col_b, str_const("[]", 2), str_const(bounds, 2)};
    Columns columns_c{col_c, str_const("[]", 2), str_const(bounds, 2)};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns_a);

    auto state_a = ManagedAggrState::create(ctx.get(), func);
    auto state_b = ManagedAggrState::create(ctx.get(), func);
    auto state_c = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state_a->state(), columns_a);
    update(ctx.get(), func, state_b->state(), columns_b);
    update(ctx.get(), func, state_c->state(), columns_c);

    // Same max on both sides: repeats add up.
    merge_into(ctx.get(), func, state_a->state(), state_b->state());
    EXPECT_EQ(R"({"mcv":[],"buckets":[["1","3","5","3","3"]]})", finalize(ctx.get(), func, state_b->state()));
    // Larger max on the incoming side: its repeats replace ours.
    merge_into(ctx.get(), func, state_c->state(), state_b->state());
    EXPECT_EQ(R"({"mcv":[],"buckets":[["1","9","7","2","4"]]})", finalize(ctx.get(), func, state_b->state()));
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_strings_count_mcv_only) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_VARCHAR, TYPE_VARCHAR, false);
    ASSERT_NE(nullptr, func);
    auto data = strings_of({"approved", "declined", "approved", "other", "approved", "a\"b"});
    {
        Columns columns{data, str_const(R"(["approved","declined","a\"b"])", data->size()),
                        str_const("[]", data->size())};
        auto ctx = make_ctx({type(TYPE_VARCHAR), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"({"mcv":[["approved","3"],["declined","1"],["a\"b","1"]],"buckets":[]})",
                  finalize(ctx.get(), func, state->state()));

        auto state_other = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state_other->state(), columns);
        merge_into(ctx.get(), func, state->state(), state_other->state());
        EXPECT_EQ(R"({"mcv":[["approved","6"],["declined","2"],["a\"b","2"]],"buckets":[]})",
                  finalize(ctx.get(), func, state_other->state()));
    }
    {
        Columns columns{data, str_const(R"(["approved"])", data->size()), str_const(R"(["a","z"])", data->size())};
        auto ctx = make_ctx({type(TYPE_VARCHAR), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        EXPECT_THROW(update(ctx.get(), func, state->state(), columns), std::runtime_error);
    }
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_no_rows_reports_zero_mcv_counts) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_BIGINT, TYPE_VARCHAR, false);
    Columns columns{column_of<int64_t>({}), str_const(R"(["5","7"])", 0), str_const(R"(["0","9"])", 0)};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
    auto state = ManagedAggrState::create(ctx.get(), func);
    EXPECT_EQ(R"({"mcv":[["5","0"],["7","0"]],"buckets":[]})", finalize(ctx.get(), func, state->state()));
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_rejects_bad_specifications) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_BIGINT, TYPE_VARCHAR, false);
    auto data = column_of<int64_t>({1, 2, 3});
    {
        Columns columns{data, str_const("not json", data->size()), str_const("[]", data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        EXPECT_THROW(update(ctx.get(), func, state->state(), columns), std::runtime_error);
    }
    {
        Columns columns{data, str_const(R"(["abc"])", data->size()), str_const("[]", data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        EXPECT_THROW(update(ctx.get(), func, state->state(), columns), std::runtime_error);
    }
    {
        Columns columns{data, str_const("[]", data->size()), str_const(R"([1, 2])", data->size())};
        auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        EXPECT_THROW(update(ctx.get(), func, state->state(), columns), std::runtime_error);
    }
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_decimal_and_date) {
    {
        const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_DECIMAL64, TYPE_VARCHAR, false);
        auto data = RunTimeColumnType<TYPE_DECIMAL64>::create(10, 2);
        data->append(150);
        data->append(150);
        data->append(275);
        Columns columns{data, str_const(R"(["1.50"])", data->size()), str_const(R"(["0.00","10.00"])", data->size())};
        auto ctx = make_ctx({UTRawType{.type = TYPE_DECIMAL64, .precision = 10, .scale = 2}, type(TYPE_VARCHAR),
                             type(TYPE_VARCHAR)},
                            columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"({"mcv":[["1.50","2"]],"buckets":[["2.75","2.75","1","1","1"]]})",
                  finalize(ctx.get(), func, state->state()));
    }
    {
        const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_DATE, TYPE_VARCHAR, false);
        auto data = column_of<DateValue>({DateValue::create(2024, 1, 1), DateValue::create(2024, 1, 5),
                                          DateValue::create(2024, 1, 1), DateValue::create(2024, 1, 9)});
        Columns columns{data, str_const(R"(["2024-01-01"])", data->size()),
                        str_const(R"(["2024-01-01","2024-01-31"])", data->size())};
        auto ctx = make_ctx({type(TYPE_DATE), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);
        auto state = ManagedAggrState::create(ctx.get(), func);
        update(ctx.get(), func, state->state(), columns);
        EXPECT_EQ(R"({"mcv":[["2024-01-01","2"]],"buckets":[["2024-01-05","2024-01-09","2","1","2"]]})",
                  finalize(ctx.get(), func, state->state()));
    }
}

TEST_F(StatsSketchAggTest, histogram_by_bounds_skips_nulls) {
    const auto* func = get_aggregate_function("histogram_by_bounds", TYPE_BIGINT, TYPE_VARCHAR, true);
    ASSERT_NE(nullptr, func);
    auto values = column_of<int64_t>({5, 0, 5, 0, 7});
    auto nulls = NullColumn::create();
    for (uint8_t is_null : {0, 1, 0, 1, 0}) {
        nulls->append(is_null);
    }
    ColumnPtr data = NullableColumn::create(std::move(values), std::move(nulls));
    Columns columns{data, str_const(R"(["5"])", data->size()), str_const(R"(["0","9"])", data->size())};
    auto ctx = make_ctx({type(TYPE_BIGINT), type(TYPE_VARCHAR), type(TYPE_VARCHAR)}, columns);

    auto state = ManagedAggrState::create(ctx.get(), func);
    update(ctx.get(), func, state->state(), columns);
    auto result = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    func->finalize_to_column(ctx.get(), state->state(), result.get());
    ASSERT_FALSE(result->is_null(0));
    EXPECT_EQ(R"({"mcv":[["5","2"]],"buckets":[["7","7","1","1","1"]]})",
              down_cast<const BinaryColumn*>(result->data_column().get())->get_slice(0).to_string());
}

} // namespace starrocks
