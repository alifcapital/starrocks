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

#include "runtime/statistic_result_writer.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/Data_types.h"
#include "runtime/buffer_control_block.h"
#include "runtime/runtime_state.h"
#include "types/timestamp_value.h"
#include "util/runtime_profile.h"
#include "util/thrift_util.h"

namespace starrocks {

namespace {

class GivenColumnExpr final : public Expr {
public:
    GivenColumnExpr(const TypeDescriptor& type, ColumnPtr column) : Expr(type), _column(std::move(column)) {}

    Expr* clone(ObjectPool* pool) const override { return pool->add(new GivenColumnExpr(type(), _column)); }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override { return _column; }

private:
    ColumnPtr _column;
};

MutableColumnPtr ints(int32_t value, size_t rows) {
    auto column = Int32Column::create();
    for (size_t i = 0; i < rows; i++) {
        column->append(value);
    }
    return column;
}

MutableColumnPtr bigints(std::vector<int64_t> values) {
    auto column = Int64Column::create();
    for (int64_t value : values) {
        column->append(value);
    }
    return column;
}

MutableColumnPtr strings(const std::vector<std::string>& values) {
    auto column = BinaryColumn::create();
    for (const auto& value : values) {
        column->append(value);
    }
    return column;
}

MutableColumnPtr datetimes(size_t rows) {
    auto column = TimestampColumn::create();
    for (size_t i = 0; i < rows; i++) {
        column->append(TimestampValue::create(2026, 11, 19, 1, 2, 3, 0));
    }
    return column;
}

// A nullable string column whose NULL rows still hold bytes, as a column that leaves the value of a NULL row unset.
MutableColumnPtr nullable_strings(const std::vector<std::string>& values, const std::vector<uint8_t>& nulls) {
    auto flags = NullColumn::create();
    for (uint8_t null : nulls) {
        flags->append(null);
    }
    return NullableColumn::create(strings(values), std::move(flags));
}

} // namespace

class StatisticResultWriterTest : public testing::Test {
protected:
    // Runs the columns through the writer and gives the statistics that it sends to the frontend.
    std::vector<TStatisticData> write(const std::vector<std::pair<LogicalType, ColumnPtr>>& columns, size_t rows) {
        std::vector<ExprContext*> ctxs;
        for (const auto& [type, column] : columns) {
            auto* expr = _pool.add(new GivenColumnExpr(TypeDescriptor::from_logical_type(type), column));
            ctxs.emplace_back(_pool.add(new ExprContext(expr)));
        }
        RuntimeState state;
        for (auto* ctx : ctxs) {
            EXPECT_TRUE(ctx->prepare(&state).ok());
            EXPECT_TRUE(ctx->open(&state).ok());
        }

        Chunk chunk;
        auto rows_column = Int32Column::create();
        rows_column->append_default(rows);
        chunk.append_column(std::move(rows_column), 0);

        TUniqueId query_id;
        query_id.hi = 0;
        query_id.lo = 0;
        BufferControlBlock sinker(query_id, 1024);
        EXPECT_TRUE(sinker.init().ok());
        RuntimeProfile profile("statistic_result_writer_test");
        StatisticResultWriter writer(&sinker, ctxs, &profile);
        EXPECT_TRUE(writer.init(&state).ok());

        auto results = writer.process_chunk(&chunk);
        EXPECT_TRUE(results.ok()) << results.status();
        std::vector<TStatisticData> statistics;
        for (const auto& result : results.value()) {
            for (const auto& row : result->result_batch.rows) {
                TStatisticData data;
                uint32_t len = row.size();
                EXPECT_TRUE(deserialize_thrift_msg(reinterpret_cast<const uint8_t*>(row.data()), &len,
                                                   TProtocolType::COMPACT, &data)
                                    .ok());
                statistics.emplace_back(std::move(data));
            }
        }
        for (auto* ctx : ctxs) {
            ctx->close(&state);
        }
        return statistics;
    }

    ObjectPool _pool;
};

// The external table summary aggregates the stored min and max of every partition. A column that has no value in
// any partition aggregates to NULL, and the writer must send '' for it, not the bytes that lie under the NULL row.
TEST_F(StatisticResultWriterTest, externalSummarySendsEmptyBoundsForNull) {
    auto statistics = write({{TYPE_INT, ints(8, 2)},
                             {TYPE_VARCHAR, strings({"all_null", "with_values"})},
                             {TYPE_BIGINT, bigints({4, 4})},
                             {TYPE_BIGINT, bigints({32, 32})},
                             {TYPE_BIGINT, bigints({0, 2})},
                             {TYPE_BIGINT, bigints({4, 0})},
                             {TYPE_VARCHAR, nullable_strings({"7920907774976", "9"}, {1, 0})},
                             {TYPE_VARCHAR, nullable_strings({"?814-02-27 <1:41:33.283444", "1"}, {1, 0})},
                             {TYPE_DATETIME, datetimes(2)}},
                            2);
    ASSERT_EQ(2, statistics.size());
    EXPECT_EQ("", statistics[0].max);
    EXPECT_EQ("", statistics[0].min);
    EXPECT_EQ("9", statistics[1].max);
    EXPECT_EQ("1", statistics[1].min);
}

// A chunk where every row is NULL can come as a constant NULL column; its bounds are '' as well.
TEST_F(StatisticResultWriterTest, externalSummarySendsEmptyBoundsForConstantNull) {
    auto statistics = write({{TYPE_INT, ints(8, 1)},
                             {TYPE_VARCHAR, strings({"all_null"})},
                             {TYPE_BIGINT, bigints({4})},
                             {TYPE_BIGINT, bigints({32})},
                             {TYPE_BIGINT, bigints({0})},
                             {TYPE_BIGINT, bigints({4})},
                             {TYPE_VARCHAR, ColumnHelper::create_const_null_column(1)},
                             {TYPE_VARCHAR, ColumnHelper::create_const_null_column(1)},
                             {TYPE_DATETIME, datetimes(1)}},
                            1);
    ASSERT_EQ(1, statistics.size());
    EXPECT_EQ("", statistics[0].max);
    EXPECT_EQ("", statistics[0].min);
}

// The statistics of a native table go through the same aggregate when the cache loads them.
TEST_F(StatisticResultWriterTest, nativeStatisticsSendEmptyBoundsForNull) {
    auto statistics = write({{TYPE_INT, ints(10, 2)},
                             {TYPE_DATETIME, datetimes(2)},
                             {TYPE_BIGINT, bigints({1, 1})},
                             {TYPE_BIGINT, bigints({2, 2})},
                             {TYPE_VARCHAR, strings({"all_null", "with_values"})},
                             {TYPE_BIGINT, bigints({1111498, 10})},
                             {TYPE_BIGINT, bigints({8, 8})},
                             {TYPE_BIGINT, bigints({0, 2})},
                             {TYPE_BIGINT, bigints({1111498, 0})},
                             {TYPE_VARCHAR, nullable_strings({"21267671679744", "9"}, {1, 0})},
                             {TYPE_VARCHAR, nullable_strings({"392223452867768", "1"}, {1, 0})},
                             {TYPE_BIGINT, bigints({0, 0})}},
                            2);
    ASSERT_EQ(2, statistics.size());
    EXPECT_EQ("", statistics[0].max);
    EXPECT_EQ("", statistics[0].min);
    EXPECT_EQ("9", statistics[1].max);
    EXPECT_EQ("1", statistics[1].min);
}

TEST_F(StatisticResultWriterTest, nativeStatisticsSendEmptyBoundsForConstantNull) {
    auto statistics = write({{TYPE_INT, ints(10, 1)},
                             {TYPE_DATETIME, datetimes(1)},
                             {TYPE_BIGINT, bigints({1})},
                             {TYPE_BIGINT, bigints({2})},
                             {TYPE_VARCHAR, strings({"all_null"})},
                             {TYPE_BIGINT, bigints({2})},
                             {TYPE_BIGINT, bigints({8})},
                             {TYPE_BIGINT, bigints({0})},
                             {TYPE_BIGINT, bigints({2})},
                             {TYPE_VARCHAR, ColumnHelper::create_const_null_column(1)},
                             {TYPE_VARCHAR, ColumnHelper::create_const_null_column(1)},
                             {TYPE_BIGINT, bigints({0})}},
                            1);
    ASSERT_EQ(1, statistics.size());
    EXPECT_EQ("", statistics[0].max);
    EXPECT_EQ("", statistics[0].min);
}

} // namespace starrocks
