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

#include "exprs/stats_functions.h"

#include <gtest/gtest.h>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "exprs/function_context.h"

namespace starrocks {

class StatsFunctionsTest : public ::testing::Test {
protected:
    static ColumnPtr nullable_strings(const std::vector<std::optional<std::string>>& values) {
        auto data = BinaryColumn::create();
        auto nulls = NullColumn::create();
        for (const auto& v : values) {
            data->append(v.value_or(""));
            nulls->append(v.has_value() ? 0 : 1);
        }
        return NullableColumn::create(std::move(data), std::move(nulls));
    }

    static std::vector<std::string> run(const Columns& columns) {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ColumnPtr result = StatsFunctions::tuple_key(ctx.get(), columns).value();
        EXPECT_FALSE(result->is_nullable());
        auto binary = ColumnHelper::cast_to<TYPE_VARCHAR>(result);
        std::vector<std::string> out;
        for (size_t i = 0; i < binary->size(); ++i) {
            out.push_back(binary->get_slice(i).to_string());
        }
        return out;
    }
};

TEST_F(StatsFunctionsTest, tuple_key_joins_escapes_and_marks_null) {
    Columns columns;
    columns.push_back(nullable_strings({"approved", "a#b", std::nullopt, "", "back\\slash"}));
    columns.push_back(nullable_strings({"0", "1", "2", std::nullopt, "\\N"}));
    columns.push_back(nullable_strings({"0", "x#", "y", "z", "#"}));

    std::vector<std::string> keys = run(columns);
    ASSERT_EQ(5, keys.size());
    EXPECT_EQ("approved#0#0", keys[0]);
    EXPECT_EQ("a\\#b#1#x\\#", keys[1]);
    EXPECT_EQ("\\N#2#y", keys[2]);
    EXPECT_EQ("#\\N#z", keys[3]);
    // A literal "\N" value escapes to "\\N", which differs from the NULL marker "\N".
    EXPECT_EQ("back\\\\slash#\\\\N#\\#", keys[4]);
}

TEST_F(StatsFunctionsTest, tuple_key_single_column_and_all_null) {
    Columns columns;
    columns.push_back(nullable_strings({"only", std::nullopt}));
    std::vector<std::string> keys = run(columns);
    ASSERT_EQ(2, keys.size());
    EXPECT_EQ("only", keys[0]);
    EXPECT_EQ("\\N", keys[1]);

    Columns two_nulls;
    two_nulls.push_back(nullable_strings({std::nullopt}));
    two_nulls.push_back(nullable_strings({std::nullopt}));
    EXPECT_EQ("\\N#\\N", run(two_nulls)[0]);
}

TEST_F(StatsFunctionsTest, tuple_key_const_input) {
    Columns columns;
    columns.push_back(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("k"), 3));
    columns.push_back(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("v"), 3));

    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnPtr result = StatsFunctions::tuple_key(ctx.get(), columns).value();
    ASSERT_TRUE(result->is_constant());
    ASSERT_EQ(3, result->size());
    EXPECT_EQ("k#v", ColumnHelper::get_const_value<TYPE_VARCHAR>(result).to_string());
}

} // namespace starrocks
