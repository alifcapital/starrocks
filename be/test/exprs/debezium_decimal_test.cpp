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

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/struct_column.h"
#include "exprs/struct_functions.h"
#include "testutil/parallel_test.h"
#include "types/large_int_value.h"

namespace starrocks {
namespace {
std::string unhex(const std::string& hex) {
    std::string result;
    for (size_t i = 0; i < hex.size(); i += 2) result.push_back(std::stoi(hex.substr(i, 2), nullptr, 16));
    return result;
}

ColumnPtr decimal_struct(const std::vector<std::string>& hex, const std::vector<int32_t>& scale) {
    auto values = BinaryColumn::create();
    auto scales = Int32Column::create();
    for (size_t i = 0; i < hex.size(); ++i) {
        values->append(unhex(hex[i]));
        scales->append(scale[i]);
    }
    return StructColumn::create(Columns{scales, values}, {"scale", "value"});
}

StatusOr<ColumnPtr> decode(const ColumnPtr& input, int precision = 38, int scale = 2) {
    auto type = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, precision, scale);
    std::unique_ptr<FunctionContext> context(FunctionContext::create_context(nullptr, nullptr, type, {}));
    return StructFunctions::debezium_decimal(context.get(), {input});
}
} // namespace

PARALLEL_TEST(DebeziumDecimalTest, SignedVariableScales) {
    auto result = decode(decimal_struct({"3a4e", "ff", "80", "0080", "ffff80", "000080", "00", "04d2"},
                                        {2, 0, 1, 2, 1, 2, INT32_MIN, 3}),
                         38, 3);
    ASSERT_TRUE(result.ok()) << result.status();
    ColumnViewer<TYPE_DECIMAL128> values(result.value());
    std::vector<int128_t> expected{149260, -1000, -12800, 1280, -12800, 1280, 0, 1234};
    ASSERT_EQ(expected.size(), values.size());
    for (size_t i = 0; i < expected.size(); ++i) EXPECT_EQ(expected[i], values.value(i));
}

PARALLEL_TEST(DebeziumDecimalTest, ExactScaleDownAndNegativeScale) {
    auto result = decode(decimal_struct({"3034", "cfcc", "7b"}, {3, 3, -2}), 10, 2);
    ASSERT_TRUE(result.ok()) << result.status();
    ColumnViewer<TYPE_DECIMAL128> values(result.value());
    EXPECT_EQ(1234, values.value(0));
    EXPECT_EQ(-1234, values.value(1));
    EXPECT_EQ(1230000, values.value(2));
}

PARALLEL_TEST(DebeziumDecimalTest, ErrorsNeverRoundOrWrap) {
    for (const auto& [hex, scale, precision, target_scale] :
         std::vector<std::tuple<std::string, int, int, int>>{{"", 0, 38, 0},
                                                             {"04d2", 3, 38, 2},
                                                             {"fb2e", 3, 38, 2},
                                                             {"64", 0, 2, 0},
                                                             {"9c", 0, 2, 0},
                                                             {"01", INT32_MIN, 38, 0},
                                                             {"01", INT32_MAX, 38, 0},
                                                             {"80000000000000000000000000000000", 0, 38, 0},
                                                             {"0100000000000000000000000000000000", 0, 38, 0}}) {
        EXPECT_FALSE(decode(decimal_struct({hex}, {scale}), precision, target_scale).ok()) << hex;
    }
}

PARALLEL_TEST(DebeziumDecimalTest, ConstantAndEmpty) {
    auto input = ConstColumn::create(decimal_struct({"3a4e"}, {2}), 23);
    auto result = decode(input);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_TRUE(result.value()->is_constant());
    EXPECT_EQ(23, result.value()->size());
    EXPECT_EQ(14926, ColumnViewer<TYPE_DECIMAL128>(result.value()).value(22));
    result = decode(decimal_struct({}, {}));
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(0, result.value()->size());
    result = decode(ColumnHelper::create_const_null_column(23));
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(result.value()->only_null());
    EXPECT_EQ(23, result.value()->size());
}

PARALLEL_TEST(DebeziumDecimalTest, NullableParentAndChildren) {
    auto input = decimal_struct({"01", "", "", ""}, {0, 0, 0, 0});
    auto* structure = down_cast<const StructColumn*>(input.get());
    auto fields = structure->fields();
    auto scale_nulls = NullColumn::create();
    auto value_nulls = NullColumn::create();
    auto parent_nulls = NullColumn::create();
    for (int i = 0; i < 4; ++i) {
        scale_nulls->append(i == 1);
        value_nulls->append(i == 2);
        parent_nulls->append(i == 3);
    }
    input = StructColumn::create(
            Columns{NullableColumn::create(fields[0], scale_nulls), NullableColumn::create(fields[1], value_nulls)},
            {"scale", "value"});
    input = NullableColumn::create(input, parent_nulls);
    auto result = decode(input);
    ASSERT_TRUE(result.ok()) << result.status();
    ColumnViewer<TYPE_DECIMAL128> values(result.value());
    EXPECT_EQ(100, values.value(0));
    for (int i = 1; i < 4; ++i) EXPECT_TRUE(values.is_null(i));
}

PARALLEL_TEST(DebeziumDecimalTest, ReorderedFields) {
    auto input = decimal_struct({"3a4e"}, {2});
    auto fields = down_cast<const StructColumn*>(input.get())->fields();
    input = StructColumn::create(Columns{fields[1], fields[0]}, {"value", "scale"});
    auto result = decode(input);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(14926, ColumnViewer<TYPE_DECIMAL128>(result.value()).value(0));
}

PARALLEL_TEST(DebeziumDecimalTest, PrecisionBoundaryAndWideInteger) {
    {
        auto result = decode(decimal_struct({"4b3b4ca85a86c47a098a223fffffffff"}, {0}), 38, 0);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("99999999999999999999999999999999999999",
                  LargeIntValue::to_string(ColumnViewer<TYPE_DECIMAL128>(result.value()).value(0)));
    }
    {
        auto result = decode(decimal_struct({"b4c4b357a5793b85f675ddc000000001"}, {0}), 38, 0);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("-99999999999999999999999999999999999999",
                  LargeIntValue::to_string(ColumnViewer<TYPE_DECIMAL128>(result.value()).value(0)));
    }
    {
        auto result =
                decode(decimal_struct({"04944ad4690751c06eaa9d625d17f4a78dedeb639e5000000000000000"}, {65}), 38, 5);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("123456789", LargeIntValue::to_string(ColumnViewer<TYPE_DECIMAL128>(result.value()).value(0)));
    }
    {
        auto result =
                decode(decimal_struct({"fb6bb52b96f8ae3f9155629da2e80b587212149c61b000000000000000"}, {65}), 38, 5);
        ASSERT_TRUE(result.ok()) << result.status();
        EXPECT_EQ("-123456789", LargeIntValue::to_string(ColumnViewer<TYPE_DECIMAL128>(result.value()).value(0)));
    }
}
} // namespace starrocks
