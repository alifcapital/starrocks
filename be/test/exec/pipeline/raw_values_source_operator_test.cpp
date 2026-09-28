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

#include "exec/pipeline/set/raw_values_source_operator.h"

#include <gtest/gtest.h>

#include "testutil/assert.h"

namespace starrocks::pipeline {

static std::vector<std::string> items(const ColumnPtr& column) {
    std::vector<std::string> result;
    for (size_t i = 0; i < column->size(); i++) {
        result.push_back(column->debug_item(i));
    }
    return result;
}

TEST(RawValuesSourceOperatorTest, integer_types) {
    for (LogicalType type : {TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT}) {
        ASSIGN_OR_ABORT(auto column,
                        RawValuesSourceOperatorFactory::build_column(TypeDescriptor(type), false, {-3, 0, 100}, {}));
        ASSERT_FALSE(column->is_nullable());
        ASSERT_EQ((std::vector<std::string>{"-3", "0", "100"}), items(column));
    }
    ASSIGN_OR_ABORT(auto column, RawValuesSourceOperatorFactory::build_column(TypeDescriptor(TYPE_BIGINT), false,
                                                                              {INT64_MIN, INT64_MAX}, {}));
    ASSERT_EQ((std::vector<std::string>{"-9223372036854775808", "9223372036854775807"}), items(column));
}

TEST(RawValuesSourceOperatorTest, string_types) {
    for (const auto& type : {TypeDescriptor::create_varchar_type(10), TypeDescriptor::create_char_type(10)}) {
        ASSIGN_OR_ABORT(auto column, RawValuesSourceOperatorFactory::build_column(type, false, {}, {"a", "", "b'c"}));
        ASSERT_EQ(3, column->size());
        ASSERT_EQ("a", column->get(0).get_slice().to_string());
        ASSERT_EQ("", column->get(1).get_slice().to_string());
        ASSERT_EQ("b'c", column->get(2).get_slice().to_string());
    }
}

TEST(RawValuesSourceOperatorTest, decimal_types) {
    for (LogicalType type : {TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128}) {
        auto decimal = TypeDescriptor::create_decimalv3_type(type, 9, 2);
        ASSIGN_OR_ABORT(auto column,
                        RawValuesSourceOperatorFactory::build_column(decimal, false, {}, {"1.50", "-2.25", "0.00"}));
        ASSERT_EQ((std::vector<std::string>{"1.50", "-2.25", "0.00"}), items(column));
    }
    auto decimal128 = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 6);
    ASSIGN_OR_ABORT(auto column, RawValuesSourceOperatorFactory::build_column(
                                         decimal128, false, {}, {"12345678901234567890123456789012.123456"}));
    ASSERT_EQ((std::vector<std::string>{"12345678901234567890123456789012.123456"}), items(column));
}

TEST(RawValuesSourceOperatorTest, date_types) {
    ASSIGN_OR_ABORT(auto dates, RawValuesSourceOperatorFactory::build_column(TypeDescriptor(TYPE_DATE), false, {},
                                                                             {"2024-01-02", "0001-01-01"}));
    ASSERT_EQ((std::vector<std::string>{"2024-01-02", "0001-01-01"}), items(dates));

    ASSIGN_OR_ABORT(auto datetimes,
                    RawValuesSourceOperatorFactory::build_column(
                            TypeDescriptor(TYPE_DATETIME), false, {},
                            {"2024-01-02 03:04:05", "2024-01-02 03:04:05.000007", "2024-01-02 00:00:00"}));
    ASSERT_EQ((std::vector<std::string>{"2024-01-02 03:04:05", "2024-01-02 03:04:05.000007", "2024-01-02 00:00:00"}),
              items(datetimes));
}

TEST(RawValuesSourceOperatorTest, nullable_slot) {
    ASSIGN_OR_ABORT(auto column,
                    RawValuesSourceOperatorFactory::build_column(TypeDescriptor(TYPE_INT), true, {1, 2}, {}));
    ASSERT_TRUE(column->is_nullable());
    ASSERT_FALSE(column->has_null());
    ASSERT_EQ((std::vector<std::string>{"1", "2"}), items(column));
}

TEST(RawValuesSourceOperatorTest, invalid_values) {
    auto decimal = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL32, 9, 2);
    ASSERT_FALSE(RawValuesSourceOperatorFactory::build_column(decimal, false, {}, {"abc"}).ok());
    ASSERT_FALSE(RawValuesSourceOperatorFactory::build_column(decimal, false, {}, {"12345678.00"}).ok());
    ASSERT_FALSE(
            RawValuesSourceOperatorFactory::build_column(TypeDescriptor(TYPE_DATE), false, {}, {"2024-02-30"}).ok());
    ASSERT_FALSE(RawValuesSourceOperatorFactory::build_column(TypeDescriptor(TYPE_DATETIME), false, {}, {"x"}).ok());

    // The values of a type come in one list
    ASSERT_FALSE(RawValuesSourceOperatorFactory::build_column(TypeDescriptor(TYPE_INT), false, {}, {"1"}).ok());
    ASSERT_FALSE(
            RawValuesSourceOperatorFactory::build_column(TypeDescriptor::create_varchar_type(10), false, {1}, {}).ok());
}

TEST(RawValuesSourceOperatorTest, unsupported_types) {
    for (LogicalType type : {TYPE_DOUBLE, TYPE_FLOAT, TYPE_BOOLEAN, TYPE_LARGEINT}) {
        ASSERT_FALSE(RawValuesSourceOperatorFactory::build_column(TypeDescriptor(type), false, {1}, {}).ok());
        ASSERT_FALSE(RawValuesSourceOperatorFactory::build_column(TypeDescriptor(type), false, {}, {"1"}).ok());
    }
}

} // namespace starrocks::pipeline
