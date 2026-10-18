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

#include "exprs/json_functions.h"

#include <glog/logging.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <velocypack/vpack.h>

#include <atomic>
#include <cstring>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "butil/time.h"
#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "column/map_column.h"
#include "column/nullable_column.h"
#include "column/struct_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "common/statusor.h"
#include "exprs/mock_vectorized_expr.h"
#include "gtest/gtest-param-test.h"
#include "gutil/casts.h"
#include "gutil/strings/strip.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/defer_op.h"
#include "util/json.h"
#include "util/json_flattener.h"

namespace starrocks {

class JsonFunctionsTest : public ::testing::Test {
public:
    void SetUp() override {
        expr_node.opcode = TExprOpcode::ADD;
        expr_node.child_type = TPrimitiveType::INT;
        expr_node.node_type = TExprNodeType::BINARY_PRED;
        expr_node.num_children = 2;
        expr_node.__isset.opcode = true;
        expr_node.__isset.child_type = true;
        expr_node.type = gen_type_desc(TPrimitiveType::BOOLEAN);
    }

    Status test_extract_from_object(std::string input, const std::string& jsonpath, std::string* output) {
        // reverse for padding.
        input.reserve(input.size() + simdjson::SIMDJSON_PADDING);

        simdjson::ondemand::parser parser;
        simdjson::ondemand::document doc;
        EXPECT_EQ(simdjson::error_code::SUCCESS, parser.iterate(input).get(doc));

        simdjson::ondemand::object obj;

        EXPECT_EQ(simdjson::error_code::SUCCESS, doc.get_object().get(obj));

        std::vector<SimpleJsonPath> path;
        RETURN_IF_ERROR(JsonFunctions::parse_json_paths(jsonpath, &path));

        simdjson::ondemand::value val;
        RETURN_IF_ERROR(JsonFunctions::extract_from_object(obj, path, &val));
        std::string_view sv = simdjson::to_json_string(val);

        output->assign(sv.data(), sv.size());
        return Status::OK();
    }

public:
    TExprNode expr_node;
};

TEST_F(JsonFunctionsTest, get_json_string_casting) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto strings = BinaryColumn::create();
    auto strings2 = BinaryColumn::create();

    std::string values[] = {
            R"({"k1":    1})",          //int, 1 key
            R"({"k1":    1, "k2": 2})", // 2 keys, get the former
            R"({"k0":    0, "k1": 1})", // 2 keys, get  the latter

            R"({"k1":    3.14159})",                 //double, 1 key
            R"({"k0":    2.71828, "k1":  3.14159})", // 2 keys, get  the former
            R"({"k1":    3.14159, "k2":  2.71828})", // 2 keys, get  the latter

            R"({"k1":    "{\"k11\":       \"v11\"}"})",                                        //string, 1 key
            R"({"k0":    "{\"k01\":       \"v01\"}",  "k1":     "{\"k11\":       \"v11\"}"})", // 2 keys, get  the former
            R"({"k1":    "{\"k11\":       \"v11\"}",  "k2":     "{\"k21\": \"v21\"}"})", // 2 keys, get  the latter

            R"({"k1":    {"k11":       "v11"}})",                             //object, 1 key
            R"({"k0":    {"k01":       "v01"},  "k1":     {"k11": "v11"}})",  // 2 keys, get  the former
            R"({"k1":    {"k11":       "v11"},  "k2":     {"k21": "v21"}})"}; // 2 keys, get  the latter

    std::string strs[] = {"$.k1", "$.k1", "$.k1", "$.k1", "$.k1", "$.k1",
                          "$.k1", "$.k1", "$.k1", "$.k1", "$.k1", "$.k1"};
    std::string length_strings[] = {"1",
                                    "1",
                                    "1",
                                    "3.14159",
                                    "3.14159",
                                    "3.14159",
                                    R"({"k11":       "v11"})",
                                    R"({"k11":       "v11"})",
                                    R"({"k11":       "v11"})",
                                    R"({"k11": "v11"})",
                                    R"({"k11": "v11"})",
                                    R"({"k11": "v11"})"};

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        strings->append(values[j]);
        strings2->append(strs[j]);
    }

    columns.emplace_back(strings);
    columns.emplace_back(strings2);

    ctx.get()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns).value();

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(length_strings[j], v->get_data()[j].to_string());
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, get_json_string_array) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto strings = BinaryColumn::create();
    auto strings2 = BinaryColumn::create();

    std::string values[] = {R"([{"key":    1}, {"key": 2 }])", R"([{"key":    1}, {"key": 2 }])"};

    std::string strs[] = {"$[*].key", "$.[*].key"};
    std::string length_strings[] = {"[1, 2]", "[1, 2]"};

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        strings->append(values[j]);
        strings2->append(strs[j]);
    }

    columns.emplace_back(strings);
    columns.emplace_back(strings2);

    ctx.get()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns).value();

    auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(result);

    for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
        ASSERT_EQ(length_strings[j], v->get_data()[j].to_string());
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, get_json_string_array_index_out_of_bounds) {
    // Mirrors the production shape GET_JSON_STRING(col, '[0].value.url') over rows whose array is empty
    // or too short: every such row must come back NULL, and rows that do have the element still resolve.
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns columns;
    auto jsons = BinaryColumn::create();
    auto paths = BinaryColumn::create();

    std::string values[] = {R"([])",
                            R"([{"value": {"url": "http://x"}}])",
                            R"([{"value": {}}])",
                            R"({"value": {"url": "http://y"}})",
                            R"([[], [1]])",
                            R"([])"};
    std::string strs[] = {"[0].value.url", "[0].value.url", "[0].value.url", "[0].value.url", "[1][0]", "$[3]"};
    bool expect_null[] = {true, false, true, true, false, true};
    std::string expected[] = {"", "http://x", "", "", "1", ""};

    for (int j = 0; j < std::size(values); ++j) {
        jsons->append(values[j]);
        paths->append(strs[j]);
    }
    columns.emplace_back(jsons);
    columns.emplace_back(paths);

    ctx.get()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns).value();
    auto v = ColumnHelper::as_column<NullableColumn>(result);
    ASSERT_EQ(std::size(values), v->size());
    for (int j = 0; j < std::size(values); ++j) {
        ASSERT_EQ(expect_null[j], v->is_null(j)) << "row " << j;
        if (!expect_null[j]) {
            ASSERT_EQ(expected[j], v->get(j).get_slice().to_string()) << "row " << j;
        }
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, get_json_string_invalid_json_respects_allow_throw_exception) {
    Columns columns;
    auto strings = BinaryColumn::create();
    auto paths = BinaryColumn::create();
    strings->append(std::string(kJSONLengthLimit + 1, '{'));
    paths->append("$.a");
    columns.emplace_back(strings);
    columns.emplace_back(paths);

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto result = JsonFunctions::get_json_string(ctx.get(), columns);
        ASSERT_TRUE(result.ok()) << result.status();
        ASSERT_EQ(1, ColumnHelper::count_nulls(result.value()));
    }

    {
        TQueryOptions query_options;
        query_options.__set_allow_throw_exception(true);
        TQueryGlobals query_globals;
        RuntimeState state(TUniqueId(), query_options, query_globals, nullptr);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ctx->set_runtime_state(&state);

        auto result = JsonFunctions::get_json_string(ctx.get(), columns);
        ASSERT_FALSE(result.ok());
        ASSERT_EQ("JSON string exceed maximum length 16MB", result.status().message());
    }
}

TEST_F(JsonFunctionsTest, get_json_emptyTest) {
    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto doubles = BinaryColumn::create();
        auto doubles2 = BinaryColumn::create();

        std::string values[] = {R"({"k1":1.3, "k2":"2"})", R"({"k1":"v1", "my.key":[1.1, 2.2, 3.3]})"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            doubles->append(values[j]);
            doubles2->append(strs[j]);
        }

        columns.emplace_back(doubles);
        columns.emplace_back(doubles2);

        ctx.get()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        ColumnPtr result = JsonFunctions::get_json_double(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto str_values = BinaryColumn::create();
        auto str_values2 = BinaryColumn::create();

        std::string values[] = {R"({"k1":1.3, "k2":"2"})", R"({"k1":"v1", "my.key":[1.1, 2.2, 3.3]})"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            str_values->append(values[j]);
            str_values2->append(strs[j]);
        }

        columns.emplace_back(str_values);
        columns.emplace_back(str_values2);

        ctx.get()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto ints = BinaryColumn::create();
        auto ints2 = BinaryColumn::create();

        std::string values[] = {R"({"k1":1.3, "k2":"2"})", R"({"k1":"v1", "my.key":[1.1, 2.2, 3.3]})"};
        std::string strs[] = {"", ""};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ints->append(values[j]);
            ints2->append(strs[j]);
        }

        columns.emplace_back(ints);
        columns.emplace_back(ints2);

        ctx.get()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        ColumnPtr result = JsonFunctions::get_json_int(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

    {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        Columns columns;
        auto ints = BinaryColumn::create();
        auto ints2 = BinaryColumn::create();

        std::string values[] = {R"({"k1":1.3, "k2":"2"})", R"({"k1":"v1", "my.key":[1.1, 2.2, 3.3]})"};
        std::string strs[] = {"$.k3", "$.k4"};

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ints->append(values[j]);
            ints2->append(strs[j]);
        }

        columns.emplace_back(ints);
        columns.emplace_back(ints2);

        ctx.get()->set_constant_columns(columns);
        ASSERT_TRUE(
                JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

        ColumnPtr result = JsonFunctions::get_json_int(ctx.get(), columns).value();

        auto v = ColumnHelper::as_column<NullableColumn>(result);

        for (int j = 0; j < sizeof(values) / sizeof(values[0]); ++j) {
            ASSERT_TRUE(v->is_null(j));
        }

        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }
}

// Test compatibility of float and double
TEST_F(JsonFunctionsTest, json_float_double) {
    namespace vpack = arangodb::velocypack;
    double x = 1.2;
    JsonValue json = JsonValue::from_double(static_cast<double>(x));
    vpack::Slice vslice = json.to_vslice();
    ASSERT_DOUBLE_EQ(1.2, vslice.getDouble());

    // check to_string
    auto json_str = json.to_string();
    ASSERT_TRUE(json_str.ok());
    ASSERT_EQ("1.2", json_str.value());
}

class JsonQueryTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string>> {};

TEST_P(JsonQueryTestFixture, json_query) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto ints = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json = std::get<0>(GetParam());
    std::string param_path = std::get<1>(GetParam());
    std::string param_result = std::get<2>(GetParam());

    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(param_json, &json).ok());
    ints->append(&json);
    if (param_path == "NULL") {
        builder.append_null();
    } else {
        builder.append(param_path);
    }

    Columns columns{ints, builder.build(true)};

    ctx.get()->set_constant_columns(columns);
    std::ignore =
            JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

    ColumnPtr result = JsonFunctions::json_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    StripWhiteSpace(&param_result);
    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        auto st = datum.get_json()->to_string();
        ASSERT_TRUE(st.ok()) << st->c_str();
        std::string json_result = datum.get_json()->to_string().value();
        StripWhiteSpace(&json_result);
        ASSERT_EQ(param_result, json_result);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

INSTANTIATE_TEST_SUITE_P(
        JsonQueryTest, JsonQueryTestFixture,
        ::testing::Values(
                // clang-format off
                // empty
                std::make_tuple(R"( {"k1":1} )", "NULL", R"(NULL)"),
                std::make_tuple(R"( {"k1":1} )", "$", R"( {"k1": 1} )"),
                std::make_tuple(R"( {"k1":1} )", "", R"(NULL)"),

                // various types
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "$.k2", R"( "hehe" )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "$.k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "$.k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", "$.k4", R"( {} )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", "$.k5", R"( NULL )"),

                // simple syntax
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "k2", R"( "hehe" )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", "k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", "k4", R"( {} )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", "k5", R"( NULL )"),

                // nested array
                std::make_tuple(R"( {"k1": [1,2,3]} )", "$.k1[0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [1,2,3]} )", "$.k1[3]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [1,2,3]} )", "$.k1[-1]", R"( NULL )"),
                // index beyond a short or empty array must yield NULL without throwing per row
                std::make_tuple(R"( [] )", "$[0]", R"( NULL )"),
                std::make_tuple(R"( {"k1": []} )", "$.k1[0]", R"( NULL )"),
                std::make_tuple(R"( {"k1": []} )", "$.k1[7]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[]]} )", "$.k1[0][0]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [{"value": {"url": "u"}}]} )", "$.k1[0].value.url", R"( "u" )"),
                std::make_tuple(R"( {"k1": [{"value": {"url": "u"}}]} )", "$.k1[1].value.url", R"( NULL )"),
                std::make_tuple(R"( {"k1": [{"value": {}}]} )", "$.k1[0].value.url", R"( NULL )"),
                std::make_tuple(R"( {"k1": [1,2,3,4,5,6,7,8,9,10]} )", "$.k1[9]", R"( 10 )"),
                std::make_tuple(R"( {"k1": [1,2,3,4,5,6,7,8,9,10]} )", "$.k1[10]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [1,"a",[2],{"b":3},null,4.5,6,7,8,9]} )", "$.k1[9]", R"( 9 )"),
                std::make_tuple(R"( {"k1": [1,"a",[2],{"b":3},null,4.5,6,7,8,9]} )", "$.k1[10]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[0][1]", R"( 2 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[0][2]", R"( 3 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[0][3]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[1][0]", R"( 4 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[1][2]", R"( 6 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[2][0]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[2]]]]]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", "$.k1[[[[[2]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[[1,2,3]]]} )", "$.k1[0][0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": [[1, 2], [3, 4]] }] } )", "$.k1[0].k2[0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": [[1, 2], [3, 4]] }] } )", "$.k1[0].k2[1][0]", R"( 3 )"),

                // nested object
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "$.k1", R"( {"k2": {"k3": 1}} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "$.k1.k2", R"( {"k3": 1} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "$.k1.k2.k3", R"( 1 )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "$.k1.k2.k3.k4", R"( NULL )"),

                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "k1", R"( {"k2": {"k3": 1}} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "k1.k2", R"( {"k3": 1} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "k1.k2.k3", R"( 1 )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", "k1.k2.k3.k4", R"( NULL )"),

                // nested object in array
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", "$.k1[0]", R"( {"k2": 1} )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", "$.k1[0].k2", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", "$.k1[0].k3", R"( NULL )"),

                // Top Level Array
                std::make_tuple(R"( [1,2,3] )", "$[1]", R"( 2 )"), 
                std::make_tuple(R"( [1,2,3] )", "$[5]", R"( NULL )"),
                std::make_tuple(R"( [1,2,3] )", "[1]",  R"( 2 )"), 
                std::make_tuple(R"( [1,2,3] )", "[5]",  R"( NULL )"),
                std::make_tuple(R"( [1,2,3] )", "[*]",  R"( [1, 2, 3] )"),
                std::make_tuple(R"( [1,2,3] )", "[*].k1",  R"( [] )"),
                std::make_tuple(R"( [{"k1": 1}, {"k1": 2}] )", "$[0]",      R"( {"k1": 1} )"),
                std::make_tuple(R"( [{"k1": 1}, {"k1": 2}] )", "$[0].k1",   R"( 1 )"),
                std::make_tuple(R"( [{"k1": 1}, {"k1": 2}] )", "$[*].k1",   R"( [1, 2] )"),
                std::make_tuple(R"( [{"k1": 1}, {"k2": 2}] )", "$[*].k1",   R"( [1] )"),
                std::make_tuple(R"( [{"k1": 1}, {"k2": 2}] )", "$[*].k2",   R"( [2] )"),
                std::make_tuple(R"( [{"k1": 1}, {"k2": 2}] )", "$[*]",      R"( [{"k1": 1}, {"k2": 2}] )"),
                std::make_tuple(R"( [[[0, 1, 2]]] )", "$[0][0][0]",      R"( 0 )"),
                std::make_tuple(R"( [[[0, 1, 2]]] )", "$[0][0][1]",      R"( 1 )"),

                // array result
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}]} )", "$.k1[*].k2", R"( [1, 2] )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}]} )", "$.k1[*]", R"( [{"k2": 1}, {"k2": 2}] )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}, {"k2": 3}]} )", "$.k1[0:2]",
                                R"( [{"k2": 1}, {"k2": 2}] )"),
                std::make_tuple(R"( {"k1": [1,2,3,4]} )", "$.k1[*]", R"( [1, 2, 3, 4] )"),
                std::make_tuple(R"( {"k1": [1,2,3,4]} )", "$.k1[1:3]", R"( [2, 3] )")
                // clang-format on
                ));

class FlatJsonQueryTestFixture
        : public ::testing::TestWithParam<std::tuple<std::string, std::vector<std::string>, std::string, std::string>> {
};

TEST_P(FlatJsonQueryTestFixture, json_query) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json = std::get<0>(GetParam());
    std::vector<std::string> param_flat_path = std::get<1>(GetParam());

    std::string param_path = std::get<2>(GetParam());
    std::string param_result = std::get<3>(GetParam());

    JsonValue json;
    ASSERT_TRUE(JsonValue::parse(param_json, &json).ok());
    json_col->append(&json);
    if (param_path == "NULL") {
        builder.append_null();
    } else {
        builder.append(param_path);
    }

    auto flat_json = JsonColumn::create();
    auto flat_json_ptr = flat_json.get();
    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns{flat_json, builder.build(true)};

    ctx.get()->set_constant_columns(columns);
    std::ignore =
            JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

    ColumnPtr result = JsonFunctions::json_query(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    StripWhiteSpace(&param_result);
    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        auto st = datum.get_json()->to_string();
        ASSERT_TRUE(st.ok()) << st->c_str();
        std::string json_result = datum.get_json()->to_string().value();
        StripWhiteSpace(&json_result);
        ASSERT_EQ(param_result, json_result);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

INSTANTIATE_TEST_SUITE_P(
        FlatJsonQueryTest, FlatJsonQueryTestFixture,
        ::testing::Values(
                // clang-format off
                // empty
                std::make_tuple(R"( {"k1":1} )", std::vector<std::string>{"k1"},  "NULL", R"(NULL)"),

                // various types
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, "$.k2", R"( "hehe" )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, "$.k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, "$.k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", std::vector<std::string>{"k1", "k2", "k4"}, "$.k4", R"( {} )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", std::vector<std::string>{"k1", "k2", "k5"}, "$.k5", R"( NULL )"),

                // simple syntax
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, "k2", R"( "hehe" )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, "k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1]} )", std::vector<std::string>{"k1", "k2", "k3"}, "k3", R"( [1] )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", std::vector<std::string>{"k1", "k4", "k3"}, "k4", R"( {} )"),
                std::make_tuple(R"( {"k1":1, "k2":"hehe", "k3":[1], "k4": {}} )", std::vector<std::string>{"k1", "k5", "k3"}, "k5", R"( NULL )"),

                // nested array
                std::make_tuple(R"( {"k1": [1,2,3]} )", std::vector<std::string>{"k1"}, "$.k1[0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [1,2,3]} )", std::vector<std::string>{"k1"}, "$.k1[3]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[0][1]", R"( 2 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[0][2]", R"( 3 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[0][3]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[1][0]", R"( 4 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[1][2]", R"( 6 )"),
                std::make_tuple(R"( {"k1": [[1,2,3], [4,5,6]]} )", std::vector<std::string>{"k1"}, "$.k1[2][0]", R"( NULL )"),
                std::make_tuple(R"( {"k1": [[[1,2,3]]]} )", std::vector<std::string>{"k1"}, "$.k1[0][0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": [[1, 2], [3, 4]] }] } )", std::vector<std::string>{"k1"}, "$.k1[0].k2[0][0]", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": [[1, 2], [3, 4]] }] } )", std::vector<std::string>{"k1"}, "$.k1[0].k2[1][0]", R"( 3 )"),

                // nested object
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, "$.k1", R"( {"k2": {"k3": 1}} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, "$.k1.k2", R"( {"k3": 1} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, "$.k1.k2.k3", R"( 1 )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, "$.k1.k2.k3.k4", R"( NULL )"),

                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, "k1", R"( {"k2": {"k3": 1}} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, "k1.k2", R"( {"k3": 1} )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, "k1.k2.k3", R"( 1 )"),
                std::make_tuple(R"( {"k1": {"k2": {"k3": 1}}} )", std::vector<std::string>{"k1"}, "k1.k2.k3.k4", R"( NULL )"),

                // nested object in array
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", std::vector<std::string>{"k1"}, "$.k1[0]", R"( {"k2": 1} )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", std::vector<std::string>{"k1"}, "$.k1[0].k2", R"( 1 )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}]} )", std::vector<std::string>{"k1"}, "$.k1[0].k3", R"( NULL )"),

                // array result
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}]} )", std::vector<std::string>{"k1"}, "$.k1[*].k2", R"( [1, 2] )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}]} )", std::vector<std::string>{"k1"}, "$.k1[*]", R"( [{"k2": 1}, {"k2": 2}] )"),
                std::make_tuple(R"( {"k1": [{"k2": 1}, {"k2": 2}, {"k2": 3}]} )", std::vector<std::string>{"k1"}, "$.k1[0:2]",
                                R"( [{"k2": 1}, {"k2": 2}] )"),
                std::make_tuple(R"( {"k1": [1,2,3,4]} )", std::vector<std::string>{"k1"}, "$.k1[*]", R"( [1, 2, 3, 4] )"),
                std::make_tuple(R"( {"k1": [1,2,3,4]} )", std::vector<std::string>{"k1"}, "$.k1[1:3]", R"( [2, 3] )")
                // clang-format on
                ));

class JsonExistTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, bool>> {};

TEST_P(JsonExistTestFixture, json_exists) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto ints = JsonColumn::create();
    ColumnBuilder<TYPE_VARCHAR> builder(1);

    std::string param_json = std::get<0>(GetParam());
    std::string param_result = std::get<1>(GetParam());
    bool param_exist = std::get<2>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    ints->append(&*json);
    if (param_result == "NULL") {
        builder.append_null();
    } else {
        builder.append(param_result);
    }

    Columns columns{ints, builder.build(true)};

    ctx.get()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    if (!st.ok()) {
        ASSERT_FALSE(param_exist);
        return;
    }

    ColumnPtr result = JsonFunctions::json_exists(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    if (param_exist) {
        ASSERT_TRUE((bool)result->get(0).get_uint8());
    } else {
        ASSERT_TRUE(result->get(0).is_null() || !(bool)result->get(0).get_uint8());
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

INSTANTIATE_TEST_SUITE_P(JsonExistTest, JsonExistTestFixture,
                         ::testing::Values(std::make_tuple(R"({ "k1":1, "k2":"2"})", "$.k1", true),
                                           std::make_tuple(R"({ "k1": [1,2,3]})", "$.k1", true),
                                           std::make_tuple(R"({ "k1": {}})", "$.k1", true),
                                           std::make_tuple(R"({ "k1": {}})", "$.k1", true),
                                           // nested object
                                           std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", "$.k1.k2.k3", true),
                                           std::make_tuple(R"({"k1": [1]})", "$.k1[0]", true),

                                           // object in array
                                           std::make_tuple(R"({"k1": [{"k2": 1}]})", "$.k1[0].k2", true),

                                           // not exists
                                           std::make_tuple(R"({ })", "$.k1", false),
                                           std::make_tuple(R"({"k1": 1})", "$.k2", false),
                                           std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", "$.k1.k2.k3.k4", false),
                                           std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", "$.k1.k2.k4", false),
                                           std::make_tuple(R"({"k1": [{"k2": 1}]})", "$.k1[0].k3", false),

                                           //  nested array
                                           std::make_tuple(R"({"k1": [[1]]})", "$.k1[0][1]", false),
                                           std::make_tuple(R"({"k1": [[1]]})", "$.k1[0][0]", true),

                                           // special case
                                           std::make_tuple(R"({ "k1": {}})", "$", true),
                                           std::make_tuple(R"({ "k1": {}})", "", false),
                                           std::make_tuple(R"( { "k1": 1} )", "NULL", false),

                                           // error case
                                           std::make_tuple(R"( {"k1": null} )", std::string(10, 0x1), false)));

class FlatJsonExistsTestFixture
        : public ::testing::TestWithParam<std::tuple<std::string, std::vector<std::string>, std::string, bool>> {};

TEST_P(FlatJsonExistsTestFixture, flat_json_exists_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = std::get<0>(GetParam());
    std::vector<std::string> param_flat_path = std::get<1>(GetParam());
    std::string param_path = std::get<2>(GetParam());
    bool param_exist = std::get<3>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.emplace_back(flat_json);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.emplace_back(path_column);
    }

    ctx.get()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    if (!st.ok()) {
        ASSERT_FALSE(param_exist);
        return;
    }

    ASSIGN_OR_ABORT(ColumnPtr result, JsonFunctions::json_exists(ctx.get(), columns))
    ASSERT_TRUE(!!result);

    if (param_exist) {
        ASSERT_TRUE((bool)result->get(0).get_uint8());
    } else {
        ASSERT_TRUE(result->get(0).is_null() || !(bool)result->get(0).get_uint8());
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(FlatJsonExistsTest, FlatJsonExistsTestFixture,
                          ::testing::Values(std::make_tuple(R"({ "k1":1, "k2":"2"})", std::vector<std::string>{"k1", "k2"}, "$.k1", true),
                                            std::make_tuple(R"({ "k1": [1,2,3]})", std::vector<std::string>({"k1", "k2"}),"$.k1", true),
                                            std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", std::vector<std::string>({"k1"}), "$.k1.k2.k3", true),
                                            std::make_tuple(R"({"k1": [{"k2": 1}]})", std::vector<std::string>({"k1"}), "$.k1[0].k2", true),
                                            std::make_tuple(R"({"k1": [{"k2": 1}]})", std::vector<std::string>({"k1"}), "$.k1[*].k2", true),
                                            std::make_tuple(R"({"k1": [{"k2": 1}]})", std::vector<std::string>({"k1"}), "$.k1[0:2].k2", true),
                                            std::make_tuple(R"({ })", std::vector<std::string>({"k1"}), "$.k1", false),
                                            std::make_tuple(R"({"k1": 1})", std::vector<std::string>({"k2"}), "$.k2", false),
                                            std::make_tuple(R"({"k1": {"k2": {"k3": 1}}})", std::vector<std::string>({"k1"}), "$.k1.k2.k3.k4", false),
                                            std::make_tuple(R"({"k1": [{"k2": 1}]})", std::vector<std::string>({"k1"}), "$.k1[0].k3", false),
                                            //  nested array
                                            std::make_tuple(R"({"k1": [[1]]})",std::vector<std::string>({"k1"}), "$.k1[0][1]", false),
                                            std::make_tuple(R"({"k1": [[1]]})",std::vector<std::string>({"k1"}), "$.k1[0][0]", true),
                                            // special case
                                            std::make_tuple(R"([{"k1": 1}, {"k2": 2}])",std::vector<std::string>({"k1"}), "$.k1[1]", false),
                                            std::make_tuple(R"("k1")",std::vector<std::string>({"k1"}), "$.k1", false)
                        ));
// clang-format on

TEST_F(JsonFunctionsTest, flat_json_invalid_path_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = R"({"k1":1, "k2":"2"})";
    std::vector<std::string> param_flat_path = std::vector<std::string>{"k1", "k2"};
    std::string param_path = "$.k3";

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.emplace_back(flat_json);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.emplace_back(ConstColumn::create(path_column));
    }

    ctx.get()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    if (!st.ok()) {
        return;
    }

    config::enable_lazy_dynamic_flat_json = false;
    auto ret = JsonFunctions::json_exists(ctx.get(), columns);
    config::enable_lazy_dynamic_flat_json = true;
    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ASSERT_FALSE(ret.ok());
}

TEST_F(JsonFunctionsTest, flat_json_invalid_constant_json_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = R"({"k1":1, "k2":"2"})";
    std::vector<std::string> param_flat_path = std::vector<std::string>{"k1", "k2"};
    std::string param_path = "$.k3";

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.emplace_back(ConstColumn::create(flat_json, 2));
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.emplace_back(ConstColumn::create(path_column, 2));
    }

    ctx.get()->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    auto ret = JsonFunctions::json_exists(ctx.get(), columns);
    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ASSERT_FALSE(ret.ok());
}

TEST_F(JsonFunctionsTest, flat_json_variable_path_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = R"({"k1":1, "k2":"2"})";
    std::vector<std::string> param_flat_path = std::vector<std::string>{"k1", "k2"};
    std::string param_path = "$.k2";

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.emplace_back(flat_json);
    flat_json->assign(10, 0);

    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        path_column->assign(10, 0);
        columns.emplace_back(path_column);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
    ASSIGN_OR_ABORT(ColumnPtr result, JsonFunctions::json_exists(ctx.get(), columns))
    ASSERT_TRUE((bool)result->get(0).get_uint8());

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

TEST_F(JsonFunctionsTest, flat_json_invalid_variable_path_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = R"({"k1":1, "k2":"2"})";
    std::vector<std::string> param_flat_path = std::vector<std::string>{"k1", "k2"};
    std::string param_path = "$.k2";

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.emplace_back(flat_json);
    flat_json->assign(2, 0);

    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        path_column->append("$.k3");
        columns.emplace_back(path_column);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    auto ret = JsonFunctions::json_exists(ctx.get(), columns);

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ASSERT_FALSE(ret.ok());
}

TEST_F(JsonFunctionsTest, flat_json_invalid_null_path_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = R"({"k1":1, "k2":"2"})";
    std::vector<std::string> param_flat_path = std::vector<std::string>{"k1", "k2"};
    std::string param_path = "$.k2";

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.emplace_back(flat_json);
    flat_json->assign(2, 0);

    if (!param_path.empty()) {
        auto path_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        path_column->append_nulls(2);
        columns.emplace_back(path_column);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    auto ret = JsonFunctions::json_exists(ctx.get(), columns);

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ASSERT_FALSE(ret.ok());
}

TEST_F(JsonFunctionsTest, flat_json_constant_path_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = R"({"k1":1, "k2":"2"})";
    std::vector<std::string> param_flat_path = std::vector<std::string>{"k1", "k2"};
    std::string param_path = "$.k2";

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.emplace_back(flat_json);

    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.emplace_back(ConstColumn::create(path_column, 1));
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ASSIGN_OR_ABORT(ColumnPtr result, JsonFunctions::json_exists(ctx.get(), columns))
    ASSERT_TRUE((bool)result->get(0).get_uint8());

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

class JsonParseTestFixture : public ::testing::TestWithParam<std::tuple<std::string, bool, std::string>> {};

TEST_P(JsonParseTestFixture, json_parse) {
    auto [param_json, param_ok, expected] = GetParam();
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto ints = BinaryColumn::create();
    ints->append(param_json);
    Columns columns{ints};
    ctx.get()->set_constant_columns(columns);

    ColumnPtr result = JsonFunctions::parse_json(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    Datum datum = result->get(0);
    if (param_ok) {
        ASSERT_FALSE(datum.is_null());
        const JsonValue* json_value = datum.get_json();
        ASSERT_TRUE(!!json_value);
        std::string json_str = json_value->to_string().value();
        StripWhiteSpace(&json_str);
        StripWhiteSpace(&param_json);
        if (!expected.empty()) {
            ASSERT_EQ(expected, param_json);
        } else {
            ASSERT_EQ(param_json, json_str);
        }
    } else {
        ASSERT_TRUE(datum.is_null());
    }
}

INSTANTIATE_TEST_SUITE_P(JsonParseTest, JsonParseTestFixture,
                         ::testing::Values(
                                 // clang-format off
                                 
                                 // Parse as json object/array
                                 std::make_tuple(R"( {"k1": 1} )", true, ""), 
                                 std::make_tuple(R"( [1, 2, 3] )", true, ""), 
                                 std::make_tuple(R"( [] )", true, ""),
                                 std::make_tuple(R"( "a" )", true, ""),
                                 std::make_tuple(R"( "1" )", true, ""),

                                 // Parse as string
                                 std::make_tuple(R"( 2.1 )", true, R"(2.1)"), 
                                 std::make_tuple(R"( 1 )", true, R"(1)"), 
                                 std::make_tuple(R"( 1e5 )", true, R"(1e5)"), 
                                 std::make_tuple(R"( a1 )", true, R"(a1)"), 
                                 std::make_tuple(R"( 1a )", true, R"(1a)"), 
                                 std::make_tuple(R"( 1+1 )", true, "1+1"),
                                 std::make_tuple(R"( 2.x )", true, "2.x"),
                                 std::make_tuple(R"( nul )", true, "nul"),

                                 // failure
                                 std::make_tuple(R"( {"k1": 1 )", false, ""),
                                 std::make_tuple(R"( [,,,,,,] )", false, ""),
                                 std::make_tuple(R"( [1, )", false, ""),
                                 std::make_tuple(R"( "1 )", false, "")

                                 // clang-format on
                                 ));

class JsonArrayTestFixture : public ::testing::TestWithParam<std::tuple<std::vector<std::string>, std::string>> {};

TEST_P(JsonArrayTestFixture, json_array) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    std::vector<std::string> param_json = std::get<0>(GetParam());
    std::string param_result = std::get<1>(GetParam());
    for (const auto& json_str : param_json) {
        auto column = JsonColumn::create();
        auto json = JsonValue::parse(json_str);
        ASSERT_TRUE(json.ok());
        column->append(&json.value());
        columns.emplace_back(std::move(column));
    }

    ctx.get()->set_constant_columns(columns);

    ColumnPtr result;
    if (param_json.empty()) {
        result = JsonFunctions::json_array_empty(ctx.get(), columns).value();
    } else {
        result = JsonFunctions::json_array(ctx.get(), columns).value();
    }
    ASSERT_TRUE(!!result);

    Datum datum = result->get(0);
    ASSERT_FALSE(datum.is_null());
    std::string json_str = datum.get_json()->to_string().value();
    StripWhiteSpace(&json_str);
    StripWhiteSpace(&param_result);
    ASSERT_EQ(param_result, json_str);
}

INSTANTIATE_TEST_SUITE_P(
        JsonArrayTest, JsonArrayTestFixture,
        ::testing::Values(std::make_tuple(std::vector<std::string>{}, "[]"),
                          std::make_tuple(std::vector<std::string>{"1", "2"}, R"(["1", "2"])"),
                          std::make_tuple(std::vector<std::string>{"1", "\"1\""}, R"(["1", "1"])"),
                          std::make_tuple(std::vector<std::string>{"1", R"({"a":1})"}, R"(["1", {"a": 1}])"),
                          std::make_tuple(std::vector<std::string>{"null", R"(1)"}, R"(["null", "1"])"),
                          std::make_tuple(std::vector<std::string>{"null", R"(null)"}, R"(["null", "null"])"),
                          std::make_tuple(std::vector<std::string>{"1", "1", "1"}, R"(["1", "1", "1"])"),
                          std::make_tuple(std::vector<std::string>{"1.1", "1.2"}, R"(["1.1", "1.2"])")));

using JsonObjectTestParam = std::tuple<std::vector<std::string>, std::string>;

class JsonObjectTestFixture : public ::testing::TestWithParam<JsonObjectTestParam> {};

TEST_P(JsonObjectTestFixture, json_object) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    Columns columns;
    std::vector<std::string> param_json = std::get<0>(GetParam());
    std::string param_result = std::get<1>(GetParam());
    for (const auto& json_str : param_json) {
        auto column = JsonColumn::create();
        auto json = JsonValue::parse(json_str);
        ASSERT_TRUE(json.ok()) << "parse json failed: " << json_str;
        column->append(&json.value());
        columns.emplace_back(std::move(column));
    }

    ctx.get()->set_constant_columns(columns);

    ColumnPtr result;
    if (param_json.empty()) {
        result = JsonFunctions::json_object_empty(ctx.get(), columns).value();
    } else {
        result = JsonFunctions::json_object(ctx.get(), columns).value();
    }
    ASSERT_TRUE(!!result);

    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
        return;
    }
    ASSERT_FALSE(datum.is_null());
    std::string json_str = datum.get_json()->to_string().value();
    StripWhiteSpace(&json_str);
    StripWhiteSpace(&param_result);
    ASSERT_EQ(param_result, json_str);
}

INSTANTIATE_TEST_SUITE_P(JsonObjectTest, JsonObjectTestFixture,
                         ::testing::Values(
                                 // clang-format off
                          JsonObjectTestParam(std::vector<std::string>{}, "{}"),
                          JsonObjectTestParam({R"("a")", "1", R"("b")", ""}, R"( {"a": "1", "b": ""} )"),
                          JsonObjectTestParam({R"("a")"}, R"({"a": null})"),
                          JsonObjectTestParam({R"("a")", R"("a")", R"("a")"}, R"({"a": "a", "a": null})"),
                          JsonObjectTestParam({R"("a")", R"("a")"}, R"({"a": "a"})"),
                          JsonObjectTestParam({R"("a")", "1"}, R"({"a": "1"})"),
                          JsonObjectTestParam({R"("a")", "1.234"}, R"({"a": "1.234"})"),
                          JsonObjectTestParam({R"("a")", "null"}, R"({"a": "null"})"),
                          JsonObjectTestParam({R"("a")", "true"}, R"({"a": "true"})"),
                          JsonObjectTestParam({R"("a")", "1", R"("b")", "2"}, R"({"a": "1", "b": "2"})"),
                          JsonObjectTestParam({R"("a")", "[1,2]"}, R"({"a": [1, 2]})"),
                          JsonObjectTestParam({R"("a")", R"({"b": 2})"}, R"({"a": {"b": 2}})"),

                          // illegal object
                          JsonObjectTestParam({"1", "1"}, R"({"1": "1"})"), 
                          JsonObjectTestParam({"1"}, R"({"1": null})"),
                          JsonObjectTestParam({R"("a")", "1", "1"}, R"({"1": null, "a": "1"})"),
                          JsonObjectTestParam({R"("")"}, R"(NULL)")

                                 // clang-format on
                                 ));

TEST_F(JsonFunctionsTest, extract_from_object_test) {
    std::string output;

    EXPECT_OK(test_extract_from_object(R"({"data" : 1})", "$.data", &output));
    EXPECT_STREQ(output.data(), "1");

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data" : 1})", "$.dataa", &output));

    EXPECT_OK(test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[1].key", &output));
    EXPECT_STREQ(output.data(), "2");

    EXPECT_STATUS(Status::NotFound(""),
                  test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[2].key", &output));

    EXPECT_STATUS(Status::NotFound(""),
                  test_extract_from_object(R"({"data": [{"key": 1},{"key": 2}]})", "$.data[3].key", &output));

    EXPECT_STATUS(Status::DataQualityError(""),
                  test_extract_from_object(R"({"data1 " : 1, "data2":})", "$.data", &output));

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data": null})", "$.data", &output));
    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data": null})", "$.data.key", &output));

    EXPECT_OK(test_extract_from_object(R"({"data": {}})", "$.data", &output));
    EXPECT_STREQ(output.data(), "{}");
    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data": {}})", "$.data.key", &output));

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"data": 1})", "$.data.key", &output));

    EXPECT_OK(test_extract_from_object(R"({"key1": [1,2]})", "$.key1[1]", &output));
    EXPECT_STREQ(output.data(), "2");

    EXPECT_OK(test_extract_from_object(R"({"key1": [{"key2":3},{"key4": 5}]})", "$.key1[1].key4", &output));
    EXPECT_STREQ(output.data(), "5");

    EXPECT_STATUS(Status::NotFound(""), test_extract_from_object(R"({"key1": null})", "$.key1[1].key4", &output));
}

TEST_F(JsonFunctionsTest, parse_json_paths_simple_syntax_keeps_first_segment) {
    // "order.id" must mean "$.order.id": the implicit-root rewrite must not drop the first token,
    // or the path silently resolves to "$.id" and extracts the wrong field.
    std::vector<SimpleJsonPath> paths;
    ASSERT_TRUE(JsonFunctions::parse_json_paths("order.id", &paths).ok());
    ASSERT_EQ(3u, paths.size());
    EXPECT_EQ("order", paths[1].key);
    EXPECT_EQ("id", paths[2].key);

    std::string output;
    EXPECT_OK(test_extract_from_object(R"({"order": {"id": 42}, "id": 7})", "order.id", &output));
    EXPECT_STREQ(output.data(), "42");

    // A single-token simple path used to parse into the root placeholder alone and fail as an
    // "empty json path"; it must select the field.
    EXPECT_OK(test_extract_from_object(R"({"k1": 1, "k2": 2})", "k2", &output));
    EXPECT_STREQ(output.data(), "2");

    EXPECT_OK(test_extract_from_object(R"({"arr": [1, 2]})", "arr[1]", &output));
    EXPECT_STREQ(output.data(), "2");
}

class JsonLengthTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, int>> {};

TEST_P(JsonLengthTestFixture, json_length_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();

    std::string param_json = std::get<0>(GetParam());
    std::string param_path = std::get<1>(GetParam());
    int expect_length = std::get<2>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_column->append(&*json);

    Columns columns;
    columns.emplace_back(json_column);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.emplace_back(path_column);
    }

    // ctx.get()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    ASSERT_OK(st);

    ColumnPtr result = JsonFunctions::json_length(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    EXPECT_EQ(expect_length, result->get(0).get_int32());

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(JsonLengthTest, JsonLengthTestFixture,
                         ::testing::Values(
                            std::make_tuple(R"({ "k1":1, "k2": 2 })", "", 2), 
                            std::make_tuple(R"({ "k1":1, "k2": {} })", "$.k2", 0), 
                            std::make_tuple(R"({ "k1":1, "k2": [1,2] })", "$.k2", 2), 
                            std::make_tuple(R"({ "k1":1, "k2": [1,2] })", "$.k3", 0), 
                            std::make_tuple(R"({ })", "", 0),
                            std::make_tuple(R"( [] )", "", 0), 
                            std::make_tuple(R"( [1] )", "", 1),
                            std::make_tuple(R"( null )", "", 1), 
                            std::make_tuple(R"( 1 )", "", 1)
                        ));
// clang-format on

class FlatJsonLengthTestFixture
        : public ::testing::TestWithParam<std::tuple<std::string, std::vector<std::string>, std::string, int>> {};

TEST_P(FlatJsonLengthTestFixture, flat_json_length_test) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = JsonColumn::create();

    std::string param_json = std::get<0>(GetParam());
    std::vector<std::string> param_flat_path = std::get<1>(GetParam());
    std::string param_path = std::get<2>(GetParam());
    int expect_length = std::get<3>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_col->append(&*json);

    Columns flat_columns;

    auto flat_json = JsonColumn::create();
    auto* flat_json_ptr = down_cast<JsonColumn*>(flat_json.get());

    std::vector<LogicalType> param_flat_type;
    for (auto _ : param_flat_path) {
        param_flat_type.emplace_back(LogicalType::TYPE_JSON);
    }
    JsonFlattener jf(param_flat_path, param_flat_type, false);
    jf.flatten(json_col.get());
    flat_json_ptr->set_flat_columns(param_flat_path, param_flat_type, jf.mutable_result());

    Columns columns;
    columns.emplace_back(flat_json);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.emplace_back(path_column);
    }

    // ctx.get()->set_constant_columns(columns);
    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    ASSERT_OK(st);

    ASSIGN_OR_ABORT(ColumnPtr result, JsonFunctions::json_length(ctx.get(), columns));
    ASSERT_TRUE(!!result);
    EXPECT_EQ(expect_length, result->get(0).get_int32());

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(FlatJsonLengthTest, FlatJsonLengthTestFixture,
                         ::testing::Values(
                            std::make_tuple(R"({ "k1":1, "k2": {} })", std::vector<std::string>({"k1", "k2"}), "$.k2", 0), 
                            std::make_tuple(R"({ "k1":1, "k2": [1,2] })", std::vector<std::string>({"k1", "k2"}), "$.k2", 2), 
                            std::make_tuple(R"({ "k1":1, "k2": [1,2] })", std::vector<std::string>({"k1", "k2", "k3"}), "$.k3", 0),
                            std::make_tuple(R"({ "k1":1, "k2": {"xx": 1} })", std::vector<std::string>{"k1", "k2"}, "$.k1", 1)
                        ));
// clang-format on

class JsonKeysTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, std::string>> {};

TEST_P(JsonKeysTestFixture, json_keys) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_column = JsonColumn::create();

    std::string param_json = std::get<0>(GetParam());
    std::string param_path = std::get<1>(GetParam());
    std::string param_result = std::get<2>(GetParam());

    auto json = JsonValue::parse(param_json);
    ASSERT_TRUE(json.ok());
    json_column->append(&*json);

    Columns columns;
    columns.emplace_back(json_column);
    if (!param_path.empty()) {
        auto path_column = BinaryColumn::create();
        path_column->append(param_path);
        columns.emplace_back(path_column);
    }

    Status st = JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    ASSERT_OK(st);

    ColumnPtr result = JsonFunctions::json_keys(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    if (param_result == "NULL") {
        EXPECT_TRUE(result->is_null(0));
    } else {
        const JsonValue* keys = result->get(0).get_json();
        std::string keys_str = keys->to_string_uncheck();
        EXPECT_EQ(param_result, keys_str);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(JsonKeysTest, JsonKeysTestFixture,
                         ::testing::Values(std::make_tuple(R"({ "k1": 1, "k2": 2 })", "", R"(["k1", "k2"])"),
                                           std::make_tuple(R"({ "k1": "v1" })",  "", R"(["k1"])"),
                                           std::make_tuple(R"({ "k1": {"k2": 1} })",  "", R"(["k1"])"),
                                           std::make_tuple(R"({ })",  "", R"([])"),
                                           std::make_tuple(R"( [] )",  "", "NULL"),
                                           std::make_tuple(R"( 1 )",  "", "NULL"),
                                           std::make_tuple(R"( "hehe")", "", "NULL"),
                                           std::make_tuple(R"({ "k1": "v1" })",  "$.k1", R"(NULL)"),
                                           std::make_tuple(R"({ "k1": "v1" })",  "$.k3", R"(NULL)"),
                                           std::make_tuple(R"({ "k1": {"k2": 1} })",  "$.k1", R"(["k2"])")
                         ));

// clang-format on

// 1: JSON Input
// 2. Path
// 3. get_json_int execpted result
// 4. get_json_string expected result
// 5. get_json_double expected result
using GetJsonXXXParam = std::tuple<std::string, std::string, int, std::string, double>;

class GetJsonXXXTestFixture : public ::testing::TestWithParam<GetJsonXXXParam> {
public:
    StatusOr<Columns> setup() {
        _ctx = std::unique_ptr<FunctionContext>(FunctionContext::create_test_context());
        auto ints = JsonColumn::create();
        ColumnBuilder<TYPE_VARCHAR> builder(1);

        std::string param_json = std::get<0>(GetParam());
        std::string param_path = std::get<1>(GetParam());

        JsonValue json;
        Status st = JsonValue::parse(param_json, &json);
        if (!st.ok()) {
            return st;
        }
        ints->append(&json);
        if (param_path == "NULL") {
            builder.append_null();
        } else {
            builder.append(param_path);
        }
        Columns columns{ints, builder.build(true)};

        _ctx->set_constant_columns(columns);
        std::ignore = JsonFunctions::native_json_path_prepare(_ctx.get(),
                                                              FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
        return columns;
    }

    void tear_down() {
        ASSERT_TRUE(JsonFunctions::native_json_path_close(
                            _ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                            .ok());
    }

public:
    std::unique_ptr<FunctionContext> _ctx;
};

TEST_P(GetJsonXXXTestFixture, get_json_int) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    int expected = std::get<2>(GetParam());

    ColumnPtr result = JsonFunctions::get_native_json_int(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    ASSERT_EQ(1, result->size());
    Datum datum = result->get(0);
    if (expected == 0) {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        int64_t value = datum.get_int32();
        ASSERT_EQ(expected, value);
    }
}

TEST_P(GetJsonXXXTestFixture, get_json_string) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    std::string param_result = std::get<3>(GetParam());
    StripWhiteSpace(&param_result);

    ColumnPtr result = JsonFunctions::get_native_json_string(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    ASSERT_EQ(1, result->size());
    Datum datum = result->get(0);
    if (param_result == "NULL") {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        auto value = datum.get_slice();
        std::string value_str(value);
        ASSERT_EQ(param_result, value_str);
    }
}

TEST_P(GetJsonXXXTestFixture, get_json_double) {
    auto maybe_columns = setup();
    ASSERT_TRUE(maybe_columns.ok());
    DeferOp defer([&]() { tear_down(); });
    Columns columns = std::move(maybe_columns.value());

    double expected = std::get<4>(GetParam());

    ColumnPtr result = JsonFunctions::get_native_json_double(_ctx.get(), columns).value();
    ASSERT_TRUE(!!result);
    ASSERT_EQ(1, result->size());

    Datum datum = result->get(0);
    if (expected == 0) {
        ASSERT_TRUE(datum.is_null());
    } else {
        ASSERT_TRUE(!datum.is_null());
        double value = datum.get_double();
        ASSERT_EQ(expected, value);
    }
}

INSTANTIATE_TEST_SUITE_P(GetJsonXXXTest, GetJsonXXXTestFixture,
                         ::testing::Values(
                                 // clang-format: off

                                 std::make_tuple(R"( {"k1":1} )", "NULL", 0, "NULL", 0.0),
                                 std::make_tuple(R"( {"k1":1} )", "$", 0, R"( {"k1": 1} )", 0.0),
                                 std::make_tuple(R"( {"k1":1} )", "", 0, R"( NULL )", 0.0),
                                 std::make_tuple(R"( {"k0": null} )", "$.k1", 0, "NULL", 0.0),
                                 std::make_tuple(R"( {"k1": 1} )", "$.k1", 1, "1", 1.0),
                                 std::make_tuple(R"( {"k1": -1} )", "$.k1", -1, R"( -1 )", -1),
                                 std::make_tuple(R"( {"k1": 1.1} )", "$.k1", 1, R"( 1.1 )", 1.1),
                                 std::make_tuple(R"( {"k1": 3.14} )", "$.k1", 3, R"( 3.14 )", 3.14),
                                 std::make_tuple(R"( {"k1": null} )", "$.k1", 0, R"( NULL )", 0.0),
                                 std::make_tuple(R"( {"k1": "value" } )", "$.k1", 0, R"( value )", 0.0),
                                 std::make_tuple(R"( {"k1": {"k2": 1}} )", "$.k1", 0, R"( {"k2": 1} )", 0.0),
                                 std::make_tuple(R"( {"k1": [1,2,3] } )", "$.k1", 0, R"( [1, 2, 3] )", 0.0),

                                 // nested path
                                 std::make_tuple(R"( {"k1.k2": [1,2,3] } )", "$.\"k1.k2\"", 0, R"( [1, 2, 3] )", 0.0)

                                 // clang-format: on
                                 ));

TEST_F(JsonFunctionsTest, struct_to_json) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    // Struct{id: xxx, name: yyy}
    std::vector<std::string> names{"id", "name"};
    std::vector<LogicalType> types{TYPE_INT, TYPE_VARCHAR};

    // Build struct column
    Columns fields{NullableColumn::create(Int64Column::create(), NullColumn::create()),
                   NullableColumn::create(BinaryColumn::create(), NullColumn::create())};
    auto struct_column = StructColumn::create(fields, names);
    struct_column->as_mutable_ptr()->append_datum(DatumStruct{int64_t(1), Slice("park")});
    struct_column->as_mutable_ptr()->append_datum(DatumStruct{int64_t(2), Slice("menlo")});

    // Cast to JSON
    Columns input_columns{struct_column};
    auto maybe_res = JsonFunctions::to_json(ctx.get(), input_columns);
    ASSERT_TRUE(maybe_res.ok());
    ColumnPtr ptr = maybe_res.value();

    ASSERT_EQ(2, ptr->size());
    Datum json1 = ptr->get(0);
    ASSERT_FALSE(json1.is_null());
    ASSERT_EQ(R"({"id": 1, "name": "park"})", json1.get_json()->to_string_uncheck());

    Datum json2 = ptr->get(1);
    ASSERT_FALSE(json2.is_null());
    ASSERT_EQ(R"({"id": 2, "name": "menlo"})", json2.get_json()->to_string_uncheck());
}

TEST_F(JsonFunctionsTest, map_to_json) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    // Build MAP<int, string> column
    {
        auto key_column = NullableColumn::create(Int64Column::create(), NullColumn::create());
        auto val_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        auto struct_column = MapColumn::create(std::move(key_column), std::move(val_column), UInt32Column::create());

        DatumMap map1;
        map1[int64_t(1)] = Slice("menlo");
        map1[int64_t(2)] = Slice("park");
        struct_column->append_datum(map1);
        DatumMap map2;
        map2[int64_t(3)] = Slice("palo");
        map2[int64_t(4)] = Slice("alto");
        struct_column->append_datum(map2);

        // Call to_json
        Columns input_columns{struct_column};
        auto maybe_res = JsonFunctions::to_json(ctx.get(), input_columns);
        ASSERT_TRUE(maybe_res.ok());
        ColumnPtr ptr = maybe_res.value();

        ASSERT_EQ(2, ptr->size());
        Datum json1 = ptr->get(0);
        ASSERT_FALSE(json1.is_null());
        ASSERT_EQ(R"({"1": "menlo", "2": "park"})", json1.get_json()->to_string_uncheck());

        Datum json2 = ptr->get(1);
        ASSERT_FALSE(json2.is_null());
        ASSERT_EQ(R"({"3": "palo", "4": "alto"})", json2.get_json()->to_string_uncheck());
    }

    // Build MAP<string, int> column
    {
        auto key_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        auto val_column = NullableColumn::create(Int64Column::create(), NullColumn::create());
        auto struct_column = MapColumn::create(std::move(key_column), std::move(val_column), UInt32Column::create());

        DatumMap map1;
        map1[Slice("menlo")] = int64_t(1);
        map1[Slice("park")] = int64_t(2);
        struct_column->append_datum(map1);
        DatumMap map2;
        map2[Slice("palo")] = int64_t(3);
        map2[Slice("")] = int64_t(4);
        struct_column->append_datum(map2);

        // Call to_json
        Columns input_columns{struct_column};
        auto maybe_res = JsonFunctions::to_json(ctx.get(), input_columns);
        ASSERT_TRUE(maybe_res.ok());
        ColumnPtr ptr = maybe_res.value();

        ASSERT_EQ(2, ptr->size());
        Datum json1 = ptr->get(0);
        ASSERT_FALSE(json1.is_null());
        ASSERT_EQ(R"({"menlo": 1, "park": 2})", json1.get_json()->to_string_uncheck());

        Datum json2 = ptr->get(1);
        ASSERT_FALSE(json2.is_null());
        ASSERT_EQ(R"({"palo": 3})", json2.get_json()->to_string_uncheck());
    }
}

TEST_F(JsonFunctionsTest, query_json_obj) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ColumnBuilder<TYPE_VARCHAR> builder(1);
    builder.append("has_active_email");

    std::vector<std::string> param_flat_path{"other"};
    std::vector<LogicalType> param_flat_type{LogicalType::TYPE_JSON};

    auto flat_json1 = JsonColumn::create();
    ASSIGN_OR_ABORT(auto jj1, JsonValue::parse(R"({"a1": 1, "b1": 2})"));
    down_cast<JsonColumn*>(flat_json1.get())->append(jj1);

    auto remain = JsonColumn::create();
    ASSIGN_OR_ABORT(auto jj2, JsonValue::parse(R"({"has_active_email": false, "c1": 3})"));
    down_cast<JsonColumn*>(remain.get())->append(jj2);

    MutableColumns flat_js;
    flat_js.emplace_back(flat_json1->as_mutable_ptr());
    flat_js.emplace_back(remain->as_mutable_ptr());

    auto json_col = JsonColumn::create();
    json_col->set_flat_columns(param_flat_path, param_flat_type, std::move(flat_js));

    Columns columns{std::move(json_col), builder.build(true)};

    ctx.get()->set_constant_columns(columns);
    std::ignore =
            JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);

    ASSIGN_OR_ABORT(ColumnPtr result, JsonFunctions::get_native_json_bool(ctx.get(), columns));
    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    ASSERT_EQ(result->debug_string(), "[0]");
}

// Test parameters for json_remove function
// Note: Implementation supports:
// - Top-level object key removal (e.g., $.key)
// - Nested path removal (e.g., $.outer.inner1)
// - Array element removal (e.g., $.arr[2])
// - Mixed path removal from nested structures
struct JsonRemoveTestParam {
    std::string json_input;
    std::vector<std::string> paths_to_remove;
    std::string expected_result;
    std::string description;
};

class JsonRemoveTestFixture : public ::testing::TestWithParam<JsonRemoveTestParam> {};

TEST_P(JsonRemoveTestFixture, json_remove) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto param = GetParam();

    // Create JSON column
    auto json_column = ColumnHelper::cast_to_nullable_column(JsonColumn::create());
    if (param.json_input == "null") {
        json_column->append_nulls(1);
    } else {
        auto json = JsonValue::parse(param.json_input);
        ASSERT_TRUE(json.ok()) << "Failed to parse JSON: " << param.json_input;
        json_column->append_datum(Datum(&json.value()));
    }

    // Create columns with JSON and all paths
    Columns columns{json_column};
    for (const auto& path : param.paths_to_remove) {
        auto path_column = ColumnHelper::cast_to_nullable_column(BinaryColumn::create());
        if (path == "null") {
            path_column->append_nulls(1);
        } else {
            path_column->append_datum(Datum(Slice(path)));
        }
        columns.emplace_back(path_column);
    }

    // Prepare JSON path context
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    // Execute json_remove function
    ColumnPtr result = JsonFunctions::json_remove(ctx.get(), columns).value();
    ASSERT_TRUE(!!result) << "json_remove returned null result for: " << param.description;

    // Verify result
    Datum datum = result->get(0);
    if (param.expected_result == "null") {
        ASSERT_TRUE(datum.is_null()) << "Expected null result for: " << param.description;
    } else {
        ASSERT_FALSE(datum.is_null()) << "Result should not be null for: " << param.description;
        std::string json_str = datum.get_json()->to_string().value();

        // Parse and re-serialize both JSON strings to normalize key ordering
        auto expected_json = JsonValue::parse(param.expected_result);
        auto actual_json = JsonValue::parse(json_str);

        ASSERT_TRUE(expected_json.ok()) << "Failed to parse expected JSON: " << param.expected_result;
        ASSERT_TRUE(actual_json.ok()) << "Failed to parse actual JSON: " << json_str;

        std::string normalized_expected = expected_json->to_string().value();
        std::string normalized_actual = actual_json->to_string().value();

        ASSERT_EQ(normalized_expected, normalized_actual)
                << "Test: " << param.description << "\nExpected: " << normalized_expected
                << "\nActual: " << normalized_actual;
    }

    // Clean up JSON path context
    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(
        JsonRemoveTests, JsonRemoveTestFixture,
        ::testing::Values(
                JsonRemoveTestParam{
                        "null", // JSON input is null
                        {"$.a"}, // Any path (should not matter)
                        "null", // Expected result
                        "Null input JSON should result in null output"},
                JsonRemoveTestParam{
                        R"({"foo": 123, "bar": 456})", // JSON input
                        {"null"}, // paths_to_remove is null
                        R"({"foo": 123, "bar": 456})", // Expected result (no change)
                        "No paths to remove: output should be identical to input"
                },
                JsonRemoveTestParam{
                        R"({"a": 1, "b": [10, 20, 30]})", 
                        {"$.a"}, 
                        R"({"b": [10, 20, 30]})", // Expected result
                        "Remove single key from object"},
                JsonRemoveTestParam{
                        R"({"a": 1, "b": [10, 20, 30], "c": "test"})",
                        {"$.a", "$.c"},
                        R"({"b": [10, 20, 30]})", // Expected result
                        "Remove multiple keys from object"},
                JsonRemoveTestParam{
                        R"({"a": 1, "b": 2})", 
                        {"invalid_path"}, 
                        R"({"a": 1, "b": 2})", // Expected result (invalid path ignored)
                        "Invalid path should be ignored"},
                JsonRemoveTestParam{
                        R"({"x": 100, "y": 200, "z": 300})",
                        {"$.y"},
                        R"({"x": 100, "z": 300})", // Expected result
                        "Remove middle key from object"},
                JsonRemoveTestParam{
                        R"({"single": "value"})",
                        {"$.single"},
                        R"({})", // Expected result (empty object)
                        "Remove single key from single-key object"},
                        // TODO
                JsonRemoveTestParam{
                        R"([1, 2, 3, {"a": 10, "b": 20}])", // JSON input is an array
                        {"$[0]", "$[1]"}, // Try to remove array elements
                        R"([3, {"a": 10, "b": 20}])", // Expected result
                        "Remove array elements from array"},
                JsonRemoveTestParam{
                        R"({"outer": {"inner1": 1, "inner2": 2}, "keep": 42})",
                        {"$.outer.inner1"},
                        R"({"keep": 42, "outer": {"inner2": 2}})", // Expected result
                        "Remove nested key from nested object"},
                JsonRemoveTestParam{
                        R"({"deep": {"level1": {"level2": {"level3": "value"}}}})",
                        {"$.deep.level1.level2"},
                        R"({"deep": {"level1": {}}})", // Expected result
                        "Remove deeply nested object"},
                JsonRemoveTestParam{
                        R"({"arr": [0, 1, 2, 3, 4]})",
                        {"$.arr[2]"},
                        R"({"arr": [0, 1, 3, 4]})", // Expected result
                        "Remove middle element from array"},
                JsonRemoveTestParam{
                        R"({"mixed": {"obj": {"key": "value"}, "arr": [1, 2, 3]}})",
                        {"$.mixed.obj.key", "$.mixed.arr[1]"},
                        R"({"mixed": {"obj": {}, "arr": [1, 3]}})", // Expected result
                        "Remove mixed paths from nested structure"},
                JsonRemoveTestParam{
                        R"({"top_level": "value", "nested": {"inner": "data"}})",
                        {"$.top_level"},
                        R"({"nested": {"inner": "data"}})", // Expected result
                        "Remove top-level key from object"},
                JsonRemoveTestParam{
                        R"({"key1": "value1", "key2": "value2", "key3": "value3"})",
                        {"$.key1", "$.key3"},
                        R"({"key2": "value2"})", // Expected result
                        "Remove multiple top-level keys from object"},
                JsonRemoveTestParam{
                        R"({"nested": {"a": 1, "b": 2, "c": 3}, "other": "value"})",
                        {"$.nested.b", "$.nested.c"},
                        R"({"nested": {"a": 1}, "other": "value"})", // Expected result
                        "Remove multiple nested keys from object"},
                JsonRemoveTestParam{
                        R"({"array": [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}, {"id": 3, "name": "Charlie"}]})",
                        {"$.array[1]"},
                        R"({"array": [{"id": 1, "name": "Alice"}, {"id": 3, "name": "Charlie"}]})", // Expected result
                        "Remove object from array"},
                JsonRemoveTestParam{
                        R"({"complex": {"users": [{"id": 1}, {"id": 2}], "settings": {"theme": "dark", "lang": "en"}}})",
                        {"$.complex.users[0]", "$.complex.settings.theme"},
                        R"({"complex": {"users": [{"id": 2}], "settings": {"lang": "en"}}})", // Expected result
                        "Remove mixed paths from complex nested structure"},
                JsonRemoveTestParam{
                        R"({"a.b": {"c": 1}, "d": 2})",
                        {"$.\"a.b\""},
                        R"({"d": 2})", // Expected result
                        "Remove key from object where path contains a dot character"},
                JsonRemoveTestParam{
                        R"({"a.b": {"c": 1}, "d": 2})",
                        {"$.\"a.b\".c"},
                        R"({"a.b": {}, "d": 2})", // Expected result
                        "Remove key from object where path contains a dot character"},
                JsonRemoveTestParam{
                        R"({"outer": {"a.b": {"x": 10, "y": 20}}, "other": 5})",
                        {"$.outer.\"a.b\".y"},
                        R"({"outer": {"a.b": {"x": 10}}, "other": 5})", // Expected result
                        "Remove nested key where intermediate path contains a dot character"}
        ));
// clang-format on

// Test cases for json_contains function
class JsonContainsTestFixture : public ::testing::TestWithParam<std::tuple<std::string, std::string, bool>> {};

TEST_P(JsonContainsTestFixture, json_contains) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto target_json = JsonColumn::create();
    auto candidate_json = JsonColumn::create();

    std::string param_target = std::get<0>(GetParam());
    std::string param_candidate = std::get<1>(GetParam());
    bool param_contains = std::get<2>(GetParam());

    auto target = JsonValue::parse(param_target);
    auto candidate = JsonValue::parse(param_candidate);
    ASSERT_TRUE(target.ok());
    ASSERT_TRUE(candidate.ok());

    target_json->append(&*target);
    candidate_json->append(&*candidate);

    Columns columns{target_json, candidate_json};

    ColumnPtr result = JsonFunctions::json_contains(ctx.get(), columns).value();
    ASSERT_TRUE(!!result);

    if (param_contains) {
        ASSERT_TRUE((bool)result->get(0).get_uint8());
    } else {
        ASSERT_FALSE((bool)result->get(0).get_uint8());
    }
}

// clang-format off
INSTANTIATE_TEST_SUITE_P(
        JsonContainsTest, JsonContainsTestFixture,
        ::testing::Values(
                // Basic scalar contains
                std::make_tuple(R"("hello")", R"("hello")", true), 
                std::make_tuple(R"("hello")", R"("world")", false),
                std::make_tuple("123", "123", true), 
                std::make_tuple("123", "456", false),
                std::make_tuple("true", "true", true), 
                std::make_tuple("true", "false", false),
                std::make_tuple("null", "null", true),

                // Object contains
                std::make_tuple(R"({"a": 1, "b": 2})", R"({"a": 1})", true),
                std::make_tuple(R"({"a": 1, "b": 2})", R"({"b": 2})", true),
                std::make_tuple(R"({"a": 1, "b": 2})", R"({"a": 1, "b": 2})", true),
                std::make_tuple(R"({"a": 1, "b": 2})", R"({"c": 3})", false),
                std::make_tuple(R"({"a": 1, "b": 2})", R"({"a": 2})", false),

                // Test cases for objects with multiple keys
                std::make_tuple(R"({"a": 1, "b": 2, "c": 3})", R"({"a": 1, "b": 2})", true),
                std::make_tuple(R"({"a": 1, "b": 2, "c": 3})", R"({"b": 2, "c": 3})", true),
                std::make_tuple(R"({"a": 1, "b": 2, "c": 3})", R"({"a": 1, "c": 3})", true),
                std::make_tuple(R"({"a": 1, "b": 2, "c": 3})", R"({"a": 1, "b": 2, "c": 3})", true),
                std::make_tuple(R"({"a": 1, "b": 2, "c": 3})", R"({"a": 1, "b": 2, "d": 4})", false),
                std::make_tuple(R"({"a": 1, "b": 2, "c": 3})", R"({"a": 2, "b": 2})", false),
                std::make_tuple(R"({"a": 1, "b": 2, "c": 3})", R"({"a": 1, "b": 3})", false),

                // Array contains
                std::make_tuple(R"([1, 2, 3])", R"([1, 2])", true), 
                std::make_tuple(R"([1, 2, 3])", R"([2, 3])", true),
                std::make_tuple(R"([1, 2, 3])", R"([1, 2, 3])", true),
                std::make_tuple(R"([1, 2, 3])", R"([4, 5])", false), 
                std::make_tuple(R"([1, "2", 3])", "2", true),
                std::make_tuple(R"([1, 2, 3])", "4", false),

                // Nested structures
                std::make_tuple(R"({"a": [1, 2], "b": {"c": 3}})", R"({"a": [1]})", false), // partial array match
                std::make_tuple(R"({"a": [1, 2], "b": {"c": 3}})", R"({"b": {"c": 3}})", true),
                std::make_tuple(R"([{"a": 1}, {"b": 2}])", R"([{"a": 1}])", true),
                std::make_tuple(R"([{"a": 1}, {"b": 2}])", R"({"a": 1})", true),
                std::make_tuple(R"([{"a": 1}, {"b": 2}])", R"({"a": 2})", false),
                std::make_tuple(R"([{"a": 1}, {"b": 2}])", R"({"a": 1, "b": 2})", true),
                std::make_tuple(R"([{"a": 1}, {"b": 2}])", R"({"a": 1, "b": 3})", false),
                std::make_tuple(R"([{"a": 1}, {"b": 2}])", R"({"a": 1, "b": 2, "c": 3})", false),
                std::make_tuple(R"([{"a": 1}, {"b": 2}])", R"({"a": 1, "b": 2, "c": 3})", false),

                // Nested structures: these cases do not perform recursive containment checks
                // (i.e., the function does not search for sub-objects deeply nested within the target)
                std::make_tuple(R"({"x": {"a": 1}})", R"({"a": 1})", false),
                std::make_tuple(R"({"x": {"a": 1}})", R"({"b": 2})", false),
                std::make_tuple(R"([{"x": {"a": 1}}])", R"({"a": 1})", false),
                std::make_tuple(R"([{"x": {"a": 1}}])", R"({"b": 2})", false))
);
// clang-format on

struct JsonSetTestParam {
    std::string json_input;
    std::vector<std::pair<std::string, std::string>> path_value_pairs;
    std::string expected_result;
    std::string description;
};

class JsonSetTestFixture : public ::testing::TestWithParam<JsonSetTestParam> {};

TEST_P(JsonSetTestFixture, json_set) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto param = GetParam();

    auto json_column = ColumnHelper::cast_to_nullable_column(JsonColumn::create());
    bool input_is_sql_null = (param.json_input == "SQL_NULL");

    if (input_is_sql_null) {
        json_column->append_nulls(1);
    } else {
        auto json = JsonValue::parse(param.json_input);
        ASSERT_TRUE(json.ok()) << "Failed to parse input JSON: " << param.json_input;
        json_column->append_datum(Datum(&json.value()));
    }

    Columns columns{json_column};

    for (const auto& pair : param.path_value_pairs) {
        auto path_column = ColumnHelper::cast_to_nullable_column(JsonColumn::create());
        if (pair.first == "SQL_NULL") {
            path_column->append_nulls(1);
        } else {
            std::string json_path_str = "\"" + pair.first + "\"";
            auto path_json = JsonValue::parse(json_path_str);
            ASSERT_TRUE(path_json.ok()) << "Failed to parse path JSON: " << json_path_str;
            path_column->append_datum(Datum(&path_json.value()));
        }
        columns.emplace_back(path_column);

        auto val_column = ColumnHelper::cast_to_nullable_column(JsonColumn::create());
        if (pair.second == "SQL_NULL") {
            val_column->append_nulls(1);
        } else {
            auto val_json = JsonValue::parse(pair.second);
            ASSERT_TRUE(val_json.ok()) << "Failed to parse value JSON: " << pair.second;
            val_column->append_datum(Datum(&val_json.value()));
        }
        columns.emplace_back(val_column);
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    auto result_status = JsonFunctions::json_set(ctx.get(), columns);

    if (param.expected_result == "SQL_ERROR") {
        ASSERT_FALSE(result_status.ok());
    } else {
        ASSERT_TRUE(result_status.ok()) << result_status.status().to_string();
        ColumnPtr result = result_status.value();
        ASSERT_TRUE(!!result);

        Datum datum = result->get(0);
        if (param.expected_result == "SQL_NULL") {
            ASSERT_TRUE(datum.is_null()) << "Expected NULL result but got: " << datum.get_json()->to_string().value();
        } else {
            ASSERT_FALSE(datum.is_null()) << "Expected " << param.expected_result << " but got NULL";
            std::string actual_str = datum.get_json()->to_string().value();

            auto expected_json = JsonValue::parse(param.expected_result);
            auto actual_json = JsonValue::parse(actual_str);
            ASSERT_TRUE(expected_json.ok());
            ASSERT_TRUE(actual_json.ok());

            ASSERT_EQ(expected_json->to_string().value(), actual_json->to_string().value())
                    << "Description: " << param.description;
        }
    }

    ASSERT_TRUE(JsonFunctions::native_json_path_close(
                        ctx.get(), FunctionContext::FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
}

INSTANTIATE_TEST_SUITE_P(
        JsonSetTests, JsonSetTestFixture,
        ::testing::Values(
                JsonSetTestParam{R"({"a": 1})", {{"$.a", "2"}}, R"({"a": "2"})", "Update existing key"},
                JsonSetTestParam{R"({"a": 1})", {{"$.b", "2"}}, R"({"a": 1, "b": "2"})", "Insert new key"},

                JsonSetTestParam{
                        R"({"arr": [10, 20]})", {{"$.arr[1]", "99"}}, R"({"arr": [10, "99"]})", "Update array index"},
                JsonSetTestParam{R"({"arr": [10, 20]})",
                                 {{"$.arr[5]", "30"}},
                                 R"({"arr": [10, 20, "30"]})",
                                 "Append to array (index > size)"},
                JsonSetTestParam{R"({"arr": []})", {{"$.arr[0]", "1"}}, R"({"arr": ["1"]})", "Append to empty array"},

                JsonSetTestParam{
                        R"({"a": {"b": 1}})", {{"$.a.b", "2"}}, R"({"a": {"b": "2"}})", "Update nested object"},
                JsonSetTestParam{R"({"a": [{"b": 1}, {"b": 2}]})",
                                 {{"$.a[1].b", "3"}},
                                 R"({"a": [{"b": 1}, {"b": "3"}]})",
                                 "Update object inside array (Recursion test)"},
                JsonSetTestParam{R"({"a": [1, [2, 3]]})",
                                 {{"$.a[1][0]", "4"}},
                                 R"({"a": [1, ["4", 3]]})",
                                 "Update nested array"},

                JsonSetTestParam{
                        R"({"a": 1})", {{"$.a[0]", "2"}}, R"({"a": 1})", "Array selector on scalar (should ignore)"},
                JsonSetTestParam{R"([1, 2])", {{"$.a", "2"}}, R"([1, 2])", "Key selector on array (should ignore)"},

                JsonSetTestParam{
                        R"({"a": 1})", {{"$.b.c", "2"}}, R"({"a": 1})", "Insert nested missing path (should ignore)"},

                JsonSetTestParam{R"({"a": 1, "b": {}})",
                                 {{"$.b.c", "2"}},
                                 R"({"a": 1, "b": {"c": "2"}})",
                                 "Insert nested key into existing parent"},

                JsonSetTestParam{
                        R"({"a": 1})", {{"$.a", "2"}, {"$.b", "3"}}, R"({"a": "2", "b": "3"})", "Chained updates"},

                JsonSetTestParam{"SQL_NULL", {{"$.a", "1"}}, "SQL_NULL", "Input is SQL NULL"},
                JsonSetTestParam{R"({"a": 1})", {{"SQL_NULL", "1"}}, "SQL_NULL", "Path is SQL NULL"},
                JsonSetTestParam{R"({"a": 1})", {{"$.a", "SQL_NULL"}}, "SQL_NULL", "Value is SQL NULL"},
                JsonSetTestParam{"null", {{"$.a", "1"}}, "null", "Input is JSON null"},

                JsonSetTestParam{R"({"a": 1})", {{"$", R"({"b": 2})"}}, R"({"b": 2})", "Replace root"},
                JsonSetTestParam{R"([1, 2])", {{"$[0]", "3"}}, R"(["3", 2])", "Update root array index"},
                JsonSetTestParam{R"([1, 2])", {{"$[5]", "3"}}, R"([1, 2, "3"])", "Append to root array"},

                JsonSetTestParam{
                        R"({"a": [1]})", {{"$.a[0]", "2"}}, R"({"a": ["2"]})", "Exact index match in object array"},
                JsonSetTestParam{R"({"a": 1})",
                                 {{"$[0]", "1"}},
                                 R"({"a": 1})",
                                 "Root array selector on object (should ignore)"}));

struct JsonPrettyTestParam {
    std::string input_json;
    std::string expected_output;
    bool is_null;
    std::string description;
};

class JsonPrettyTestFixture : public ::testing::TestWithParam<JsonPrettyTestParam> {};

TEST_P(JsonPrettyTestFixture, json_pretty) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto param = GetParam();

    Columns columns;
    auto json_col = JsonColumn::create();

    if (param.is_null) {
        auto nullable_json = ColumnHelper::cast_to_nullable_column(json_col);
        nullable_json->append_nulls(1);
        columns.emplace_back(nullable_json);
    } else {
        auto json_val = JsonValue::parse(param.input_json);
        ASSERT_TRUE(json_val.ok()) << "Failed to parse input JSON: " << param.input_json;
        json_col->append(&*json_val);
        columns.emplace_back(json_col);
    }

    auto result = JsonFunctions::json_pretty(ctx.get(), columns);
    ASSERT_TRUE(result.ok());

    auto res_col = result.value();

    if (param.is_null) {
        ASSERT_TRUE(res_col->is_null(0)) << "Expected NULL result for: " << param.description;
    } else {
        ASSERT_FALSE(res_col->is_null(0));
        auto v_col = ColumnHelper::cast_to<TYPE_VARCHAR>(res_col);
        std::string actual = v_col->get_data()[0].to_string();

        ASSERT_EQ(param.expected_output, actual) << "Test Description: " << param.description << "\nExpected:\n"
                                                 << param.expected_output << "\nActual:\n"
                                                 << actual;
    }
}

INSTANTIATE_TEST_SUITE_P(JsonPrettyTest, JsonPrettyTestFixture,
                         ::testing::Values(JsonPrettyTestParam{R"({"a":1})",
                                                               R"({
  "a" : 1
})",
                                                               false, "Simple object formatting (Space before colon)"},
                                           JsonPrettyTestParam{R"([1,2])",
                                                               R"([
  1,
  2
])",
                                                               false, "Simple array formatting"},
                                           JsonPrettyTestParam{R"({"b":3, "a":[1,2]})",
                                                               R"({
  "a" : [
    1,
    2
  ],
  "b" : 3
})",
                                                               false,
                                                               "Nested structure formatting & Alphabetical sorting"},
                                           JsonPrettyTestParam{R"({"key": "value with { braces }"})",
                                                               R"({
  "key" : "value with { braces }"
})",
                                                               false, "Braces inside string value should be preserved"},
                                           JsonPrettyTestParam{R"({})",
                                                               R"({
})",
                                                               false, "Empty object (Occupies lines)"},
                                           JsonPrettyTestParam{R"([])",
                                                               R"([
])",
                                                               false, "Empty array (Occupies lines)"},
                                           JsonPrettyTestParam{"", "", true, "Null input should return null"}));

// ===================================================================================
// Extraction from text. For a document v and a path p every form gives the value of
// <consumer over JSON>(parse_json(v), p): get_json_*(v, p) and
// <consumer over JSON>(json_extract_paths(v, p1, ..., pn), '$.i').
// ===================================================================================

namespace {

using DenseFn = StatusOr<ColumnPtr> (*)(FunctionContext*, const Columns&);
using SelectedFn = StatusOr<ColumnPtr> (*)(FunctionContext*, const SelectedColumns&, size_t);
using StateFn = Status (*)(FunctionContext*, FunctionContext::FunctionStateScope);

struct JsonKernel {
    const char* name;
    DenseFn dense;
    SelectedFn selected;
    StateFn prepare;
    StateFn close;
    LogicalType result_type;
};

const JsonKernel kParseJson{
        "parse_json", JsonFunctions::parse_json, JsonFunctions::parse_json_selected, nullptr, nullptr, TYPE_JSON};
const JsonKernel kExtractPaths{"json_extract_paths",
                               JsonFunctions::json_extract_paths,
                               JsonFunctions::json_extract_paths_selected,
                               JsonFunctions::json_extract_paths_prepare,
                               JsonFunctions::json_extract_paths_close,
                               TYPE_JSON};

#define JSON_PATH_KERNEL(name, fn, type)                                                                \
    JsonKernel {                                                                                        \
        name, JsonFunctions::fn, JsonFunctions::fn##_selected, JsonFunctions::native_json_path_prepare, \
                JsonFunctions::native_json_path_close, type                                             \
    }

// Consumers over JSON.
const std::vector<JsonKernel> kJsonConsumers{
        JSON_PATH_KERNEL("get_json_string(json)", get_native_json_string, TYPE_VARCHAR),
        JSON_PATH_KERNEL("get_json_int(json) bigint", get_native_json_bigint, TYPE_BIGINT),
        JSON_PATH_KERNEL("get_json_int(json) int", get_native_json_int, TYPE_INT),
        JSON_PATH_KERNEL("get_json_double(json)", get_native_json_double, TYPE_DOUBLE),
        JSON_PATH_KERNEL("get_json_bool(json)", get_native_json_bool, TYPE_BOOLEAN),
        JSON_PATH_KERNEL("json_query", json_query, TYPE_JSON),
        JSON_PATH_KERNEL("json_exists", json_exists, TYPE_BOOLEAN),
        JSON_PATH_KERNEL("json_length", json_length, TYPE_INT),
};

// get_json_*(VARCHAR, VARCHAR) and the consumer over JSON with the same result.
const std::vector<std::pair<JsonKernel, size_t>> kTextGetters{
        {JSON_PATH_KERNEL("get_json_string(varchar)", get_json_string, TYPE_VARCHAR), 0},
        {JSON_PATH_KERNEL("get_json_int(varchar) bigint", get_json_bigint, TYPE_BIGINT), 1},
        {JSON_PATH_KERNEL("get_json_int(varchar) int", get_json_int, TYPE_INT), 2},
        {JSON_PATH_KERNEL("get_json_double(varchar)", get_json_double, TYPE_DOUBLE), 3},
        {JSON_PATH_KERNEL("get_json_bool(varchar)", get_json_bool, TYPE_BOOLEAN), 4},
};

#undef JSON_PATH_KERNEL

RuntimeState* query_state(bool strict) {
    static auto make = [](bool allow_throw) {
        TQueryOptions options;
        options.__set_allow_throw_exception(allow_throw);
        return std::make_unique<RuntimeState>(TUniqueId(), options, TQueryGlobals(), nullptr);
    };
    static std::unique_ptr<RuntimeState> relaxed = make(false);
    static std::unique_ptr<RuntimeState> strict_state = make(true);
    return strict ? strict_state.get() : relaxed.get();
}

ColumnPtr const_text(const std::string& value, size_t rows) {
    auto data = BinaryColumn::create();
    data->append(value);
    return ConstColumn::create(std::move(data), rows);
}

ColumnPtr text_column(const std::vector<std::optional<std::string>>& values) {
    ColumnBuilder<TYPE_VARCHAR> builder(values.size());
    for (const auto& value : values) {
        if (value.has_value()) {
            builder.append(Slice(*value));
        } else {
            builder.append_null();
        }
    }
    return builder.build_nullable_column();
}

ColumnPtr gather(const ColumnPtr& column, const std::vector<uint32_t>& rows) {
    if (column->is_constant()) {
        const ColumnPtr& data = down_cast<const ConstColumn*>(column.get())->data_column();
        auto result = data->clone_empty();
        for (size_t i = 0; i < rows.size(); ++i) {
            result->append(*data, 0, 1);
        }
        return result;
    }
    auto result = column->clone_empty();
    result->append_selective(*column, rows.data(), 0, rows.size());
    return result;
}

// Calls a kernel on columns; through its selected kernel when rows is not null.
StatusOr<ColumnPtr> call_kernel(const JsonKernel& kernel, const Columns& columns, bool strict,
                                const std::vector<uint32_t>* rows = nullptr) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    ctx->set_runtime_state(query_state(strict));
    ctx->set_constant_columns(columns);
    if (kernel.prepare != nullptr) {
        RETURN_IF_ERROR(kernel.prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL));
        RETURN_IF_ERROR(kernel.prepare(ctx.get(), FunctionContext::THREAD_LOCAL));
    }
    DeferOp close([&] {
        if (kernel.close != nullptr) {
            (void)kernel.close(ctx.get(), FunctionContext::THREAD_LOCAL);
            (void)kernel.close(ctx.get(), FunctionContext::FRAGMENT_LOCAL);
        }
    });
    if (rows == nullptr) {
        return kernel.dense(ctx.get(), columns);
    }
    SelectedColumns selected;
    for (const auto& column : columns) {
        selected.push_back({column, rows});
    }
    return kernel.selected(ctx.get(), selected, rows->size());
}

std::string to_hex(const char* data, size_t size) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    for (size_t i = 0; i < size; ++i) {
        const auto byte = static_cast<unsigned char>(data[i]);
        out += digits[byte >> 4];
        out += digits[byte & 15];
    }
    return out;
}

// Text of a value that tells apart every two different values: the bytes of a JSON value and of a string,
// the bits of a double.
std::string value_text(const ColumnPtr& column, size_t row, LogicalType type) {
    if (column->is_null(row)) {
        return "NULL";
    }
    if (column->is_constant()) {
        return value_text(down_cast<const ConstColumn*>(column.get())->data_column(), 0, type);
    }
    switch (type) {
    case TYPE_JSON: {
        ColumnViewer<TYPE_JSON> viewer(column);
        const JsonValue* json = viewer.value(row);
        const auto slice = json->to_vslice();
        return json->to_string_uncheck() + " " + to_hex(reinterpret_cast<const char*>(slice.start()), slice.byteSize());
    }
    case TYPE_VARCHAR: {
        ColumnViewer<TYPE_VARCHAR> viewer(column);
        const Slice value = viewer.value(row);
        return value.to_string() + " " + to_hex(value.data, value.size);
    }
    case TYPE_DOUBLE: {
        ColumnViewer<TYPE_DOUBLE> viewer(column);
        const double value = viewer.value(row);
        uint64_t bits;
        std::memcpy(&bits, &value, sizeof(bits));
        return column->debug_item(row) + " " + std::to_string(bits);
    }
    default:
        return column->debug_item(row);
    }
}

void expect_same_result(const StatusOr<ColumnPtr>& expected, const StatusOr<ColumnPtr>& actual, LogicalType type,
                        size_t rows, const std::string& trace) {
    SCOPED_TRACE(trace);
    ASSERT_EQ(expected.ok(), actual.ok()) << "expected: " << expected.status() << " actual: " << actual.status();
    if (!expected.ok()) {
        ASSERT_EQ(expected.status().code(), actual.status().code());
        ASSERT_EQ(expected.status().message(), actual.status().message());
        return;
    }
    ASSERT_EQ(rows, expected.value()->size());
    ASSERT_EQ(rows, actual.value()->size());
    for (size_t row = 0; row < rows; ++row) {
        ASSERT_EQ(value_text(expected.value(), row, type), value_text(actual.value(), row, type)) << "row " << row;
    }
}

// Checks every form of extraction of paths from docs against parse_json() + extraction: dense, or through
// the selected kernels on rows when rows is not null.
void check_extraction(const ColumnPtr& docs, const std::vector<std::string>& paths, bool strict,
                      const std::vector<uint32_t>* rows, bool all_consumers = true) {
    const ColumnPtr reference_docs = rows == nullptr ? docs : gather(docs, *rows);
    const size_t num_rows = reference_docs->size();
    const StatusOr<ColumnPtr> parsed = call_kernel(kParseJson, {reference_docs}, strict);

    auto expected_value = [&](const JsonKernel& consumer, const std::string& path) -> StatusOr<ColumnPtr> {
        RETURN_IF_ERROR(parsed.status());
        return call_kernel(consumer, {parsed.value(), const_text(path, num_rows)}, strict);
    };

    for (const auto& path : paths) {
        for (const auto& [getter, consumer_index] : kTextGetters) {
            const JsonKernel& consumer = kJsonConsumers[consumer_index];
            expect_same_result(expected_value(consumer, path),
                               call_kernel(getter, {docs, const_text(path, docs->size())}, strict, rows),
                               consumer.result_type, num_rows, std::string(getter.name) + " " + path);
        }
    }

    // A path with a wildcard or a slice sends every row of json_extract_paths() to VPack, so the paths are
    // checked together and the simple ones once more without the others.
    std::vector<std::string> simple_paths;
    for (const auto& path : paths) {
        if (path.find_first_of("*:") == std::string::npos && path.find(".$") == std::string::npos) {
            simple_paths.push_back(path);
        }
    }
    const std::vector<const std::vector<std::string>*> groups{&paths, &simple_paths};
    for (const auto* group : groups) {
        if (group->empty() || (group == &simple_paths && simple_paths.size() == paths.size())) {
            continue;
        }
        Columns arguments{docs};
        for (const auto& path : *group) {
            arguments.push_back(const_text(path, docs->size()));
        }
        const StatusOr<ColumnPtr> extracted = call_kernel(kExtractPaths, arguments, strict, rows);
        for (size_t i = 0; i < group->size(); ++i) {
            for (const auto& consumer : kJsonConsumers) {
                if (!all_consumers && consumer.result_type != TYPE_JSON) {
                    continue;
                }
                const StatusOr<ColumnPtr> actual =
                        extracted.ok()
                                ? call_kernel(consumer,
                                              {extracted.value(), const_text("$." + std::to_string(i), num_rows)},
                                              strict)
                                : StatusOr<ColumnPtr>(extracted.status());
                expect_same_result(expected_value(consumer, (*group)[i]), actual, consumer.result_type, num_rows,
                                   std::string("json_extract_paths ") + consumer.name + " " + (*group)[i]);
            }
        }
    }
}

std::vector<uint32_t> reversed_rows(size_t num_rows) {
    std::vector<uint32_t> rows;
    for (size_t row = num_rows; row > 0; --row) {
        rows.push_back(row - 1);
    }
    if (num_rows > 0) {
        rows.push_back(0);
    }
    return rows;
}

std::string nested(const std::string& open, const std::string& leaf, const std::string& close, int depth) {
    std::string out;
    for (int i = 0; i < depth; ++i) out += open;
    out += leaf;
    for (int i = 0; i < depth; ++i) out += close;
    return out;
}

std::vector<std::optional<std::string>> edge_documents() {
    std::vector<std::optional<std::string>> docs{
            std::nullopt,
            "",
            "   ",
            "abc",
            R"("abc")",
            "123",
            "null",
            "true",
            // well-formed objects and the bytes before them
            R"({"a":"x","b":2})",
            " \t\n{\"a\":\"x\",\"b\":2} \r\n",
            "\f{\"a\":\"x\",\"b\":2}",
            "\v{\"a\":\"x\",\"b\":2}",
            // malformed documents
            R"({"a":1}x)",
            R"({"a":1}{"a":2})",
            R"({"a":1,"b":tru})",
            R"({"b":tru,"a":1})",
            R"({"a":1,"b":01})",
            R"({"a":01})",
            R"({"a":1,)",
            R"({"a":1,"b":"x)",
            R"({"a":1])",
            R"({"a":1,})",
            R"({'a':1})",
            R"({"a":NaN,"b":1})",
            R"({"a":1 /* c */})",
            R"({"n":1e400})",
            "{",
            "[1,2,",
            // arrays
            R"({"a":[1,2,{"c":3}]})",
            R"([1,{"a":2},[3]])",
            R"([{"a":1},{"a":2}])",
            // keys with dots and other alphabets
            R"({"a.b":1,"a":{"b":2}})",
            R"({"slrRefData":{"sw5.chain.id":"c1"}})",
            R"({"статус":"ok"})",
            R"({"A":1})",
            // strings
            R"({"a":"x\"y"})",
            R"({"a":"t\tn\n/\/b\\"})",
            "{\"a\":\"\xC3\xA9\xE4\xB8\xAD\"}",
            "{\"a\":\"\xF0\x9F\x98\x80\"}",
            R"({"a":"😀"})",
            R"({"a":"\ud800"})",
            R"({"a":"\udc00x"})",
            R"({"a":"k"})",
            R"({"ab":1,"ab":2})",
            "{\"a\":\"\xFF\"}",
            "{\"a\":\"\xC3\"}",
            "{\"a\":\"\xED\xA0\x80\"}",
            "{\"a\":\"x\x01y\"}",
            "{\"b\":\"x\x01y\",\"a\":1}",
            std::string("{\"a\":\"x\0y\"}", 11),
            R"({"a":"\u0000"})",
            // duplicate keys
            R"({"a":1,"a":2})",
            R"({"k0":0,"a":1,"k1":1,"k2":2,"k3":3,"k4":4,"k5":5,"k6":6,"a":2})",
            R"({"o":{"a":1,"a":2},"a":{"b":1,"b":2}})",
            R"({"o":{"x":1,"x":2,"y":3}})",
            R"({"a":{"b":1},"a":{"b":2},"k1":1,"k2":2,"k3":3,"k4":4,"k5":5,"k6":6,"k7":7})",
            // null and other values on the path
            R"({"a":null})",
            R"({"a":{"b":null}})",
            R"({"a":"x"})",
            "{}",
            "[]",
            R"({"":1})",
            R"({"":{"":2}})",
            // numbers
            R"({"n":1,"a":1.0,"b":1e3})",
            R"({"n":1E+3})",
            R"({"n":-0})",
            R"({"n":-0.0})",
            R"({"n":0.1})",
            R"({"n":0.30000000000000004})",
            R"({"n":2147483647})",
            R"({"n":2147483648})",
            R"({"n":-2147483648})",
            R"({"n":-2147483649})",
            R"({"n":9223372036854775807})",
            R"({"n":-9223372036854775808})",
            R"({"n":9223372036854775808})",
            R"({"n":18446744073709551615})",
            R"({"n":18446744073709551616})",
            R"({"n":-9223372036854775809})",
            R"({"n":123456789012345678901234567890})",
            R"({"n":-123456789012345678901234567890})",
            R"({"n":1.7976931348623157e308})",
            R"({"n":4.9e-324})",
            R"({"n":1e-400})",
            R"({"n":0e400})",
            R"({"n":0.00001e310})",
            R"({"n":"12"})",
            R"({"n":"1.5"})",
            R"({"n":"x1"})",
            R"({"n":[1.5,-2,3e2]})",
            "{\"n\":1" + std::string(300, '0') + ".5}",
            "{\"n\":0." + std::string(300, '1') + "}",
            // booleans
            R"({"b":true})",
            R"({"b":false})",
            R"({"b":"true"})",
            R"({"b":"0"})",
            R"({"b":"1"})",
            R"({"b":"yes"})",
            R"({"b":2})",
            R"({"b":0.0})",
            // a hexadecimal id with an exponent-like part
            R"({"a":"550e8400-e29b-41d4-a716-446655440000","b":2})",
            // nesting
            nested("{\"d\":", "1", "}", 1000),
            nested("{\"d\":", "1", "}", 1100),
            nested("{\"d\":", "1", "}", 2000),
            nested("[", "1", "]", 1100),
            // longer than kJSONLengthLimit
            "{\"a\":\"" + std::string(kJSONLengthLimit - 7, 'x') + "\"}",
    };
    return docs;
}

const std::vector<std::string> kEdgePaths{"$",          "$.a",
                                          "$.b",        "$.c",
                                          "a",          "$.a.b",
                                          "$.a[0]",     "$.a[1]",
                                          "$.a[5]",     "$.a[2].c",
                                          "$.a[0].c",   "$[1].a",
                                          "$[0]",       "$[1]",
                                          R"($."a.b")", R"($.slrRefData."sw5.chain.id")",
                                          "$.статус",   R"($."статус")",
                                          "$.ab",       "$.o",
                                          "$.o.a",      "$.o.x",
                                          "$.n",        "$.n[2]",
                                          R"($."")",    "$.",
                                          "$.d.d.d",    "$.a[*]",
                                          "$.a[1:2]",   "$.a.$.b",
                                          "$.$.a"};

} // namespace

TEST_F(JsonFunctionsTest, extraction_from_text_equals_parse_json) {
    const auto docs = edge_documents();
    const ColumnPtr column = text_column(docs);
    const auto rows = reversed_rows(column->size());
    check_extraction(column, kEdgePaths, false, nullptr);
    check_extraction(column, kEdgePaths, false, &rows);
    // In strict mode the first malformed document fails the whole column, so every document is checked alone.
    for (const auto& doc : docs) {
        SCOPED_TRACE(doc.has_value() ? doc->substr(0, 64) : "NULL");
        check_extraction(text_column({doc}), kEdgePaths, true, nullptr);
    }
}

TEST_F(JsonFunctionsTest, extraction_from_constant_text) {
    for (const std::string doc : {R"({"a":"x","b":[1,2]})", R"({"a":1,"b":tru})", "abc", R"({"a":1,"a":2})"}) {
        SCOPED_TRACE(doc);
        const ColumnPtr constant = const_text(doc, 3);
        const std::vector<uint32_t> rows{2, 0};
        for (bool strict : {false, true}) {
            check_extraction(constant, {"$.a", "$.b[1]", "$.c"}, strict, nullptr);
            check_extraction(constant, {"$.a", "$.b[1]", "$.c"}, strict, &rows);
        }
        auto result = call_kernel(kExtractPaths, {constant, const_text("$.a", 3)}, false);
        ASSERT_OK(result.status());
        ASSERT_TRUE(result.value()->is_constant());
        ASSERT_EQ(3, result.value()->size());
    }
    auto null_input =
            call_kernel(kExtractPaths, {ColumnHelper::create_const_null_column(5), const_text("$.a", 5)}, false);
    ASSERT_OK(null_input.status());
    ASSERT_EQ(5, ColumnHelper::count_nulls(null_input.value()));
    auto empty = call_kernel(kExtractPaths, {BinaryColumn::create(), const_text("$.a", 0)}, false);
    ASSERT_OK(empty.status());
    ASSERT_EQ(0, empty.value()->size());
}

TEST_F(JsonFunctionsTest, json_extract_paths_result) {
    const ColumnPtr docs =
            text_column({R"({"a":1,"b":null,"c":{"d":[1,"x"]}})", R"({"a":1,"a":2})", "abc", "[1", std::nullopt});
    auto result = call_kernel(
            kExtractPaths,
            {docs, const_text("$.a", 5), const_text("$.b", 5), const_text("$.c.d[1]", 5), const_text("$.missing", 5)},
            false);
    ASSERT_OK(result.status());
    ColumnViewer<TYPE_JSON> viewer(result.value());
    ASSERT_EQ(R"({"0": 1, "1": null, "2": "x"})", viewer.value(0)->to_string_uncheck());
    // VPack finds the first of two equal keys in an object with two members.
    ASSERT_EQ(R"({"0": 1})", viewer.value(1)->to_string_uncheck());
    ASSERT_EQ("{}", viewer.value(2)->to_string_uncheck());
    ASSERT_TRUE(viewer.is_null(3));
    ASSERT_TRUE(viewer.is_null(4));
}

TEST_F(JsonFunctionsTest, json_extract_paths_rejects_bad_paths) {
    const ColumnPtr docs = text_column({R"({"a":1})"});
    ASSERT_FALSE(call_kernel(kExtractPaths, {docs, const_text("$.a[", 1)}, false).ok());
    ASSERT_FALSE(call_kernel(kExtractPaths, {docs, text_column({"$.a"})}, false).ok());
    ASSERT_FALSE(call_kernel(kExtractPaths, {docs, ColumnHelper::create_const_null_column(1)}, false).ok());
}

namespace {

// Documents shaped like solar_operations.original_payload.
class PayloadGenerator {
public:
    explicit PayloadGenerator(uint64_t seed) : _random(seed) {}

    std::string document() {
        std::string s = "{\"id\":\"" + uuid() + "\",";
        s += "\"posData\":{\"terminalType\":\"" + std::string(pick(30) ? "ATM" : "POS") +
             "\",\"entryMode\":" + std::to_string(_random() % 100) +
             ",\"cardholderPresent\":" + (pick(2) ? "true" : "false") + "},";
        s += "\"merchantData\":{\"terminalId\":\"" + std::string(pick(30) ? "A0400" : "B1200") +
             std::to_string(_random() % 1000) + "\",\"name\":" + text() +
             ",\"mcc\":" + std::to_string(5000 + _random() % 1000) + "},";
        s += "\"referenceData\":{\"rrn\":" + (pick(5) ? std::string("null") : "\"" + std::to_string(_random()) + "\"") +
             ",\"stan\":" + std::to_string(_random() % 1000000) + "},";
        s += "\"amount\":" + number() + ",\"fee\":" + number() + ",";
        if (pick(50)) {
            s += "\"posData\":{\"terminalType\":\"DUP\"},";
        }
        s += "\"items\":[";
        const int items = 3 + _random() % 10;
        for (int i = 0; i < items; ++i) {
            s += (i ? "," : "");
            s += "{\"k\":" + std::to_string(i) + ",\"v\":" + text() + ",\"n\":" + number() + ",\"u\":\"" + uuid() +
                 "\",\"arr\":[1,2.5,-3,\"x\",null,{},[]]}";
        }
        s += "]}";
        return s;
    }

    // Replaces, inserts or deletes a byte, or cuts the document.
    std::string corrupt(std::string doc) {
        static const char pool[] = "{}[]\":,0123456789eE+-.tfn\\ \x01\xff";
        const size_t pos = _random() % doc.size();
        const char c = pool[_random() % (sizeof(pool) - 1)];
        switch (_random() % 4) {
        case 0:
            doc[pos] = c;
            break;
        case 1:
            doc.insert(doc.begin() + pos, c);
            break;
        case 2:
            doc.erase(doc.begin() + pos);
            break;
        default:
            doc.resize(pos);
            break;
        }
        return doc;
    }

private:
    bool pick(int one_in) { return _random() % one_in == 0; }

    std::string uuid() {
        static const char* hex = "0123456789abcdef";
        std::string s;
        for (int i = 0; i < 32; ++i) {
            if (i == 8 || i == 12 || i == 16 || i == 20) s += '-';
            s += hex[_random() % 16];
        }
        return s;
    }

    std::string number() {
        switch (_random() % 12) {
        case 0:
            return "-" + std::to_string(_random() % 100000);
        case 1:
            return std::to_string(_random() % 1000) + "e" + std::to_string(_random() % 30);
        case 2:
            return "9223372036854775808";
        case 3:
            return "123456789012345678901234567890";
        case 4:
            return "0.1";
        default:
            return std::to_string(_random() % 100000) + "." + std::to_string(_random() % 100);
        }
    }

    std::string text() {
        static const char* pieces[] = {"shop",          "\\\"q\\\"", "\\n", "\\u041c\\u0430\\u0433", "\xd0\xb0",
                                       "\\/",           "\\\\",      "\\t", "\xf0\x9f\x98\x80",      " ",
                                       "\\ud83d\\ude00"};
        std::string s = "\"";
        const int n = _random() % 8;
        for (int i = 0; i < n; ++i) {
            s += pieces[_random() % (sizeof(pieces) / sizeof(pieces[0]))];
        }
        if (pick(200)) {
            s += "\\ud800";
        }
        return s + "\"";
    }

    std::mt19937_64 _random;
};

} // namespace

TEST_F(JsonFunctionsTest, extraction_from_generated_documents) {
    PayloadGenerator generator(20261014);
    std::vector<std::string> documents;
    for (int i = 0; i < 2000; ++i) {
        documents.push_back(generator.document());
    }
    const std::vector<std::string> paths{"$.posData.terminalType", "$.merchantData.terminalId", "$.referenceData.rrn"};
    const std::vector<std::string> more_paths{"$.items[2].n", "$.amount", "$.items[1].arr[5]"};
    constexpr size_t kChunk = 4096;
    std::vector<std::optional<std::string>> chunk;
    size_t produced = 0;
    auto flush = [&]() {
        const ColumnPtr column = text_column(chunk);
        check_extraction(column, paths, false, nullptr);
        if (produced < 3 * kChunk) {
            const auto rows = reversed_rows(column->size());
            check_extraction(column, paths, false, &rows);
            check_extraction(column, more_paths, false, nullptr, false);
        }
        chunk.clear();
    };
    for (const auto& doc : documents) {
        chunk.emplace_back(doc);
    }
    for (int i = 0; i < 100000; ++i) {
        chunk.emplace_back(generator.corrupt(documents[i % documents.size()]));
        if (chunk.size() == kChunk) {
            flush();
            ASSERT_FALSE(HasFatalFailure());
            produced += kChunk;
        }
    }
    flush();
}

// Byte order marks, trailing NUL bytes, surrogates, long leaves and long numbers, each compared with parse_json()
// and extraction.
TEST_F(JsonFunctionsTest, extraction_from_more_inputs) {
    std::string big_leaf = "{\"o\":{";
    for (int i = 0; i < 40; ++i) {
        big_leaf += "\"k" + std::to_string(i) + "\":\"" + std::string(150 + i, 'x') + "\",";
    }
    big_leaf += "\"k3\":-5,\"n\":[1,-1,2.5,{\"z\":null}]}}";
    const std::vector<std::optional<std::string>> docs{
            R"({"ab":1})",
            R"({"ab":1,"ab":2})",
            R"({"ab":1,"ab":2,"c":3,"d":4,"e":5,"f":6,"g":7,"h":8,"i":9})",
            "\xEF\xBB\xBF{\"ab\":1}",
            " \xEF\xBB\xBF{\"ab\":1}",
            std::string("{\"ab\":1}\0", 8),
            std::string("{\"ab\":1}\n\0", 9),
            R"({"ab":"\udc00\ud800"})",
            R"({"ab":"\ud800𐀀"})",
            R"({"ab":"\ud800A"})",
            R"({"ab":"􏿿"})",
            big_leaf,
            "{\"n\":1" + std::string(150, '0') + "e99}",
            "{\"n\":1" + std::string(158, '0') + "e-99}",
            "{\"n\":-1" + std::string(158, '0') + ".5e99}",
            R"({"n":1E+99,"m":-1e-99,"k":1.5e00})",
            R"({"a":[[1,2],[3,[4,5]]]})",
            R"(  [ {"a":1} , {"a":2} ]  )",
            R"({"a":{"b":{"c":{"d":"deep"}}},"a.b":7})",
            "{\"ab\":\"\xF4\x90\x80\x80\"}",
            "{\"ab\":\"\xC0\xAF\"}",
            R"({"ab":"x"} )",
            "{\"ab\":1}\r\n\t ",
            "{\"ab\":1}\f",
    };
    const std::vector<std::string> paths{"$.ab",      "$.o",          "$.o.k3", "$.n",     "$.m",        "$.k",
                                         "$.a[0][1]", "$.a[1][1][0]", "$[1].a", "$.a.b.c", R"($."a.b")", "$"};
    const ColumnPtr column = text_column(docs);
    const auto rows = reversed_rows(column->size());
    check_extraction(column, paths, false, nullptr);
    check_extraction(column, paths, false, &rows);
    for (const auto& doc : docs) {
        SCOPED_TRACE(doc.has_value() ? doc->substr(0, 64) : "NULL");
        check_extraction(text_column({doc}), paths, true, nullptr);
    }
}

TEST_F(JsonFunctionsTest, parse_json_constant_column) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto input = BinaryColumn::create();
    input->append(R"({"a":1})");
    auto result = JsonFunctions::parse_json(ctx.get(), {ConstColumn::create(input, 4096)});
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value()->is_constant());
    ASSERT_EQ(4096, result.value()->size());
    ASSERT_EQ(1, down_cast<const ConstColumn*>(result.value().get())->data_column()->size());
    ASSERT_EQ(result.value()->debug_item(0), result.value()->debug_item(4095));
}

TEST_F(JsonFunctionsTest, parse_json_constant_respects_allow_throw_exception) {
    for (bool strict : {false, true}) {
        TQueryOptions options;
        options.__set_allow_throw_exception(strict);
        RuntimeState state(TUniqueId(), options, TQueryGlobals(), nullptr);
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        ctx->set_runtime_state(&state);
        auto input = BinaryColumn::create();
        input->append(R"({"a":invalid})");
        auto result = JsonFunctions::parse_json(ctx.get(), {ConstColumn::create(input, 3)});
        if (strict) {
            ASSERT_FALSE(result.ok());
        } else {
            ASSERT_TRUE(result.ok());
            ASSERT_EQ(3, ColumnHelper::count_nulls(result.value()));
        }
    }
}

// Stress test: large batch with mixed escape patterns, so the parser and the builder of the thread are reused
// across rows of every length. Run under ASAN.
TEST_F(JsonFunctionsTest, asan_stress_get_json_string_escapes) {
    constexpr int kNumRows = 100000;
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto json_col = BinaryColumn::create();
    auto path_col = BinaryColumn::create();

    // Each row carries a mix of escape sequences. Periodically vary escape density.
    for (int i = 0; i < kNumRows; ++i) {
        std::string payload = R"({"a":{"k":"x)";
        // Varying length and escape density per row.
        for (int j = 0; j < (i % 32); ++j) {
            payload += "\\\""; // \"
        }
        payload += R"(","u":"éé","t":")";
        payload += std::string(i % 64, 'a');
        payload += R"("}})";
        json_col->append(payload);
    }
    path_col->append("$.a.k");

    Columns columns;
    columns.emplace_back(json_col);
    columns.emplace_back(ConstColumn::create(path_col, 1));

    ctx->set_constant_columns(columns);
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());
    ASSERT_TRUE(
            JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());

    ColumnPtr result = JsonFunctions::get_json_string(ctx.get(), columns).value();
    ASSERT_EQ(kNumRows, result->size());
    ColumnViewer<TYPE_VARCHAR> viewer(result);
    for (int i = 0; i < kNumRows; ++i) {
        ASSERT_EQ("x" + std::string(i % 32, '"'), viewer.value(i).to_string()) << i;
    }

    ASSERT_TRUE(
            JsonFunctions::native_json_path_close(ctx.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL).ok());
    ASSERT_TRUE(
            JsonFunctions::native_json_path_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
}

// A pipeline ProjectOperator hands the SAME ExprContext (hence the same FunctionContext) to every
// per-`pipeline_dop` driver without cloning, so the reusable parser and builder must be per-OS-thread
// (thread_local), not shared FunctionContext state. A shared parser is raced by concurrent drivers and
// scrambles values across rows. This test shares one context across threads and asserts each thread reads
// back only its own values.
TEST_F(JsonFunctionsTest, get_json_string_concurrent_shared_context) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());

    auto seed_json = BinaryColumn::create();
    seed_json->append(R"({"k":"seed"})");
    auto seed_path = BinaryColumn::create();
    seed_path->append("$.k");
    Columns const_cols;
    const_cols.emplace_back(seed_json);
    const_cols.emplace_back(ConstColumn::create(seed_path, 1));
    ctx->set_constant_columns(const_cols);
    ASSERT_TRUE(JsonFunctions::native_json_path_prepare(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL)
                        .ok());

    constexpr int kThreads = 8;
    constexpr int kIters = 1000;
    constexpr int kRows = 16;
    std::atomic<bool> failed{false};

    std::vector<std::thread> workers;
    workers.reserve(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        workers.emplace_back([&, t]() {
            // A value unique to this thread, so any cross-thread contamination is detectable.
            const std::string expected = "val_" + std::to_string(t);
            auto json_col = BinaryColumn::create();
            for (int r = 0; r < kRows; ++r) {
                json_col->append(R"({"k":")" + expected + R"(","n":{"k":")" + expected + R"("}})");
            }
            Columns cols;
            cols.emplace_back(json_col);
            cols.emplace_back(const_cols[1]);

            for (int it = 0; it < kIters && !failed.load(std::memory_order_relaxed); ++it) {
                auto res = JsonFunctions::get_json_string(ctx.get(), cols);
                if (!res.ok()) {
                    failed.store(true);
                    return;
                }
                auto v = ColumnHelper::cast_to<TYPE_VARCHAR>(res.value());
                for (int r = 0; r < kRows; ++r) {
                    if (v->get_slice(r).to_string() != expected) {
                        failed.store(true);
                        return;
                    }
                }
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }
    ASSERT_FALSE(failed.load()) << "get_json_string scrambled values across concurrent drivers sharing one context";

    ASSERT_TRUE(
            JsonFunctions::native_json_path_close(ctx.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL).ok());
}

TEST_F(JsonFunctionsTest, json_extract_paths_concurrent_shared_context) {
    std::unique_ptr<FunctionContext> context(FunctionContext::create_test_context());
    Columns constants{nullptr, const_text("$.nested.a", 16), const_text("$.nested.b", 16)};
    context->set_constant_columns(constants);
    ASSERT_OK(JsonFunctions::json_extract_paths_prepare(context.get(), FunctionContext::FRAGMENT_LOCAL));
    std::atomic<bool> failed{false};
    std::vector<std::thread> workers;
    for (int thread = 0; thread < 8; ++thread) {
        workers.emplace_back([&, thread]() {
            std::string value = "thread_" + std::to_string(thread);
            auto input = BinaryColumn::create();
            for (int row = 0; row < 16; ++row) {
                input->append("{\"nested\":{\"b\":\"" + value + "\",\"a\":\"" + value + "\"}}");
            }
            Columns columns{input, constants[1], constants[2]};
            for (int iteration = 0; iteration < 200 && !failed.load(); ++iteration) {
                auto result = JsonFunctions::json_extract_paths(context.get(), columns);
                if (!result.ok()) {
                    failed = true;
                    break;
                }
                ColumnViewer<TYPE_JSON> viewer(result.value());
                for (int row = 0; row < 16; ++row) {
                    if (viewer.is_null(row)) {
                        failed = true;
                        break;
                    }
                    auto object = viewer.value(row)->to_vslice();
                    for (const auto& key : {"0", "1"}) {
                        auto actual = object.get(key);
                        if (!actual.isString() || actual.copyString() != value) {
                            failed = true;
                        }
                    }
                }
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    ASSERT_FALSE(failed.load());
    ASSERT_OK(JsonFunctions::json_extract_paths_close(context.get(), FunctionContext::FRAGMENT_LOCAL));
}

} // namespace starrocks
