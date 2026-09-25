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

#include "column/column_builder.h"
#include "common/config.h"
#include "exprs/bitmap_functions.h"
#include "exprs/encryption_functions.h"
#include "exprs/hyperloglog_functions.h"
#include "exprs/like_predicate.h"
#include "exprs/selected_column.h"
#include "exprs/time_functions.h"

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

class SelectedFunctionsTest : public ::testing::Test {
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

TEST_F(SelectedFunctionsTest, CryptoAndEncoding) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns input{strings({"abc", nullptr, "unselected", "", "Größe"})};
    compare(ctx.get(), EncryptionFunctions::md5, EncryptionFunctions::md5_selected, input);
    compare(ctx.get(), EncryptionFunctions::to_base64, EncryptionFunctions::to_base64_selected, input);
    compare(ctx.get(), EncryptionFunctions::from_base64, EncryptionFunctions::from_base64_selected,
            {strings({"YWJj", nullptr, "unselected", "", "////"})});
    Columns multiple{input[0], strings({"left", "right", "unselected", nullptr, "tail"})};
    compare(ctx.get(), EncryptionFunctions::md5sum, EncryptionFunctions::md5sum_selected, multiple);
    compare(ctx.get(), EncryptionFunctions::md5sum_numeric, EncryptionFunctions::md5sum_numeric_selected, multiple);
    auto key = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("secret"), 5);
    auto iv = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(""), 5);
    auto mode = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("AES_128_ECB"), 5);
    compare(ctx.get(), EncryptionFunctions::aes_encrypt_with_mode, EncryptionFunctions::aes_encrypt_with_mode_selected,
            {input[0], key, iv, mode});
    auto encrypted = EncryptionFunctions::aes_encrypt_with_mode(ctx.get(), {input[0], key, iv, mode});
    ASSERT_TRUE(encrypted.ok());
    compare(ctx.get(), EncryptionFunctions::aes_decrypt_with_mode, EncryptionFunctions::aes_decrypt_with_mode_selected,
            {encrypted.value(), key, iv, mode});
}

TEST_F(SelectedFunctionsTest, ConstantAndOnlyNullInputs) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto constant = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("abc"), 5);
    compare(ctx.get(), EncryptionFunctions::md5, EncryptionFunctions::md5_selected, {constant});
    compare(ctx.get(), EncryptionFunctions::md5sum, EncryptionFunctions::md5sum_selected,
            {constant, ColumnHelper::create_const_null_column(5)});
    compare(ctx.get(), EncryptionFunctions::from_base64, EncryptionFunctions::from_base64_selected,
            {ColumnHelper::create_const_null_column(5)});
}

TEST_F(SelectedFunctionsTest, UnselectedRowsCannotRaiseErrors) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    config::max_length_for_to_base64 = 64;
    std::string oversized(1024, 'x');
    auto input = strings({"abc", nullptr, oversized.c_str(), "", "tail"});
    compare(ctx.get(), EncryptionFunctions::to_base64, EncryptionFunctions::to_base64_selected, {input});
    std::vector<uint32_t> invalid{2};
    EXPECT_ANY_THROW(EncryptionFunctions::to_base64_selected(ctx.get(), {{input, &invalid}}, 1));

    auto text = strings({"abc123", nullptr, "unused", "", "xx456yy"});
    auto patterns = strings({"[0-9]+", "[a-z]+", "[", ".*", "456"});
    ctx->set_constant_columns({nullptr, nullptr});
    ASSERT_TRUE(LikePredicate::regex_prepare(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
    compare(ctx.get(), LikePredicate::regex, LikePredicate::regex_selected, {text, patterns});
    EXPECT_FALSE(ctx->has_error());
    auto result = LikePredicate::regex_selected(ctx.get(), {{text, &invalid}, {patterns, &invalid}}, 1);
    ASSERT_TRUE(result.ok());
    EXPECT_TRUE(ctx->has_error());
    EXPECT_TRUE(result.value()->is_null(0));
    ASSERT_TRUE(LikePredicate::regex_close(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
}

TEST_F(SelectedFunctionsTest, BitmapObjectsRemainUnchanged) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    Columns text{strings({"1,2,3", nullptr, "unused", "", "7,9,11"})};
    compare(ctx.get(), BitmapFunctions::bitmap_from_string, BitmapFunctions::bitmap_from_string_selected, text);
    compare(ctx.get(), BitmapFunctions::bitmap_hash, BitmapFunctions::bitmap_hash_selected, text);
    compare(ctx.get(), BitmapFunctions::bitmap_hash64, BitmapFunctions::bitmap_hash64_selected, text);
    auto bitmap = BitmapFunctions::bitmap_from_string(ctx.get(), text);
    ASSERT_TRUE(bitmap.ok());
    Columns input{bitmap.value()};
    compare(ctx.get(), BitmapFunctions::bitmap_count, BitmapFunctions::bitmap_count_selected, input);
    compare(ctx.get(), BitmapFunctions::bitmap_to_string, BitmapFunctions::bitmap_to_string_selected, input);
    compare(ctx.get(), BitmapFunctions::bitmap_to_array, BitmapFunctions::bitmap_to_array_selected, input);
    compare(ctx.get(), BitmapFunctions::bitmap_to_binary, BitmapFunctions::bitmap_to_binary_selected, input);
    compare(ctx.get(), BitmapFunctions::bitmap_to_base64, BitmapFunctions::bitmap_to_base64_selected, input);
    input.emplace_back(bitmap.value());
    compare(ctx.get(), BitmapFunctions::bitmap_and, BitmapFunctions::bitmap_and_selected, input);
    compare(ctx.get(), BitmapFunctions::bitmap_or, BitmapFunctions::bitmap_or_selected, input);
    compare(ctx.get(), BitmapFunctions::bitmap_xor, BitmapFunctions::bitmap_xor_selected, input);
    compare(ctx.get(), BitmapFunctions::bitmap_andnot, BitmapFunctions::bitmap_andnot_selected, input);
    compare(ctx.get(), BitmapFunctions::bitmap_has_any, BitmapFunctions::bitmap_has_any_selected, input);
}

TEST_F(SelectedFunctionsTest, DateParserAndHll) {
    std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
    auto dates = strings({"2024-02-29", nullptr, "invalid unselected", "1900-03-01", "2026-09-25"});
    auto format = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice("%Y-%m-%d"), 5);
    ctx->set_constant_columns({nullptr, format});
    ASSERT_TRUE(TimeFunctions::str_to_date_prepare(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    compare(ctx.get(), TimeFunctions::str_to_date, TimeFunctions::str_to_date_selected, {dates, format});
    ASSERT_TRUE(TimeFunctions::str_to_date_close(ctx.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    compare(ctx.get(), HyperloglogFunctions::hll_hash, HyperloglogFunctions::hll_hash_selected, {dates});
    auto hll = HyperloglogFunctions::hll_hash(ctx.get(), {dates});
    ASSERT_TRUE(hll.ok());
    compare(ctx.get(), HyperloglogFunctions::hll_cardinality, HyperloglogFunctions::hll_cardinality_selected,
            {hll.value()});
}

TEST_F(SelectedFunctionsTest, RegexPreservesPreparedEngine) {
    auto text = strings({"abc123", nullptr, "invalid unselected", "", "xx456yy"});
    for (const auto* pattern : {"^[a-z]+[0-9]+$", "^abc", "yy$", "456", "^abc123$"}) {
        std::unique_ptr<FunctionContext> ctx(FunctionContext::create_test_context());
        auto pat = ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(pattern), 5);
        ctx->set_constant_columns({nullptr, pat});
        ASSERT_TRUE(LikePredicate::regex_prepare(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
        compare(ctx.get(), LikePredicate::regex, LikePredicate::regex_selected, {text, pat});
        ASSERT_TRUE(LikePredicate::regex_close(ctx.get(), FunctionContext::THREAD_LOCAL).ok());
    }
}
} // namespace starrocks
