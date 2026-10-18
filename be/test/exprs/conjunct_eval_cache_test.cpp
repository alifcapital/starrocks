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

#include <algorithm>
#include <atomic>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

#include "column/binary_column.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "common/object_pool.h"
#include "exec/exec_node.h"
#include "exprs/binary_predicate.h"
#include "exprs/case_expr.h"
#include "exprs/column_ref.h"
#include "exprs/compound_predicate.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/function_call_expr.h"
#include "exprs/json_functions.h"
#include "exprs/literal.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "simd/simd.h"
#include "testutil/assert.h"

namespace starrocks {

// Records whether an evaluation sees a conjunct cache, through its own context and through another one.
class CacheProbeExpr final : public Expr {
public:
    CacheProbeExpr() : Expr(TypeDescriptor(TYPE_BOOLEAN), false) {}
    Expr* clone(ObjectPool* pool) const override { return pool->add(new CacheProbeExpr(*this)); }
    StatusOr<ColumnPtr> evaluate_checked(ExprContext* context, Chunk* chunk) override {
        own = ExprContext::conjunct_eval_cache(context) != nullptr;
        through_other = other != nullptr && ExprContext::conjunct_eval_cache(other) != nullptr;
        return ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, chunk->num_rows());
    }

    const ExprContext* other = nullptr;
    bool own = false;
    bool through_other = false;
};

// Conjuncts over json_extract_paths() of one column share its result through ExprContext::ConjunctEvalCache.
class ConjunctEvalCacheTest : public ::testing::Test {
protected:
    static constexpr SlotId kDocs = 1;
    static constexpr SlotId kOtherDocs = 2;
    static constexpr SlotId kFlag = 3;
    static constexpr size_t kRows = 4096;
    static constexpr int64_t kParsedRows = kRows;

    void SetUp() override {
        TQueryOptions options;
        options.__set_enable_conditional_two_phase_eval(true);
        _state = std::make_unique<RuntimeState>(TUniqueId(), options, TQueryGlobals(), nullptr);
        options.__set_allow_throw_exception(true);
        _strict_state = std::make_unique<RuntimeState>(TUniqueId(), options, TQueryGlobals(), nullptr);
    }

    void TearDown() override {
        for (auto& [context, state] : _contexts) {
            context->close(state);
        }
    }

    static TypeDescriptor varchar_type() { return TypeDescriptor::create_varchar_type(1 << 20); }
    static TypeDescriptor json_type() { return TypeDescriptor::create_json_type(); }

    // Documents whose "a" is true on every `one_in`-th row (never when one_in is 0).
    static ColumnPtr documents(size_t one_in) {
        auto column = BinaryColumn::create();
        for (size_t row = 0; row < kRows; ++row) {
            const bool a = one_in != 0 && row % one_in == 0;
            column->append(std::string("{\"a\":") + (a ? "true" : "false") + ",\"b\":\"s" + std::to_string(row) +
                           "\",\"c\":" + std::to_string(row % 4 == 1 ? -1 : static_cast<int>(row)) + "}");
        }
        return column;
    }

    Chunk chunk(size_t one_in) {
        Chunk chunk;
        chunk.append_column(documents(one_in), kDocs);
        chunk.append_column(documents(2), kOtherDocs);
        auto flag = BooleanColumn::create();
        for (size_t row = 0; row < kRows; ++row) {
            flag->append(row % 3 == 0);
        }
        chunk.append_column(std::move(flag), kFlag);
        return chunk;
    }

    Expr* col(SlotId slot, const TypeDescriptor& type) { return _pool.add(new ColumnRef(type, slot)); }

    Expr* text(const std::string& value) {
        return _pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(value), 1),
                                               varchar_type()));
    }

    Expr* call(const char* name, int64_t fid, const TypeDescriptor& result_type, const std::vector<Expr*>& args) {
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_function_name(name);
        fn.__set_name(fn_name);
        fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
        fn.__set_fid(fid);
        fn.__set_has_var_args(false);
        std::vector<TTypeDesc> arg_types;
        for (const auto* arg : args) {
            arg_types.push_back(arg->type().to_thrift());
        }
        fn.__set_arg_types(arg_types);
        fn.__set_ret_type(result_type.to_thrift());

        TExprNode node;
        node.__set_node_type(TExprNodeType::FUNCTION_CALL);
        node.__set_num_children(args.size());
        node.__set_is_nullable(true);
        node.__set_type(result_type.to_thrift());
        node.__set_fn(fn);
        auto* expr = _pool.add(new VectorizedFunctionCallExpr(node));
        for (auto* arg : args) {
            expr->add_child(arg);
        }
        return expr;
    }

    // json_extract_paths(slot, paths...), a new expression on every call as in separate conjuncts.
    Expr* extract(SlotId slot, const std::vector<std::string>& paths) {
        std::vector<Expr*> args{col(slot, varchar_type())};
        for (const auto& path : paths) {
            args.push_back(text(path));
        }
        return call("json_extract_paths", 1100108, json_type(), args);
    }

    // get_json_bool(E, '$.0'): "a" is true.
    Expr* a_is_true(Expr* extracted) {
        return call("get_json_bool", 110021, TypeDescriptor(TYPE_BOOLEAN), {extracted, text("$.0")});
    }
    // json_exists(E, '$.1'): "b" is present.
    Expr* b_exists(Expr* extracted) {
        return call("json_exists", 110007, TypeDescriptor(TYPE_BOOLEAN), {extracted, text("$.1")});
    }
    // get_json_int(E, '$.2') >= 0.
    Expr* c_not_negative(Expr* extracted) {
        Expr* value = call("get_json_int", 110023, TypeDescriptor(TYPE_BIGINT), {extracted, text("$.2")});
        TExprNode node;
        node.__set_node_type(TExprNodeType::BINARY_PRED);
        node.__set_opcode(TExprOpcode::GE);
        node.__set_num_children(2);
        node.__set_is_nullable(true);
        node.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
        node.__set_child_type_desc(TypeDescriptor(TYPE_BIGINT).to_thrift());
        Expr* predicate = _pool.add(VectorizedBinaryPredicateFactory::from_thrift(node));
        predicate->add_child(value);
        predicate->add_child(_pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_BIGINT>(0, 1),
                                                             TypeDescriptor(TYPE_BIGINT))));
        return predicate;
    }

    // CASE WHEN flag THEN get_json_bool(E, '$.0') ELSE true END
    Expr* case_a_is_true(Expr* extracted) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::CASE_EXPR);
        node.__set_num_children(3);
        node.__set_is_nullable(true);
        node.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
        node.__set_child_type(TPrimitiveType::BOOLEAN);
        TCaseExpr case_expr;
        case_expr.__set_has_case_expr(false);
        case_expr.__set_has_else_expr(true);
        node.__set_case_expr(case_expr);
        Expr* expr = _pool.add(VectorizedCaseExprFactory::from_thrift(node));
        expr->add_child(col(kFlag, TypeDescriptor(TYPE_BOOLEAN)));
        expr->add_child(a_is_true(extracted));
        expr->add_child(_pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_BOOLEAN>(true, 1),
                                                        TypeDescriptor(TYPE_BOOLEAN))));
        return expr;
    }

    // The three conjuncts, each with its own json_extract_paths() over the same paths.
    std::vector<Expr*> conjuncts(SlotId slot = kDocs) {
        const std::vector<std::string> paths{"$.a", "$.b", "$.c"};
        return {a_is_true(extract(slot, paths)), b_exists(extract(slot, paths)), c_not_negative(extract(slot, paths))};
    }

    std::vector<ExprContext*> open(const std::vector<Expr*>& roots, bool strict = false) {
        RuntimeState* state = strict ? _strict_state.get() : _state.get();
        std::vector<ExprContext*> contexts;
        for (auto* root : roots) {
            auto* context = _pool.add(new ExprContext(root));
            EXPECT_OK(context->prepare(state));
            EXPECT_OK(context->open(state));
            _contexts.emplace_back(context, state);
            contexts.push_back(context);
        }
        return contexts;
    }

    Expr* compound(TExprOpcode::type opcode, Expr* left, Expr* right) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::COMPOUND_PRED);
        node.__set_opcode(opcode);
        node.__set_num_children(2);
        node.__set_is_nullable(true);
        node.__set_type(TypeDescriptor(TYPE_BOOLEAN).to_thrift());
        Expr* expr = _pool.add(VectorizedCompoundPredicateFactory::from_thrift(node));
        expr->add_child(left);
        expr->add_child(right);
        return expr;
    }

    // The rows that pass every conjunct, each conjunct evaluated on the whole chunk without a cache.
    static Filter expected_filter(const std::vector<ExprContext*>& contexts, Chunk* chunk) {
        Filter filter(chunk->num_rows(), 1);
        for (auto* context : contexts) {
            auto column = context->evaluate(chunk);
            EXPECT_OK(column.status());
            ColumnHelper::merge_two_filters(column.value(), &filter);
        }
        return filter;
    }

    static std::vector<uint32_t> selected(const Filter& filter) {
        std::vector<uint32_t> rows;
        for (uint32_t row = 0; row < filter.size(); ++row) {
            if (filter[row]) rows.push_back(row);
        }
        return rows;
    }

    // Evaluates the conjuncts one after another on the rows that the previous ones kept, as the selected
    // paths of the parquet reader do, and returns the rows that pass all of them.
    static std::vector<uint32_t> kept_rows(const std::vector<ExprContext*>& contexts, Chunk* chunk,
                                           ExprContext::ConjunctEvalCache* cache, std::vector<uint32_t> rows) {
        for (auto* context : contexts) {
            if (rows.empty()) break;
            auto result = cache != nullptr ? context->evaluate_selected_with_conjunct_cache(chunk, rows, cache)
                                           : context->root()->evaluate_selected(context, chunk, rows);
            EXPECT_OK(result.status());
            Filter pass(rows.size(), 1);
            ColumnHelper::merge_two_filters(result.value(), &pass);
            std::vector<uint32_t> next;
            for (size_t i = 0; i < rows.size(); ++i) {
                if (pass[i]) next.push_back(rows[i]);
            }
            rows = std::move(next);
        }
        return rows;
    }

    std::unique_ptr<RuntimeState> _state;
    std::unique_ptr<RuntimeState> _strict_state;
    ObjectPool _pool;
    std::vector<std::pair<ExprContext*, RuntimeState*>> _contexts;
};

TEST_F(ConjunctEvalCacheTest, SameFilterAndOneParsePerRow) {
    // The first conjunct keeps 0%, 3%, 50% and 100% of the rows.
    for (size_t one_in : {size_t{0}, size_t{33}, size_t{2}, size_t{1}}) {
        std::vector<size_t> order{0, 1, 2};
        do {
            SCOPED_TRACE("one_in=" + std::to_string(one_in) + " order=" + std::to_string(order[0]) +
                         std::to_string(order[1]) + std::to_string(order[2]));
            Chunk input = chunk(one_in);
            const auto all = conjuncts();
            std::vector<Expr*> roots;
            for (size_t i : order) roots.push_back(all[i]);
            const auto contexts = open(roots);
            const Filter expected = expected_filter(contexts, &input);

            json_extract_paths_rows_for_test = 0;
            Filter filter(kRows, 1);
            auto hit = ExecNode::eval_conjuncts_into_filter(contexts, &input, &filter);
            ASSERT_OK(hit.status());
            ASSERT_EQ(SIMD::count_nonzero(expected), hit.value());
            if (hit.value() > 0) {
                ASSERT_EQ(expected, filter);
            }
            ASSERT_EQ(kParsedRows, json_extract_paths_rows_for_test.load());

            std::vector<uint32_t> all_rows(kRows);
            std::iota(all_rows.begin(), all_rows.end(), 0);
            const auto expected_rows = kept_rows(contexts, &input, nullptr, all_rows);
            ASSERT_EQ(selected(expected), expected_rows);
            json_extract_paths_rows_for_test = 0;
            ExprContext::ConjunctEvalCache cache;
            ASSERT_EQ(expected_rows, kept_rows(contexts, &input, &cache, all_rows));
            // The first conjunct parses every row it sees; the others read its values.
            ASSERT_EQ(kParsedRows, json_extract_paths_rows_for_test.load());
            ASSERT_EQ(1u, cache.shared_results.size());
        } while (std::next_permutation(order.begin(), order.end()));
    }
}

TEST_F(ConjunctEvalCacheTest, RowsMissedByTheFirstConjunctAreComputedLater) {
    Chunk input = chunk(2);
    const auto all = conjuncts();
    const auto contexts = open(all);
    const Filter expected = expected_filter(contexts, &input);

    // The first conjunct sees the even rows only; the second one asks for every row.
    std::vector<uint32_t> even;
    for (uint32_t row = 0; row < kRows; row += 2) even.push_back(row);
    json_extract_paths_rows_for_test = 0;
    ExprContext::ConjunctEvalCache cache;
    auto first = contexts[0]->evaluate_selected_with_conjunct_cache(&input, even, &cache);
    ASSERT_OK(first.status());
    ASSERT_EQ(static_cast<int64_t>(even.size()), json_extract_paths_rows_for_test.load());
    auto second = contexts[1]->evaluate_with_conjunct_cache(&input, nullptr, &cache);
    ASSERT_OK(second.status());
    ASSERT_EQ(kParsedRows, json_extract_paths_rows_for_test.load());
    auto third = contexts[2]->evaluate_with_conjunct_cache(&input, nullptr, &cache);
    ASSERT_OK(third.status());
    ASSERT_EQ(kParsedRows, json_extract_paths_rows_for_test.load());

    Filter filter(kRows, 1);
    ColumnHelper::merge_two_filters(second.value(), &filter);
    ColumnHelper::merge_two_filters(third.value(), &filter);
    auto first_all = contexts[0]->evaluate(&input);
    ASSERT_OK(first_all.status());
    ColumnHelper::merge_two_filters(first_all.value(), &filter);
    ASSERT_EQ(expected, filter);
}

TEST_F(ConjunctEvalCacheTest, CaseBranchRowsAreComputedLater) {
    Chunk input = chunk(2);
    const std::vector<std::string> paths{"$.a", "$.b", "$.c"};
    const std::vector<Expr*> roots{case_a_is_true(extract(kDocs, paths)), b_exists(extract(kDocs, paths)),
                                   c_not_negative(extract(kDocs, paths))};
    const auto contexts = open(roots);
    const Filter expected = expected_filter(contexts, &input);

    Filter filter(kRows, 1);
    auto hit = ExecNode::eval_conjuncts_into_filter(contexts, &input, &filter);
    ASSERT_OK(hit.status());
    ASSERT_EQ(SIMD::count_nonzero(expected), hit.value());
    ASSERT_EQ(expected, filter);

    std::vector<uint32_t> all_rows(kRows);
    std::iota(all_rows.begin(), all_rows.end(), 0);
    ExprContext::ConjunctEvalCache cache;
    ASSERT_EQ(selected(expected), kept_rows(contexts, &input, &cache, all_rows));
}

TEST_F(ConjunctEvalCacheTest, EntriesAreKeyedByPathsAndInput) {
    Chunk input = chunk(1);
    const std::vector<Expr*> roots{
            a_is_true(extract(kDocs, {"$.a", "$.b", "$.c"})), a_is_true(extract(kDocs, {"$.a", "$.c"})),
            a_is_true(extract(kOtherDocs, {"$.a", "$.b", "$.c"})), b_exists(extract(kDocs, {"$.a", "$.b", "$.c"}))};
    const auto contexts = open(roots);
    json_extract_paths_rows_for_test = 0;
    ExprContext::ConjunctEvalCache cache;
    std::vector<ColumnPtr> results;
    for (auto* context : contexts) {
        auto result = context->evaluate_with_conjunct_cache(&input, nullptr, &cache);
        ASSERT_OK(result.status());
        results.push_back(result.value());
    }
    ASSERT_EQ(3u, cache.shared_results.size());
    ASSERT_EQ(3 * kParsedRows, json_extract_paths_rows_for_test.load());
    for (size_t i = 0; i < contexts.size(); ++i) {
        auto expected = contexts[i]->evaluate(&input);
        ASSERT_OK(expected.status());
        for (size_t row = 0; row < kRows; ++row) {
            ASSERT_EQ(expected.value()->debug_item(row), results[i]->debug_item(row)) << i << " " << row;
        }
    }
    // The other column holds "a" true on even rows only.
    ASSERT_EQ(kRows / 2, ColumnHelper::count_true_with_notnull(results[2]));
}

TEST_F(ConjunctEvalCacheTest, PrunedChunkDoesNotReuseEntries) {
    // eval_conjuncts() prunes a chunk with few columns once a conjunct drops most rows; the later conjuncts
    // must see the values of the rows left.
    for (size_t one_in : {size_t{33}, size_t{2}}) {
        Chunk input = chunk(one_in);
        Chunk reference = chunk(one_in);
        const auto contexts = open(conjuncts());
        const Filter expected = expected_filter(contexts, &reference);
        ASSERT_OK(ExecNode::eval_conjuncts(contexts, &input));
        ASSERT_EQ(SIMD::count_nonzero(expected), input.num_rows());
        reference.filter(expected);
        const auto& kept = input.get_column_by_slot_id(kDocs);
        const auto& want = reference.get_column_by_slot_id(kDocs);
        for (size_t row = 0; row < input.num_rows(); ++row) {
            ASSERT_EQ(want->debug_item(row), kept->debug_item(row));
        }
    }
}

// Pipeline operators evaluate one ExprContext from several threads at once (SelectOperator keeps a reference to
// the conjuncts of its factory; lake and OLAP chunk sources evaluate the conjuncts of the scan node). Each thread
// must use only its own cache and leave no cache installed.
TEST_F(ConjunctEvalCacheTest, SharedContextAcrossThreads) {
    const auto contexts = open(conjuncts());
    constexpr int kThreads = 4;
    constexpr int kIterations = 1000;
    const std::vector<size_t> one_in{1, 2, 33, 3};
    std::vector<size_t> expected(kThreads);
    for (int t = 0; t < kThreads; ++t) {
        Chunk input = chunk(one_in[t]);
        expected[t] = SIMD::count_nonzero(expected_filter(contexts, &input));
    }
    std::atomic<int> wrong{0};
    std::atomic<int> errors{0};
    std::atomic<int> dangling{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t] {
            Chunk input = chunk(one_in[t]);
            for (int i = 0; i < kIterations; ++i) {
                FilterPtr filter;
                auto st = ExecNode::eval_conjuncts(contexts, &input, &filter, false);
                if (!st.ok()) {
                    errors++;
                    continue;
                }
                if (SIMD::count_nonzero(*filter) != expected[t]) {
                    wrong++;
                }
            }
            for (auto* context : contexts) {
                dangling += ExprContext::conjunct_eval_cache(context) != nullptr;
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(0, wrong.load());
    EXPECT_EQ(0, errors.load());
    EXPECT_EQ(0, dangling.load());
}

TEST_F(ConjunctEvalCacheTest, CacheIsGivenOnlyToItsContext) {
    Chunk input = chunk(1);
    auto* probe = _pool.add(new CacheProbeExpr());
    auto* other = _pool.add(new CacheProbeExpr());
    const auto contexts = open({probe, other});
    probe->other = contexts[1];
    ExprContext::ConjunctEvalCache cache;
    ASSERT_OK(contexts[0]->evaluate_with_conjunct_cache(&input, nullptr, &cache).status());
    EXPECT_TRUE(probe->own);
    EXPECT_FALSE(probe->through_other);
    EXPECT_EQ(nullptr, ExprContext::conjunct_eval_cache(contexts[0]));
}

TEST_F(ConjunctEvalCacheTest, StrictModeSkipsRightOperandOfAnd) {
    // Malformed documents, and a flag that is false on every row. In strict mode json_extract_paths() fails on
    // them, but AND does not evaluate its right operand on a chunk whose left operand is all false.
    Chunk input;
    auto docs = BinaryColumn::create();
    auto flag = BooleanColumn::create();
    for (size_t row = 0; row < kRows; ++row) {
        docs->append(row % 2 == 0 ? std::string("{\"a\":true,") : std::string("{\"a\":tru}"));
        flag->append(0);
    }
    input.append_column(std::move(docs), kDocs);
    input.append_column(std::move(flag), kFlag);
    const std::vector<std::string> paths{"$.a", "$.b"};
    const auto contexts = open({compound(TExprOpcode::COMPOUND_AND, col(kFlag, TypeDescriptor(TYPE_BOOLEAN)),
                                         a_is_true(extract(kDocs, paths))),
                                a_is_true(extract(kDocs, paths))},
                               true);
    Filter filter(kRows, 1);
    auto hit = ExecNode::eval_conjuncts_into_filter({contexts[0]}, &input, &filter);
    ASSERT_OK(hit.status());
    EXPECT_EQ(0u, hit.value());
    EXPECT_FALSE(contexts[1]->evaluate(&input).ok());
}

} // namespace starrocks
