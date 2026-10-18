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

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "column/array_column.h"
#include "column/chunk.h"
#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/json_column.h"
#include "column/nullable_column.h"
#include "common/object_pool.h"
#include "exprs/arithmetic_expr.h"
#include "exprs/binary_predicate.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/compound_predicate.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/function_call_expr.h"
#include "exprs/in_predicate.h"
#include "exprs/is_null_predicate.h"
#include "exprs/literal.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "util/json.h"

namespace starrocks {
namespace {

// Slots of the test chunk. The row at index 2 holds values that make the evaluation fail (a zero divisor), and
// it is never selected by the main row set, so a selected evaluation must not see it.
constexpr SlotId kS = 1;     // nullable VARCHAR
constexpr SlotId kT = 2;     // VARCHAR
constexpr SlotId kJs = 3;    // nullable VARCHAR with JSON documents
constexpr SlotId kJn = 4;    // nullable VARCHAR with JSON nulls
constexpr SlotId kBi = 5;    // BIGINT
constexpr SlotId kNi = 6;    // nullable BIGINT
constexpr SlotId kZb = 7;    // BIGINT with a zero at row 2
constexpr SlotId kArr = 8;   // nullable ARRAY<INT>
constexpr SlotId kArr2 = 9;  // ARRAY<INT>
constexpr SlotId kJson = 10; // nullable JSON with SQL NULL and JSON null values
const std::vector<SlotId> kSlots = {kS, kT, kJs, kJn, kBi, kNi, kZb, kArr, kArr2, kJson};

// md5('abc')
const char* const kAbcMd5 = "900150983cd24fb0d6963f7d28e17f72";

const std::vector<std::vector<uint32_t>> kRowSets = {{4, 1, 0, 4}, {}, {5}, {0, 1, 2, 3, 4, 5}, {3, 3, 3}, {2, 4}};

const std::vector<TExprOpcode::type> kCompareOps = {TExprOpcode::EQ,         TExprOpcode::NE, TExprOpcode::LT,
                                                    TExprOpcode::LE,         TExprOpcode::GT, TExprOpcode::GE,
                                                    TExprOpcode::EQ_FOR_NULL};

TypeDescriptor varchar_type() {
    return TypeDescriptor::create_varchar_type(255);
}
TypeDescriptor bigint_type() {
    return TypeDescriptor(TYPE_BIGINT);
}
TypeDescriptor boolean_type() {
    return TypeDescriptor(TYPE_BOOLEAN);
}
TypeDescriptor int_array() {
    return TypeDescriptor::create_array_type(TypeDescriptor(TYPE_INT));
}

ColumnPtr strings(std::initializer_list<const char*> values, bool nullable) {
    ColumnBuilder<TYPE_VARCHAR> builder(values.size());
    for (const char* value : values) {
        if (value == nullptr) {
            builder.append_null();
        } else {
            builder.append(Slice(value));
        }
    }
    return nullable ? builder.build_nullable_column() : builder.build(false);
}

// Rows 2 and 4 are JSON null, row 1 is SQL NULL.
ColumnPtr json_values() {
    ColumnBuilder<TYPE_JSON> builder(6);
    auto object = JsonValue::parse(R"({"a":1})");
    CHECK(object.ok());
    builder.append(std::move(object.value()));
    builder.append_null();
    builder.append(JsonValue::from_null());
    builder.append(JsonValue::from_int(1));
    builder.append(JsonValue::from_null());
    builder.append(JsonValue::from_int(7));
    return builder.build_nullable_column();
}

ColumnPtr arrays(const std::vector<std::vector<int32_t>>& values, const std::vector<uint8_t>& null_rows) {
    auto elements = NullableColumn::create(Int32Column::create(), NullColumn::create());
    auto offsets = UInt32Column::create();
    offsets->append(0);
    for (const auto& row : values) {
        for (auto value : row) {
            elements->append_datum(Datum(value));
        }
        offsets->append(elements->size());
    }
    auto array = ArrayColumn::create(std::move(elements), std::move(offsets));
    if (null_rows.empty()) {
        return array;
    }
    auto nulls = NullColumn::create();
    nulls->append_numbers(null_rows.data(), null_rows.size());
    return NullableColumn::create(std::move(array), std::move(nulls));
}

// The logical value of row `i`: a constant column holds it in row 0 of its data column.
std::string item(const ColumnPtr& column, size_t i) {
    if (column->is_constant()) {
        return down_cast<const ConstColumn&>(*column).data_column()->debug_item(0);
    }
    return column->debug_item(i);
}

std::string to_string(const std::vector<uint32_t>& rows) {
    std::string result = "rows:";
    for (auto row : rows) {
        result += " " + std::to_string(row);
    }
    return result;
}

// Evaluates a selected row, as a leaf of an operator, from the original chunk. It records what the operator
// asks for. Evaluation of a copy of the input is an error: that is what the operator must not do.
class ProbeExpr final : public Expr {
public:
    // `kernel` is a function call below the probe. It makes the operators above see a function with a
    // selection-aware kernel in their subtree. The probe never evaluates it.
    ProbeExpr(const TypeDescriptor& type, SlotId slot, Expr* kernel) : Expr(type, false), _slot(slot) {
        add_child(kernel);
    }
    Expr* clone(ObjectPool* pool) const override { return pool->add(new ProbeExpr(*this)); }
    bool is_constant() const override { return false; }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        return Status::InternalError("probe is evaluated on a copy of the input");
    }
    StatusOr<ColumnPtr> evaluate_selected(ExprContext*, Chunk* chunk, const std::vector<uint32_t>& rows) override {
        ++calls;
        seen_chunk = chunk;
        seen_rows = rows;
        auto source = chunk->get_column_by_slot_id(_slot);
        auto result = source->clone_empty();
        result->append_selective(*source, rows.data(), 0, rows.size());
        return result;
    }

    int calls = 0;
    Chunk* seen_chunk = nullptr;
    std::vector<uint32_t> seen_rows;

private:
    SlotId _slot;
};

// An expression that must not be evaluated at all.
class MustNotRunExpr final : public Expr {
public:
    explicit MustNotRunExpr(const TypeDescriptor& type) : Expr(type, false) {}
    Expr* clone(ObjectPool* pool) const override { return pool->add(new MustNotRunExpr(*this)); }
    bool is_constant() const override { return false; }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        ++calls;
        return Status::InternalError("must not be evaluated");
    }
    StatusOr<ColumnPtr> evaluate_selected(ExprContext*, Chunk*, const std::vector<uint32_t>&) override {
        ++calls;
        return Status::InternalError("must not be evaluated");
    }

    int calls = 0;
};

// Reads a slot from the chunk it is evaluated on and records that chunk. It has no selection-aware kernel.
class SlotReaderExpr final : public Expr {
public:
    SlotReaderExpr(const TypeDescriptor& type, SlotId slot) : Expr(type, false), _slot(slot) {}
    Expr* clone(ObjectPool* pool) const override { return pool->add(new SlotReaderExpr(*this)); }
    bool is_constant() const override { return false; }
    int get_slot_ids(std::vector<SlotId>* slots) const override {
        slots->push_back(_slot);
        return 1;
    }

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk* chunk) override {
        seen_chunks.push_back(chunk);
        seen_rows.push_back(chunk->num_rows());
        return chunk->get_column_by_slot_id(_slot);
    }

    std::vector<Chunk*> seen_chunks;
    std::vector<size_t> seen_rows;

private:
    SlotId _slot;
};

} // namespace

class SelectedOperatorsTest : public ::testing::Test {
protected:
    void SetUp() override {
        _chunk.append_column(strings({"abc", nullptr, "zzz", "", "abc", "tail"}, true), kS);
        _chunk.append_column(strings({"abc", "def", "ghi", "jkl", "mno", "abc"}, false), kT);
        _chunk.append_column(strings({R"({"a":1,"b":2})", nullptr, R"({"a":5,"b":0})", "not json", R"({"a":3,"b":1})",
                                      R"({"a":null,"b":7})"},
                                     true),
                             kJs);
        _chunk.append_column(strings({R"({"a":1})", nullptr, "null", "1", "null", "[1]"}, true), kJn);
        _chunk.append_column(ColumnTestHelper::build_column<int64_t>({10, 20, 30, 40, 50, 60}), kBi);
        _chunk.append_column(ColumnTestHelper::build_nullable_column<int64_t>({1, 2, 3, 4, 5, 6}, {0, 1, 0, 0, 0, 0}),
                             kNi);
        _chunk.append_column(ColumnTestHelper::build_column<int64_t>({1, 2, 0, 4, 5, 6}), kZb);
        _chunk.append_column(arrays({{3, 1, 2}, {}, {5}, {}, {9, 8}, {1}}, {0, 1, 0, 0, 0, 0}), kArr);
        _chunk.append_column(arrays({{1, 2, 3}, {7}, {5}, {}, {8, 9}, {2}}, {}), kArr2);
        _chunk.append_column(json_values(), kJson);
    }

    TypeDescriptor type_of(SlotId slot) {
        if (slot == kBi || slot == kNi || slot == kZb) return bigint_type();
        if (slot == kArr || slot == kArr2) return int_array();
        if (slot == kJson) return TypeDescriptor::create_json_type();
        return varchar_type();
    }

    Expr* col(SlotId slot) { return _pool.add(new ColumnRef(type_of(slot), slot)); }

    // Literals
    Expr* text(const std::string& value) {
        return _pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(value), 1),
                                               varchar_type()));
    }
    Expr* bigint_literal(int64_t value) {
        return _pool.add(
                new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_BIGINT>(value, 1), bigint_type()));
    }
    Expr* double_literal(double value) {
        return _pool.add(new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_DOUBLE>(value, 1),
                                               TypeDescriptor(TYPE_DOUBLE)));
    }
    Expr* bool_literal(bool value) {
        return _pool.add(
                new VectorizedLiteral(ColumnHelper::create_const_column<TYPE_BOOLEAN>(value, 1), boolean_type()));
    }
    Expr* null_literal(const TypeDescriptor& type = TypeDescriptor(TYPE_NULL)) {
        return _pool.add(new VectorizedLiteral(ColumnHelper::create_const_null_column(1), type));
    }
    Expr* array_literal(const std::vector<int32_t>& value) {
        return _pool.add(new VectorizedLiteral(ConstColumn::create(arrays({value}, {}), 1), int_array()));
    }

    // Function calls. All of them have a kernel that reads its input through row ids.
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
    Expr* md5(Expr* arg) { return call("md5", 120140, varchar_type(), {arg}); }
    Expr* json_int(Expr* json, const char* path) {
        return call("get_json_int", 110022, bigint_type(), {json, text(path)});
    }
    Expr* json_double(Expr* json, const char* path) {
        return call("get_json_double", 110001, TypeDescriptor(TYPE_DOUBLE), {json, text(path)});
    }
    Expr* parse_json(Expr* arg) { return call("parse_json", 110003, TypeDescriptor::create_json_type(), {arg}); }
    Expr* array_sort(Expr* arg) { return call("array_sort", 150113, int_array(), {arg}); }

    // Operators
    Expr* make(Expr* expr, const std::vector<Expr*>& children) {
        CHECK(expr != nullptr);
        _pool.add(expr);
        for (auto* child : children) {
            expr->add_child(child);
        }
        return expr;
    }
    Expr* binary(TExprOpcode::type opcode, const TypeDescriptor& child_type, Expr* l, Expr* r) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::BINARY_PRED);
        node.__set_opcode(opcode);
        node.__set_num_children(2);
        node.__set_is_nullable(true);
        node.__set_type(boolean_type().to_thrift());
        node.__set_child_type_desc(child_type.to_thrift());
        return make(VectorizedBinaryPredicateFactory::from_thrift(node), {l, r});
    }
    Expr* compound(TExprOpcode::type opcode, const std::vector<Expr*>& children) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::COMPOUND_PRED);
        node.__set_opcode(opcode);
        node.__set_num_children(children.size());
        node.__set_is_nullable(true);
        node.__set_type(boolean_type().to_thrift());
        return make(VectorizedCompoundPredicateFactory::from_thrift(node), children);
    }
    Expr* and_(Expr* l, Expr* r) { return compound(TExprOpcode::COMPOUND_AND, {l, r}); }
    Expr* or_(Expr* l, Expr* r) { return compound(TExprOpcode::COMPOUND_OR, {l, r}); }
    Expr* not_(Expr* child) { return compound(TExprOpcode::COMPOUND_NOT, {child}); }
    Expr* is_null(bool is_not_null, Expr* child) {
        TFunction fn;
        TFunctionName fn_name;
        fn_name.__set_function_name(is_not_null ? "is_not_null_pred" : "is_null_pred");
        fn.__set_name(fn_name);
        TExprNode node;
        node.__set_node_type(TExprNodeType::FUNCTION_CALL);
        node.__set_num_children(1);
        node.__set_is_nullable(false);
        node.__set_type(boolean_type().to_thrift());
        node.__set_fn(fn);
        return make(VectorizedIsNullPredicateFactory::from_thrift(node), {child});
    }
    Expr* in_pred(bool is_not_in, const TypeDescriptor& child_type, Expr* lhs, const std::vector<Expr*>& values) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::IN_PRED);
        node.__set_opcode(is_not_in ? TExprOpcode::FILTER_NOT_IN : TExprOpcode::FILTER_IN);
        node.__set_num_children(values.size() + 1);
        node.__set_is_nullable(true);
        node.__set_type(boolean_type().to_thrift());
        node.__set_child_type_desc(child_type.to_thrift());
        TInPredicate in;
        in.is_not_in = is_not_in;
        node.__set_in_predicate(in);
        std::vector<Expr*> children = {lhs};
        children.insert(children.end(), values.begin(), values.end());
        return make(VectorizedInPredicateFactory::from_thrift(node), children);
    }
    Expr* arith(TExprOpcode::type opcode, const TypeDescriptor& type, const std::vector<Expr*>& children) {
        TExprNode node;
        node.__set_node_type(TExprNodeType::ARITHMETIC_EXPR);
        node.__set_opcode(opcode);
        node.__set_num_children(children.size());
        node.__set_is_nullable(true);
        node.__set_type(type.to_thrift());
        return make(VectorizedArithmeticExprFactory::from_thrift(node), children);
    }
    Expr* cast(const TypeDescriptor& from, const TypeDescriptor& to, Expr* child) {
        Expr* expr = VectorizedCastExprFactory::from_type(from, to, child, &_pool);
        CHECK(expr != nullptr);
        return expr;
    }

    // Evaluation
    ExprContext* open(Expr* root, RuntimeState* state = nullptr) {
        auto it = _contexts.find(root);
        if (it != _contexts.end()) {
            return it->second.get();
        }
        if (state == nullptr) {
            state = &_state;
        }
        auto context = std::make_unique<ExprContext>(root);
        Status status = context->prepare(state);
        CHECK(status.ok()) << status;
        status = context->open(state);
        CHECK(status.ok()) << status;
        return (_contexts[root] = std::move(context)).get();
    }

    std::vector<std::string> snapshot() {
        std::vector<std::string> result;
        for (SlotId slot : kSlots) {
            result.push_back(_chunk.get_column_by_slot_id(slot)->debug_string());
        }
        return result;
    }

    // `selected` must hold the values of `full` at `rows`, in the order of `rows`.
    void expect_gathered(const ColumnPtr& full, const ColumnPtr& selected, const std::vector<uint32_t>& rows) {
        ASSERT_NE(nullptr, selected.get());
        ASSERT_EQ(rows.size(), selected->size());
        for (size_t i = 0; i < rows.size(); ++i) {
            EXPECT_EQ(item(full, rows[i]), item(selected, i)) << "row " << rows[i] << " at " << i;
        }
    }

    // Compare the selected evaluation of the tree with the full one, for several row lists.
    void check(Expr* root, RuntimeState* state = nullptr) {
        ExprContext* context = open(root, state);
        auto before = snapshot();
        auto full = root->evaluate_checked(context, &_chunk);
        ASSERT_TRUE(full.ok()) << full.status();
        for (const auto& rows : kRowSets) {
            SCOPED_TRACE(to_string(rows) + " of " + root->debug_string());
            auto selected = root->evaluate_selected(context, &_chunk, rows);
            ASSERT_TRUE(selected.ok()) << selected.status();
            expect_gathered(full.value(), selected.value(), rows);
        }
        EXPECT_EQ(before, snapshot());
    }

    ColumnPtr evaluate_selected(Expr* root, const std::vector<uint32_t>& rows, RuntimeState* state = nullptr) {
        auto result = root->evaluate_selected(open(root, state), &_chunk, rows);
        CHECK(result.ok()) << result.status();
        return result.value();
    }

    // `build` makes an operator tree around the leaf it is given. The probe is evaluated as the leaf, and
    // the same tree with a column reference as the leaf is the reference of the result.
    void expect_pass_through(const std::function<Expr*(Expr*)>& build, SlotId slot) {
        const std::vector<uint32_t> rows{4, 1, 0, 4};
        auto* probe = _pool.add(new ProbeExpr(type_of(slot), slot, md5(col(kT))));
        Expr* tree = build(probe);
        Expr* reference = build(col(slot));
        auto full = reference->evaluate_checked(open(reference), &_chunk);
        ASSERT_TRUE(full.ok()) << full.status();

        auto selected = tree->evaluate_selected(open(tree), &_chunk, rows);
        ASSERT_TRUE(selected.ok()) << selected.status();
        expect_gathered(full.value(), selected.value(), rows);
        EXPECT_EQ(1, probe->calls);
        EXPECT_EQ(&_chunk, probe->seen_chunk);
        EXPECT_EQ(rows, probe->seen_rows);

        probe->calls = 0;
        auto empty = tree->evaluate_selected(open(tree), &_chunk, {});
        ASSERT_TRUE(empty.ok()) << empty.status();
        EXPECT_EQ(0, empty.value()->size());
        EXPECT_EQ(0, probe->calls);
    }

    RuntimeState _state;
    std::unique_ptr<RuntimeState> _zero_division_state;
    Chunk _chunk;
    ObjectPool _pool;
    std::map<Expr*, std::unique_ptr<ExprContext>> _contexts;
};

TEST_F(SelectedOperatorsTest, HasSelectedKernel) {
    EXPECT_FALSE(col(kS)->has_selected_kernel());
    EXPECT_FALSE(text("abc")->has_selected_kernel());
    Expr* kernel = md5(col(kS));
    open(kernel);
    EXPECT_TRUE(kernel->has_selected_kernel());
    Expr* above = binary(TExprOpcode::EQ, varchar_type(), kernel, text("abc"));
    EXPECT_TRUE(above->has_selected_kernel());
    Expr* none = binary(TExprOpcode::EQ, varchar_type(), col(kS), text("abc"));
    EXPECT_FALSE(none->has_selected_kernel());
}

// We fear a walk of the subtree on every call, so each node keeps its result. A copy starts without it and computes
// its own result.
TEST_F(SelectedOperatorsTest, HasSelectedKernelIsKeptInTheNode) {
    Expr* tree = binary(TExprOpcode::EQ, varchar_type(), col(kS), text("abc"));
    ASSERT_FALSE(tree->has_selected_kernel());

    // A change of the tree after the first call does not change the kept result.
    Expr* kernel = md5(col(kT));
    open(kernel);
    tree->add_child(kernel);
    EXPECT_FALSE(tree->has_selected_kernel());

    Expr* copy = Expr::copy(&_pool, tree);
    EXPECT_TRUE(copy->has_selected_kernel());
}

TEST_F(SelectedOperatorsTest, BinaryPredicates) {
    for (auto op : kCompareOps) {
        SCOPED_TRACE(std::to_string(static_cast<int>(op)));
        check(binary(op, varchar_type(), md5(col(kS)), md5(col(kT))));
        check(binary(op, varchar_type(), md5(col(kS)), text(kAbcMd5)));
        check(binary(op, varchar_type(), text(kAbcMd5), md5(col(kS))));
        check(binary(op, varchar_type(), md5(col(kS)), null_literal()));
        check(binary(op, varchar_type(), null_literal(), md5(col(kT))));
        check(binary(op, varchar_type(), md5(col(kT)), col(kS)));
        check(binary(op, bigint_type(), json_int(col(kJs), "$.a"), col(kBi)));
        check(binary(op, bigint_type(), json_int(col(kJs), "$.a"), col(kNi)));
        check(binary(op, bigint_type(), json_int(col(kJs), "$.a"), bigint_literal(3)));
        check(binary(op, bigint_type(), bigint_literal(3), json_int(col(kJs), "$.a")));
        check(binary(op, bigint_type(), json_int(col(kJs), "$.a"), json_int(col(kJs), "$.b")));
    }

    Expr* equals = binary(TExprOpcode::EQ, varchar_type(), md5(col(kS)), text(kAbcMd5));
    auto result = evaluate_selected(equals, {4, 1, 0, 4});
    ASSERT_EQ(4, result->size());
    EXPECT_EQ(1, result->get(0).get_uint8());
    EXPECT_TRUE(result->is_null(1));
    EXPECT_EQ(1, result->get(2).get_uint8());
    EXPECT_EQ(1, result->get(3).get_uint8());
}

TEST_F(SelectedOperatorsTest, ArrayPredicates) {
    for (auto op : kCompareOps) {
        SCOPED_TRACE(std::to_string(static_cast<int>(op)));
        check(binary(op, int_array(), array_sort(col(kArr)), col(kArr2)));
        check(binary(op, int_array(), col(kArr2), array_sort(col(kArr))));
        check(binary(op, int_array(), array_sort(col(kArr)), array_literal({1, 2, 3})));
        check(binary(op, int_array(), array_literal({1, 2, 3}), array_sort(col(kArr))));
        check(binary(op, int_array(), array_sort(col(kArr)), null_literal()));
    }
    Expr* equals = binary(TExprOpcode::EQ, int_array(), array_sort(col(kArr)), col(kArr2));
    auto result = evaluate_selected(equals, {4, 1, 0, 4});
    ASSERT_EQ(4, result->size());
    EXPECT_EQ(1, result->get(0).get_uint8());
    EXPECT_TRUE(result->is_null(1));
    EXPECT_EQ(1, result->get(2).get_uint8());
    EXPECT_EQ(1, result->get(3).get_uint8());
}

TEST_F(SelectedOperatorsTest, CompoundPredicates) {
    auto matches = [&] { return binary(TExprOpcode::EQ, varchar_type(), md5(col(kS)), text(kAbcMd5)); };
    auto differs = [&] { return binary(TExprOpcode::NE, varchar_type(), md5(col(kT)), text(kAbcMd5)); };
    auto large = [&] { return binary(TExprOpcode::GT, bigint_type(), json_int(col(kJs), "$.a"), bigint_literal(2)); };

    check(and_(matches(), differs()));
    check(or_(matches(), differs()));
    check(and_(differs(), matches()));
    check(or_(differs(), matches()));
    check(not_(matches()));
    check(not_(not_(matches())));
    check(and_(or_(matches(), large()), not_(differs())));
    check(or_(and_(matches(), large()), not_(differs())));
    // A side that is not a function call
    check(and_(matches(), binary(TExprOpcode::GT, bigint_type(), col(kBi), bigint_literal(15))));
    check(or_(binary(TExprOpcode::GT, bigint_type(), col(kBi), bigint_literal(45)), matches()));
    // Constant sides
    for (bool value : {false, true}) {
        check(and_(bool_literal(value), matches()));
        check(and_(matches(), bool_literal(value)));
        check(or_(bool_literal(value), matches()));
        check(or_(matches(), bool_literal(value)));
    }
    check(and_(null_literal(boolean_type()), matches()));
    check(or_(null_literal(boolean_type()), matches()));
    check(and_(matches(), null_literal(boolean_type())));
    check(or_(matches(), null_literal(boolean_type())));
    check(not_(null_literal(boolean_type())));
    check(not_(bool_literal(true)));
}

// When the left side decides every selected row, the right side is not evaluated.
TEST_F(SelectedOperatorsTest, CompoundPredicateKeepsShortCircuitOfLeftSide) {
    const std::vector<uint32_t> rows{4, 1, 0, 4};
    auto* skipped_and = _pool.add(new MustNotRunExpr(boolean_type()));
    // md5 of a non-null column is never 'zzz'
    Expr* never = binary(TExprOpcode::EQ, varchar_type(), md5(col(kT)), text("zzz"));
    Expr* and_tree = and_(never, skipped_and);
    auto and_result = evaluate_selected(and_tree, rows);
    ASSERT_EQ(rows.size(), and_result->size());
    EXPECT_EQ(0, skipped_and->calls);
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_FALSE(and_result->is_null(i));
        EXPECT_EQ(0, and_result->get(i).get_uint8());
    }

    auto* skipped_or = _pool.add(new MustNotRunExpr(boolean_type()));
    Expr* always = binary(TExprOpcode::NE, varchar_type(), md5(col(kT)), text("zzz"));
    Expr* or_tree = or_(always, skipped_or);
    auto or_result = evaluate_selected(or_tree, rows);
    ASSERT_EQ(rows.size(), or_result->size());
    EXPECT_EQ(0, skipped_or->calls);
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(1, or_result->get(i).get_uint8());
    }

    // A left side that leaves a row open: the right side is evaluated for all selected rows.
    auto* evaluated = _pool.add(new MustNotRunExpr(boolean_type()));
    Expr* open_left = and_(binary(TExprOpcode::EQ, varchar_type(), md5(col(kS)), text(kAbcMd5)), evaluated);
    auto failed = open_left->evaluate_selected(open(open_left), &_chunk, rows);
    EXPECT_FALSE(failed.ok());
    EXPECT_EQ(1, evaluated->calls);
}

TEST_F(SelectedOperatorsTest, IsNullPredicates) {
    for (bool is_not_null : {false, true}) {
        SCOPED_TRACE(std::to_string(static_cast<int>(is_not_null)));
        check(is_null(is_not_null, md5(col(kS))));
        check(is_null(is_not_null, md5(col(kT))));
        check(is_null(is_not_null, json_int(col(kJs), "$.a")));
        check(is_null(is_not_null, json_int(col(kJs), "$.b")));
        check(is_null(is_not_null, parse_json(col(kJn))));
        check(is_null(is_not_null, md5(text("abc"))));
        check(is_null(is_not_null, null_literal()));
        check(is_null(is_not_null, text("abc")));
        check(is_null(is_not_null, col(kS)));
    }

    // A JSON null is a NULL for these predicates, as in the full evaluation. The JSON column of the chunk is
    // read through a probe, because no function with a kernel makes a JSON null from text.
    for (bool is_not_null : {false, true}) {
        SCOPED_TRACE(std::to_string(static_cast<int>(is_not_null)));
        expect_pass_through([&](Expr* leaf) { return is_null(is_not_null, leaf); }, kJson);

        auto* probe = _pool.add(new ProbeExpr(type_of(kJson), kJson, md5(col(kT))));
        Expr* tree = is_null(is_not_null, probe);
        auto flags = evaluate_selected(tree, {4, 1, 0, 4});
        ASSERT_EQ(4, flags->size());
        // JSON null, SQL NULL, an object, JSON null
        const std::vector<uint8_t> expected = {1, 1, 0, 1};
        for (size_t i = 0; i < expected.size(); ++i) {
            EXPECT_EQ(is_not_null ? 1 - expected[i] : expected[i], flags->get(i).get_uint8()) << i;
        }
    }
}

TEST_F(SelectedOperatorsTest, InConstPredicates) {
    for (bool is_not_in : {false, true}) {
        SCOPED_TRACE(std::to_string(static_cast<int>(is_not_in)));
        check(in_pred(is_not_in, varchar_type(), md5(col(kS)), {text(kAbcMd5), text("zzz")}));
        check(in_pred(is_not_in, varchar_type(), md5(col(kS)), {text(kAbcMd5), null_literal()}));
        check(in_pred(is_not_in, varchar_type(), md5(col(kT)), {text(kAbcMd5)}));
        check(in_pred(is_not_in, varchar_type(), md5(text("abc")), {text(kAbcMd5), text("zzz")}));
        check(in_pred(is_not_in, varchar_type(), md5(null_literal()), {text(kAbcMd5)}));
        check(in_pred(is_not_in, bigint_type(), json_int(col(kJs), "$.a"), {bigint_literal(1), bigint_literal(3)}));
        check(in_pred(is_not_in, bigint_type(), json_int(col(kJs), "$.a"), {bigint_literal(1), null_literal()}));
        check(in_pred(is_not_in, varchar_type(), col(kS), {text("abc"), text("zzz")}));
        check(in_pred(is_not_in, int_array(), array_sort(col(kArr)), {array_literal({1, 2, 3}), array_literal({5})}));
        check(in_pred(is_not_in, int_array(), array_sort(col(kArr)), {array_literal({8, 9}), null_literal()}));
    }

    auto result = evaluate_selected(in_pred(false, bigint_type(), json_int(col(kJs), "$.a"), {bigint_literal(3)}),
                                    {4, 1, 0, 4});
    ASSERT_EQ(4, result->size());
    EXPECT_EQ(1, result->get(0).get_uint8());
    EXPECT_TRUE(result->is_null(1));
    EXPECT_EQ(0, result->get(2).get_uint8());
    EXPECT_EQ(1, result->get(3).get_uint8());
}

TEST_F(SelectedOperatorsTest, ArithmeticOnIntegers) {
    const std::vector<TExprOpcode::type> ops = {TExprOpcode::ADD,
                                                TExprOpcode::SUBTRACT,
                                                TExprOpcode::MULTIPLY,
                                                TExprOpcode::DIVIDE,
                                                TExprOpcode::INT_DIVIDE,
                                                TExprOpcode::MOD,
                                                TExprOpcode::BITAND,
                                                TExprOpcode::BITOR,
                                                TExprOpcode::BITXOR,
                                                TExprOpcode::BIT_SHIFT_LEFT,
                                                TExprOpcode::BIT_SHIFT_RIGHT,
                                                TExprOpcode::BIT_SHIFT_RIGHT_LOGICAL};
    for (auto op : ops) {
        SCOPED_TRACE(std::to_string(static_cast<int>(op)));
        check(arith(op, bigint_type(), {json_int(col(kJs), "$.a"), json_int(col(kJs), "$.b")}));
        // The values of kBi are too big for a shift count
        if (op != TExprOpcode::BIT_SHIFT_LEFT && op != TExprOpcode::BIT_SHIFT_RIGHT &&
            op != TExprOpcode::BIT_SHIFT_RIGHT_LOGICAL) {
            check(arith(op, bigint_type(), {json_int(col(kJs), "$.a"), col(kBi)}));
        }
        check(arith(op, bigint_type(), {col(kBi), json_int(col(kJs), "$.a")}));
        check(arith(op, bigint_type(), {json_int(col(kJs), "$.a"), col(kNi)}));
        check(arith(op, bigint_type(), {json_int(col(kJs), "$.a"), bigint_literal(2)}));
        check(arith(op, bigint_type(), {bigint_literal(7), json_int(col(kJs), "$.b")}));
        check(arith(op, bigint_type(), {json_int(col(kJs), "$.a"), null_literal(bigint_type())}));
        // Operators over operators
        check(arith(op, bigint_type(),
                    {arith(TExprOpcode::ADD, bigint_type(), {json_int(col(kJs), "$.a"), col(kBi)}),
                     json_int(col(kJs), "$.b")}));
    }
    check(arith(TExprOpcode::BITNOT, bigint_type(), {json_int(col(kJs), "$.a")}));
    check(arith(TExprOpcode::BITNOT, bigint_type(), {col(kBi)}));
    check(arith(TExprOpcode::BITNOT, bigint_type(), {bigint_literal(5)}));

    auto sum = evaluate_selected(arith(TExprOpcode::ADD, bigint_type(), {json_int(col(kJs), "$.a"), col(kBi)}),
                                 {4, 1, 0, 4});
    ASSERT_EQ(4, sum->size());
    EXPECT_EQ(53, sum->get(0).get_int64());
    EXPECT_TRUE(sum->is_null(1));
    EXPECT_EQ(11, sum->get(2).get_int64());
    EXPECT_EQ(53, sum->get(3).get_int64());
}

TEST_F(SelectedOperatorsTest, ArithmeticOnDoubles) {
    const TypeDescriptor type(TYPE_DOUBLE);
    for (auto op :
         {TExprOpcode::ADD, TExprOpcode::SUBTRACT, TExprOpcode::MULTIPLY, TExprOpcode::DIVIDE, TExprOpcode::MOD}) {
        SCOPED_TRACE(std::to_string(static_cast<int>(op)));
        check(arith(op, type, {json_double(col(kJs), "$.a"), json_double(col(kJs), "$.b")}));
        check(arith(op, type, {json_double(col(kJs), "$.a"), double_literal(1.5)}));
        check(arith(op, type, {double_literal(1.5), json_double(col(kJs), "$.b")}));
    }
}

// Both operands are casts between decimals of the same scale, so the multiplication reads the operands below
// the casts.
TEST_F(SelectedOperatorsTest, DecimalMultiplicationReadsOperandsBelowCasts) {
    const auto decimal64 = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL64, 18, 2);
    const auto decimal128 = TypeDescriptor::create_decimalv3_type(TYPE_DECIMAL128, 38, 2);
    auto operand = [&](const char* path) {
        Expr* decimal = cast(TypeDescriptor(TYPE_DOUBLE), decimal64, json_double(col(kJs), path));
        return cast(decimal64, decimal128, decimal);
    };
    check(arith(TExprOpcode::MULTIPLY, decimal128, {operand("$.a"), operand("$.b")}));
    check(arith(TExprOpcode::ADD, decimal128, {operand("$.a"), operand("$.b")}));
    check(arith(TExprOpcode::SUBTRACT, decimal128, {operand("$.a"), operand("$.b")}));
}

TEST_F(SelectedOperatorsTest, ErrorForDivisionByZeroSeesOnlySelectedRows) {
    TQueryOptions options;
    options.__set_error_for_division_by_zero(true);
    _zero_division_state = std::make_unique<RuntimeState>(TUniqueId(), options, TQueryGlobals(), nullptr);
    RuntimeState* strict = _zero_division_state.get();
    const std::vector<uint32_t> rows{4, 1, 0, 4};
    const std::vector<uint32_t> zero_row{2};

    for (auto op : {TExprOpcode::DIVIDE, TExprOpcode::INT_DIVIDE, TExprOpcode::MOD}) {
        SCOPED_TRACE(std::to_string(static_cast<int>(op)));
        // Row 2 has a zero divisor: $.b is 0 there. A function below the operator reads the selected rows only.
        auto kernel_tree = [&] {
            return arith(op, bigint_type(), {json_int(col(kJs), "$.a"), json_int(col(kJs), "$.b")});
        };
        // The same on plain columns: the zero is in the third row of the divisor.
        auto plain_tree = [&] { return arith(op, bigint_type(), {col(kBi), col(kZb)}); };
        for (const auto& build : {std::function<Expr*()>(kernel_tree), std::function<Expr*()>(plain_tree)}) {
            Expr* lenient = build();
            Expr* failing = build();
            auto expected = lenient->evaluate_checked(open(lenient), &_chunk);
            ASSERT_TRUE(expected.ok()) << expected.status();

            auto full = failing->evaluate_checked(open(failing, strict), &_chunk);
            ASSERT_FALSE(full.ok());
            EXPECT_NE(std::string::npos, full.status().message().find("Division by zero"));

            auto selected = failing->evaluate_selected(open(failing, strict), &_chunk, rows);
            ASSERT_TRUE(selected.ok()) << selected.status();
            expect_gathered(expected.value(), selected.value(), rows);

            auto failed = failing->evaluate_selected(open(failing, strict), &_chunk, zero_row);
            ASSERT_FALSE(failed.ok());
            EXPECT_NE(std::string::npos, failed.status().message().find("Division by zero"));
        }
    }
}

// An operator with a function that reads through row ids below it passes the selection down, and it does
// not copy the input of that function.
TEST_F(SelectedOperatorsTest, OperatorsPassSelectionToChildren) {
    auto equals_abc = [&](Expr* leaf) { return binary(TExprOpcode::EQ, varchar_type(), leaf, text("abc")); };
    expect_pass_through(equals_abc, kS);
    expect_pass_through([&](Expr* leaf) { return binary(TExprOpcode::EQ_FOR_NULL, varchar_type(), text("abc"), leaf); },
                        kS);
    expect_pass_through([&](Expr* leaf) { return not_(equals_abc(leaf)); }, kS);
    expect_pass_through([&](Expr* leaf) { return not_(not_(equals_abc(leaf))); }, kS);
    expect_pass_through([&](Expr* leaf) { return is_null(false, leaf); }, kS);
    expect_pass_through([&](Expr* leaf) { return is_null(true, leaf); }, kS);
    expect_pass_through(
            [&](Expr* leaf) {
                return in_pred(false, varchar_type(), leaf, {text("abc"), text("zzz")});
            },
            kS);
    expect_pass_through(
            [&](Expr* leaf) {
                return in_pred(true, varchar_type(), leaf, {text("abc"), null_literal()});
            },
            kS);
    expect_pass_through([&](Expr* leaf) { return in_pred(false, int_array(), leaf, {array_literal({5})}); }, kArr);
    expect_pass_through([&](Expr* leaf) { return binary(TExprOpcode::LT, int_array(), leaf, col(kArr2)); }, kArr);
    expect_pass_through([&](Expr* leaf) { return binary(TExprOpcode::EQ, int_array(), leaf, col(kArr2)); }, kArr);
    expect_pass_through([&](Expr* leaf) { return binary(TExprOpcode::EQ_FOR_NULL, int_array(), leaf, col(kArr2)); },
                        kArr);
    for (auto op : {TExprOpcode::ADD, TExprOpcode::DIVIDE, TExprOpcode::MOD, TExprOpcode::BIT_SHIFT_LEFT}) {
        SCOPED_TRACE(std::to_string(static_cast<int>(op)));
        expect_pass_through([&](Expr* leaf) { return arith(op, bigint_type(), {leaf, bigint_literal(2)}); }, kNi);
        expect_pass_through([&](Expr* leaf) { return arith(op, bigint_type(), {col(kBi), leaf}); }, kNi);
    }
    expect_pass_through([&](Expr* leaf) { return arith(TExprOpcode::BITNOT, bigint_type(), {leaf}); }, kNi);
}

TEST_F(SelectedOperatorsTest, CompoundPredicatePassesSelectionToBothSides) {
    const std::vector<uint32_t> rows{4, 1, 0, 4};
    for (bool use_and : {true, false}) {
        auto build = [&](Expr* l, Expr* r) {
            Expr* left = binary(TExprOpcode::EQ, varchar_type(), l, text("abc"));
            Expr* right = binary(TExprOpcode::NE, varchar_type(), r, text("def"));
            return use_and ? and_(left, right) : or_(left, right);
        };
        auto* left_probe = _pool.add(new ProbeExpr(varchar_type(), kS, md5(col(kT))));
        auto* right_probe = _pool.add(new ProbeExpr(varchar_type(), kT, md5(col(kT))));
        Expr* tree = build(left_probe, right_probe);
        Expr* reference = build(col(kS), col(kT));
        auto full = reference->evaluate_checked(open(reference), &_chunk);
        ASSERT_TRUE(full.ok()) << full.status();
        auto selected = tree->evaluate_selected(open(tree), &_chunk, rows);
        ASSERT_TRUE(selected.ok()) << selected.status();
        expect_gathered(full.value(), selected.value(), rows);
        for (auto* probe : {left_probe, right_probe}) {
            EXPECT_EQ(1, probe->calls);
            EXPECT_EQ(&_chunk, probe->seen_chunk);
            EXPECT_EQ(rows, probe->seen_rows);
        }
    }
}

// Without a function that reads through row ids, the inputs of the whole tree are copied once, for all
// the operators together.
TEST_F(SelectedOperatorsTest, TreeWithoutKernelUsesOneSharedCopy) {
    const std::vector<uint32_t> rows{4, 1, 0, 4};
    auto* a = _pool.add(new SlotReaderExpr(bigint_type(), kBi));
    auto* b = _pool.add(new SlotReaderExpr(bigint_type(), kNi));
    Expr* tree =
            binary(TExprOpcode::GE, bigint_type(), arith(TExprOpcode::ADD, bigint_type(), {a, b}), bigint_literal(30));
    ExprContext* context = open(tree);
    ASSERT_FALSE(tree->has_selected_kernel());

    auto full = tree->evaluate_checked(context, &_chunk);
    ASSERT_TRUE(full.ok()) << full.status();
    a->seen_chunks.clear();
    b->seen_chunks.clear();
    a->seen_rows.clear();
    b->seen_rows.clear();

    auto selected = tree->evaluate_selected(context, &_chunk, rows);
    ASSERT_TRUE(selected.ok()) << selected.status();
    expect_gathered(full.value(), selected.value(), rows);
    ASSERT_EQ(1, a->seen_chunks.size());
    ASSERT_EQ(1, b->seen_chunks.size());
    EXPECT_EQ(a->seen_chunks[0], b->seen_chunks[0]);
    EXPECT_NE(&_chunk, a->seen_chunks[0]);
    EXPECT_EQ(rows.size(), a->seen_rows[0]);
    EXPECT_EQ(rows.size(), b->seen_rows[0]);

    // A different, plain tree over columns
    check(binary(TExprOpcode::GE, bigint_type(), arith(TExprOpcode::ADD, bigint_type(), {col(kBi), col(kNi)}),
                 bigint_literal(0)));
    check(and_(binary(TExprOpcode::GE, bigint_type(), arith(TExprOpcode::ADD, bigint_type(), {col(kBi), col(kNi)}),
                      bigint_literal(30)),
               not_(is_null(false, col(kNi)))));
    check(in_pred(false, bigint_type(), arith(TExprOpcode::MULTIPLY, bigint_type(), {col(kBi), col(kZb)}),
                  {bigint_literal(40), bigint_literal(250)}));
}

// A side without a function that reads through row ids is copied once for its own subtree, while the
// other side of the operator passes the selection down.
TEST_F(SelectedOperatorsTest, MixedTreeCopiesOnlyTheSideWithoutKernel) {
    const std::vector<uint32_t> rows{4, 1, 0, 4};
    auto* a = _pool.add(new SlotReaderExpr(bigint_type(), kBi));
    auto* b = _pool.add(new SlotReaderExpr(bigint_type(), kZb));
    Expr* plain_side = arith(TExprOpcode::ADD, bigint_type(), {a, b});
    Expr* tree = arith(TExprOpcode::ADD, bigint_type(), {json_int(col(kJs), "$.a"), plain_side});
    ExprContext* context = open(tree);
    ASSERT_TRUE(tree->has_selected_kernel());
    ASSERT_FALSE(plain_side->has_selected_kernel());

    auto full = tree->evaluate_checked(context, &_chunk);
    ASSERT_TRUE(full.ok()) << full.status();
    for (auto* reader : {a, b}) {
        reader->seen_chunks.clear();
        reader->seen_rows.clear();
    }

    auto selected = tree->evaluate_selected(context, &_chunk, rows);
    ASSERT_TRUE(selected.ok()) << selected.status();
    expect_gathered(full.value(), selected.value(), rows);
    ASSERT_EQ(1, a->seen_chunks.size());
    ASSERT_EQ(1, b->seen_chunks.size());
    EXPECT_EQ(a->seen_chunks[0], b->seen_chunks[0]);
    EXPECT_NE(&_chunk, a->seen_chunks[0]);
    EXPECT_EQ(rows.size(), a->seen_rows[0]);
}

// An operator over a function that reads through row ids passes the selection down. We expect the result on the
// selected rows to be the rows of the full evaluation of the same tree, for every kind of operator.
TEST_F(SelectedOperatorsTest, OperatorsOverKernelsGiveTheRowsOfTheFullEvaluation) {
    Expr* kernel_tree = binary(TExprOpcode::EQ, varchar_type(), md5(col(kS)), text(kAbcMd5));
    open(kernel_tree);
    ASSERT_TRUE(kernel_tree->has_selected_kernel());
    EXPECT_TRUE(pass_selection_to_children(kernel_tree));

    check(kernel_tree);
    check(binary(TExprOpcode::EQ_FOR_NULL, bigint_type(), json_int(col(kJs), "$.a"), col(kBi)));
    check(binary(TExprOpcode::LT, int_array(), array_sort(col(kArr)), col(kArr2)));
    check(binary(TExprOpcode::EQ, int_array(), array_sort(col(kArr)), col(kArr2)));
    check(binary(TExprOpcode::EQ_FOR_NULL, int_array(), array_sort(col(kArr)), col(kArr2)));
    check(and_(binary(TExprOpcode::EQ, varchar_type(), md5(col(kS)), text(kAbcMd5)),
               binary(TExprOpcode::NE, varchar_type(), md5(col(kT)), text(kAbcMd5))));
    check(or_(binary(TExprOpcode::EQ, varchar_type(), md5(col(kS)), text(kAbcMd5)),
              binary(TExprOpcode::NE, varchar_type(), md5(col(kT)), text(kAbcMd5))));
    check(not_(binary(TExprOpcode::EQ, varchar_type(), md5(col(kS)), text(kAbcMd5))));
    check(is_null(false, parse_json(col(kJn))));
    check(is_null(true, md5(col(kS))));
    check(in_pred(false, varchar_type(), md5(col(kS)), {text(kAbcMd5), text("zzz")}));
    check(in_pred(true, int_array(), array_sort(col(kArr)), {array_literal({1, 2, 3}), array_literal({5})}));
    check(arith(TExprOpcode::ADD, bigint_type(), {json_int(col(kJs), "$.a"), col(kBi)}));
    check(arith(TExprOpcode::DIVIDE, bigint_type(), {json_int(col(kJs), "$.a"), json_int(col(kJs), "$.b")}));
    check(arith(TExprOpcode::MOD, bigint_type(), {json_int(col(kJs), "$.a"), bigint_literal(2)}));
    check(arith(TExprOpcode::BITNOT, bigint_type(), {json_int(col(kJs), "$.a")}));
    check(arith(TExprOpcode::BIT_SHIFT_LEFT, bigint_type(), {col(kBi), json_int(col(kJs), "$.a")}));
    check(arith(TExprOpcode::ADD, TypeDescriptor(TYPE_DOUBLE),
                {json_double(col(kJs), "$.a"), json_double(col(kJs), "$.b")}));
}

// Without a function that reads through row ids an operator copies the inputs of its tree once. We expect the
// result on the selected rows to be the rows of the full evaluation for every kind of operator in this case too.
TEST_F(SelectedOperatorsTest, OperatorsOverPlainColumnsGiveTheRowsOfTheFullEvaluation) {
    Expr* plain_tree = binary(TExprOpcode::EQ, varchar_type(), col(kS), text("abc"));
    open(plain_tree);
    ASSERT_FALSE(plain_tree->has_selected_kernel());
    EXPECT_FALSE(pass_selection_to_children(plain_tree));

    check(plain_tree);
    check(binary(TExprOpcode::EQ_FOR_NULL, bigint_type(), col(kNi), col(kBi)));
    check(binary(TExprOpcode::LT, int_array(), col(kArr), col(kArr2)));
    check(binary(TExprOpcode::EQ, int_array(), col(kArr), col(kArr2)));
    check(binary(TExprOpcode::EQ_FOR_NULL, int_array(), col(kArr), col(kArr2)));
    check(and_(binary(TExprOpcode::EQ, varchar_type(), col(kS), text("abc")),
               binary(TExprOpcode::NE, varchar_type(), col(kT), text("abc"))));
    check(or_(binary(TExprOpcode::EQ, varchar_type(), col(kS), text("abc")),
              binary(TExprOpcode::NE, varchar_type(), col(kT), text("abc"))));
    check(not_(binary(TExprOpcode::EQ, varchar_type(), col(kS), text("abc"))));
    check(is_null(false, col(kJn)));
    check(is_null(true, col(kS)));
    check(in_pred(false, varchar_type(), col(kS), {text("abc"), text("zzz")}));
    check(in_pred(true, int_array(), col(kArr), {array_literal({1, 2, 3}), array_literal({5})}));
    check(arith(TExprOpcode::ADD, bigint_type(), {col(kBi), col(kNi)}));
    check(arith(TExprOpcode::DIVIDE, bigint_type(), {col(kBi), col(kNi)}));
    check(arith(TExprOpcode::MOD, bigint_type(), {col(kBi), bigint_literal(2)}));
    check(arith(TExprOpcode::BITNOT, bigint_type(), {col(kBi)}));
    check(arith(TExprOpcode::BIT_SHIFT_LEFT, bigint_type(), {col(kBi), bigint_literal(2)}));
}

} // namespace starrocks
