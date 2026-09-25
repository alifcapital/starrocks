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

#include "exprs/conditional_two_phase.h"

#include <gtest/gtest.h>

#include <functional>
#include <optional>
#include <random>
#include <string>

#include "testutil/assert.h"
#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "common/object_pool.h"
#include "exprs/cast_expr.h"
#include "exprs/column_ref.h"
#include "exprs/expr.h"
#include "exprs/literal.h"
#include "exprs/selected_column.h"
#include "types/logical_type.h"
#include "runtime/types.h"

namespace starrocks {

// A value/guard child that reads a column straight from the chunk it is evaluated on. Because the two-phase
// helper evaluates a deferred branch on a routed *subchunk*, this mock returns exactly the routed rows (in
// routed order) -- so an incorrect pos[]/scatter would surface as a value mismatch. `expensive` controls
// whether the branch is filtered (true) or evaluated full (false), exercising both code paths.
template <LogicalType Type>
class SlotReaderExpr final : public Expr {
public:
    SlotReaderExpr(TypeDescriptor type, SlotId slot, bool expensive)
            : Expr(std::move(type), true), declared_slots{slot}, _slot(slot), _expensive(expensive) {}

    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk* chunk) override {
        seen_columns = chunk->num_columns();
        seen_rows = chunk->num_rows();
        return chunk->get_column_by_slot_id(_slot);
    }
    Expr* clone(ObjectPool* pool) const override { return pool->add(new SlotReaderExpr(*this)); }
    bool is_constant() const override { return false; }
    bool is_expensive_node() const override { return _expensive; }
    int get_slot_ids(std::vector<SlotId>* slots) const override {
        slots->insert(slots->end(), declared_slots.begin(), declared_slots.end());
        return declared_slots.size();
    }
    bool is_dictmapping_expr() const override { return indirect_inputs; }

    std::vector<SlotId> declared_slots;
    bool indirect_inputs = false;
    size_t seen_columns = 0;
    size_t seen_rows = 0;

private:
    SlotId _slot;
    bool _expensive;
};

class ConditionalTwoPhaseTest : public ::testing::Test {
protected:
    static constexpr int kRows = 97; // prime, so routed/full sizes never coincide by accident

    TypeDescriptor int_type() { return TypeDescriptor(TYPE_INT); }

    // bool column, value true at rows where pred(row) holds.
    ColumnPtr make_bool(const std::function<bool(int)>& pred) {
        auto col = RunTimeColumnType<TYPE_BOOLEAN>::create();
        for (int r = 0; r < kRows; ++r) {
            col->append(pred(r) ? 1 : 0);
        }
        return col;
    }

    // non-nullable int column, value = f(row).
    ColumnPtr make_int(const std::function<int(int)>& f) {
        auto col = RunTimeColumnType<TYPE_INT>::create();
        for (int r = 0; r < kRows; ++r) {
            col->append(f(r));
        }
        return col;
    }

    // nullable int column: null where is_null(row), else value f(row).
    ColumnPtr make_nullable_int(const std::function<bool(int)>& is_null, const std::function<int(int)>& f) {
        auto data = RunTimeColumnType<TYPE_INT>::create();
        auto nulls = NullColumn::create();
        for (int r = 0; r < kRows; ++r) {
            data->append(f(r));
            nulls->append(is_null(r) ? 1 : 0);
        }
        auto col = NullableColumn::create(std::move(data), std::move(nulls));
        col->update_has_null();
        return col;
    }

    Expr* slot(ObjectPool& pool, SlotId id, bool expensive) {
        return pool.add(new SlotReaderExpr<TYPE_INT>(int_type(), id, expensive));
    }

    TypeDescriptor varchar_type() {
        TypeDescriptor t(TYPE_VARCHAR);
        t.len = 64;
        return t;
    }

    Expr* varchar_slot(ObjectPool& pool, SlotId id, bool expensive) {
        return pool.add(new SlotReaderExpr<TYPE_VARCHAR>(varchar_type(), id, expensive));
    }

    // variable-length (BinaryColumn) value column = f(row); exercises the variable-length assembly append.
    ColumnPtr make_varchar(const std::function<std::string(int)>& f) {
        auto col = RunTimeColumnType<TYPE_VARCHAR>::create();
        for (int r = 0; r < kRows; ++r) {
            std::string s = f(r);
            col->append(Slice(s));
        }
        return col;
    }

    void expect_str(const ColumnPtr& result, const std::function<std::optional<std::string>(int)>& expected) {
        ASSERT_EQ(result->size(), kRows);
        for (int r = 0; r < kRows; ++r) {
            auto want = expected(r);
            if (!want.has_value()) {
                ASSERT_TRUE(result->is_null(r)) << "row " << r << " should be NULL";
            } else {
                ASSERT_FALSE(result->is_null(r)) << "row " << r << " should not be NULL";
                ASSERT_EQ(result->get(r).get_slice().to_string(), *want) << "row " << r;
            }
        }
    }

    // assert result[r] == expected(r); std::nullopt means SQL NULL.
    void expect(const ColumnPtr& result, const std::function<std::optional<int>(int)>& expected) {
        ASSERT_EQ(result->size(), kRows);
        for (int r = 0; r < kRows; ++r) {
            auto want = expected(r);
            if (!want.has_value()) {
                ASSERT_TRUE(result->is_null(r)) << "row " << r << " should be NULL";
            } else {
                ASSERT_FALSE(result->is_null(r)) << "row " << r << " should not be NULL";
                ASSERT_EQ(result->get(r).get_int32(), *want) << "row " << r;
            }
        }
    }
};

// IF(guard, then[expensive], else): then is filtered to guard-true rows, else fills the rest.
TEST_F(ConditionalTwoPhaseTest, predicate_routed_if) {
    ObjectPool pool;
    ColumnPtr guard = make_bool([](int r) { return r % 3 == 0; });
    ColumnPtr then_val = make_int([](int r) { return r; });
    ColumnPtr else_val = make_int([](int r) { return -r - 1; });
    Chunk chunk;
    chunk.append_column(guard, 0);
    chunk.append_column(then_val, 1);
    chunk.append_column(else_val, 2);

    std::vector<Expr*> then_exprs = {slot(pool, 1, /*expensive=*/true)};
    auto guard_fn = [&](int) -> StatusOr<ColumnPtr> { return chunk.get_column_by_slot_id(0); };
    ASSIGN_OR_ABORT(ColumnPtr result,
                    two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), /*num_branches=*/1, guard_fn,
                                                    then_exprs, /*then_two_phase=*/{1}, slot(pool, 2, false),
                                                    /*else_two_phase=*/false, /*shortcut=*/true));
    expect(result, [](int r) -> std::optional<int> { return r % 3 == 0 ? r : (-r - 1); });
}

// Unused variable-length columns must not be copied into a selected branch's input.
TEST_F(ConditionalTwoPhaseTest, copies_only_branch_inputs) {
    Chunk chunk;
    auto guard = make_bool([](int r) { return r % 3 == 0; });
    chunk.append_column(make_varchar([](int) { return std::string(4096, 'x'); }), 90);
    chunk.append_column(make_int([](int r) { return r; }), 17);
    chunk.append_column(make_nullable_int([](int r) { return r % 2 == 0; }, [](int r) { return -r; }), 31);
    SlotReaderExpr<TYPE_INT> value(int_type(), 17, true);
    value.declared_slots = {17, 17}; // repeated references must not duplicate the input column
    auto guard_fn = [&](int) -> StatusOr<ColumnPtr> { return guard; };
    ASSIGN_OR_ABORT(auto result, two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), 1, guard_fn, {&value},
                                                                 {1}, nullptr, false, true));
    EXPECT_EQ(1, value.seen_columns);
    EXPECT_EQ(33, value.seen_rows);
    expect(result, [](int r) -> std::optional<int> { return r % 3 == 0 ? std::optional<int>(r) : std::nullopt; });
    // The same projection applies to null-routed branches and leaves the original chunk intact.
    SlotReaderExpr<TYPE_INT> prefix(int_type(), 31, false);
    ASSIGN_OR_ABORT(auto coalesced, two_phase_eval_null_routed(nullptr, &chunk, int_type(), {&prefix, &value}, {0, 1}));
    EXPECT_EQ(1, value.seen_columns);
    EXPECT_EQ(49, value.seen_rows);
    expect(coalesced, [](int r) -> std::optional<int> { return r % 2 == 0 ? r : -r; });
    EXPECT_EQ(3, chunk.num_columns());
    EXPECT_EQ(kRows, chunk.num_rows());
    EXPECT_EQ(4096, chunk.get_column_by_slot_id(90)->get(0).get_slice().size);
}

TEST_F(ConditionalTwoPhaseTest, retains_input_when_dependencies_are_indirect_or_unknown) {
    Chunk chunk;
    chunk.append_column(make_int([](int r) { return r; }), 17);
    chunk.append_column(make_int([](int r) { return -r; }), 31);
    auto guard = make_bool([](int r) { return r % 3 == 0; });
    auto guard_fn = [&](int) -> StatusOr<ColumnPtr> { return guard; };
    for (int mode = 0; mode < 3; ++mode) {
        SlotReaderExpr<TYPE_INT> value(int_type(), 17, true);
        if (mode == 0) value.declared_slots.clear();
        if (mode == 1) value.declared_slots = {999};
        if (mode == 2) {
            value.declared_slots = {31}; // logical input differs from physical dictionary input
            value.indirect_inputs = true;
        }
        ASSIGN_OR_ABORT(auto result, two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), 1, guard_fn, {&value},
                                                                     {1}, nullptr, false, true));
        EXPECT_EQ(2, value.seen_columns);
        EXPECT_EQ(33, value.seen_rows);
        expect(result, [](int r) -> std::optional<int> { return r % 3 == 0 ? std::optional<int>(r) : std::nullopt; });
    }
}

// CASE WHEN g0 THEN v0[expensive] WHEN g1 THEN v1[expensive] ELSE e END, overlapping guards (first wins).
TEST_F(ConditionalTwoPhaseTest, predicate_routed_case_multi_branch) {
    ObjectPool pool;
    ColumnPtr g0 = make_bool([](int r) { return r % 2 == 0; });
    ColumnPtr g1 = make_bool([](int r) { return r % 3 == 0; }); // overlaps g0 on multiples of 6
    ColumnPtr v0 = make_int([](int r) { return r * 10; });
    ColumnPtr v1 = make_int([](int r) { return r * 100; });
    ColumnPtr e = make_int([](int r) { return -1; });
    Chunk chunk;
    chunk.append_column(g0, 0);
    chunk.append_column(g1, 1);
    chunk.append_column(v0, 2);
    chunk.append_column(v1, 3);
    chunk.append_column(e, 4);

    std::vector<Expr*> then_exprs = {slot(pool, 2, true), slot(pool, 3, true)};
    auto guard_fn = [&](int i) -> StatusOr<ColumnPtr> { return chunk.get_column_by_slot_id(i); };
    ASSIGN_OR_ABORT(ColumnPtr result,
                    two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), 2, guard_fn, then_exprs, {1, 1},
                                                    slot(pool, 4, false), false, true));
    expect(result, [](int r) -> std::optional<int> {
        if (r % 2 == 0) return r * 10;  // g0 wins (incl. multiples of 6)
        if (r % 3 == 0) return r * 100; // g1, only odd multiples of 3
        return -1;                      // else
    });
}

// CASE with no ELSE: unmatched rows are SQL NULL.
TEST_F(ConditionalTwoPhaseTest, predicate_routed_no_else_is_null) {
    ObjectPool pool;
    ColumnPtr g0 = make_bool([](int r) { return r % 4 == 0; });
    ColumnPtr v0 = make_int([](int r) { return r + 1000; });
    Chunk chunk;
    chunk.append_column(g0, 0);
    chunk.append_column(v0, 1);

    std::vector<Expr*> then_exprs = {slot(pool, 1, true)};
    auto guard_fn = [&](int) -> StatusOr<ColumnPtr> { return chunk.get_column_by_slot_id(0); };
    ASSIGN_OR_ABORT(ColumnPtr result, two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), 1, guard_fn,
                                                                      then_exprs, {1}, /*else=*/nullptr, false,
                                                                      /*shortcut=*/true));
    expect(result, [](int r) -> std::optional<int> {
        if (r % 4 == 0) return r + 1000;
        return std::nullopt;
    });
}

// COALESCE(arg0(nullable), arg1[expensive]): first non-null wins; all-null => NULL (arg1 here is non-null).
TEST_F(ConditionalTwoPhaseTest, null_routed_coalesce) {
    ObjectPool pool;
    ColumnPtr a0 = make_nullable_int([](int r) { return r % 5 != 0; }, [](int r) { return r; }); // non-null on %5==0
    ColumnPtr a1 = make_int([](int r) { return r * 7; });
    Chunk chunk;
    chunk.append_column(a0, 0);
    chunk.append_column(a1, 1);

    std::vector<Expr*> arg_exprs = {slot(pool, 0, false), slot(pool, 1, true)};
    ASSIGN_OR_ABORT(ColumnPtr result,
                    two_phase_eval_null_routed(nullptr, &chunk, int_type(), arg_exprs, /*arg_two_phase=*/{0, 1}));
    expect(result, [](int r) -> std::optional<int> { return r % 5 == 0 ? r : (r * 7); });
}

// IFNULL where some rows are null in both args => those rows are NULL.
TEST_F(ConditionalTwoPhaseTest, null_routed_ifnull_all_null_rows) {
    ObjectPool pool;
    ColumnPtr a0 = make_nullable_int([](int r) { return r % 2 == 0; }, [](int r) { return r; });  // null on even
    ColumnPtr a1 = make_nullable_int([](int r) { return r % 4 == 0; }, [](int r) { return -r; }); // null on %4
    Chunk chunk;
    chunk.append_column(a0, 0);
    chunk.append_column(a1, 1);

    std::vector<Expr*> arg_exprs = {slot(pool, 0, false), slot(pool, 1, true)};
    ASSIGN_OR_ABORT(ColumnPtr result, two_phase_eval_null_routed(nullptr, &chunk, int_type(), arg_exprs, {0, 1}));
    expect(result, [](int r) -> std::optional<int> {
        if (r % 2 != 0) return r;  // a0 non-null (odd)
        if (r % 4 != 0) return -r; // a0 null (even) but a1 non-null (even, not mult of 4)
        return std::nullopt;       // both null (multiples of 4)
    });
}

// VARCHAR result: exercises the variable-length (BinaryColumn) assembly append path, distinct from INT.
TEST_F(ConditionalTwoPhaseTest, predicate_routed_varchar) {
    ObjectPool pool;
    ColumnPtr guard = make_bool([](int r) { return r % 3 == 0; });
    ColumnPtr then_val = make_varchar([](int r) { return "then_" + std::to_string(r); });
    ColumnPtr else_val = make_varchar([](int r) { return "else_" + std::to_string(r); });
    Chunk chunk;
    chunk.append_column(guard, 0);
    chunk.append_column(then_val, 1);
    chunk.append_column(else_val, 2);

    std::vector<Expr*> then_exprs = {varchar_slot(pool, 1, /*expensive=*/true)};
    auto guard_fn = [&](int) -> StatusOr<ColumnPtr> { return chunk.get_column_by_slot_id(0); };
    ASSIGN_OR_ABORT(ColumnPtr result,
                    two_phase_eval_predicate_routed(nullptr, &chunk, varchar_type(), 1, guard_fn, then_exprs, {1},
                                                    varchar_slot(pool, 2, false), false, true));
    expect_str(result, [](int r) -> std::optional<std::string> {
        return r % 3 == 0 ? ("then_" + std::to_string(r)) : ("else_" + std::to_string(r));
    });
}

// COALESCE over VARCHAR, with a deferred (expensive) second arg.
TEST_F(ConditionalTwoPhaseTest, null_routed_varchar) {
    ObjectPool pool;
    auto data = RunTimeColumnType<TYPE_VARCHAR>::create();
    auto nulls = NullColumn::create();
    for (int r = 0; r < kRows; ++r) {
        std::string s = "a0_" + std::to_string(r);
        data->append(Slice(s));
        nulls->append(r % 4 == 0 ? 0 : 1); // non-null only on multiples of 4
    }
    auto a0 = NullableColumn::create(std::move(data), std::move(nulls));
    a0->update_has_null();
    ColumnPtr a0_col = std::move(a0);
    ColumnPtr a1 = make_varchar([](int r) { return "a1_" + std::to_string(r); });
    Chunk chunk;
    chunk.append_column(a0_col, 0);
    chunk.append_column(a1, 1);

    std::vector<Expr*> args = {varchar_slot(pool, 0, false), varchar_slot(pool, 1, true)};
    ASSIGN_OR_ABORT(ColumnPtr result, two_phase_eval_null_routed(nullptr, &chunk, varchar_type(), args, {0, 1}));
    expect_str(result, [](int r) -> std::optional<std::string> {
        return r % 4 == 0 ? ("a0_" + std::to_string(r)) : ("a1_" + std::to_string(r));
    });
}

// First surviving guard is all-true => that branch's value is returned directly (the shortcut path).
TEST_F(ConditionalTwoPhaseTest, predicate_routed_first_all_true_shortcut) {
    ObjectPool pool;
    ColumnPtr guard = make_bool([](int) { return true; });
    ColumnPtr then_val = make_int([](int r) { return r * 5; });
    Chunk chunk;
    chunk.append_column(guard, 0);
    chunk.append_column(then_val, 1);

    std::vector<Expr*> then_exprs = {slot(pool, 1, /*expensive=*/true)};
    auto guard_fn = [&](int) -> StatusOr<ColumnPtr> { return chunk.get_column_by_slot_id(0); };
    ASSIGN_OR_ABORT(ColumnPtr result, two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), 1, guard_fn,
                                                                      then_exprs, {1}, /*else=*/nullptr, false,
                                                                      /*shortcut=*/true));
    expect(result, [](int r) -> std::optional<int> { return r * 5; });
}

// Randomized differential: random guard patterns / values / expensive flags, compared to a closed-form
// first-match oracle. Several fixed seeds for reproducibility.
TEST_F(ConditionalTwoPhaseTest, random_predicate_routed) {
    for (uint32_t seed : {1u, 7u, 42u, 999u, 31337u}) {
        ObjectPool pool;
        std::mt19937 rng(seed);
        std::uniform_int_distribution<int> coin(0, 1);
        const int branches = 3;
        std::vector<std::vector<int>> guard_bits(branches, std::vector<int>(kRows));

        Chunk chunk;
        for (int b = 0; b < branches; ++b) { // guards at slots [0, branches)
            auto g = RunTimeColumnType<TYPE_BOOLEAN>::create();
            for (int r = 0; r < kRows; ++r) {
                int v = coin(rng);
                guard_bits[b][r] = v;
                g->append(v);
            }
            ColumnPtr gc = std::move(g);
            chunk.append_column(gc, b);
        }
        for (int b = 0; b < branches; ++b) { // then values at slots [branches, 2*branches)
            ColumnPtr tc = make_int([b](int r) { return b * 100000 + r; });
            chunk.append_column(tc, branches + b);
        }
        ColumnPtr ec = make_int([](int r) { return -r - 1; }); // else at slot 2*branches
        chunk.append_column(ec, 2 * branches);

        std::vector<Expr*> then_exprs;
        std::vector<uint8_t> then_two_phase;
        for (int b = 0; b < branches; ++b) {
            bool expensive = coin(rng);
            then_exprs.push_back(slot(pool, branches + b, expensive));
            then_two_phase.push_back(expensive ? 1 : 0);
        }
        bool else_expensive = coin(rng);
        auto guard_fn = [&](int i) -> StatusOr<ColumnPtr> { return chunk.get_column_by_slot_id(i); };
        ASSIGN_OR_ABORT(ColumnPtr result,
                        two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), branches, guard_fn, then_exprs,
                                                        then_two_phase, slot(pool, 2 * branches, else_expensive),
                                                        else_expensive, true));
        expect(result, [&](int r) -> std::optional<int> {
            for (int b = 0; b < branches; ++b) {
                if (guard_bits[b][r]) return b * 100000 + r; // first-match wins
            }
            return -r - 1; // else
        });
    }
}

// Randomized differential for null-routing: random null patterns over all args (some rows all-null => NULL).
TEST_F(ConditionalTwoPhaseTest, random_null_routed) {
    for (uint32_t seed : {2u, 13u, 77u, 4242u}) {
        ObjectPool pool;
        std::mt19937 rng(seed);
        std::uniform_int_distribution<int> coin(0, 1);
        const int args = 3;
        std::vector<std::vector<int>> is_null(args, std::vector<int>(kRows));

        Chunk chunk;
        for (int a = 0; a < args; ++a) {
            auto data = RunTimeColumnType<TYPE_INT>::create();
            auto nulls = NullColumn::create();
            for (int r = 0; r < kRows; ++r) {
                int nu = coin(rng);
                is_null[a][r] = nu;
                data->append(a * 100000 + r);
                nulls->append(nu);
            }
            auto col = NullableColumn::create(std::move(data), std::move(nulls));
            col->update_has_null();
            ColumnPtr cc = std::move(col);
            chunk.append_column(cc, a);
        }

        std::vector<Expr*> arg_exprs;
        std::vector<uint8_t> arg_two_phase;
        for (int a = 0; a < args; ++a) {
            bool expensive = (a > 0) && coin(rng); // arg0 is always full
            arg_exprs.push_back(slot(pool, a, expensive));
            arg_two_phase.push_back(expensive ? 1 : 0);
        }
        ASSIGN_OR_ABORT(ColumnPtr result,
                        two_phase_eval_null_routed(nullptr, &chunk, int_type(), arg_exprs, arg_two_phase));
        expect(result, [&](int r) -> std::optional<int> {
            for (int a = 0; a < args; ++a) {
                if (!is_null[a][r]) return a * 100000 + r; // first non-null wins
            }
            return std::nullopt; // all null
        });
    }
}

class SelectionOnlyExpr final : public Expr {
public:
    explicit SelectionOnlyExpr(Chunk* original) : Expr(TypeDescriptor(TYPE_INT), false), original(original) {}
    Expr* clone(ObjectPool* pool) const override { return pool->add(new SelectionOnlyExpr(*this)); }
    bool is_constant() const override { return false; }
    StatusOr<ColumnPtr> evaluate_checked(ExprContext*, Chunk*) override {
        return Status::InternalError("unexpected full evaluation");
    }
    StatusOr<ColumnPtr> evaluate_selected(ExprContext*, Chunk* chunk, const std::vector<uint32_t>& rows) override {
        if (chunk != original) return Status::InternalError("input chunk was copied");
        seen = rows;
        auto result = Int32Column::create();
        for (uint32_t row : rows) result->append(row * 7);
        return result;
    }
    Chunk* original;
    std::vector<uint32_t> seen;
};

TEST_F(ConditionalTwoPhaseTest, empty_chunk_does_not_evaluate_values) {
    Chunk chunk;
    chunk.append_column(Int32Column::create(), 1);
    SelectionOnlyExpr value(&chunk);
    auto guard_fn = [](int) -> StatusOr<ColumnPtr> { return BooleanColumn::create(); };
    ASSIGN_OR_ABORT(auto result, two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), 1, guard_fn, {&value},
                                                                 {1}, nullptr, false, true));
    ASSERT_EQ(0, result->size());
    ASSERT_TRUE(value.seen.empty());
}

TEST_F(ConditionalTwoPhaseTest, selected_dispatch_preserves_original_chunk) {
    Chunk chunk;
    chunk.append_column(make_int([](int r) { return r; }), 1);
    SelectionOnlyExpr value(&chunk);
    auto guard = make_bool([](int r) { return r % 3 == 0; });
    auto guard_fn = [&](int) -> StatusOr<ColumnPtr> { return guard; };
    ASSIGN_OR_ABORT(auto result, two_phase_eval_predicate_routed(nullptr, &chunk, int_type(), 1, guard_fn, {&value},
                                                                 {1}, nullptr, false, true));
    expect(result, [](int r) -> std::optional<int> { return r % 3 == 0 ? std::optional<int>(r * 7) : std::nullopt; });
    ASSERT_EQ(33, value.seen.size());
    for (size_t i = 0; i < value.seen.size(); ++i) ASSERT_EQ(i * 3, value.seen[i]);
}

TEST_F(ConditionalTwoPhaseTest, cast_propagates_selection_to_child) {
    Chunk chunk;
    chunk.append_column(make_int([](int r) { return r; }), 1);
    SelectionOnlyExpr value(&chunk);
    TExprNode node;
    node.__set_node_type(TExprNodeType::CAST_EXPR);
    node.__set_opcode(TExprOpcode::CAST);
    node.__set_num_children(1);
    node.__set_is_nullable(true);
    node.__set_type(TypeDescriptor(TYPE_BIGINT).to_thrift());
    node.__set_child_type(TPrimitiveType::INT);
    node.__set_child_type_desc(TypeDescriptor(TYPE_INT).to_thrift());
    ObjectPool pool;
    auto* cast = pool.add(VectorizedCastExprFactory::from_thrift(&pool, node));
    ASSERT_NE(nullptr, cast);
    cast->add_child(&value);
    ASSIGN_OR_ABORT(auto result, cast->evaluate_selected(nullptr, &chunk, {8, 2, 8}));
    ASSERT_EQ(3, result->size());
    ColumnViewer<TYPE_BIGINT> viewer(result);
    ASSERT_EQ(56, viewer.value(0));
    ASSERT_EQ(14, viewer.value(1));
    ASSERT_EQ(56, viewer.value(2));
    value.seen.clear();
    ASSIGN_OR_ABORT(auto empty, cast->evaluate_selected(nullptr, &chunk, {}));
    ASSERT_EQ(0, empty->size());
    ASSERT_TRUE(value.seen.empty());
}

TEST_F(ConditionalTwoPhaseTest, cast_to_string_propagates_selection_to_child) {
    Chunk chunk;
    chunk.append_column(make_int([](int r) { return r; }), 1);
    SelectionOnlyExpr value(&chunk);
    TExprNode node;
    node.__set_node_type(TExprNodeType::CAST_EXPR);
    node.__set_opcode(TExprOpcode::CAST);
    node.__set_num_children(1);
    node.__set_is_nullable(true);
    node.__set_type(TypeDescriptor(TYPE_VARCHAR).to_thrift());
    node.__set_child_type(TPrimitiveType::INT);
    node.__set_child_type_desc(TypeDescriptor(TYPE_INT).to_thrift());
    ObjectPool pool;
    auto* cast = pool.add(VectorizedCastExprFactory::from_thrift(&pool, node));
    ASSERT_NE(nullptr, cast);
    cast->add_child(&value);
    ASSIGN_OR_ABORT(auto result, cast->evaluate_selected(nullptr, &chunk, {8, 2, 8}));
    ASSERT_EQ(3, result->size());
    ColumnViewer<TYPE_VARCHAR> viewer(result);
    ASSERT_EQ("56", viewer.value(0).to_string());
    ASSERT_EQ("14", viewer.value(1).to_string());
    ASSERT_EQ("56", viewer.value(2).to_string());
    value.seen.clear();
    ASSIGN_OR_ABORT(auto empty, cast->evaluate_selected(nullptr, &chunk, {}));
    ASSERT_EQ(0, empty->size());
    ASSERT_TRUE(value.seen.empty());
}

TEST_F(ConditionalTwoPhaseTest, selected_inputs_map_original_and_compact_coordinates) {
    auto input = make_nullable_int([](int r) { return r == 8; }, [](int r) { return r; });
    const std::vector<uint32_t> rows{8, 2, 8, 0};
    SelectedColumnViewer<TYPE_INT> original({input, &rows});
    auto compact = Int32Column::create();
    compact->append(18);
    compact->append(12);
    compact->append(18);
    compact->append(10);
    SelectedColumnViewer<TYPE_INT> dense({compact, nullptr});
    auto constant = ColumnHelper::create_const_column<TYPE_INT>(42, rows.size());
    SelectedColumnViewer<TYPE_INT> literal({constant, nullptr});
    for (size_t i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows[i] == 8, original.is_null(i));
        EXPECT_EQ(rows[i] + 10, dense.value(i));
        EXPECT_EQ(42, literal.value(i));
    }
    Chunk chunk;
    chunk.append_column(input, 9);
    ColumnRef ref(int_type(), 9);
    ASSIGN_OR_ABORT(auto selected, ref.evaluate_selected(nullptr, &chunk, rows));
    ASSERT_EQ(rows.size(), selected->size());
    ASSERT_TRUE(selected->is_null(0));
    ASSERT_EQ(2, selected->get(1).get_int32());
    ASSERT_TRUE(selected->is_null(2));
    ASSERT_EQ(kRows, input->size());
}

TEST_F(ConditionalTwoPhaseTest, constant_branches_and_null_runs) {
    Chunk chunk;
    chunk.append_column(make_int([](int r) { return r; }), 1);
    std::string text(4096, 'x');
    VectorizedLiteral value(ColumnHelper::create_const_column<TYPE_VARCHAR>(Slice(text), kRows), varchar_type());
    for (bool contiguous : {false, true}) {
        auto guard = make_bool([&](int r) { return contiguous ? r < 60 : r % 3 == 0; });
        auto guard_fn = [&](int) -> StatusOr<ColumnPtr> { return guard; };
        ASSIGN_OR_ABORT(auto result, two_phase_eval_predicate_routed(nullptr, &chunk, varchar_type(), 1, guard_fn,
                                                                     {&value}, {0}, nullptr, false, true));
        expect_str(result, [&](int r) -> std::optional<std::string> {
            return (contiguous ? r < 60 : r % 3 == 0) ? std::optional<std::string>(text) : std::nullopt;
        });
    }
}

} // namespace starrocks
