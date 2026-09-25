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

#pragma once

#include <cstdint>
#include <functional>
#include <vector>

#include "column/column.h"
#include "common/statusor.h"
#include "runtime/types.h"

namespace starrocks {

class Expr;
class ExprContext;
class Chunk;

// Two-phase (filtered) evaluation of the conditional family (CASE / IF / IFNULL / COALESCE).
//
// An expensive, non-constant value-branch is evaluated only on the rows that route to it; cheap branches are
// evaluated over the full chunk as before. Per-row result value+null match the eager path exactly for
// deterministic functions. Design: handbook/notes/two-phase-case-evaluation-design.md (§9).
//
// Selected branches use Expr::evaluate_selected; full branches use Expr::evaluate_checked.
// Each branch column has the conditional's result type (the FE inserts the branch casts).
// The result is assembled in original row order.

// Predicate-routed (searched CASE, IF).
//
// guard_fn(i) lazily yields the boolean guard column for branch i in [0, num_branches); branches are tried in
// order and the first whose guard is true-and-not-null claims a row (first-match-wins). then_exprs[i] is the
// value of branch i; else_expr is the ELSE value (nullptr => SQL NULL for unmatched rows). then_two_phase[i]
// (and else_two_phase) mark a branch whose expensive subtree is filtered. When enable_first_all_true_shortcut
// is true and the first surviving branch's guard is all-true, that branch's value column is returned directly
// (mirrors the eager first-all-true direct return; searched CASE and IF have it, simple CASE does not).
StatusOr<ColumnPtr> two_phase_eval_predicate_routed(ExprContext* context, Chunk* chunk,
                                                    const TypeDescriptor& result_type, int num_branches,
                                                    const std::function<StatusOr<ColumnPtr>(int)>& guard_fn,
                                                    const std::vector<Expr*>& then_exprs,
                                                    const std::vector<uint8_t>& then_two_phase, Expr* else_expr,
                                                    bool else_two_phase, bool enable_first_all_true_shortcut);

// Null-routed (IFNULL, COALESCE).
//
// A row takes the first arg that is non-null at that row; routing masks are built interleaved as args are
// evaluated (an arg's null-ness is the routing signal, so it cannot be deferred past its own evaluation).
// arg0 -- and any arg reached while no row is yet resolved -- is evaluated full. arg_two_phase[i] marks a
// deferrable expensive arg; arg_two_phase[0] is ignored (arg0 is always full). All-null rows yield SQL NULL.
StatusOr<ColumnPtr> two_phase_eval_null_routed(ExprContext* context, Chunk* chunk, const TypeDescriptor& result_type,
                                               const std::vector<Expr*>& arg_exprs,
                                               const std::vector<uint8_t>& arg_two_phase);

} // namespace starrocks
