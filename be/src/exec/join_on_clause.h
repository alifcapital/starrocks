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

#include <vector>

#include "exprs/expr_context.h"

namespace starrocks {

/**
 * Represents one disjunct (OR branch) in a hash join with OR conditions.
 * For example: ON t1.x = t2.a OR t1.y = t2.b
 * would have two JoinOnClause entries:
 *   clause[0]: probe_expr_ctxs = [t1.x], build_expr_ctxs = [t2.a]
 *   clause[1]: probe_expr_ctxs = [t1.y], build_expr_ctxs = [t2.b]
 *
 * Each clause can have additional non-equality predicates (AND'd within the clause).
 */
struct JoinOnClause {
    // Equality predicates for hash join: probe side expressions (left table)
    std::vector<ExprContext*> probe_expr_ctxs;

    // Equality predicates for hash join: build side expressions (right table)
    std::vector<ExprContext*> build_expr_ctxs;

    // Whether each equality predicate is null-safe (EQ_FOR_NULL)
    std::vector<bool> is_null_safes;

    // Additional non-equality predicates for this clause (AND'd with equality predicates)
    std::vector<ExprContext*> other_conjunct_ctxs;

    size_t num_eq_predicates() const { return probe_expr_ctxs.size(); }
    bool has_other_conjuncts() const { return !other_conjunct_ctxs.empty(); }
};

/**
 * Helper class for managing multiple JoinOnClause instances for OR joins.
 */
class DisjunctiveJoinClauses {
public:
    DisjunctiveJoinClauses() = default;

    void add_clause(JoinOnClause&& clause) { _clauses.push_back(std::move(clause)); }

    size_t num_clauses() const { return _clauses.size(); }

    bool is_disjunctive() const { return _clauses.size() > 1; }

    const JoinOnClause& clause(size_t index) const { return _clauses[index]; }

    JoinOnClause& clause(size_t index) { return _clauses[index]; }

    const std::vector<JoinOnClause>& clauses() const { return _clauses; }

private:
    std::vector<JoinOnClause> _clauses;
};

} // namespace starrocks
