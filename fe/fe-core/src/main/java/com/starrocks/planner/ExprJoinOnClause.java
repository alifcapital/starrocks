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

package com.starrocks.planner;

import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.ExprToSql;

import java.util.List;
import java.util.Objects;

/**
 * Represents one disjunct (OR branch) in a hash join with OR conditions.
 * This is the planner-layer representation using Expr instead of ScalarOperator.
 *
 * For example: ON t1.x = t2.a OR t1.y = t2.b
 * would have two ExprJoinOnClause entries:
 *   clause[0]: eqJoinConjuncts = [t1.x = t2.a]
 *   clause[1]: eqJoinConjuncts = [t1.y = t2.b]
 *
 * Each clause can have additional non-equality predicates (AND'd within the clause).
 */
public class ExprJoinOnClause {
    // Equality predicates for hash join (combined with AND within this clause)
    private final List<BinaryPredicate> eqJoinConjuncts;

    // Additional non-equality predicates for this clause (AND'd with eqJoinConjuncts)
    private final List<Expr> otherConjuncts;

    public ExprJoinOnClause(List<BinaryPredicate> eqJoinConjuncts, List<Expr> otherConjuncts) {
        this.eqJoinConjuncts = eqJoinConjuncts;
        this.otherConjuncts = otherConjuncts;
    }

    public List<BinaryPredicate> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    public List<Expr> getOtherConjuncts() {
        return otherConjuncts;
    }

    public boolean hasEqJoinConjuncts() {
        return eqJoinConjuncts != null && !eqJoinConjuncts.isEmpty();
    }

    public boolean hasOtherConjuncts() {
        return otherConjuncts != null && !otherConjuncts.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExprJoinOnClause that = (ExprJoinOnClause) o;
        return Objects.equals(eqJoinConjuncts, that.eqJoinConjuncts) &&
                Objects.equals(otherConjuncts, that.otherConjuncts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eqJoinConjuncts, otherConjuncts);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ExprJoinOnClause{eq=[");
        if (eqJoinConjuncts != null) {
            for (int i = 0; i < eqJoinConjuncts.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(ExprToSql.toSql(eqJoinConjuncts.get(i)));
            }
        }
        sb.append("]");
        if (otherConjuncts != null && !otherConjuncts.isEmpty()) {
            sb.append(", other=[");
            for (int i = 0; i < otherConjuncts.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(ExprToSql.toSql(otherConjuncts.get(i)));
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }
}
