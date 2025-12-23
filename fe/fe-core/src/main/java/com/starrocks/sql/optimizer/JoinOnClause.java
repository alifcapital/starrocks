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

package com.starrocks.sql.optimizer;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.List;
import java.util.Objects;

/**
 * Represents one disjunct (OR branch) in a hash join with OR conditions.
 * For example: ON t1.x = t2.a OR t1.y = t2.b
 * would have two JoinOnClause entries:
 *   clause[0]: eqJoinConjuncts = [t1.x = t2.a]
 *   clause[1]: eqJoinConjuncts = [t1.y = t2.b]
 * Each clause can have additional non-equality predicates (AND'd within the clause).
 */
public class JoinOnClause {
    // Equality predicates for hash join (combined with AND within this clause)
    private final List<BinaryPredicateOperator> eqJoinConjuncts;

    // Additional non-equality predicates for this clause (AND'd with eqJoinConjuncts)
    private final List<ScalarOperator> otherConjuncts;

    // Left and right column IDs for the equality predicates
    private final List<Integer> leftKeyColumnIds;
    private final List<Integer> rightKeyColumnIds;

    public JoinOnClause(List<BinaryPredicateOperator> eqJoinConjuncts,
                        List<ScalarOperator> otherConjuncts,
                        ColumnRefSet leftColumns,
                        ColumnRefSet rightColumns) {
        this.eqJoinConjuncts = eqJoinConjuncts;
        this.otherConjuncts = otherConjuncts;
        this.leftKeyColumnIds = Lists.newArrayList();
        this.rightKeyColumnIds = Lists.newArrayList();

        // Extract column IDs from equality predicates
        for (BinaryPredicateOperator eq : eqJoinConjuncts) {
            ColumnRefSet leftUsed = eq.getChild(0).getUsedColumns();
            ColumnRefSet rightUsed = eq.getChild(1).getUsedColumns();

            if (leftColumns.containsAll(leftUsed) && rightColumns.containsAll(rightUsed)) {
                leftKeyColumnIds.add(leftUsed.getFirstId());
                rightKeyColumnIds.add(rightUsed.getFirstId());
            } else if (leftColumns.containsAll(rightUsed) && rightColumns.containsAll(leftUsed)) {
                leftKeyColumnIds.add(rightUsed.getFirstId());
                rightKeyColumnIds.add(leftUsed.getFirstId());
            }
        }
    }

    public List<BinaryPredicateOperator> getEqJoinConjuncts() {
        return eqJoinConjuncts;
    }

    public List<ScalarOperator> getOtherConjuncts() {
        return otherConjuncts;
    }

    public List<Integer> getLeftKeyColumnIds() {
        return leftKeyColumnIds;
    }

    public List<Integer> getRightKeyColumnIds() {
        return rightKeyColumnIds;
    }

    public boolean hasEqJoinConjuncts() {
        return !eqJoinConjuncts.isEmpty();
    }

    public boolean hasOtherConjuncts() {
        return !otherConjuncts.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JoinOnClause that = (JoinOnClause) o;
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
        sb.append("JoinOnClause{eq=[");
        for (int i = 0; i < eqJoinConjuncts.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(eqJoinConjuncts.get(i));
        }
        sb.append("]");
        if (!otherConjuncts.isEmpty()) {
            sb.append(", other=[");
            for (int i = 0; i < otherConjuncts.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(otherConjuncts.get(i));
            }
            sb.append("]");
        }
        sb.append("}");
        return sb.toString();
    }
}
