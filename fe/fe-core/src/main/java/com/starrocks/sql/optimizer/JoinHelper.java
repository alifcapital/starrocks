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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.stream.PhysicalStreamJoinOperator;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.ast.expression.BinaryType.EQ_FOR_NULL;

public class JoinHelper {
    private final JoinOperator type;
    private final ColumnRefSet leftChildColumns;
    private final ColumnRefSet rightChildColumns;

    private final ScalarOperator onPredicate;
    private final String hint;

    private List<BinaryPredicateOperator> equalsPredicate;

    private List<DistributionCol> leftOnCols;

    private List<DistributionCol> rightOnCols;

    public static JoinHelper of(Operator join, ColumnRefSet leftInput, ColumnRefSet rightInput) {
        JoinHelper helper = new JoinHelper(join, leftInput, rightInput);
        helper.init();
        return helper;
    }

    private JoinHelper(Operator join, ColumnRefSet leftInput, ColumnRefSet rightInput) {
        this.leftChildColumns = leftInput;
        this.rightChildColumns = rightInput;

        if (join instanceof LogicalJoinOperator) {
            LogicalJoinOperator ljo = ((LogicalJoinOperator) join);
            type = ljo.getJoinType();
            onPredicate = ljo.getOnPredicate();
            hint = ljo.getJoinHint();
        } else if (join instanceof PhysicalJoinOperator) {
            PhysicalJoinOperator phjo = ((PhysicalJoinOperator) join);
            type = phjo.getJoinType();
            onPredicate = phjo.getOnPredicate();
            hint = phjo.getJoinHint();
        } else if (join instanceof PhysicalStreamJoinOperator) {
            PhysicalStreamJoinOperator operator = (PhysicalStreamJoinOperator) join;
            type = operator.getJoinType();
            onPredicate = operator.getOnPredicate();
            hint = operator.getJoinHint();
        } else {
            type = null;
            onPredicate = null;
            hint = null;
            Preconditions.checkState(false, "Operator must be join operator");
        }
    }

    private void init() {
        equalsPredicate = getEqualsPredicate(leftChildColumns, rightChildColumns, Utils.extractConjuncts(onPredicate));
        leftOnCols = Lists.newArrayList();
        rightOnCols = Lists.newArrayList();

        boolean leftTableAggStrict = type.isAnyLeftOuterJoin() || type.isFullOuterJoin();
        boolean rightTableAggStrict = type.isRightOuterJoin() || type.isFullOuterJoin();

        for (BinaryPredicateOperator binaryPredicate : equalsPredicate) {
            boolean nullStrict = binaryPredicate.getBinaryType() == EQ_FOR_NULL;
            leftTableAggStrict = leftTableAggStrict || nullStrict;
            rightTableAggStrict = rightTableAggStrict || nullStrict;
            ColumnRefSet leftUsedColumns = binaryPredicate.getChild(0).getUsedColumns();
            ColumnRefSet rightUsedColumns = binaryPredicate.getChild(1).getUsedColumns();
            // Join on expression had pushed down to project node, so there must be one column
            if (leftUsedColumns.cardinality() > 1 || rightUsedColumns.cardinality() > 1) {
                throw new StarRocksPlannerException(
                        "we do not support equal on predicate have multi columns in left or right",
                        ErrorType.UNSUPPORTED);
            }

            if (leftChildColumns.containsAll(leftUsedColumns) && rightChildColumns.containsAll(rightUsedColumns)) {
                leftOnCols.add(new DistributionCol(leftUsedColumns.getFirstId(), nullStrict, leftTableAggStrict));
                rightOnCols.add(new DistributionCol(rightUsedColumns.getFirstId(), nullStrict, rightTableAggStrict));
            } else if (leftChildColumns.containsAll(rightUsedColumns) &&
                    rightChildColumns.containsAll(leftUsedColumns)) {
                leftOnCols.add(new DistributionCol(rightUsedColumns.getFirstId(), nullStrict, leftTableAggStrict));
                rightOnCols.add(new DistributionCol(leftUsedColumns.getFirstId(), nullStrict, rightTableAggStrict));
            } else {
                Preconditions.checkState(false, "shouldn't reach here");
            }
        }
    }

    public List<Integer> getLeftOnColumnIds() {
        return leftOnCols.stream().map(DistributionCol::getColId).collect(Collectors.toList());
    }

    public List<Integer> getRightOnColumnIds() {
        return rightOnCols.stream().map(DistributionCol::getColId).collect(Collectors.toList());
    }

    public List<DistributionCol> getLeftCols() {
        return leftOnCols;
    }

    public List<DistributionCol> getRightCols() {
        return rightOnCols;
    }

    public boolean isCrossJoin() {
        return type.isCrossJoin() || equalsPredicate.isEmpty();
    }

    public boolean onlyBroadcast() {
        // Cross join only support broadcast join
        // Disjunctive joins (OR in ON clause) require broadcast
        return onlyBroadcast(type, equalsPredicate, hint) || isDisjunctiveJoin(onPredicate);
    }

    public boolean onlyShuffle() {
        return type.isRightJoin() || type.isFullOuterJoin() || HintNode.HINT_JOIN_SHUFFLE.equals(hint) ||
                HintNode.HINT_JOIN_BUCKET.equals(hint) || HintNode.HINT_JOIN_SKEW.equals(hint);
    }

    public static List<BinaryPredicateOperator> getEqualsPredicate(ColumnRefSet leftColumns, ColumnRefSet rightColumns,
                                                                   List<ScalarOperator> conjunctList) {
        List<BinaryPredicateOperator> eqConjuncts = Lists.newArrayList();
        for (ScalarOperator predicate : conjunctList) {
            if (isEqualBinaryPredicate(leftColumns, rightColumns, predicate)) {
                eqConjuncts.add((BinaryPredicateOperator) predicate);
            }
        }
        return eqConjuncts;
    }

    public static Pair<List<BinaryPredicateOperator>, List<ScalarOperator>> separateEqualPredicatesFromOthers(
            OptExpression optExpression) {
        Preconditions.checkArgument(optExpression.getOp() instanceof LogicalJoinOperator);
        LogicalJoinOperator joinOp = optExpression.getOp().cast();
        List<ScalarOperator> onPredicates = Utils.extractConjuncts(joinOp.getOnPredicate());

        ColumnRefSet leftChildColumns = optExpression.inputAt(0).getOutputColumns();
        ColumnRefSet rightChildColumns = optExpression.inputAt(1).getOutputColumns();

        List<BinaryPredicateOperator> eqOnPredicates = JoinHelper.getEqualsPredicate(
                leftChildColumns, rightChildColumns, onPredicates);

        onPredicates.removeAll(eqOnPredicates);
        List<BinaryPredicateOperator> lhsEqRhsOnPredicates = Lists.newArrayList();
        for (BinaryPredicateOperator s : eqOnPredicates) {
            if (!leftChildColumns.containsAll(s.getChild(0).getUsedColumns())) {
                lhsEqRhsOnPredicates.add(new BinaryPredicateOperator(s.getBinaryType(), s.getChild(1), s.getChild(0)));
            } else {
                lhsEqRhsOnPredicates.add(s);
            }
        }
        return Pair.create(lhsEqRhsOnPredicates, onPredicates);
    }


    /**
     * Conditions should contain:
     * 1. binary predicate operator is EQ or EQ_FOR_NULL type
     * 2. operands in each side of operator should totally belong to each side of join's input
     *
     * @param leftColumns
     * @param rightColumns
     * @param predicate
     * @return
     */
    private static boolean isEqualBinaryPredicate(ColumnRefSet leftColumns, ColumnRefSet rightColumns,
                                                  ScalarOperator predicate) {
        if (predicate instanceof BinaryPredicateOperator) {
            BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) predicate;
            if (!binaryPredicate.getBinaryType().isEquivalence()) {
                return false;
            }

            ColumnRefSet leftUsedColumns = binaryPredicate.getChild(0).getUsedColumns();
            ColumnRefSet rightUsedColumns = binaryPredicate.getChild(1).getUsedColumns();

            // Constant predicate
            if (leftUsedColumns.isEmpty() || rightUsedColumns.isEmpty()) {
                return false;
            }

            return leftColumns.containsAll(leftUsedColumns) && rightColumns.containsAll(rightUsedColumns) ||
                    leftColumns.containsAll(rightUsedColumns) && rightColumns.containsAll(leftUsedColumns);
        }
        return false;
    }

    public static boolean onlyBroadcast(JoinOperator type, List<BinaryPredicateOperator> equalOnPredicate,
                                        String hint) {
        // Cross join only support broadcast join
        return type.isCrossJoin() || JoinOperator.NULL_AWARE_LEFT_ANTI_JOIN.equals(type) ||
                (type.isInnerJoin() && equalOnPredicate.isEmpty()) || HintNode.HINT_JOIN_BROADCAST.equals(hint);
    }

    /**
     * Check if the ON predicate contains OR at the top level, making it a disjunctive join.
     * Disjunctive joins require BROADCAST because different disjuncts have different join keys,
     * and SHUFFLE would only distribute data correctly for one key - matches on other keys
     * could end up on different nodes and be missed.
     *
     * @param onPredicate The ON predicate
     * @return true if the predicate has top-level OR (disjunctive join)
     */
    public static boolean isDisjunctiveJoin(ScalarOperator onPredicate) {
        if (onPredicate == null) {
            return false;
        }
        List<ScalarOperator> disjuncts = Utils.extractDisjunctive(onPredicate);
        return disjuncts.size() > 1;
    }

    /**
     * Apply commutative transformation to a binary predicate
     * For comparison operators (>, <, >=, <=), swap operands and transform operators
     * For AsOf join scenarios where we need: left_table.column OP right_table.column
     *
     * @param predicate The predicate to transform
     * @param leftColumns Columns from left table
     * @param rightColumns Columns from right table
     * @return Transformed predicate with proper left-right operand order
     */
    public static ScalarOperator applyCommutativeToPredicates(ScalarOperator predicate,
                                                             ColumnRefSet leftColumns,
                                                             ColumnRefSet rightColumns) {
        if (predicate instanceof BinaryPredicateOperator binaryPred) {

            // Only apply to comparison operators (>, <, >=, <=)
            if (binaryPred.getBinaryType().isRange()) {
                if (!leftColumns.containsAll(binaryPred.getChild(0).getUsedColumns()) &&
                        rightColumns.containsAll(binaryPred.getChild(0).getUsedColumns())) {
                    return binaryPred.commutative();
                } else {
                    return predicate;
                }
            } else {
                return predicate;
            }
        } else {
            return predicate;
        }
    }

    /**
     * Extract JoinOnClauses from an ON predicate that may contain OR conditions.
     * For example: ON t1.x = t2.a OR t1.y = t2.b
     * would return two JoinOnClause entries.
     *
     * @param leftColumns  Columns from left table
     * @param rightColumns Columns from right table
     * @param onPredicate  The ON predicate (may contain OR)
     * @return List of JoinOnClause if all disjuncts have equality predicates, null otherwise
     */
    public static List<JoinOnClause> extractJoinOnClauses(ColumnRefSet leftColumns,
                                                          ColumnRefSet rightColumns,
                                                          ScalarOperator onPredicate) {
        if (onPredicate == null) {
            return null;
        }

        // Extract disjuncts (OR branches)
        List<ScalarOperator> disjuncts = Utils.extractDisjunctive(onPredicate);

        // If no OR or single predicate, return null (use standard hash join)
        if (disjuncts.size() <= 1) {
            return null;
        }

        List<JoinOnClause> clauses = Lists.newArrayList();

        for (ScalarOperator disjunct : disjuncts) {
            // Extract conjuncts (AND branches) within this disjunct
            List<ScalarOperator> conjuncts = Lists.newArrayList(Utils.extractConjuncts(disjunct));

            // Find equality predicates
            List<BinaryPredicateOperator> eqPredicates = getEqualsPredicate(leftColumns, rightColumns, conjuncts);

            // If no equality predicates in this disjunct, cannot use hash join with OR
            if (eqPredicates.isEmpty()) {
                return null;
            }

            // Remaining predicates are other conjuncts
            conjuncts.removeAll(eqPredicates);

            // Normalize equality predicates: ensure left child uses left columns
            List<BinaryPredicateOperator> normalizedEqPredicates = Lists.newArrayList();
            for (BinaryPredicateOperator eq : eqPredicates) {
                if (!leftColumns.containsAll(eq.getChild(0).getUsedColumns())) {
                    // Swap: right = left -> left = right
                    normalizedEqPredicates.add(new BinaryPredicateOperator(
                            eq.getBinaryType(), eq.getChild(1), eq.getChild(0)));
                } else {
                    normalizedEqPredicates.add(eq);
                }
            }

            clauses.add(new JoinOnClause(normalizedEqPredicates, conjuncts, leftColumns, rightColumns));
        }

        return clauses;
    }

    /**
     * Check if the given ON predicate can be handled by hash join with OR support.
     * This is true if:
     * 1. The predicate contains OR at the top level
     * 2. Each OR branch has at least one equality predicate suitable for hash join
     *
     * @param leftColumns  Columns from left table
     * @param rightColumns Columns from right table
     * @param onPredicate  The ON predicate
     * @return true if hash join with OR can be used
     */
    public static boolean canUseHashJoinWithOr(ColumnRefSet leftColumns,
                                               ColumnRefSet rightColumns,
                                               ScalarOperator onPredicate) {
        List<JoinOnClause> clauses = extractJoinOnClauses(leftColumns, rightColumns, onPredicate);
        return clauses != null && clauses.size() > 1;
    }
}
