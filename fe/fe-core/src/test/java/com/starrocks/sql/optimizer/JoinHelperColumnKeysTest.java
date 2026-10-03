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

import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class JoinHelperColumnKeysTest {
    @Test
    public void equalityKeysFollowJoinInputsAndStrictness() {
        // We build random ON predicates over overlapping left and right inputs and compute the expected
        // result from the rules below. We expect getEqualsPredicate to keep only equivalence predicates whose
        // two sides come from different inputs, and JoinHelper to orient each key to its input, mark it null
        // strict for <=>, mark it agg strict for the outer side of the join, and reject keys over more than
        // one column.
        Random random = new Random(1729);
        List<ScalarOperator> operands = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            operands.add(new ColumnRefOperator(i + 1, IntegerType.INT, "c" + i, true));
        }
        operands.add(new CastOperator(IntegerType.BIGINT, operands.get(0)));
        operands.add(ConstantOperator.createInt(7));
        operands.add(new ColumnRefOperator(1, IntegerType.INT, "lambda", true, true));
        operands.add(new CallOperator("add", IntegerType.INT, List.of(operands.get(0), operands.get(1))));
        BinaryType[] comparisons = {BinaryType.EQ, BinaryType.EQ_FOR_NULL, BinaryType.NE, BinaryType.LT};
        JoinOperator[] joins = {JoinOperator.INNER_JOIN, JoinOperator.LEFT_OUTER_JOIN,
                JoinOperator.RIGHT_OUTER_JOIN, JoinOperator.FULL_OUTER_JOIN, JoinOperator.LEFT_SEMI_JOIN,
                JoinOperator.LEFT_ANTI_JOIN};
        for (int trial = 0; trial < 2000; trial++) {
            ColumnRefSet left = new ColumnRefSet();
            ColumnRefSet right = new ColumnRefSet();
            for (int id = 1; id <= 6; id++) {
                if (random.nextBoolean()) {
                    left.union(id);
                }
                if (random.nextBoolean()) {
                    right.union(id);
                }
            }
            List<ScalarOperator> predicates = new ArrayList<>();
            for (int i = 0; i < 6; i++) {
                predicates.add(new BinaryPredicateOperator(comparisons[random.nextInt(comparisons.length)],
                        operands.get(random.nextInt(operands.size())), operands.get(random.nextInt(operands.size()))));
            }
            JoinOperator type = joins[trial % joins.length];
            List<BinaryPredicateOperator> accepted = new ArrayList<>();
            List<DistributionCol> expectedLeft = new ArrayList<>();
            List<DistributionCol> expectedRight = new ArrayList<>();
            boolean leftStrict = type.isAnyLeftOuterJoin() || type.isFullOuterJoin();
            boolean rightStrict = type.isRightOuterJoin() || type.isFullOuterJoin();
            boolean unsupported = false;
            for (ScalarOperator predicate : predicates) {
                BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
                ColumnRefSet a = binary.getChild(0).getUsedColumns();
                ColumnRefSet b = binary.getChild(1).getUsedColumns();
                boolean forward = left.containsAll(a) && right.containsAll(b);
                boolean reverse = left.containsAll(b) && right.containsAll(a);
                if (!binary.getBinaryType().isEquivalence() || a.isEmpty() || b.isEmpty() || !(forward || reverse)) {
                    continue;
                }
                accepted.add(binary);
                if (a.size() > 1 || b.size() > 1) {
                    unsupported = true;
                    continue;
                }
                boolean nullStrict = binary.getBinaryType() == BinaryType.EQ_FOR_NULL;
                leftStrict |= nullStrict;
                rightStrict |= nullStrict;
                expectedLeft.add(new DistributionCol(forward ? a.getFirstId() : b.getFirstId(), nullStrict, leftStrict));
                expectedRight.add(new DistributionCol(forward ? b.getFirstId() : a.getFirstId(), nullStrict, rightStrict));
            }
            Assertions.assertEquals(accepted, JoinHelper.getEqualsPredicate(left, right, predicates));
            Assertions.assertEquals(!accepted.isEmpty(),
                    JoinHelper.hasEqualsPredicate(left, right, Utils.compoundAnd(predicates)));
            LogicalJoinOperator join = new LogicalJoinOperator(type, Utils.compoundAnd(predicates));
            if (unsupported) {
                Assertions.assertThrows(StarRocksPlannerException.class, () -> JoinHelper.of(join, left, right));
            } else {
                JoinHelper helper = JoinHelper.of(join, left, right);
                compare(expectedLeft, helper.getLeftCols());
                compare(expectedRight, helper.getRightCols());
            }
        }
    }

    private static void compare(List<DistributionCol> expected, List<DistributionCol> actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertEquals(expected.get(i).getColId(), actual.get(i).getColId());
            Assertions.assertEquals(expected.get(i).isNullStrict(), actual.get(i).isNullStrict());
            Assertions.assertEquals(expected.get(i).isAggStrict(), actual.get(i).isAggStrict());
        }
    }
}
