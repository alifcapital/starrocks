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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OrRangePredicateTest {
    private static RangePredicate equal(ColumnRefOperator column, int value) {
        TreeRangeSet<ConstantOperator> ranges = TreeRangeSet.create();
        ranges.add(Range.singleton(ConstantOperator.createInt(value)));
        return new ColumnRangePredicate(column, ranges);
    }

    @Test
    public void testValuesOfSameNamedColumnsAreNotMerged() {
        // Both sides of a self join have a column named a. Only the values of one column ref form an IN list.
        ColumnRefOperator left = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator right = new ColumnRefOperator(2, IntegerType.INT, "a", true);
        ScalarOperator result =
                new OrRangePredicate(Lists.newArrayList(equal(left, 1), equal(right, 2))).toScalarOperator();
        Assertions.assertEquals(CompoundPredicateOperator.or(
                BinaryPredicateOperator.eq(left, ConstantOperator.createInt(1)),
                BinaryPredicateOperator.eq(right, ConstantOperator.createInt(2))), result);

        result = new OrRangePredicate(Lists.newArrayList(equal(left, 1), equal(right, 2), equal(left, 3)))
                .toScalarOperator();
        Assertions.assertEquals(CompoundPredicateOperator.or(
                BinaryPredicateOperator.eq(right, ConstantOperator.createInt(2)),
                new InPredicateOperator(false, left, ConstantOperator.createInt(1), ConstantOperator.createInt(3))),
                result);
    }
}
