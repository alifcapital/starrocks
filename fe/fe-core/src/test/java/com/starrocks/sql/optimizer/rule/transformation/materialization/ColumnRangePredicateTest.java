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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

public class ColumnRangePredicateTest {

    @Test
    public void testCastDate() {
        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "dt", true);
            CastOperator dateOp = new CastOperator(DateType.DATE, columnRef);
            LocalDateTime lowerDt = LocalDateTime.of(2023, 1, 1, 0, 0);
            ConstantOperator lowerConstant = ConstantOperator.createDate(lowerDt);
            LocalDateTime upperDt = LocalDateTime.of(2023, 10, 1, 0, 0);
            ConstantOperator upperConstant = ConstantOperator.createDate(upperDt);
            Range<ConstantOperator> range = Range.atLeast(lowerConstant);
            range = range.intersection(Range.atMost(upperConstant));
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(dateOp, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assertions.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assertions.assertEquals("2023-01-01", ((ConstantOperator) ranges1.get(0).lowerEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assertions.assertEquals("20230101", ((ConstantOperator) ranges2.get(0).lowerEndpoint()).getVarchar());

            ConstantOperator low = ConstantOperator.createVarchar("20230101");
            ConstantOperator up = ConstantOperator.createVarchar("20231001");
            Range<ConstantOperator> r = Range.atLeast(low);
            r = r.intersection(Range.atMost(up));
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assertions.assertEquals(ConstantOperator.TRUE, ret);
        }

        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "dt", true);
            CastOperator dateOp = new CastOperator(DateType.DATE, columnRef);
            LocalDateTime upperDt = LocalDateTime.of(2023, 10, 1, 0, 0);
            ConstantOperator upperConstant = ConstantOperator.createDate(upperDt);
            Range<ConstantOperator> range = Range.atMost(upperConstant);
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(dateOp, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assertions.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assertions.assertEquals("2023-10-01", ((ConstantOperator) ranges1.get(0).upperEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assertions.assertEquals("20231001", ((ConstantOperator) ranges2.get(0).upperEndpoint()).getVarchar());

            ConstantOperator up = ConstantOperator.createVarchar("2023-10-01");
            Range<ConstantOperator> r = Range.atMost(up);
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assertions.assertEquals(ConstantOperator.TRUE, ret);
        }

        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "dt", true);
            CastOperator dateOp = new CastOperator(DateType.DATE, columnRef);
            LocalDateTime lowerDt = LocalDateTime.of(2023, 1, 1, 0, 0);
            ConstantOperator lowerConstant = ConstantOperator.createDate(lowerDt);
            Range<ConstantOperator> range = Range.atLeast(lowerConstant);
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(dateOp, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assertions.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assertions.assertEquals("2023-01-01", ((ConstantOperator) ranges1.get(0).lowerEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assertions.assertEquals("20230101", ((ConstantOperator) ranges2.get(0).lowerEndpoint()).getVarchar());

            ConstantOperator low = ConstantOperator.createVarchar("20230101");
            Range<ConstantOperator> r = Range.atLeast(low);
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assertions.assertEquals(ConstantOperator.TRUE, ret);
        }
    }

    @Test
    public void testStr2Date() {
        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "dt", true);
            ConstantOperator format = ConstantOperator.createVarchar("%Y-%m-%d");
            List<ScalarOperator> args = Lists.newArrayList(columnRef, format);
            CallOperator call = new CallOperator(FunctionSet.STR2DATE, DateType.DATE, args);
            LocalDateTime lowerDt = LocalDateTime.of(2023, 1, 1, 0, 0);
            ConstantOperator lowerConstant = ConstantOperator.createDate(lowerDt);
            LocalDateTime upperDt = LocalDateTime.of(2023, 10, 1, 0, 0);
            ConstantOperator upperConstant = ConstantOperator.createDate(upperDt);
            Range<ConstantOperator> range = Range.atLeast(lowerConstant);
            range = range.intersection(Range.atMost(upperConstant));
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(call, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assertions.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assertions.assertEquals("2023-01-01", ((ConstantOperator) ranges1.get(0).lowerEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assertions.assertEquals("20230101", ((ConstantOperator) ranges2.get(0).lowerEndpoint()).getVarchar());

            ConstantOperator low = ConstantOperator.createVarchar("20230101");
            ConstantOperator up = ConstantOperator.createVarchar("20231001");
            Range<ConstantOperator> r = Range.atLeast(low);
            r = r.intersection(Range.atMost(up));
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assertions.assertEquals(ConstantOperator.TRUE, ret);
        }

        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "dt", true);
            ConstantOperator format = ConstantOperator.createVarchar("%Y-%m-%d");
            List<ScalarOperator> args = Lists.newArrayList(columnRef, format);
            CallOperator call = new CallOperator(FunctionSet.STR2DATE, DateType.DATE, args);
            LocalDateTime upperDt = LocalDateTime.of(2023, 10, 1, 0, 0);
            ConstantOperator upperConstant = ConstantOperator.createDate(upperDt);
            Range<ConstantOperator> range = Range.atMost(upperConstant);
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(call, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assertions.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assertions.assertEquals("2023-10-01", ((ConstantOperator) ranges1.get(0).upperEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assertions.assertEquals("20231001", ((ConstantOperator) ranges2.get(0).upperEndpoint()).getVarchar());

            ConstantOperator up = ConstantOperator.createVarchar("2023-10-01");
            Range<ConstantOperator> r = Range.atMost(up);
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assertions.assertEquals(ConstantOperator.TRUE, ret);
        }

        {
            ColumnRefOperator columnRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "dt", true);
            ConstantOperator format = ConstantOperator.createVarchar("%Y-%m-%d");
            List<ScalarOperator> args = Lists.newArrayList(columnRef, format);
            CallOperator call = new CallOperator(FunctionSet.STR2DATE, DateType.DATE, args);
            LocalDateTime lowerDt = LocalDateTime.of(2023, 1, 1, 0, 0);
            ConstantOperator lowerConstant = ConstantOperator.createDate(lowerDt);
            Range<ConstantOperator> range = Range.atLeast(lowerConstant);
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.add(range);

            ColumnRangePredicate columnRangePredicate = new ColumnRangePredicate(call, rangeSet);
            List<ColumnRangePredicate> results = columnRangePredicate.getEquivalentRangePredicates();
            Assertions.assertEquals(2, results.size());
            List<Range> ranges1 = Lists.newArrayList(results.get(0).getColumnRanges().asRanges());
            Assertions.assertEquals("2023-01-01", ((ConstantOperator) ranges1.get(0).lowerEndpoint()).getVarchar());
            List<Range> ranges2 = Lists.newArrayList(results.get(1).getColumnRanges().asRanges());
            Assertions.assertEquals("20230101", ((ConstantOperator) ranges2.get(0).lowerEndpoint()).getVarchar());

            ConstantOperator low = ConstantOperator.createVarchar("20230101");
            Range<ConstantOperator> r = Range.atLeast(low);
            TreeRangeSet<ConstantOperator> rs = TreeRangeSet.create();
            rs.add(r);
            ColumnRangePredicate result = new ColumnRangePredicate(columnRef, rs);
            ScalarOperator ret = columnRangePredicate.simplify(result);
            Assertions.assertEquals(ConstantOperator.TRUE, ret);
        }
    }

    @Test
    public void testNonCanonicalBigintRangePredicate() {
        ColumnRefOperator columnRef = new ColumnRefOperator(1, IntegerType.BIGINT, "col", true);
        ConstantOperator maxValue = ConstantOperator.createBigint(9223372036854775807L);
        BinaryPredicateOperator pred = new BinaryPredicateOperator(BinaryType.GT, columnRef, maxValue);
        PredicateExtractor extractor = new PredicateExtractor();
        PredicateExtractor.PredicateExtractorContext context = new PredicateExtractor.PredicateExtractorContext();
        RangePredicate rangePredicate = extractor.visitBinaryPredicate(pred, context);
        String s  = rangePredicate.toScalarOperator().toString();
        Assertions.assertEquals("1: col > 9223372036854775807", s, s);
    }

    @Test
    public void testEqualsOfExpressionsOfOneColumn() {
        ColumnRefOperator columnRef = new ColumnRefOperator(1, DateType.DATE, "dt", true);
        TreeRangeSet<ConstantOperator> dates = TreeRangeSet.create();
        dates.add(Range.singleton(ConstantOperator.createDate(LocalDateTime.of(1991, 3, 30, 0, 0))));
        ColumnRangePredicate onColumn = new ColumnRangePredicate(columnRef, dates);

        // days_sub(cast(dt as datetime), 1) = '1991-03-29 00:00:00' has a DATETIME range on the same column
        CastOperator cast = new CastOperator(DateType.DATETIME, columnRef);
        CallOperator daysSub = new CallOperator(FunctionSet.DAYS_SUB, DateType.DATETIME,
                List.<ScalarOperator>of(cast, ConstantOperator.createInt(1)));
        TreeRangeSet<ConstantOperator> datetimes = TreeRangeSet.create();
        datetimes.add(Range.singleton(ConstantOperator.createDatetime(LocalDateTime.of(1991, 3, 29, 0, 0))));
        ColumnRangePredicate onExpression = new ColumnRangePredicate(daysSub, datetimes);

        Assertions.assertNotEquals(onColumn, onExpression);
        Assertions.assertNotEquals(onExpression, onColumn);
        Assertions.assertEquals(onColumn, new ColumnRangePredicate(columnRef, dates));
    }

    // Evaluates a predicate over integer columns, so that a test can compare a rewrite with the ranges row by row.
    private static boolean evaluate(ScalarOperator operator, int value) {
        if (operator instanceof ConstantOperator) {
            return ((ConstantOperator) operator).getBoolean();
        }
        if (operator instanceof CompoundPredicateOperator) {
            CompoundPredicateOperator compound = (CompoundPredicateOperator) operator;
            if (compound.isAnd()) {
                return compound.getChildren().stream().allMatch(child -> evaluate(child, value));
            }
            if (compound.isOr()) {
                return compound.getChildren().stream().anyMatch(child -> evaluate(child, value));
            }
            return !evaluate(compound.getChild(0), value);
        }
        if (operator instanceof InPredicateOperator) {
            boolean found = operator.getChildren().stream().skip(1)
                    .anyMatch(child -> ((ConstantOperator) child).getInt() == value);
            return ((InPredicateOperator) operator).isNotIn() != found;
        }
        BinaryPredicateOperator binary = (BinaryPredicateOperator) operator;
        int right = ((ConstantOperator) binary.getChild(1)).getInt();
        switch (binary.getBinaryType()) {
            case EQ:
                return value == right;
            case NE:
                return value != right;
            case LT:
                return value < right;
            case LE:
                return value <= right;
            case GT:
                return value > right;
            case GE:
                return value >= right;
            default:
                throw new IllegalStateException(binary.toString());
        }
    }

    @Test
    public void testRangesWithOneMissingValueKeepTheirRows() {
        ColumnRefOperator column = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ConstantOperator seven = ConstantOperator.createInt(7);
        ConstantOperator eight = ConstantOperator.createInt(8);
        ConstantOperator nine = ConstantOperator.createInt(9);
        List<List<Range<ConstantOperator>>> cases = List.of(
                // a <= 7 OR a > 8 leaves out 8, the upper endpoint of the gap (7, 8].
                List.of(Range.atMost(seven), Range.greaterThan(eight)),
                // a < 8 OR a >= 9 leaves out 8, the lower endpoint of the gap [8, 9).
                List.of(Range.lessThan(eight), Range.atLeast(nine)),
                // a <= 7 OR a >= 9 leaves out 8, inside the open gap (7, 9).
                List.of(Range.atMost(seven), Range.atLeast(nine)),
                // 7 < a < 9 is a = 8.
                List.of(Range.open(seven, nine)),
                // Two single values given as open ranges.
                List.of(Range.open(seven, nine), Range.open(ConstantOperator.createInt(10), ConstantOperator.createInt(12))));
        for (List<Range<ConstantOperator>> ranges : cases) {
            TreeRangeSet<ConstantOperator> set = TreeRangeSet.create();
            ranges.forEach(set::add);
            ScalarOperator predicate = new ColumnRangePredicate(column, TreeRangeSet.create(set)).toScalarOperator();
            for (int value = 0; value <= 15; value++) {
                Assertions.assertEquals(set.contains(ConstantOperator.createInt(value)), evaluate(predicate, value),
                        set + " as " + predicate + " at " + value);
            }
        }
    }
}
