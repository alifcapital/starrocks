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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.DateTruncEquivalent;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.TimeSliceRewriteEquivalent;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StringType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PredicateExtractorTest {
    private static Map<ScalarType, List<ScalarOperator>> DATA = ImmutableMap.<ScalarType, List<ScalarOperator>>builder()
            .put(IntegerType.TINYINT, Lists.newArrayList(ConstantOperator.createTinyInt((byte) 10),
                    ConstantOperator.createTinyInt((byte) 20), ConstantOperator.createTinyInt((byte) 30),
                    ConstantOperator.createTinyInt((byte) 50), ConstantOperator.createTinyInt((byte) 50)))
            .put(IntegerType.SMALLINT, Lists.newArrayList(ConstantOperator.createSmallInt((short) 10),
                    ConstantOperator.createSmallInt((short) 20), ConstantOperator.createSmallInt((short) 30),
                    ConstantOperator.createSmallInt((short) 50), ConstantOperator.createSmallInt((short) 50)))
            .put(IntegerType.INT, Lists.newArrayList(ConstantOperator.createInt(10),
                    ConstantOperator.createInt(20), ConstantOperator.createInt(30),
                    ConstantOperator.createInt(50), ConstantOperator.createInt(50)))
            .put(IntegerType.BIGINT, Lists.newArrayList(ConstantOperator.createBigint(10),
                    ConstantOperator.createBigint(20), ConstantOperator.createBigint(30),
                    ConstantOperator.createBigint(50), ConstantOperator.createBigint(50)))
            .put(IntegerType.LARGEINT, Lists.newArrayList(ConstantOperator.createLargeInt(BigInteger.valueOf(10)),
                    ConstantOperator.createLargeInt(BigInteger.valueOf(20)),
                    ConstantOperator.createLargeInt(BigInteger.valueOf(30)),
                    ConstantOperator.createLargeInt(BigInteger.valueOf(50)),
                    ConstantOperator.createLargeInt(BigInteger.valueOf(50))))
            .put(FloatType.FLOAT, Lists.newArrayList(ConstantOperator.createFloat(10),
                    ConstantOperator.createFloat(20), ConstantOperator.createFloat(30),
                    ConstantOperator.createFloat(50), ConstantOperator.createFloat(50)))
            .put(FloatType.DOUBLE, Lists.newArrayList(ConstantOperator.createDouble(10),
                    ConstantOperator.createDouble(20), ConstantOperator.createDouble(30),
                    ConstantOperator.createDouble(50), ConstantOperator.createDouble(50)))
            .put(DateType.DATE, Lists.newArrayList(ConstantOperator.createDate(LocalDateTime.of(2023, 9, 10, 0, 0)),
                    ConstantOperator.createDate(LocalDateTime.of(2023, 9, 11, 0, 0)),
                    ConstantOperator.createDate(LocalDateTime.of(2023, 9, 12, 0, 0)),
                    ConstantOperator.createDate(LocalDateTime.of(2023, 9, 15, 0, 0)),
                    ConstantOperator.createDate(LocalDateTime.of(2023, 9, 15, 0, 0))))
            .put(DateType.DATETIME, Lists.newArrayList(ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 10, 0, 0)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 11, 0, 0)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 12, 0, 0)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 15, 0, 0)),
                    ConstantOperator.createDatetime(LocalDateTime.of(2023, 9, 15, 0, 0))))
            .build();

    @Test
    public void testMergeKeepsCompoundChildrenAndColumnOrder() {
        for (CompoundPredicateOperator.CompoundType type : List.of(
                CompoundPredicateOperator.CompoundType.AND, CompoundPredicateOperator.CompoundType.OR)) {
            CompoundPredicateOperator.CompoundType nestedType = type == CompoundPredicateOperator.CompoundType.AND
                    ? CompoundPredicateOperator.CompoundType.OR : CompoundPredicateOperator.CompoundType.AND;
            List<ScalarOperator> columns = new ArrayList<>();
            for (int i = 1; i <= 32; i++) {
                columns.add(BinaryPredicateOperator.eq(
                        new ColumnRefOperator(i, IntegerType.INT, "c" + i, false), ConstantOperator.createInt(i)));
            }
            ScalarOperator nested = new CompoundPredicateOperator(nestedType, columns.get(0), columns.get(1));
            List<ScalarOperator> children = new ArrayList<>();
            children.add(columns.get(2));
            children.add(nested);
            children.addAll(columns.subList(3, columns.size()));
            children.add(columns.get(2)); // duplicate column range must merge, not duplicate the output
            ScalarOperator input = new CompoundPredicateOperator(type, children);
            RangePredicate extracted = input.accept(new PredicateExtractor(),
                    new PredicateExtractor.PredicateExtractorContext());
            List<RangePredicate> ranges = extracted.getChildPredicates();
            Assertions.assertEquals(31, ranges.size());
            Assertions.assertEquals(nested, ranges.get(0).toScalarOperator());
            for (int i = 1; i < ranges.size(); i++) {
                Assertions.assertEquals(columns.get(i + 1), ranges.get(i).toScalarOperator());
            }
            Assertions.assertEquals(children, input.getChildren());
        }
    }

    @Test
    public void testAdmissionKeepsResidualAndEqualityClassification() {
        ColumnRefOperator x = new ColumnRefOperator(1, IntegerType.INT, "x", false);
        ColumnRefOperator y = new ColumnRefOperator(2, IntegerType.INT, "y", false);
        ConstantOperator one = ConstantOperator.createInt(1);
        PredicateExtractor extractor = new PredicateExtractor();
        PredicateExtractor.PredicateExtractorContext context = new PredicateExtractor.PredicateExtractorContext();
        ScalarOperator equality = BinaryPredicateOperator.eq(x, y);
        ScalarOperator inequality = BinaryPredicateOperator.gt(x, y);
        Assertions.assertNull(equality.accept(extractor, context));
        Assertions.assertNull(inequality.accept(extractor, context));
        Assertions.assertEquals(List.of(equality), extractor.getColumnEqualityPredicates());
        Assertions.assertEquals(List.of(inequality), extractor.getResidualPredicates());
        for (ScalarOperator predicate : List.of(BinaryPredicateOperator.eq(x, one),
                BinaryPredicateOperator.eq(one, x))) {
            RangePredicate range = predicate.accept(extractor, context);
            Assertions.assertEquals(BinaryPredicateOperator.eq(x, one), range.toScalarOperator());
        }
        CallOperator repeated = new CallOperator("add", IntegerType.INT, List.of(x, x));
        ScalarOperator residual = BinaryPredicateOperator.eq(repeated, one);
        Assertions.assertNull(residual.accept(extractor, context));
        Assertions.assertEquals(List.of(inequality, residual), extractor.getResidualPredicates());
        PredicateExtractor disjunction = new PredicateExtractor();
        Assertions.assertNull(equality.accept(disjunction,
                new PredicateExtractor.PredicateExtractorContext().setAnd(false)));
        Assertions.assertTrue(disjunction.getColumnEqualityPredicates().isEmpty());
        Assertions.assertTrue(disjunction.getResidualPredicates().isEmpty());
    }

    @Test
    public void testSingleRangeThenMergePreservesInput() {
        ColumnRefOperator x = new ColumnRefOperator(1, IntegerType.INT, "x", false);
        ScalarOperator lower = BinaryPredicateOperator.ge(x, ConstantOperator.createInt(1));
        ScalarOperator upper = BinaryPredicateOperator.le(x, ConstantOperator.createInt(3));
        for (CompoundPredicateOperator.CompoundType type : List.of(
                CompoundPredicateOperator.CompoundType.AND, CompoundPredicateOperator.CompoundType.OR)) {
            ScalarOperator singleton = new CompoundPredicateOperator(type, lower);
            PredicateExtractor extractor = new PredicateExtractor();
            RangePredicate range = singleton.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
            Assertions.assertEquals(lower, range.toScalarOperator());
            ScalarOperator combined = new CompoundPredicateOperator(type, lower, upper);
            range = combined.accept(new PredicateExtractor(), new PredicateExtractor.PredicateExtractorContext());
            if (type == CompoundPredicateOperator.CompoundType.AND) {
                Assertions.assertEquals(combined, range.toScalarOperator());
            } else {
                // Existing extractor drops the unbounded column range from the OR wrapper.
                Assertions.assertTrue(range.getChildPredicates().isEmpty());
            }
            Assertions.assertEquals(List.of(lower, upper), combined.getChildren());
        }
    }

    @Test
    public void testRangePredicates() {
        for (Type type : DATA.keySet()) {
            ColumnRefOperator col1 = new ColumnRefOperator(1, type, "col1", false);
            ColumnRefOperator col2 = new ColumnRefOperator(2, type, "col2", false);
            BinaryPredicateOperator binary1 = BinaryPredicateOperator.ge(col1, DATA.get(type).get(0));
            BinaryPredicateOperator binary2 = BinaryPredicateOperator.lt(col2, DATA.get(type).get(1));
            CompoundPredicateOperator compound1 = CompoundPredicateOperator.or(binary1,  binary2).cast();

            BinaryPredicateOperator binary3 = BinaryPredicateOperator.lt(col1, DATA.get(type).get(2));
            BinaryPredicateOperator binary4 = BinaryPredicateOperator.gt(col2, DATA.get(type).get(3));
            CompoundPredicateOperator compound2 = CompoundPredicateOperator.or(binary3,  binary4).cast();
            CompoundPredicateOperator compound3 = CompoundPredicateOperator.or(compound1, compound2).cast();

            PredicateExtractor extractor = new PredicateExtractor();
            RangePredicate rangeOperator = compound3.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
            Assertions.assertTrue(rangeOperator instanceof ColumnRangePredicate);
            ColumnRangePredicate columnRangePredicate1 = (ColumnRangePredicate) rangeOperator;
            Assertions.assertEquals("col2", columnRangePredicate1.getColumnRef().getName());

            BinaryPredicateOperator binary5 = BinaryPredicateOperator.ge(col1, DATA.get(type).get(4));
            CompoundPredicateOperator compound4 = CompoundPredicateOperator.or(binary2,  binary5).cast();
            CompoundPredicateOperator compound5 = CompoundPredicateOperator.or(compound4, compound2).cast();
            RangePredicate rangeOperator2 = compound5.accept(extractor, new PredicateExtractor.PredicateExtractorContext());
            Assertions.assertTrue(rangeOperator2 instanceof OrRangePredicate);
            OrRangePredicate orRangePredicate1 = (OrRangePredicate) rangeOperator2;
            Assertions.assertEquals(2, orRangePredicate1.getChildPredicates().size());
            Assertions.assertTrue(orRangePredicate1.getChildPredicates().get(0) instanceof ColumnRangePredicate);
            Assertions.assertTrue(orRangePredicate1.getChildPredicates().get(1) instanceof ColumnRangePredicate);
        }
    }

    @Test
    public void testColumnPredicate() {
        {
            ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "col1", false);
            Range<ConstantOperator> range1 = Range.atLeast(ConstantOperator.createInt(10));
            TreeRangeSet<ConstantOperator> columnRange1 = TreeRangeSet.create();
            columnRange1.add(range1);
            ColumnRangePredicate columnRangePredicate1 = new ColumnRangePredicate(col1, columnRange1);

            Range<ConstantOperator> range2 = Range.atMost(ConstantOperator.createInt(100));
            TreeRangeSet<ConstantOperator> columnRange2 = TreeRangeSet.create();
            columnRange2.add(range2);
            ColumnRangePredicate columnRangePredicate2 = new ColumnRangePredicate(col1, columnRange2);

            ColumnRangePredicate orRange = ColumnRangePredicate.orRange(columnRangePredicate1, columnRangePredicate2);
            Assertions.assertEquals(ConstantOperator.TRUE, orRange.toScalarOperator());

            ColumnRangePredicate andRange = ColumnRangePredicate.andRange(columnRangePredicate1, columnRangePredicate2);
            Assertions.assertEquals("1: col1 >= 10 AND 1: col1 <= 100", andRange.toScalarOperator().toString());
        }

        {
            ColumnRefOperator col1 = new ColumnRefOperator(1, IntegerType.INT, "col1", false);
            Range<ConstantOperator> range1 = Range.atLeast(ConstantOperator.createInt(100));
            TreeRangeSet<ConstantOperator> columnRange1 = TreeRangeSet.create();
            columnRange1.add(range1);
            ColumnRangePredicate columnRangePredicate1 = new ColumnRangePredicate(col1, columnRange1);

            Range<ConstantOperator> range2 = Range.atMost(ConstantOperator.createInt(10));
            TreeRangeSet<ConstantOperator> columnRange2 = TreeRangeSet.create();
            columnRange2.add(range2);
            ColumnRangePredicate columnRangePredicate2 = new ColumnRangePredicate(col1, columnRange2);

            ColumnRangePredicate andRange = ColumnRangePredicate.andRange(columnRangePredicate1, columnRangePredicate2);
            Assertions.assertEquals(ConstantOperator.FALSE, andRange.toScalarOperator());
        }
    }

    private static RangePredicate extractRange(ScalarOperator predicate) {
        return predicate.accept(new PredicateExtractor(), new PredicateExtractor.PredicateExtractorContext());
    }

    private static ColumnRangePredicate expectedRange(ScalarOperator expression, BinaryType type,
                                                      ConstantOperator value) {
        TreeRangeSet<ConstantOperator> set = TreeRangeSet.create();
        switch (type) {
            case EQ:
                set.add(Range.singleton(value));
                break;
            case GE:
                set.add(Range.atLeast(value));
                break;
            case GT:
                set.add(Range.greaterThan(value));
                break;
            case LE:
                set.add(Range.atMost(value));
                break;
            case LT:
                set.add(Range.lessThan(value));
                break;
            case NE:
                set.add(Range.greaterThan(value));
                set.add(Range.lessThan(value));
                break;
            default:
                throw new IllegalArgumentException(type.toString());
        }
        return new ColumnRangePredicate(expression, set);
    }

    @Test
    public void testFunctionOfColumnAgainstConstantUsesEquivalentOnlyWhenItApplies() {
        ColumnRefOperator dt = new ColumnRefOperator(1, DateType.DATE, "dt", false);
        CallOperator trunc = new CallOperator(FunctionSet.DATE_TRUNC, DateType.DATE,
                List.of(ConstantOperator.createVarchar("month"), dt));
        ConstantOperator monthStart = ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0));
        ConstantOperator midMonth = ConstantOperator.createDate(LocalDateTime.of(2024, 1, 15, 0, 0));
        Assertions.assertTrue(DateTruncEquivalent.INSTANCE.isEquivalent(trunc, monthStart));
        Assertions.assertFalse(DateTruncEquivalent.INSTANCE.isEquivalent(trunc, midMonth));

        // supported types on a month start are rewritten to the plain column
        for (BinaryType type : List.of(BinaryType.GE, BinaryType.GT, BinaryType.LT)) {
            RangePredicate range = extractRange(new BinaryPredicateOperator(type, trunc, monthStart));
            Assertions.assertEquals(expectedRange(dt, type, monthStart), range);
        }
        // not equivalent or not a supported type: the range is built on the function itself
        for (BinaryType type : List.of(BinaryType.EQ, BinaryType.NE, BinaryType.LE, BinaryType.GE)) {
            ConstantOperator bound = type == BinaryType.GE ? midMonth : monthStart;
            RangePredicate range = extractRange(new BinaryPredicateOperator(type, trunc, bound));
            Assertions.assertEquals(expectedRange(trunc, type, bound), range);
        }

        ColumnRefOperator ts = new ColumnRefOperator(2, DateType.DATETIME, "ts", false);
        CallOperator slice = new CallOperator(FunctionSet.TIME_SLICE, DateType.DATETIME,
                List.of(ts, ConstantOperator.createInt(1), ConstantOperator.createVarchar("day"),
                        ConstantOperator.createVarchar("floor")));
        ConstantOperator dayStart = ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 3, 0, 0));
        ConstantOperator midDay = ConstantOperator.createDatetime(LocalDateTime.of(2024, 1, 3, 5, 0));
        Assertions.assertTrue(TimeSliceRewriteEquivalent.INSTANCE.isEquivalent(slice, dayStart));
        Assertions.assertFalse(TimeSliceRewriteEquivalent.INSTANCE.isEquivalent(slice, midDay));
        for (BinaryType type : List.of(BinaryType.EQ, BinaryType.NE, BinaryType.GE, BinaryType.LT)) {
            Assertions.assertEquals(expectedRange(ts, type, dayStart),
                    extractRange(new BinaryPredicateOperator(type, slice, dayStart)));
            Assertions.assertEquals(expectedRange(slice, type, midDay),
                    extractRange(new BinaryPredicateOperator(type, slice, midDay)));
        }
    }

    @Test
    public void testFunctionOfColumnAgainstConstantRefusals() {
        ColumnRefOperator dt = new ColumnRefOperator(1, DateType.DATE, "dt", false);
        CallOperator trunc = new CallOperator(FunctionSet.DATE_TRUNC, DateType.DATE,
                List.of(ConstantOperator.createVarchar("month"), dt));
        ConstantOperator monthStart = ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0));
        // an unsupported binary type is refused the same way whether or not the function is equivalent
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> extractRange(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, trunc, monthStart)));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> extractRange(new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, trunc,
                        ConstantOperator.createDate(LocalDateTime.of(2024, 1, 15, 0, 0)))));
        // a not-equal on a constant that is neither numeric nor date has no range
        CallOperator upper = new CallOperator("upper", StringType.DEFAULT_STRING, List.of(
                new ColumnRefOperator(3, StringType.DEFAULT_STRING, "s", false)));
        Assertions.assertNull(extractRange(new BinaryPredicateOperator(BinaryType.NE, upper,
                ConstantOperator.createVarchar("A"))));
        Assertions.assertNull(extractRange(new BinaryPredicateOperator(BinaryType.NE, trunc,
                ConstantOperator.createVarchar("A"))));
    }
}
