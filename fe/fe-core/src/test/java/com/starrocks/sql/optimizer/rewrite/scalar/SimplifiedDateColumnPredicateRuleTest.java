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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

public class SimplifiedDateColumnPredicateRuleTest {
    private static final ConstantOperator DATE_BEGIN = ConstantOperator.createVarchar("20240506");
    private static final ConstantOperator DATE_BEGIN2 = ConstantOperator.createVarchar("2024-05-06");

    private final SimplifiedDateColumnPredicateRule rule = new SimplifiedDateColumnPredicateRule();

    @Test
    public void testDateFormat() {
        // date_format shapes are handled by InvertMonotonicPredicateRule; asserted through
        // the full default rule list
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        ScalarOperator dateColumn = new ColumnRefOperator(1, DateType.DATE, "dt", true);
        ScalarOperator datetimeColumn = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);

        // a real analyzer resolves date_format over a DATE column to the DATE signature;
        // without it ImplicitCastRule wraps the column and the point shape is lost
        Function dateFormatDate = new Function(new FunctionName("date_format"),
                new Type[] {DateType.DATE, VarcharType.VARCHAR}, VarcharType.VARCHAR, true);
        ScalarOperator dateCall = new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(dateColumn, ConstantOperator.createVarchar("%Y%m%d")), dateFormatDate);
        // a day period on a DATE column is a point: the equality shape survives
        Assertions.assertEquals("1: dt = 2024-05-06", rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.EQ, dateCall, DATE_BEGIN),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
        Assertions.assertEquals("1: dt >= 2024-05-06", rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.GE, dateCall, DATE_BEGIN),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
        Assertions.assertEquals("1: dt != 2024-05-06", rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.NE, dateCall, DATE_BEGIN),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
        // admission rejections: wrong rendering for the format, wrong length, non-strict
        // values, surrounding whitespace
        for (ConstantOperator bad : new ConstantOperator[] {DATE_BEGIN2,
                ConstantOperator.createVarchar("2024050600"),
                ConstantOperator.createVarchar("20240500"),
                ConstantOperator.createVarchar(" 20240506 ")}) {
            Assertions.assertTrue(rewriter.rewrite(
                    new BinaryPredicateOperator(BinaryType.GT, dateCall, bad),
                    ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString().contains("date_format"));
        }

        ScalarOperator datetimeCall = new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(datetimeColumn, ConstantOperator.createVarchar("%Y%m%d")));
        Assertions.assertEquals("1: dt >= 2024-05-07 00:00:00", rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.GT, datetimeCall, DATE_BEGIN),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
        Assertions.assertEquals("1: dt >= 2024-05-06 00:00:00", rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.GE, datetimeCall, DATE_BEGIN),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
        Assertions.assertEquals("1: dt < 2024-05-06 00:00:00", rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.LT, datetimeCall, DATE_BEGIN),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
        Assertions.assertEquals("1: dt < 2024-05-07 00:00:00", rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.LE, datetimeCall, DATE_BEGIN),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
        // a day period on a DATETIME column is a range, so the equality becomes two bounds
        // and NE becomes the NULL-safe disjunction of the period complement
        Assertions.assertEquals("1: dt >= 2024-05-06 00:00:00 AND 1: dt < 2024-05-07 00:00:00",
                rewriter.rewrite(new BinaryPredicateOperator(BinaryType.EQ, datetimeCall, DATE_BEGIN),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
        Assertions.assertEquals("1: dt < 2024-05-06 00:00:00 OR 1: dt >= 2024-05-07 00:00:00",
                rewriter.rewrite(new BinaryPredicateOperator(BinaryType.NE, datetimeCall, DATE_BEGIN),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());

        // the last day of the DATETIME domain has no next period start: no rewrite, the
        // predicate must not turn into a NULL-comparison
        Assertions.assertTrue(rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.LE, datetimeCall, ConstantOperator.createVarchar("99991231")),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString().contains("date_format"));
        Assertions.assertTrue(rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.GT, datetimeCall, ConstantOperator.createVarchar("99991231")),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString().contains("date_format"));
        Assertions.assertEquals("1: dt < 9999-12-31 00:00:00", rewriter.rewrite(
                new BinaryPredicateOperator(BinaryType.LE, datetimeCall, ConstantOperator.createVarchar("99991230")),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES).toString());
    }

    @Test
    public void testSubstr() {
        for (String fn : new String[] {"substr", "substring"}) {
            {
                // dt is date
                ScalarOperator call = new CallOperator(fn, VarcharType.VARCHAR, ImmutableList.of(
                        new CastOperator(VarcharType.VARCHAR, new ColumnRefOperator(1, DateType.DATE, "dt", true)),
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(10)
                ));
                verifyDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN2));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN));
                verifyDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN2));
            }
            {
                // dt is datetime
                ScalarOperator datetimeColumn = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
                ScalarOperator call = new CallOperator(fn, VarcharType.VARCHAR, ImmutableList.of(
                        new CastOperator(VarcharType.VARCHAR, datetimeColumn),
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(10)
                ));
                verifyNotDateTime(new BinaryPredicateOperator(BinaryType.GT, call, DATE_BEGIN));
                verifyDateTime(new BinaryPredicateOperator(BinaryType.GT, call, DATE_BEGIN2));
                verifyDateTime(new BinaryPredicateOperator(BinaryType.GE, call, DATE_BEGIN2));
                verifyDateTime(new BinaryPredicateOperator(BinaryType.LT, call, DATE_BEGIN2));
                verifyDateTime(new BinaryPredicateOperator(BinaryType.LE, call, DATE_BEGIN2));

                Function func = new Function(new FunctionName(fn),
                        new Type[] {VarcharType.VARCHAR, IntegerType.INT, IntegerType.INT},
                        VarcharType.VARCHAR, true);
                ScalarOperator substringCall = new CallOperator(fn, VarcharType.VARCHAR, ImmutableList.of(
                        datetimeColumn,
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(10)),
                        func
                );

                ScalarOperatorRewriter scalarRewriter = new ScalarOperatorRewriter();
                ScalarOperator result = scalarRewriter.rewrite(
                        new BinaryPredicateOperator(BinaryType.GT, substringCall, DATE_BEGIN2),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                Assertions.assertEquals("1: dt >= 2024-05-07 00:00:00", result.toString());

                result = scalarRewriter.rewrite(new BinaryPredicateOperator(BinaryType.GE, substringCall, DATE_BEGIN2),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                Assertions.assertEquals("1: dt >= 2024-05-06 00:00:00", result.toString());

                result = scalarRewriter.rewrite(new BinaryPredicateOperator(BinaryType.EQ, substringCall, DATE_BEGIN2),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                Assertions.assertEquals(fn + "(cast(1: dt as varchar), 1, 10) = 2024-05-06", result.toString());

                result = scalarRewriter.rewrite(new BinaryPredicateOperator(BinaryType.LE, substringCall, DATE_BEGIN2),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                Assertions.assertEquals("1: dt < 2024-05-07 00:00:00", result.toString());

                result = scalarRewriter.rewrite(new BinaryPredicateOperator(BinaryType.LT, substringCall, DATE_BEGIN2),
                        ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
                Assertions.assertEquals("1: dt < 2024-05-06 00:00:00", result.toString());
            }
            {
                // dt is varchar
                ScalarOperator varcharCall = new CallOperator(fn, VarcharType.VARCHAR, ImmutableList.of(
                        new ColumnRefOperator(1, VarcharType.VARCHAR, "dt", true),
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(10)
                ));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, varcharCall, DATE_BEGIN2));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, varcharCall, DATE_BEGIN2));
                // dt is date, but substr end offset is not 10
                ScalarOperator call = new CallOperator(fn, VarcharType.VARCHAR, ImmutableList.of(
                        new ColumnRefOperator(1, DateType.DATE, "dt", true),
                        ConstantOperator.createInt(1),
                        ConstantOperator.createInt(9)
                ));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, call, DATE_BEGIN2));
                verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, call, DATE_BEGIN2));
            }
        }
    }

    @Test
    public void testReplaceAndSubstr() {
        {
            // dt is date
            ScalarOperator call = new CallOperator(FunctionSet.SUBSTR, VarcharType.VARCHAR, ImmutableList.of(
                    new CastOperator(VarcharType.VARCHAR, new ColumnRefOperator(1, DateType.DATE, "dt", true)),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(10)
            ));
            ScalarOperator replaceCall = new CallOperator(FunctionSet.REPLACE, VarcharType.VARCHAR, ImmutableList.of(
                    call,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("")
            ));
            verifyDate(new BinaryPredicateOperator(BinaryType.EQ, replaceCall, DATE_BEGIN));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN2));
        }
        {
            // dt is varchar
            ScalarOperator varcharCall = new CallOperator(FunctionSet.SUBSTR, VarcharType.VARCHAR, ImmutableList.of(
                    new ColumnRefOperator(1, VarcharType.VARCHAR, "dt", true),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(10)
            ));
            CallOperator replaceCall = new CallOperator(FunctionSet.REPLACE, VarcharType.VARCHAR, ImmutableList.of(
                    varcharCall,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("")
            ));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, replaceCall, DATE_BEGIN2));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN2));
        }
        {
            // dt is date
            ScalarOperator call = new CallOperator(FunctionSet.SUBSTR, VarcharType.VARCHAR, ImmutableList.of(
                    new CastOperator(VarcharType.VARCHAR, new ColumnRefOperator(1, DateType.DATE, "dt", true)),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(10)
            ));
            // not replace '-' to ''
            CallOperator replaceCall = new CallOperator(FunctionSet.REPLACE, VarcharType.VARCHAR, ImmutableList.of(
                    call,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("a")
            ));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, replaceCall, DATE_BEGIN2));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN2));
        }
        {
            // dt is date, but substr end offset is not 10
            ScalarOperator call = new CallOperator(FunctionSet.SUBSTR, VarcharType.VARCHAR, ImmutableList.of(
                    new ColumnRefOperator(1, DateType.DATE, "dt", true),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(9)
            ));
            CallOperator replaceCall = new CallOperator(FunctionSet.REPLACE, VarcharType.VARCHAR, ImmutableList.of(
                    call,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("")
            ));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.EQ, replaceCall, DATE_BEGIN2));
            verifyNotDate(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN2));
        }
        {
            // dt is datetime
            ScalarOperator datetimeColumn = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
            ScalarOperator call = new CallOperator(FunctionSet.SUBSTR, VarcharType.VARCHAR, ImmutableList.of(
                    new CastOperator(VarcharType.VARCHAR, datetimeColumn),
                    ConstantOperator.createInt(1),
                    ConstantOperator.createInt(10)
            ));
            CallOperator replaceCall = new CallOperator(FunctionSet.REPLACE, VarcharType.VARCHAR, ImmutableList.of(
                    call,
                    ConstantOperator.createVarchar("-"),
                    ConstantOperator.createVarchar("")
            ));
            verifyNotDateTime(new BinaryPredicateOperator(BinaryType.GT, replaceCall, DATE_BEGIN2));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.GT, replaceCall, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.GE, replaceCall, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.LT, replaceCall, DATE_BEGIN));
            verifyDateTime(new BinaryPredicateOperator(BinaryType.LE, replaceCall, DATE_BEGIN));
        }
    }

    private void verifyDate(ScalarOperator operator) {
        ScalarOperator result = rule.apply(operator, null);
        assertSame(PrimitiveType.DATE, result.getChild(0).getType().getPrimitiveType());
    }

    private void verifyNotDate(ScalarOperator operator) {
        ScalarOperator result = rule.apply(operator, null);
        assertNotSame(PrimitiveType.DATE, result.getChild(0).getType().getPrimitiveType());
    }

    private void verifyDateTime(ScalarOperator operator) {
        ScalarOperator result = rule.apply(operator, null);
        assertSame(PrimitiveType.DATETIME, result.getChild(0).getType().getPrimitiveType());
    }

    private void verifyNotDateTime(ScalarOperator operator) {
        ScalarOperator result = rule.apply(operator, null);
        assertNotSame(PrimitiveType.DATETIME, result.getChild(0).getType().getPrimitiveType());
    }
}