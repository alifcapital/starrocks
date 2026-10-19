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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BitmapType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import mockit.Expectations;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ScalarOperatorEvaluatorTest {
    @Test
    public void evaluationNotConstant() {
        CallOperator operator = new CallOperator(FunctionSet.IFNULL, IntegerType.INT,
                Lists.newArrayList(new ColumnRefOperator(1, IntegerType.INT, "test", true), ConstantOperator.createInt(2)));

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(result, operator);
    }

    @Test
    public void evaluationNull() {
        CallOperator operator = new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR,
                Lists.newArrayList(ConstantOperator.createVarchar("test"), ConstantOperator.createNull(VarcharType.VARCHAR)));

        Function fn = new Function(new FunctionName(FunctionSet.CONCAT),
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertTrue(((ConstantOperator) result).isNull());
    }

    @Test
    public void evaluationArrayArgs() {
        CallOperator operator = new CallOperator(FunctionSet.CONCAT, VarcharType.VARCHAR,
                Lists.newArrayList(ConstantOperator.createVarchar("test"), ConstantOperator.createVarchar("123")));

        Function fn = new Function(new FunctionName(FunctionSet.CONCAT),
                new Type[] {VarcharType.VARCHAR}, VarcharType.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertEquals("test123", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void evaluationRegexpReplace() {
        CallOperator operator = new CallOperator(FunctionSet.REGEXP_REPLACE, VarcharType.VARCHAR,
                Lists.newArrayList(
                        ConstantOperator.createVarchar("a b c"),
                        ConstantOperator.createVarchar("(b)"),
                        ConstantOperator.createVarchar("<\\1>")));

        Function fn = new Function(new FunctionName(FunctionSet.REGEXP_REPLACE),
                new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);
        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertEquals("a <b> c", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void evaluationRegexpReplaceFallbackToBE() {
        CallOperator operator = new CallOperator(FunctionSet.REGEXP_REPLACE, VarcharType.VARCHAR,
                Lists.newArrayList(
                        ConstantOperator.createVarchar("abcd"),
                        ConstantOperator.createVarchar("(unclosed"),
                        ConstantOperator.createVarchar("xx")));

        Function fn = new Function(new FunctionName(FunctionSet.REGEXP_REPLACE),
                new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);
        assertEquals(operator, result);
    }

    @Test
    public void evaluationRegexpReplaceEmptyPatternFallbackToBE() {
        CallOperator operator = new CallOperator(FunctionSet.REGEXP_REPLACE, VarcharType.VARCHAR,
                Lists.newArrayList(
                        ConstantOperator.createVarchar(""),
                        ConstantOperator.createVarchar(""),
                        ConstantOperator.createVarchar("xx")));

        Function fn = new Function(new FunctionName(FunctionSet.REGEXP_REPLACE),
                new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);
        assertEquals(operator, result);
    }

    @Test
    public void evaluationRegexpReplaceGlobal() {
        CallOperator operator = new CallOperator(FunctionSet.REGEXP_REPLACE, VarcharType.VARCHAR,
                Lists.newArrayList(
                        ConstantOperator.createVarchar("xxxx"),
                        ConstantOperator.createVarchar("xx"),
                        ConstantOperator.createVarchar("-")));

        Function fn = new Function(new FunctionName(FunctionSet.REGEXP_REPLACE),
                new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);
        assertEquals(OperatorType.CONSTANT, result.getOpType());
        assertEquals("--", ((ConstantOperator) result).getVarchar());
    }

    @Test
    public void evaluationFromUtc() {
        CallOperator operator = new CallOperator(FunctionSet.STR_TO_DATE, VarcharType.VARCHAR, Lists.newArrayList(
                ConstantOperator.createVarchar("2003-10-11 23:56:25"),
                ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")
        ));

        Function fn =
                new Function(new FunctionName(FunctionSet.STR_TO_DATE), new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR},
                        DateType.DATETIME,
                        false);

        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);
        assertEquals(LocalDateTime.of(2003, 10, 11, 23, 56, 25), ((ConstantOperator) result).getDatetime());
    }

    private static CallOperator parseCall(String name, Type result, ScalarOperator... arguments) {
        Function fn = new Function(new FunctionName(name), new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR}, result, false);
        return new CallOperator(name, result, Lists.newArrayList(arguments), fn);
    }

    // The BE parses a string by the format of the row. It ignores the white space of the format, and a character that is no
    // specifier must be in the string as it is, so 'yyyy-MM-dd' is a text that no date matches, and it is no date format
    // of these functions. We fear an order that the BE does not give, so the FE reads these formats the same way, and a
    // format that is no constant gives no order.
    @Test
    public void testParseFormatsAreReadAsTheBackendReadsThem() {
        ColumnRefOperator text = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
        for (String name : List.of(FunctionSet.STR_TO_DATE, FunctionSet.STR2DATE)) {
            Type result = FunctionSet.STR2DATE.equals(name) ? DateType.DATE : DateType.DATETIME;
            for (String format : List.of("%Y-%m-%d", "%Y-%m-%d %H:%i:%s", "%Y%m%d", "%Y/%m/%d", " %Y-%m-%d ",
                    "\t%Y-%m-%d\n", "%Y-%m-%d %H:%i:%s.%f", "")) {
                assertTrue(ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(
                        parseCall(name, result, text, ConstantOperator.createVarchar(format))), name + " " + format);
            }
            for (String format : List.of("yyyy-MM-dd", " yyyy-MM-dd ", "yyyyMMdd", "\tyyyyMMdd\n", "yyyy-MM-dd HH:mm:ss",
                    "%m-%d-%Y", "%y-%m-%d")) {
                assertFalse(ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(
                        parseCall(name, result, text, ConstantOperator.createVarchar(format))), name + " " + format);
            }
            // the format of each row, and a NULL format
            ColumnRefOperator formats = new ColumnRefOperator(2, VarcharType.VARCHAR, "Y", true);
            assertFalse(ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(parseCall(name, result, text, formats)), name);
            assertFalse(ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(
                    parseCall(name, result, text, ConstantOperator.createNull(VarcharType.VARCHAR))), name);
        }
    }

    // to_datetime takes a scale, not a format, and the scale of each row gives no order either.
    @Test
    public void testToDatetimeNeedsAConstantScale() {
        Function fn = new Function(new FunctionName(FunctionSet.TO_DATETIME),
                new Type[] {IntegerType.BIGINT, IntegerType.INT}, DateType.DATETIME, false);
        ColumnRefOperator epoch = new ColumnRefOperator(1, IntegerType.BIGINT, "ep", true);
        assertTrue(ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(new CallOperator(FunctionSet.TO_DATETIME,
                DateType.DATETIME, Lists.newArrayList(epoch, ConstantOperator.createInt(3)), fn)));
        ColumnRefOperator scales = new ColumnRefOperator(2, IntegerType.INT, "Y", true);
        assertFalse(ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(new CallOperator(FunctionSet.TO_DATETIME,
                DateType.DATETIME, Lists.newArrayList(epoch, scales), fn)));
    }

    @Test
    public void evaluationNonNullableFunc() {
        CallOperator operator = new CallOperator(FunctionSet.BITMAP_COUNT, IntegerType.BIGINT,
                Lists.newArrayList(ConstantOperator.createNull(BitmapType.BITMAP)));

        Function fn = new Function(new FunctionName(FunctionSet.BITMAP_COUNT),
                new Type[] {BitmapType.BITMAP}, IntegerType.BIGINT, false);
        new Expectations(operator) {
            {
                operator.getFunction();
                result = fn;
            }
        };

        ScalarOperator result = ScalarOperatorEvaluator.INSTANCE.evaluation(operator);

        assertEquals(result, operator);
    }

    @Test
    public void testCreateConstantValue() {
        ConstantOperator tinyInt = ConstantOperator.createExampleValueByType(IntegerType.TINYINT);
        Assertions.assertTrue(tinyInt.getTinyInt() == 1);
        ConstantOperator smallInt = ConstantOperator.createExampleValueByType(IntegerType.SMALLINT);
        Assertions.assertTrue(smallInt.getSmallint() == 1);
        ConstantOperator intValue = ConstantOperator.createExampleValueByType(IntegerType.INT);
        Assertions.assertTrue(intValue.getInt() == 1);
        ConstantOperator bigInt = ConstantOperator.createExampleValueByType(IntegerType.BIGINT);
        Assertions.assertTrue(bigInt.getBigint() == 1L);
        ConstantOperator largeInt = ConstantOperator.createExampleValueByType(IntegerType.LARGEINT);
        Assertions.assertTrue(largeInt.getLargeInt().equals(new BigInteger("1")));
    }

}