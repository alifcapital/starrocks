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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;

public class StatisticsCastEvaluatorTest {
    private static ConstantOperator cast(ConstantOperator value, Type target) {
        return StatisticsCastEvaluator.cast(value, target).orElseThrow();
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static DecimalType decimal(PrimitiveType type, int precision, int scale) {
        return new DecimalType(type, precision, scale);
    }

    // The value as DATETIME, and its date as DATE: the BE parses both the same way and checks the time for DATE
    private static void assertDatetime(String input, String expected) {
        ConstantOperator datetime = cast(text(input), DateType.DATETIME);
        ConstantOperator date = cast(text(input), DateType.DATE);
        if (expected == null) {
            Assertions.assertTrue(datetime.isNull(), input);
            Assertions.assertTrue(date.isNull(), input);
            return;
        }
        LocalDateTime value = LocalDateTime.parse(expected);
        Assertions.assertEquals(value, datetime.getDatetime(), input);
        Assertions.assertEquals(value.toLocalDate().atStartOfDay(), date.getDate(), input);
    }

    @Test
    public void testBooleanSemanticsFromBeColumnOracle() {
        for (long value : new long[] {-2, -1, 0, 1, 2}) {
            Assertions.assertEquals(value != 0, cast(ConstantOperator.createBigint(value),
                    BooleanType.BOOLEAN).getBoolean());
        }
        for (String value : new String[] {"2", "-2", "true", " 01 ", "+2"}) {
            Assertions.assertTrue(cast(text(value), BooleanType.BOOLEAN).getBoolean());
        }
        for (String value : new String[] {"1.5", "1e3", "2147483648", "bad", ""}) {
            Assertions.assertTrue(cast(text(value), BooleanType.BOOLEAN).isNull());
        }
    }

    // be/test/column/timestamp_value_test.cpp
    @Test
    public void testTimestampValueTestCases() {
        for (String input : new String[] {"4294969322-09-22", "2026-4294967305-22", "2026-09-4294967318",
                "2026-09-22 4294967313:26:26", "2026-09-22 17:4294967322:26", "2026-09-22 17:26:4294967322",
                "2147483648-01-01", "9".repeat(100) + "-01-01"}) {
            assertDatetime(input, null);
        }
        assertDatetime("0000000000002026-00000009-00000022 00000017:00000026:00000026.123456",
                "2026-09-22T17:26:26.123456");
        String[][] valid = {
                {"2026-09-22T17:26:26.679658Z", "2026-09-22T17:26:26.679658"},
                {"2023-12-25 12", "2023-12-25T12:00:00"},
                {"2023-12-25 12:34", "2023-12-25T12:34:00"},
                {"2026-09-22T17:26:26.1Z", "2026-09-22T17:26:26.100"},
                {"2026-09-22T17:26:26.123Z", "2026-09-22T17:26:26.123"},
                {"2026-09-22T17:26:26.1234567Z", "2026-09-22T17:26:26.123456"},
                {" 2026-09-22T17:26:26.679658Z\t", "2026-09-22T17:26:26.679658"},
                {"2026/09/22 17:26:26", "2026-09-22T17:26:26"},
                {"2000-02-29T23:59:59.999999Z", "2000-02-29T23:59:59.999999"},
        };
        for (String[] item : valid) {
            assertDatetime(item[0], item[1]);
        }
        for (String input : new String[] {"2024-01-01 01:61:00", "2024-01-01 24:00:00", "2024-01-01 00:00:60",
                "1900-02-29T00:00:00.000000Z", "2026-00-22T17:26:26.679658Z", "2026-09-00T17:26:26.679658Z",
                "2026-09-31T17:26:26.679658Z"}) {
            assertDatetime(input, null);
        }
        // Fixed width at the ends of the date range, for the three lengths of the fast path of the BE
        for (int year : new int[] {0, 1, 1582, 1900, 1989, 1990, 2000, 2049, 2050, 9999}) {
            for (int month = 1; month <= 12; month++) {
                int last = StatisticsDatetimeParser.daysInMonth(year, month);
                for (int day : new int[] {1, last}) {
                    String full = String.format("%04d-%02d-%02dT23:59:59.999999Z", year, month, day);
                    for (int length : new int[] {19, 26, 27}) {
                        LocalDateTime expected = LocalDateTime.of(year, month, day, 23, 59, 59,
                                length == 19 ? 0 : 999999000);
                        Assertions.assertEquals(expected,
                                cast(text(full.substring(0, length)), DateType.DATETIME).getDatetime());
                    }
                }
            }
        }
    }

    // stringCastDate*, stringCastTimestmap* in be/test/exprs/cast_expr_test.cpp
    @Test
    public void testCastExprTestCases() {
        assertDatetime("2023-12-02", "2023-12-02T00:00:00");
        assertDatetime("   2023-12-02    ", "2023-12-02T00:00:00");
        assertDatetime("2023-12-asdf", null);
        assertDatetime("2022-02-03 11:23:45", "2022-02-03T11:23:45");
        assertDatetime("    2022-02-03 11:23:45 ", "2022-02-03T11:23:45");
        assertDatetime("2022-02-03     11:23:45", "2022-02-03T11:23:45");
        assertDatetime("2022-02-03T11:23:45", "2022-02-03T11:23:45");
        // The time part fails to parse; the date stays
        assertDatetime("2022-02-03 asdfa", "2022-02-03T00:00:00");
        // IntCastToDate, BigIntCastToTimestamp
        Assertions.assertEquals(LocalDateTime.of(2011, 11, 1, 0, 0),
                cast(ConstantOperator.createInt(20111101), DateType.DATE).getDate());
        Assertions.assertEquals(LocalDateTime.of(2022, 2, 3, 11, 23, 45),
                cast(ConstantOperator.createBigint(20220203112345L), DateType.DATETIME).getDatetime());
    }

    @Test
    public void testTextEdges() {
        assertDatetime("2026x08y14", "2026-08-14T00:00:00");
        assertDatetime("2026-08-01abc", "2026-08-01T00:00:00");
        assertDatetime("2026-08-01 1x:00:00", "2026-08-01T01:00:00");
        assertDatetime("2026-08-01 25:00:00", null);
        assertDatetime("0000-02-29", null);
        assertDatetime("1900-02-29", null);
        assertDatetime("2000-02-29", "2000-02-29T00:00:00");
        assertDatetime("260814", "2026-08-14T00:00:00");
        assertDatetime("2608141900", "2026-08-14T19:00:00");
        assertDatetime("20260814T190000", "2026-08-14T19:00:00");
        assertDatetime("2026081419", null);
        assertDatetime("20260814", "2026-08-14T00:00:00");
        assertDatetime("20261301", null);
        assertDatetime("202608", null);
        assertDatetime("2026", null);
        assertDatetime(" 2026-09-22T17:26:26.1Z\t", "2026-09-22T17:26:26.100");
        assertDatetime("2026-09-22T17:26:26.1234567Z", "2026-09-22T17:26:26.123456");
        assertDatetime("2026-10-13T02:5", "2026-10-13T02:05:00");
        assertDatetime("126-08-14", "0126-08-14T00:00:00");
        assertDatetime("2023-12-25 12", "2023-12-25T12:00:00");
        assertDatetime("2026-08-14+05:00", "2026-08-14T00:00:00");
        assertDatetime("2026-08-14 10:00:00+05:00", "2026-08-14T10:00:00");
        assertDatetime("2026-08-14T10:00:00Zjunk", "2026-08-14T10:00:00");
        assertDatetime("2018-08-15T13:16:23.509905Z", "2018-08-15T13:16:23.509905");
        assertDatetime("2026-8-1", "2026-08-01T00:00:00");
        assertDatetime("99-12-31", "1999-12-31T00:00:00");
        assertDatetime("69-12-31", "2069-12-31T00:00:00");
        assertDatetime("70-01-01", "1970-01-01T00:00:00");
        assertDatetime("26-10-13 02:52:5", "2026-10-13T02:52:05");
        assertDatetime("2026 -08-01", null);
        assertDatetime("", null);
        assertDatetime("   ", null);
        assertDatetime("abc", null);
        assertDatetime("-2026-08-01", null);
        // Not ASCII: the bytes the BE sees, and the characters after the date are ignored
        assertDatetime("2026-08-01é", "2026-08-01T00:00:00");
        assertDatetime("2026é08-01", null);
    }

    @Test
    public void testNumberToDate() {
        Object[][] cases = {
                {101L, "2000-01-01T00:00:00"}, {691231L, "2069-12-31T00:00:00"}, {691232L, null},
                {700101L, "1970-01-01T00:00:00"}, {991231L, "1999-12-31T00:00:00"}, {100L, null},
                {9999999L, null}, {10000101L, "1000-01-01T00:00:00"}, {20260814L, "2026-08-14T00:00:00"},
                {99991231L, "9999-12-31T00:00:00"}, {20261301L, null}, {20260230L, null},
                {101000000L, "2000-01-01T00:00:00"}, {260814190000L, "2026-08-14T19:00:00"},
                {991231235959L, "1999-12-31T23:59:59"}, {1000101000000L, "0100-01-01T00:00:00"},
                {20260814190000L, "2026-08-14T19:00:00"}, {20260814250000L, null},
                {99991231235959L, "9999-12-31T23:59:59"}, {100000000000000L, null}, {0L, null}, {-20260814L, null},
        };
        for (Object[] item : cases) {
            long number = (Long) item[0];
            String expected = (String) item[1];
            ConstantOperator datetime = cast(ConstantOperator.createBigint(number), DateType.DATETIME);
            ConstantOperator date = cast(ConstantOperator.createBigint(number), DateType.DATE);
            if (expected == null) {
                Assertions.assertTrue(datetime.isNull(), String.valueOf(number));
                Assertions.assertTrue(date.isNull(), String.valueOf(number));
            } else {
                LocalDateTime value = LocalDateTime.parse(expected);
                Assertions.assertEquals(value, datetime.getDatetime(), String.valueOf(number));
                Assertions.assertEquals(value.toLocalDate().atStartOfDay(), date.getDate(), String.valueOf(number));
            }
        }
        // A double and a decimal lose their fraction toward zero first
        Assertions.assertEquals(LocalDateTime.of(2026, 8, 14, 0, 0),
                cast(ConstantOperator.createDouble(20260814.9), DateType.DATE).getDate());
        Assertions.assertTrue(cast(ConstantOperator.createDouble(-1), DateType.DATE).isNull());
        Assertions.assertTrue(cast(ConstantOperator.createDouble(-0.0), DateType.DATETIME).isNull());
        Assertions.assertTrue(cast(ConstantOperator.createDouble(1e300), DateType.DATETIME).isNull());
        Assertions.assertEquals(LocalDateTime.of(2026, 8, 14, 0, 0), cast(ConstantOperator.createDecimal(
                new BigDecimal("20260814.99"), decimal(PrimitiveType.DECIMAL64, 10, 2)), DateType.DATE).getDate());
        // CAST AS DATETIME reads the low 64 bits of a positive LARGEINT
        BigInteger wide = BigInteger.ONE.shiftLeft(64).add(BigInteger.valueOf(20220203112345L));
        Assertions.assertEquals(LocalDateTime.of(2022, 2, 3, 11, 23, 45),
                cast(ConstantOperator.createLargeInt(wide), DateType.DATETIME).getDatetime());
    }

    @Test
    public void testDateToNumberAndText() {
        ConstantOperator date = ConstantOperator.createDate(LocalDateTime.of(2026, 8, 14, 0, 0));
        ConstantOperator datetime = ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 14, 19, 0, 0, 500000000));
        Assertions.assertEquals(20260814, cast(date, IntegerType.INT).getInt());
        Assertions.assertEquals((byte) 20260814, cast(date, IntegerType.TINYINT).getTinyInt());
        Assertions.assertEquals((short) 20260814, cast(date, IntegerType.SMALLINT).getSmallint());
        Assertions.assertEquals(20260814.0, cast(date, FloatType.DOUBLE).getDouble());
        Assertions.assertEquals(20260814190000L, cast(datetime, IntegerType.BIGINT).getBigint());
        Assertions.assertEquals((int) 20260814190000L, cast(datetime, IntegerType.INT).getInt());
        Assertions.assertEquals((double) (float) 20260814190000.0, cast(datetime, FloatType.FLOAT).getFloat());
        // DECIMAL: the literal times the scale, NULL past the physical integer
        Assertions.assertTrue(cast(datetime, decimal(PrimitiveType.DECIMAL32, 9, 0)).isNull());
        Assertions.assertEquals(new BigDecimal("20260814190000.00000"),
                cast(datetime, decimal(PrimitiveType.DECIMAL64, 18, 5)).getDecimal());
        Assertions.assertTrue(cast(datetime, decimal(PrimitiveType.DECIMAL64, 18, 6)).isNull());
        Assertions.assertEquals(new BigDecimal("20260814.00"), cast(date, decimal(PrimitiveType.DECIMAL32, 9, 2))
                .getDecimal());
        Assertions.assertTrue(cast(ConstantOperator.createDate(LocalDateTime.of(9999, 12, 31, 0, 0)),
                decimal(PrimitiveType.DECIMAL32, 9, 2)).isNull());
        Assertions.assertEquals("2026-08-14 19:00:00.500000", cast(datetime, VarcharType.VARCHAR).getVarchar());
        Assertions.assertEquals("2026-08-14 00:00:00", cast(ConstantOperator.createDatetime(
                LocalDateTime.of(2026, 8, 14, 0, 0)), VarcharType.VARCHAR).getVarchar());
        Assertions.assertEquals("0126-08-14", cast(ConstantOperator.createDate(LocalDateTime.of(126, 8, 14, 0, 0)),
                VarcharType.VARCHAR).getVarchar());
    }

    @Test
    public void testIntegerBoundariesAndScientificNotation() {
        Assertions.assertTrue(cast(ConstantOperator.createDouble(-128.9), IntegerType.TINYINT).isNull());
        Assertions.assertEquals(127, cast(ConstantOperator.createDouble(127.9), IntegerType.TINYINT).getTinyInt());
        Assertions.assertEquals(Long.MIN_VALUE, cast(ConstantOperator.createDouble(-0x1p63), IntegerType.BIGINT)
                .getBigint());
        Assertions.assertEquals(100000000, cast(ConstantOperator.createDouble(1e8), IntegerType.BIGINT).getBigint());
        Assertions.assertEquals(-1, cast(ConstantOperator.createDouble(-1.7), IntegerType.BIGINT).getBigint());
        Assertions.assertEquals(0, cast(ConstantOperator.createDouble(-0.0), IntegerType.INT).getInt());
        Assertions.assertTrue(cast(ConstantOperator.createDouble(1e20), IntegerType.BIGINT).isNull());
        Assertions.assertEquals(2147483647, cast(ConstantOperator.createDouble(2147483647.9), IntegerType.INT)
                .getInt());
        Assertions.assertTrue(cast(ConstantOperator.createDouble(2147483648.0), IntegerType.INT).isNull());
        Assertions.assertTrue(cast(ConstantOperator.createDouble(-2147483648.9), IntegerType.INT).isNull());
        Assertions.assertTrue(cast(ConstantOperator.createInt(128), IntegerType.TINYINT).isNull());
        Assertions.assertEquals(-128, cast(ConstantOperator.createInt(-128), IntegerType.TINYINT).getTinyInt());
        Assertions.assertTrue(cast(ConstantOperator.createInt(-129), IntegerType.TINYINT).isNull());
        Assertions.assertEquals(12, cast(ConstantOperator.createDecimal(new BigDecimal("12.99"),
                decimal(PrimitiveType.DECIMAL64, 10, 2)), IntegerType.INT).getInt());
        Assertions.assertEquals(-12, cast(ConstantOperator.createDecimal(new BigDecimal("-12.99"),
                decimal(PrimitiveType.DECIMAL64, 10, 2)), IntegerType.INT).getInt());
        Assertions.assertTrue(cast(ConstantOperator.createDecimal(new BigDecimal("99999999.99"),
                decimal(PrimitiveType.DECIMAL64, 10, 2)), IntegerType.TINYINT).isNull());
        Assertions.assertEquals(Long.MIN_VALUE, cast(text("-9223372036854775808"), IntegerType.BIGINT).getBigint());
        for (String value : new String[] {"9223372036854775808", "1.5", "1e3", "", "-", "+", "1 2", "- 1"}) {
            Assertions.assertTrue(cast(text(value), IntegerType.BIGINT).isNull(), value);
        }
        Assertions.assertEquals(42, cast(text(" 42 \t"), IntegerType.INT).getInt());
        Assertions.assertEquals(7, cast(text("+7"), IntegerType.INT).getInt());
        Assertions.assertTrue(cast(text("170141183460469231731687303715884105728"), IntegerType.LARGEINT).isNull());
    }

    @Test
    public void testFloatingPoint() {
        Assertions.assertEquals(1.5, cast(text(" 1.5 "), FloatType.DOUBLE).getDouble());
        Assertions.assertEquals(1000.0, cast(text("1e3"), FloatType.DOUBLE).getDouble());
        Assertions.assertEquals(100000.0, cast(text("1e+5"), FloatType.DOUBLE).getDouble());
        Assertions.assertEquals(1.0, cast(text("1."), FloatType.DOUBLE).getDouble());
        Assertions.assertEquals(0.5, cast(text(".5"), FloatType.DOUBLE).getDouble());
        Assertions.assertEquals(-2.0, cast(text("-2"), FloatType.DOUBLE).getDouble());
        for (String value : new String[] {"inf", "-inf", "nan", "Infinity", "1e400", ".", "", "1d", "0x10", "--1",
                "1e", "e5", "1.2.3", "1e5.5", "1 2"}) {
            Assertions.assertTrue(cast(text(value), FloatType.DOUBLE).isNull(), value);
        }
        Assertions.assertTrue(cast(text("1e39"), FloatType.FLOAT).isNull());
        Assertions.assertTrue(StatisticsCastEvaluator.cast(text("1e-400"), FloatType.DOUBLE).isEmpty());
        Assertions.assertTrue(cast(ConstantOperator.createDouble(1e39), FloatType.FLOAT).isNull());
        Assertions.assertEquals((double) Float.MAX_VALUE, cast(ConstantOperator.createDouble(Float.MAX_VALUE),
                FloatType.FLOAT).getFloat());
        Assertions.assertEquals(0x1p53, cast(ConstantOperator.createBigint((1L << 53) + 1), FloatType.DOUBLE)
                .getDouble());
        Assertions.assertEquals(12.99, cast(ConstantOperator.createDecimal(new BigDecimal("12.99"),
                decimal(PrimitiveType.DECIMAL64, 10, 2)), FloatType.DOUBLE).getDouble());
    }

    @Test
    public void testDecimalRoundingAndExtremeExponent() {
        DecimalType type = decimal(PrimitiveType.DECIMAL64, 10, 1);
        Assertions.assertEquals(new BigDecimal("-1.3"), cast(text("-1.25"), type).getDecimal());
        Assertions.assertEquals(new BigDecimal("1.3"), cast(text("1.25"), type).getDecimal());
        Assertions.assertEquals(new BigDecimal("1000.0"), cast(text("1e3"), type).getDecimal());
        Assertions.assertEquals(new BigDecimal("0.0"), cast(text("1e-10000000"), type).getDecimal());
        Assertions.assertEquals(new BigDecimal("0.0"), cast(text("1e-200"), type).getDecimal());
        Assertions.assertEquals(new BigDecimal("0.0"), cast(text("."), type).getDecimal());
        Assertions.assertEquals(new BigDecimal("1.5"), cast(text(" 1.5 "), type).getDecimal());
        for (String value : new String[] {"1e10000000", "1e200", "", "-", "abc", "1.2.3", "1e", "1e 5"}) {
            Assertions.assertTrue(cast(text(value), type).isNull(), value);
        }
        // The physical integer decides, not the declared precision: 18 digits for DECIMAL64
        DecimalType wide = decimal(PrimitiveType.DECIMAL64, 10, 2);
        Assertions.assertEquals(new BigDecimal("1234567890123456.00"),
                cast(text("1234567890123456"), wide).getDecimal());
        Assertions.assertTrue(cast(text("12345678901234567"), wide).isNull());
        // A smaller scale rounds half away from zero
        DecimalType source = decimal(PrimitiveType.DECIMAL64, 10, 3);
        DecimalType target = decimal(PrimitiveType.DECIMAL64, 10, 2);
        Assertions.assertEquals(new BigDecimal("1.24"), cast(ConstantOperator.createDecimal(new BigDecimal("1.235"),
                source), target).getDecimal());
        Assertions.assertEquals(new BigDecimal("-1.24"), cast(ConstantOperator.createDecimal(
                new BigDecimal("-1.235"), source), target).getDecimal());
        Assertions.assertTrue(cast(ConstantOperator.createDecimal(new BigDecimal("3000000000"),
                decimal(PrimitiveType.DECIMAL128, 38, 0)), decimal(PrimitiveType.DECIMAL32, 9, 0)).isNull());
        // From a double the BE rounds in double arithmetic: 100 * 1.005 is 100.49999999999999
        Assertions.assertEquals(new BigDecimal("1.00"), cast(ConstantOperator.createDouble(1.005), target)
                .getDecimal());
        Assertions.assertEquals(new BigDecimal("-1.00"), cast(ConstantOperator.createDouble(-1.005), target)
                .getDecimal());
        Assertions.assertEquals(new BigDecimal("0.13"), cast(ConstantOperator.createDouble(0.125), target)
                .getDecimal());
        Assertions.assertEquals(new BigDecimal("0.10"), cast(ConstantOperator.createFloat(0.1), target)
                .getDecimal());
        Assertions.assertTrue(cast(ConstantOperator.createDouble(1e30), target).isNull());
        Assertions.assertEquals(new BigDecimal("123.00"), cast(ConstantOperator.createInt(123), target)
                .getDecimal());
    }

    @Test
    public void testUnsupportedIsNotSqlNull() {
        Assertions.assertTrue(cast(text("2025-02-29"), DateType.DATE).isNull());
        Assertions.assertFalse(cast(text("2024-02-29"), DateType.DATE).isNull());
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createDouble(1e20),
                VarcharType.VARCHAR).isEmpty());
        Assertions.assertTrue(StatisticsCastEvaluator.cast(ConstantOperator.createBoolean(true),
                DateType.DATE).isEmpty());
        Assertions.assertTrue(cast(ConstantOperator.createNull(DateType.DATE), IntegerType.INT).isNull());
    }

    @Test
    public void testFloatHeadMatchesStoredFloatAndWideningRetainsItsValue() {
        var type = FloatType.FLOAT;
        ConstantOperator number = cast(text("0.1"), type);
        Assertions.assertEquals(MultiColumnJoinMcvEstimator.canonical(type, "0.1"),
                MultiColumnJoinMcvEstimator.canonical(type, number.toString()));
        var source = new com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator(1, type, "f", true);
        var cast = new com.starrocks.sql.optimizer.operator.scalar.CastOperator(FloatType.DOUBLE, source);
        Assertions.assertEquals((double) 0.1f, MultiColumnMcvEstimator.evaluate(cast, source, "0.1").orElseThrow()
                .getDouble());
    }
}
