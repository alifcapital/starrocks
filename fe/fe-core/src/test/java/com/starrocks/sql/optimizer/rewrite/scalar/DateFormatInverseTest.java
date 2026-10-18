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
import com.starrocks.common.util.DateFormatModel;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicFilterDerivation;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorFunctions;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * date_format(x, format) cmp string inverts into the period of the last field of the format for every format that
 * renders in the order of the dates and renders every row, see DateFormatModel, and for the constants that the format
 * renders. The inverse replaces the condition in the plan, so each case is exact, and each refusal keeps the condition.
 */
public class DateFormatInverseTest {
    private final InvertMonotonicPredicateRule rule = new InvertMonotonicPredicateRule();
    private final ColumnRefOperator ts = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private final ColumnRefOperator epoch = new ColumnRefOperator(2, IntegerType.BIGINT, "ep", true);

    @BeforeEach
    public void setUp() {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setTimeZone("+08:00");
        context.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private ScalarOperator compare(ScalarOperator rendered, BinaryType type, String constant) {
        return new BinaryPredicateOperator(type, rendered, ConstantOperator.createVarchar(constant));
    }

    private ScalarOperator dateFormat(ColumnRefOperator column, String format, BinaryType type, String constant) {
        CallOperator call = new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(column, ConstantOperator.createVarchar(format)));
        return compare(call, type, constant);
    }

    // The format, a string that it renders, and the period of the string
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "%Y|2026|2026-01-01 00:00:00|2027-01-01 00:00:00",
            "%Y%m|202608|2026-08-01 00:00:00|2026-09-01 00:00:00",
            "%Y-%m|2026-08|2026-08-01 00:00:00|2026-09-01 00:00:00",
            "%Y%m%d|20260801|2026-08-01 00:00:00|2026-08-02 00:00:00",
            "%Y-%m-%d|2026-08-01|2026-08-01 00:00:00|2026-08-02 00:00:00",
            "yyyy-MM-dd|2026-08-01|2026-08-01 00:00:00|2026-08-02 00:00:00",
            "yyyyMMdd|20260801|2026-08-01 00:00:00|2026-08-02 00:00:00",
            "%Y-%m-%d %H|2026-08-01 10|2026-08-01 10:00:00|2026-08-01 11:00:00",
            "%Y%m%d%H|2026080110|2026-08-01 10:00:00|2026-08-01 11:00:00",
            "%Y-%m-%d %H:%i|2026-08-01 10:20|2026-08-01 10:20:00|2026-08-01 10:21:00",
            "%Y-%m-%d %H:%i:%s|2026-08-01 10:20:30|2026-08-01 10:20:30|2026-08-01 10:20:31",
            "yyyy-MM-dd HH:mm:ss|2026-08-01 10:20:30|2026-08-01 10:20:30|2026-08-01 10:20:31",
            "%Y-%m-%dT%H:%i:%s|2026-08-01T10:20:30|2026-08-01 10:20:30|2026-08-01 10:20:31",
            "%Y-%m-%dT%H:%i:%S|2026-08-01T10:20:30|2026-08-01 10:20:30|2026-08-01 10:20:31",
            "%Y-01-01 00:00:00|2026-01-01 00:00:00|2026-01-01 00:00:00|2027-01-01 00:00:00",
            "%Y-%m-01 00:00:00|2026-08-01 00:00:00|2026-08-01 00:00:00|2026-09-01 00:00:00",
            "%Y-%m-%d 00:00:00|2026-08-01 00:00:00|2026-08-01 00:00:00|2026-08-02 00:00:00",
            "%Y-%m-%d %H:00:00|2026-08-01 10:00:00|2026-08-01 10:00:00|2026-08-01 11:00:00",
            "%Y-%m-%d %H:%i:00|2026-08-01 10:20:00|2026-08-01 10:20:00|2026-08-01 10:21:00"})
    public void testEveryComparisonOfARenderedStringIsThePeriod(String format, String constant, String from, String to) {
        assertEquals("1: ts >= " + from + " AND 1: ts < " + to,
                rule.apply(dateFormat(ts, format, BinaryType.EQ, constant), null).toString(), "= " + format);
        assertEquals("1: ts >= " + from, rule.apply(dateFormat(ts, format, BinaryType.GE, constant), null).toString(),
                ">= " + format);
        assertEquals("1: ts >= " + to, rule.apply(dateFormat(ts, format, BinaryType.GT, constant), null).toString(),
                "> " + format);
        assertEquals("1: ts < " + from, rule.apply(dateFormat(ts, format, BinaryType.LT, constant), null).toString(),
                "< " + format);
        assertEquals("1: ts < " + to, rule.apply(dateFormat(ts, format, BinaryType.LE, constant), null).toString(),
                "<= " + format);
        assertEquals("1: ts < " + from + " OR 1: ts >= " + to,
                rule.apply(dateFormat(ts, format, BinaryType.NE, constant), null).toString(), "!= " + format);
    }

    // The format and a string that it does not render: a date inside a period, a wrong separator, a wrong width
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "%Y-%m-01 00:00:00|2026-08-15 00:00:00",
            "%Y-%m-01 00:00:00|2026-08-01 00:00:01",
            "%Y-%m-01 00:00:00|2026-08-01",
            "%Y-%m-%d %H:00:00|2026-08-01 10:20:00",
            "%Y-%m-%d %H:%i:00|2026-08-01 10:20:30",
            "%Y-%m-%dT%H:%i:%s|2026-08-01 10:20:30",
            "%Y-%m-%dT%H:%i:%s|2026-8-1T10:20:30",
            "%Y-%m-%dT%H:%i:%s|2026-02-30T10:20:30",
            "%Y%m%d%H|202608011",
            "%Y%m%d%H|2026083124",
            "%Y-%m|2026-13",
            "%Y-%m|2026-08-01"})
    public void testStringThatTheFormatDoesNotRenderKeepsTheCondition(String format, String constant) {
        for (BinaryType type : new BinaryType[] {BinaryType.EQ, BinaryType.GE, BinaryType.LT, BinaryType.NE}) {
            ScalarOperator predicate = dateFormat(ts, format, type, constant);
            assertSame(predicate, rule.apply(predicate, null), type + " " + format + " " + constant);
        }
    }

    // The order of the strings is not the order of the dates, or the formats look so and are not rendered as they are
    @ParameterizedTest
    @CsvSource(delimiter = '|', value = {
            "%d.%m.%Y|01.08.2026",
            "%e%c%Y|182026",
            "%d.%m.%y|01.08.26",
            "%m/%d/%Y %h|08/01/2026 10",
            "%y-%m|26-08",
            "%Y-%c|2026-8",
            "%Y-%M|2026-August",
            "%Y-%j|2026-213",
            "%x|2026",
            "%Y-%m-%d %T|2026-08-01 10:20:30",
            "%Y%m%d%i|202608012",
            "%m|08",
            "year|year",
            "' yyyy-MM-dd'|2026-08-01",
            "'yyyy-MM-dd '|2026-08-01"})
    public void testFormatThatIsNotOrderedKeepsTheCondition(String format, String constant) {
        for (BinaryType type : new BinaryType[] {BinaryType.EQ, BinaryType.GE, BinaryType.LT, BinaryType.NE}) {
            ScalarOperator predicate = dateFormat(ts, format, type, constant);
            assertSame(predicate, rule.apply(predicate, null), type + " " + format);
        }
    }

    // The BE gives NULL for the rows whose string is 127 bytes or longer, and for all rows when the format is 128 bytes
    // or longer: the NULL rows are not in the period, and the condition is an equivalence on every row
    @Test
    public void testFormatThatDoesNotRenderEveryRowKeepsTheCondition() {
        String fits = "%Y" + "x".repeat(122);
        assertEquals("1: ts >= 2026-01-01 00:00:00 AND 1: ts < 2027-01-01 00:00:00",
                rule.apply(dateFormat(ts, fits, BinaryType.EQ, "2026" + "x".repeat(122)), null).toString());
        // the string of 127 bytes, the format of 125
        String longString = "%Y" + "x".repeat(123);
        ScalarOperator predicate = dateFormat(ts, longString, BinaryType.EQ, "2026" + "x".repeat(123));
        assertSame(predicate, rule.apply(predicate, null));
        // the format of 128 bytes
        String longFormat = "%Y" + "x".repeat(126);
        predicate = dateFormat(ts, longFormat, BinaryType.EQ, "2026" + "x".repeat(126));
        assertSame(predicate, rule.apply(predicate, null));
    }

    // A BE of an older version returns the string up to the first zero byte of the format: date_format(ts, '%Y-%m-%d\0x')
    // is '2026-08-01' and never equals '2026-08-01\0x', and date_format(ts, '%Y-%m\0') is '2026-08', which is below
    // '2026-08\0'. A period of ts would return the rows of a day for the first and cut the partition of August for the
    // second, so no condition of such a format is replaced, whatever the constant is, the cut string included.
    @Test
    public void testFormatWithAZeroByteKeepsTheCondition() {
        ColumnRefOperator day = new ColumnRefOperator(3, DateType.DATE, "d", true);
        String[][] cases = {
                {"%Y-%m-%d\0x", "2026-08-01\0x"}, {"%Y-%m-%d\0x", "2026-08-01"}, {"%Y-%m\0", "2026-08\0"},
                {"%Y-%m\0", "2026-08"}, {"\0%Y", "\0" + "2026"}, {"\0%Y", ""}, {"%Y\0-%m", "2026\0-08"},
                {"%Y\0-%m", "2026"}, {"%Y-%m-%d\0", "2026-08-01\0"}, {"%Y-%m-%d\0", "2026-08-01"},
                {"%Y-%m-%d %H:%i:%s\0", "2026-08-01 10:20:30\0"}};
        for (ColumnRefOperator column : new ColumnRefOperator[] {ts, day}) {
            for (String[] row : cases) {
                for (BinaryType type : new BinaryType[] {BinaryType.EQ, BinaryType.NE, BinaryType.GE, BinaryType.GT,
                        BinaryType.LE, BinaryType.LT}) {
                    ScalarOperator predicate = dateFormat(column, row[0], type, row[1]);
                    assertSame(predicate, rule.apply(predicate, null), type + " " + row[0].replace("\0", "\\0") + " "
                            + row[1].replace("\0", "\\0"));
                }
            }
        }
    }

    // from_unixtime cuts the string at the zero byte as well: in +08:00 the epoch 1785542400 renders '2026-08-01', which is
    // below '2026-08-01\0', and a bound ep < 1785513600 would cut that row
    @Test
    public void testEpochRenderingWithAZeroByteHasNoBounds() {
        String[][] cases = {
                {"%Y-%m-%d\0", "2026-08-01\0"}, {"%Y-%m-%d\0", "2026-08-01"}, {"%Y-%m\0x", "2026-08\0x"},
                {"%Y-%m\0x", "2026-08"}, {"\0%Y", "\0" + "2026"}};
        for (String[] row : cases) {
            for (BinaryType type : new BinaryType[] {BinaryType.EQ, BinaryType.GE, BinaryType.GT, BinaryType.LE,
                    BinaryType.LT}) {
                assertNull(scanBounds(fromUnixtime(row[0], type, row[1])),
                        type + " " + row[0].replace("\0", "\\0") + " " + row[1].replace("\0", "\\0"));
            }
        }
    }

    // from_unixtime fails to prepare a constant format of more than 100 bytes. A bound on the epoch could leave the scan
    // with no partition, and then the query would return no rows where the BE returns an error. date_format has no such
    // limit: it renders the format row by row, so its condition is replaced.
    @Test
    public void testFormatOfMoreThan100BytesIsInvertedForDateFormatOnly() {
        String fits = "%Y" + "x".repeat(98);
        String tooLong = "%Y" + "x".repeat(99);
        assertEquals("2: ep >= 1767196800 AND 2: ep < 1798732800",
                scanBounds(fromUnixtime(fits, BinaryType.EQ, "2026" + "x".repeat(98))).toString());
        assertNull(scanBounds(fromUnixtime(tooLong, BinaryType.EQ, "2026" + "x".repeat(99))));
        assertNull(scanBounds(fromUnixtime(tooLong, BinaryType.GE, "2026" + "x".repeat(99))));
        assertEquals("1: ts >= 2026-01-01 00:00:00 AND 1: ts < 2027-01-01 00:00:00",
                rule.apply(dateFormat(ts, tooLong, BinaryType.EQ, "2026" + "x".repeat(99)), null).toString());
        // the limit counts bytes: %Y and 49 characters of two bytes are 100 bytes, and one more character makes 102
        String wide = "%Y" + "é".repeat(49);
        assertNull(scanBounds(fromUnixtime(wide + "é", BinaryType.EQ, "2026" + "é".repeat(50))));
        assertEquals("2: ep >= 1767196800 AND 2: ep < 1798732800",
                scanBounds(fromUnixtime(wide, BinaryType.EQ, "2026" + "é".repeat(49))).toString());
    }

    @ParameterizedTest
    @ValueSource(strings = {"%Y%m%d", "%Y-%m-%d", "yyyyMMdd"})
    public void testDayFormatOfADateIsAPoint(String format) {
        ColumnRefOperator day = new ColumnRefOperator(3, DateType.DATE, "d", true);
        String constant = format.contains("-") ? "2026-08-01" : "20260801";
        assertEquals("3: d = 2026-08-01", rule.apply(dateFormat(day, format, BinaryType.EQ, constant), null).toString());
    }

    // A DATE renders as its midnight, so the fields below the day are zero in every string. A constant with the time of
    // the day at zero is a day, the equality of a constant with another time holds for no date, and a range rounds to the
    // day. The point on the date for a constant with a time would be another condition.
    @ParameterizedTest
    @ValueSource(strings = {"%Y-%m-%d %H:00:00", "%Y-%m-%d %H:%i:%s", "%Y-%m-%d %H", "%Y%m%d%H", "%Y-%m-%dT%H:%i:%s",
            "%Y-%m-%d %H:%i:00"})
    public void testFormatBelowTheDayOnADate(String format) {
        ColumnRefOperator day = new ColumnRefOperator(3, DateType.DATE, "d", true);
        String midnight = render(format, "2026-08-01 00:00:00");
        assertEquals("3: d = 2026-08-01", rule.apply(dateFormat(day, format, BinaryType.EQ, midnight), null).toString());
        assertEquals("3: d != 2026-08-01", rule.apply(dateFormat(day, format, BinaryType.NE, midnight), null).toString());
        assertEquals("3: d >= 2026-08-01", rule.apply(dateFormat(day, format, BinaryType.GE, midnight), null).toString());
        assertEquals("3: d >= 2026-08-02", rule.apply(dateFormat(day, format, BinaryType.GT, midnight), null).toString());
        assertEquals("3: d < 2026-08-01", rule.apply(dateFormat(day, format, BinaryType.LT, midnight), null).toString());
        assertEquals("3: d < 2026-08-02", rule.apply(dateFormat(day, format, BinaryType.LE, midnight), null).toString());

        String morning = render(format, "2026-08-01 05:00:00");
        // the empty interval, which is NULL for a NULL date as the equality is
        assertEquals("3: d >= 2026-08-01 AND 3: d < 2026-08-01",
                rule.apply(dateFormat(day, format, BinaryType.EQ, morning), null).toString());
        ScalarOperator notEqual = dateFormat(day, format, BinaryType.NE, morning);
        assertSame(notEqual, rule.apply(notEqual, null));
        assertEquals("3: d >= 2026-08-02", rule.apply(dateFormat(day, format, BinaryType.GE, morning), null).toString());
        assertEquals("3: d >= 2026-08-02", rule.apply(dateFormat(day, format, BinaryType.GT, morning), null).toString());
        assertEquals("3: d < 2026-08-02", rule.apply(dateFormat(day, format, BinaryType.LE, morning), null).toString());
        assertEquals("3: d < 2026-08-02", rule.apply(dateFormat(day, format, BinaryType.LT, morning), null).toString());
    }

    private static final String[] FORMATS = {"%Y", "%Y%m", "%Y-%m", "%Y%m%d", "%Y-%m-%d", "yyyyMMdd", "%Y-%m-%d %H",
            "%Y%m%d%H", "%Y-%m-%d %H:%i", "%Y-%m-%d %H:%i:%s", "yyyy-MM-dd HH:mm:ss", "%Y-%m-%dT%H:%i:%s",
            "%Y-%m-%dT%H:%i:%S", "%Y-%m-01 00:00:00", "%Y-01-01 00:00:00", "%Y-%m-%d 00:00:00", "%Y-%m-%d %H:00:00",
            "%Y-%m-%d %H:%i:00"};

    private static final String[] SAMPLES = {"2025-12-31 23:59:59", "2026-01-01 00:00:00", "2026-01-01 00:00:01",
            "2026-07-31 23:59:59", "2026-08-01 00:00:00", "2026-08-01 00:00:30", "2026-08-01 05:00:00",
            "2026-08-01 10:19:59", "2026-08-01 10:20:00", "2026-08-01 10:20:30", "2026-08-01 10:20:59",
            "2026-08-01 10:21:00", "2026-08-01 11:00:00", "2026-08-02 00:00:00", "2026-08-31 23:59:59",
            "2026-09-01 00:00:00", "2026-12-31 23:59:59", "2027-01-01 00:00:00"};

    private static boolean compareStrings(String left, BinaryType type, String right) {
        int order = left.compareTo(right);
        return switch (type) {
            case EQ -> order == 0;
            case NE -> order != 0;
            case GE -> order >= 0;
            case GT -> order > 0;
            case LE -> order <= 0;
            case LT -> order < 0;
            default -> throw new IllegalStateException(type.toString());
        };
    }

    private static boolean holds(ScalarOperator predicate, LocalDateTime x) {
        if (predicate instanceof CompoundPredicateOperator compound) {
            boolean left = holds(compound.getChild(0), x);
            boolean right = holds(compound.getChild(1), x);
            return compound.isAnd() ? left && right : left || right;
        }
        BinaryPredicateOperator binary = (BinaryPredicateOperator) predicate;
        int order = x.compareTo(((ConstantOperator) binary.getChild(1)).getDatetime());
        return switch (binary.getBinaryType()) {
            case EQ -> order == 0;
            case NE -> order != 0;
            case GE -> order >= 0;
            case GT -> order > 0;
            case LE -> order <= 0;
            case LT -> order < 0;
            default -> throw new IllegalStateException(binary.getBinaryType().toString());
        };
    }

    // The fold of the FE is the reference: the inverse must hold for exactly the dates for which the comparison of the
    // strings holds, for every format that is admitted, every comparison, a DATETIME and a DATE, and the strings that the
    // format renders for the samples and for their neighbours.
    @Test
    public void testInverseAgreesWithTheComparisonOfTheStringsOnEverySample() {
        int inverted = 0;
        for (String format : FORMATS) {
            for (boolean dateChild : new boolean[] {false, true}) {
                ColumnRefOperator column = dateChild ? new ColumnRefOperator(3, DateType.DATE, "d", true) : ts;
                List<LocalDateTime> xs = new ArrayList<>();
                for (String sample : SAMPLES) {
                    LocalDateTime x = LocalDateTime.parse(sample.replace(' ', 'T'));
                    xs.add(dateChild ? x.toLocalDate().atStartOfDay() : x);
                }
                Set<String> constants = new TreeSet<>();
                for (LocalDateTime x : xs) {
                    constants.add(DateFormatModel.of(format).render(x));
                    constants.add(DateFormatModel.of(format).render(x.plusSeconds(1)));
                    constants.add(DateFormatModel.of(format).render(x.minusSeconds(1)));
                    constants.add(DateFormatModel.of(format).render(x.plusHours(1)));
                    constants.add(DateFormatModel.of(format).render(x.plusDays(1)));
                    constants.add(DateFormatModel.of(format).render(x.plusMonths(1)));
                }
                for (String constant : constants) {
                    for (BinaryType type : new BinaryType[] {BinaryType.EQ, BinaryType.NE, BinaryType.GE, BinaryType.GT,
                            BinaryType.LE, BinaryType.LT}) {
                        ScalarOperator predicate = dateFormat(column, format, type, constant);
                        ScalarOperator result = rule.apply(predicate, null);
                        if (result == predicate) {
                            continue;
                        }
                        inverted++;
                        for (LocalDateTime x : xs) {
                            ConstantOperator value = dateChild ? ConstantOperator.createDate(x)
                                    : ConstantOperator.createDatetime(x);
                            String rendered = ScalarOperatorFunctions.dateFormat(value,
                                    ConstantOperator.createVarchar(format)).getVarchar();
                            assertEquals(compareStrings(rendered, type, constant), holds(result, x),
                                    (dateChild ? "date " : "datetime ") + format + " " + type + " '" + constant + "' at "
                                            + x + " -> " + result);
                        }
                    }
                }
            }
        }
        assertTrue(inverted > 3000, "" + inverted);
    }

    private ScalarOperator fromUnixtime(String format, BinaryType type, String constant) {
        CallOperator call = new CallOperator("from_unixtime", VarcharType.VARCHAR,
                ImmutableList.of(epoch, ConstantOperator.createVarchar(format)));
        return compare(call, type, constant);
    }

    private static ScalarOperator scanBounds(ScalarOperator predicate) {
        ScalarOperator result = MonotonicFilterDerivation.addScanBounds(predicate, unused -> null);
        return result == predicate ? null : Utils.compoundAnd(Utils.extractConjuncts(result).stream()
                .filter(conjunct -> !conjunct.equals(predicate)).toList());
    }

    // The wall clock of a zone that changes it goes back, and a rendering of any field goes back with it: the preimage
    // of a day is an interval of epochs only when no transition is near. The bounds of the scan are necessary conditions
    // for a rendering that can be NULL, so they are the inverse of from_unixtime.
    @Test
    public void testRenderingOfEpochIsRefusedNearATransitionWhateverTheField() {
        ConnectContext.get().getSessionVariable().setTimeZone("America/New_York");
        // 2024-03-10 is the day that starts summer time, and 2024-11-03 the day that ends it
        for (String format : new String[] {"%Y-%m-%d", "%Y-%m-%d %H", "%Y-%m-%d %H:%i:%s", "%Y-%m", "%Y"}) {
            String march = DateFormatInverseTest.render(format, "2024-03-10 03:00:00");
            assertNull(scanBounds(fromUnixtime(format, BinaryType.EQ, march)), format);
            String november = DateFormatInverseTest.render(format, "2024-11-03 03:00:00");
            assertNull(scanBounds(fromUnixtime(format, BinaryType.EQ, november)), format);
        }
        // a day far from the transitions keeps its interval of epochs: 2024-06-15 in New York is UTC-4
        assertEquals("2: ep >= 1718424000 AND 2: ep < 1718510400",
                scanBounds(fromUnixtime("%Y-%m-%d", BinaryType.EQ, "2024-06-15")).toString());
        assertEquals("2: ep >= 1718424000 AND 2: ep < 1718427600",
                scanBounds(fromUnixtime("%Y-%m-%d %H", BinaryType.EQ, "2024-06-15 00")).toString());
        // a zone with no transitions inverts them all
        ConnectContext.get().getSessionVariable().setTimeZone("+08:00");
        assertEquals("2: ep >= 1710000000 AND 2: ep < 1710086400",
                scanBounds(fromUnixtime("%Y-%m-%d", BinaryType.EQ, "2024-03-10")).toString());
        assertEquals("2: ep >= 1709222400 AND 2: ep < 1711900800",
                scanBounds(fromUnixtime("%Y-%m", BinaryType.EQ, "2024-03")).toString());
    }

    // The string that the format renders for a time, cut to the length of the format: the tests need one string per format
    private static String render(String format, String time) {
        return DateFormatModel.of(format).render(LocalDateTime.parse(time.replace(' ', 'T')));
    }
}
