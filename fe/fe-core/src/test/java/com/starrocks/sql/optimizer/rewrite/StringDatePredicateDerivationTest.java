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

import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StringDatePredicateDerivationTest {
    private TimeZone savedJvmZone;
    private final ColumnRefOperator column = new ColumnRefOperator(1, VarcharType.VARCHAR, "s", true);
    private ConnectContext context;

    @BeforeEach
    public void setUp() {
        savedJvmZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        context = new ConnectContext();
        context.setThreadLocalInfo();
        context.getSessionVariable().setEnableStringDatePredicatePushdown(true);
        context.getSessionVariable().setStringDatePredicateFormat("%Y%m%d");
    }

    @AfterEach
    public void tearDown() {
        TimeZone.setDefault(savedJvmZone);
        ConnectContext.remove();
    }

    private BinaryPredicateOperator predicate(Type type, BinaryType comparison, LocalDateTime value) {
        return new BinaryPredicateOperator(comparison, new CastOperator(type, column),
                type.isDate() ? ConstantOperator.createDate(value) : ConstantOperator.createDatetime(value));
    }

    @Test
    public void testDateEqualityCoversWholeTimestampDay() {
        context.getSessionVariable().setStringDatePredicateFormat("%Y-%m-%dT%H:%i:%s.%f");
        var original = predicate(DateType.DATE, BinaryType.EQ, LocalDateTime.of(2024, 2, 29, 0, 0));
        var bound = StringDatePredicateDerivation.derive(original);
        assertEquals("1: s >= 2024-02-29T00:00:00.000000 AND 1: s < 2024-03-01T00:00:00.000000", bound.toString());
        var combined = MonotonicFilterDerivation.addScanBounds(original);
        assertTrue(Utils.extractConjuncts(combined).contains(original));
        assertSame(combined, MonotonicFilterDerivation.addScanBounds(combined));
    }

    @Test
    public void testDatetimeWithDateOnlyStringsRoundsFractionalBoundary() {
        var value = LocalDateTime.of(2024, 2, 29, 12, 0);
        assertEquals("1: s >= 20240301", StringDatePredicateDerivation.derive(
                predicate(DateType.DATETIME, BinaryType.GE, value)).toString());
        assertEquals("1: s < 20240301", StringDatePredicateDerivation.derive(
                predicate(DateType.DATETIME, BinaryType.LT, value)).toString());
    }

    @Test
    public void testStrictAndInclusiveSecondBoundaries() {
        context.getSessionVariable().setStringDatePredicateFormat("%Y-%m-%d %H:%i:%s");
        LocalDateTime value = LocalDateTime.of(2024, 12, 31, 23, 59, 59, 500000000);
        assertEquals("1: s >= 2025-01-01 00:00:00", StringDatePredicateDerivation.derive(
                predicate(DateType.DATETIME, BinaryType.GE, value)).toString());
        assertEquals("1: s < 2025-01-01 00:00:00", StringDatePredicateDerivation.derive(
                predicate(DateType.DATETIME, BinaryType.LE, value)).toString());
        var equality = predicate(DateType.DATETIME, BinaryType.EQ, value);
        assertSame(equality, StringDatePredicateDerivation.derive(equality));
    }

    @Test
    public void testMaxDateKeepsOnlyRepresentableBound() {
        var original = predicate(DateType.DATE, BinaryType.EQ, LocalDateTime.of(9999, 12, 31, 0, 0));
        assertEquals("1: s >= 99991231", StringDatePredicateDerivation.derive(original).toString());
        var after = predicate(DateType.DATE, BinaryType.GT, LocalDateTime.of(9999, 12, 31, 0, 0));
        assertSame(after, StringDatePredicateDerivation.derive(after));
        var minimum = predicate(DateType.DATE, BinaryType.GE, LocalDateTime.of(0, 1, 1, 0, 0));
        assertEquals("1: s >= 00000101", StringDatePredicateDerivation.derive(minimum).toString());
    }

    @Test
    public void testSettingsAndUnsupportedFormats() {
        var original = predicate(DateType.DATE, BinaryType.EQ, LocalDateTime.of(2024, 3, 1, 0, 0));
        context.getSessionVariable().setEnableStringDatePredicatePushdown(false);
        assertSame(original, MonotonicFilterDerivation.addScanBounds(original));
        context.getSessionVariable().setEnableStringDatePredicatePushdown(true);
        context.getSessionVariable().setStringDatePredicateFormat("");
        assertSame(original, MonotonicFilterDerivation.addScanBounds(original));
        for (String invalid : List.of("%d-%m-%Y", "%y%m%d", "%Y%m", "%Y-%c-%e",
                "%Y-%m-%dT%H:%i:%s%z", "%Y-%m-%dT%H:%i:%s+05:00")) {
            assertThrows(IllegalArgumentException.class,
                    () -> context.getSessionVariable().setStringDatePredicateFormat(invalid));
        }
        context.getSessionVariable().setStringDatePredicateFormat("%Y%m%d");
        context.getSessionVariable().setEnableMonotonicPredicateRewrite(false);
        assertTrue(Utils.extractConjuncts(MonotonicFilterDerivation.addScanBounds(original)).size() > 1);
        ConnectContext.remove();
        assertSame(original, StringDatePredicateDerivation.derive(original));
    }

    @Test
    public void testDoesNotReplaceValueOrNullContext() {
        var original = predicate(DateType.DATE, BinaryType.EQ, LocalDateTime.of(2024, 3, 1, 0, 0));
        var isNull = new IsNullPredicateOperator(false, original);
        assertSame(isNull, MonotonicFilterDerivation.addScanBounds(isNull));
        assertEquals(original, new ScalarOperatorRewriter().rewrite(original.clone(),
                ScalarOperatorRewriter.DEFAULT_REWRITE_RULES));
        var nullSafe = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL,
                new CastOperator(DateType.DATE, column), ConstantOperator.createNull(DateType.DATE));
        assertSame(nullSafe, MonotonicFilterDerivation.addScanBounds(nullSafe));
        var notEqual = predicate(DateType.DATE, BinaryType.NE, LocalDateTime.of(2024, 3, 1, 0, 0));
        assertSame(notEqual, StringDatePredicateDerivation.derive(notEqual));
    }

    @Test
    public void testTimezoneIndependentCastAndTimezoneDependentEpoch() {
        context.getSessionVariable().setStringDatePredicateFormat("%Y-%m-%d %H:%i:%s");
        var cast = new CastOperator(DateType.DATETIME, column);
        for (String zone : List.of("+00:00", "+05:00", "America/New_York", "Europe/Berlin")) {
            context.getSessionVariable().setTimeZone(zone);
            LocalDateTime wall = LocalDateTime.of(2024, 3, 1, 12, 0);
            var direct = predicate(DateType.DATETIME, BinaryType.GE, wall);
            assertEquals("1: s >= 2024-03-01 12:00:00", StringDatePredicateDerivation.derive(direct).toString());
            long seconds = wall.atZone(ZoneId.of(zone)).toEpochSecond();
            var epoch = new BinaryPredicateOperator(BinaryType.EQ,
                    new CallOperator("unix_timestamp", IntegerType.BIGINT, List.of(cast)),
                    ConstantOperator.createBigint(seconds));
            var derived = MonotonicFilterDerivation.addScanBounds(epoch);
            assertTrue(Utils.extractConjuncts(derived).stream().anyMatch(
                    p -> p.toString().equals("1: s >= 2024-03-01 12:00:00")), derived.toString());
            assertTrue(Utils.extractConjuncts(derived).contains(epoch));
        }
        context.getSessionVariable().setTimeZone("America/New_York");
        for (LocalDateTime wall : List.of(LocalDateTime.of(2024, 3, 10, 3, 0),
                LocalDateTime.of(2024, 11, 3, 1, 30))) {
            var epoch = new BinaryPredicateOperator(BinaryType.EQ,
                    new CallOperator("unix_timestamp", IntegerType.BIGINT, List.of(cast)),
                    ConstantOperator.createBigint(wall.atZone(ZoneId.of("America/New_York")).toEpochSecond()));
            assertSame(epoch, MonotonicFilterDerivation.addScanBounds(epoch));
        }
    }

    @ParameterizedTest
    @EnumSource(StringDateFormat.class)
    public void testBoundsAgreeWithCalendarComparisons(StringDateFormat format) {
        context.getSessionVariable().setStringDatePredicateFormat(format.getSqlFormat());
        List<LocalDateTime> values = List.of(
                LocalDateTime.of(0, 1, 1, 0, 0), LocalDateTime.of(1999, 12, 31, 23, 59, 59, 999999000),
                LocalDateTime.of(2024, 2, 28, 23, 59, 59, 999999000), LocalDateTime.of(2024, 2, 29, 0, 0),
                LocalDateTime.of(2024, 2, 29, 12, 30, 0, 1_000), LocalDateTime.of(2024, 3, 1, 0, 0),
                LocalDateTime.of(9999, 12, 31, 23, 59, 59, 999999000));
        for (Type type : List.of(DateType.DATE, DateType.DATETIME)) {
            for (BinaryType comparison : List.of(BinaryType.EQ, BinaryType.EQ_FOR_NULL,
                    BinaryType.GT, BinaryType.GE, BinaryType.LT, BinaryType.LE)) {
                for (LocalDateTime boundary : values) {
                    LocalDateTime constant = type.isDate() ? boundary.truncatedTo(ChronoUnit.DAYS) : boundary;
                    // FE constants stop at the last whole second; BE inputs can contain its microseconds.
                    if (constant.isAfter(ConstantOperator.MAX_DATETIME)) {
                        continue;
                    }
                    var original = predicate(type, comparison, constant);
                    var derived = StringDatePredicateDerivation.derive(original);
                    if (derived == original) {
                        continue;
                    }
                    for (LocalDateTime input : values) {
                        LocalDateTime stored = input.truncatedTo(format.getPrecision());
                        LocalDateTime cast = type.isDate() ? stored.truncatedTo(ChronoUnit.DAYS) : stored;
                        boolean expected = compare(cast.compareTo(constant), comparison);
                        boolean actual = Utils.extractConjuncts(derived).stream().allMatch(p -> compare(
                                format.format(stored).compareTo(((ConstantOperator) p.getChild(1)).getVarchar()),
                                ((BinaryPredicateOperator) p).getBinaryType()));
                        assertEquals(expected, actual, () -> format + " " + type + " " + original + " at " + input);
                    }
                }
            }
        }
    }

    @Test
    public void testUtcSuffixUsesBackendFieldsAndRequiresUtcJvm() {
        var format = StringDateFormat.ISO_DATETIME_MICROS_UTC;
        context.getSessionVariable().setStringDatePredicateFormat(format.getSqlFormat());
        for (String sessionZone : List.of("UTC", "Asia/Dushanbe", "Asia/Shanghai")) {
            context.getSessionVariable().setTimeZone(sessionZone);
            for (String input : List.of("2026-09-24T19:22:17.235962Z", "2026-09-24T19:22:17.827734Z",
                    "2026-09-24T23:59:59.999999Z")) {
                assertTrue(format.matches(input));
                var folded = ConstantOperator.createVarchar(input).castTo(DateType.DATETIME).orElseThrow();
                LocalDateTime fields = LocalDateTime.parse(input.substring(0, input.length() - 1));
                assertEquals(fields, folded.getDatetime());
                var original = predicate(DateType.DATETIME, BinaryType.GE, fields);
                assertEquals("1: s >= " + input, StringDatePredicateDerivation.derive(original).toString());
            }
        }
        var original = predicate(DateType.DATETIME, BinaryType.GE, LocalDateTime.of(2026, 9, 24, 19, 0));
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        assertSame(original, StringDatePredicateDerivation.derive(original));
        TimeZone.setDefault(TimeZone.getTimeZone("Europe/London"));
        assertSame(original, StringDatePredicateDerivation.derive(original));
    }

    private boolean compare(int result, BinaryType comparison) {
        return switch (comparison) {
            case EQ, EQ_FOR_NULL -> result == 0;
            case GT -> result > 0;
            case GE -> result >= 0;
            case LT -> result < 0;
            case LE -> result <= 0;
            default -> throw new AssertionError(comparison);
        };
    }
}
