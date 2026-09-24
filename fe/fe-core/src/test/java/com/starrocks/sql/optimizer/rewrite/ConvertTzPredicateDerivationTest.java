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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.InvertMonotonicPredicateRule;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConvertTzPredicateDerivationTest {
    private final ColumnRefOperator column = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private ConnectContext context;

    @BeforeEach
    public void setUp() {
        context = new ConnectContext();
        context.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private CallOperator call(String from, String to) {
        return new CallOperator("convert_tz", DateType.DATETIME,
                List.of(column, ConstantOperator.createVarchar(from), ConstantOperator.createVarchar(to)));
    }

    private BinaryPredicateOperator predicate(String from, String to, BinaryType cmp, LocalDateTime target) {
        return new BinaryPredicateOperator(cmp, call(from, to), ConstantOperator.createDatetime(target));
    }

    @Test
    public void testOffsetsAndNamedZonesPreserveComparisonAndFraction() {
        LocalDateTime target = LocalDateTime.of(2024, 3, 1, 0, 0, 0, 235962000);
        for (String[] zones : List.of(new String[] {"+00:00", "+05:00"},
                new String[] {"UTC", "Asia/Dushanbe"}, new String[] {"Etc/UTC", "Asia/Dushanbe"})) {
            for (BinaryType cmp : List.of(BinaryType.EQ, BinaryType.EQ_FOR_NULL, BinaryType.LT,
                    BinaryType.LE, BinaryType.GT, BinaryType.GE)) {
                ScalarOperator bound = ConvertTzPredicateDerivation.invert(call(zones[0], zones[1]), column,
                        cmp, ConstantOperator.createDatetime(target)).orElseThrow();
                assertEquals(cmp, ((BinaryPredicateOperator) bound).getBinaryType());
                assertEquals(target.minusHours(5), ((ConstantOperator) bound.getChild(1)).getDatetime());
            }
        }
        ScalarOperator reverse = ConvertTzPredicateDerivation.invert(call("Asia/Dushanbe", "UTC"), column,
                BinaryType.GE, ConstantOperator.createDatetime(target)).orElseThrow();
        assertEquals(target.plusHours(5), ((ConstantOperator) reverse.getChild(1)).getDatetime());
    }

    @Test
    public void testSessionTimezoneDoesNotChangeExplicitZones() {
        LocalDateTime target = LocalDateTime.of(2024, 3, 1, 0, 0);
        var original = predicate("UTC", "Asia/Dushanbe", BinaryType.GE, target);
        for (String zone : List.of("UTC", "Asia/Dushanbe", "America/New_York")) {
            context.getSessionVariable().setTimeZone(zone);
            var result = MonotonicFilterDerivation.addScanBounds(original);
            assertTrue(Utils.extractConjuncts(result).contains(original));
            assertTrue(result.toString().contains("1: ts >= 2024-02-29 19:00:00"), result.toString());
            assertSame(result, MonotonicFilterDerivation.addScanBounds(result));
        }
    }

    @Test
    public void testDstZonesAwayFromTransitionsUseSeasonalOffset() {
        for (LocalDateTime target : List.of(LocalDateTime.of(2024, 1, 15, 12, 0),
                LocalDateTime.of(2024, 7, 15, 12, 0))) {
            var bound = ConvertTzPredicateDerivation.invert(call("UTC", "America/New_York"), column,
                    BinaryType.GE, ConstantOperator.createDatetime(target)).orElseThrow();
            int hours = target.getMonthValue() == 1 ? 5 : 4;
            assertEquals(target.plusHours(hours), ((ConstantOperator) bound.getChild(1)).getDatetime());
        }
    }

    @Test
    public void testDstGapsOverlapsAndNearbyBoundariesAreDeclined() {
        for (LocalDateTime target : List.of(LocalDateTime.of(2024, 3, 10, 2, 30),
                LocalDateTime.of(2024, 3, 10, 4, 0), LocalDateTime.of(2024, 11, 3, 1, 30),
                LocalDateTime.of(2024, 11, 3, 4, 0))) {
            for (String[] zones : List.of(new String[] {"UTC", "America/New_York"},
                    new String[] {"America/New_York", "UTC"})) {
                var original = predicate(zones[0], zones[1], BinaryType.GE, target);
                assertSame(original, MonotonicFilterDerivation.addScanBounds(original));
            }
        }
        var skippedDay = predicate("UTC", "Pacific/Apia", BinaryType.GE,
                LocalDateTime.of(2011, 12, 30, 12, 0));
        assertSame(skippedDay, MonotonicFilterDerivation.addScanBounds(skippedDay));
    }

    @Test
    public void testDistantTransitionsCannotViolateDerivedComparison() {
        LocalDateTime target = LocalDateTime.of(2024, 7, 15, 12, 0, 0, 235962000);
        for (String[] zones : List.of(new String[] {"UTC", "America/New_York"},
                new String[] {"America/New_York", "Asia/Dushanbe"},
                new String[] {"Pacific/Apia", "UTC"})) {
            for (BinaryType cmp : List.of(BinaryType.EQ, BinaryType.LT, BinaryType.LE, BinaryType.GT, BinaryType.GE)) {
                var bound = ConvertTzPredicateDerivation.invert(call(zones[0], zones[1]), column,
                        cmp, ConstantOperator.createDatetime(target)).orElseThrow();
                LocalDateTime source = ((ConstantOperator) bound.getChild(1)).getDatetime();
                for (LocalDateTime input : List.of(source.minusNanos(1000), source, source.plusNanos(1000),
                        LocalDateTime.of(2024, 3, 10, 2, 30), LocalDateTime.of(2024, 11, 3, 1, 30),
                        LocalDateTime.of(2011, 12, 30, 12, 0))) {
                    LocalDateTime output = input.atZone(ZoneId.of(zones[0]))
                            .withZoneSameInstant(ZoneId.of(zones[1])).toLocalDateTime();
                    assertEquals(compare(output.compareTo(target), cmp), compare(input.compareTo(source), cmp),
                            zones[0] + " -> " + zones[1] + " " + input + " " + cmp);
                }
            }
        }
    }

    @Test
    public void testNullValueContextsInvalidZonesAndOverflowStayUnchanged() {
        LocalDateTime target = LocalDateTime.of(2024, 3, 1, 0, 0);
        var original = predicate("UTC", "Asia/Dushanbe", BinaryType.GE, target);
        assertSame(original, new InvertMonotonicPredicateRule().apply(original, null));
        var isNull = new IsNullPredicateOperator(false, original);
        assertSame(isNull, MonotonicFilterDerivation.addScanBounds(isNull));
        var nullValue = new BinaryPredicateOperator(BinaryType.EQ_FOR_NULL, call("UTC", "Asia/Dushanbe"),
                ConstantOperator.createNull(DateType.DATETIME));
        assertSame(nullValue, MonotonicFilterDerivation.addScanBounds(nullValue));
        var badZone = predicate("UTC", "not/a/zone", BinaryType.GE, target);
        assertSame(badZone, MonotonicFilterDerivation.addScanBounds(badZone));
        var dynamicZone = new CallOperator("convert_tz", DateType.DATETIME, List.of(column,
                ConstantOperator.createVarchar("UTC"), new ColumnRefOperator(2, VarcharType.VARCHAR, "zone", true)));
        var dynamic = new BinaryPredicateOperator(BinaryType.GE, dynamicZone, ConstantOperator.createDatetime(target));
        assertSame(dynamic, MonotonicFilterDerivation.addScanBounds(dynamic));
        var underflow = predicate("UTC", "+05:00", BinaryType.GE, ConstantOperator.MIN_DATETIME);
        assertSame(underflow, MonotonicFilterDerivation.addScanBounds(underflow));
        var overflow = predicate("+05:00", "UTC", BinaryType.LE, ConstantOperator.MAX_DATETIME);
        assertSame(overflow, MonotonicFilterDerivation.addScanBounds(overflow));
        context.getSessionVariable().setEnableMonotonicPredicateRewrite(false);
        assertSame(original, MonotonicFilterDerivation.addScanBounds(original));
    }

    private static boolean compare(int result, BinaryType cmp) {
        return switch (cmp) {
            case EQ -> result == 0;
            case LT -> result < 0;
            case LE -> result <= 0;
            case GT -> result > 0;
            case GE -> result >= 0;
            default -> throw new AssertionError(cmp);
        };
    }
}
