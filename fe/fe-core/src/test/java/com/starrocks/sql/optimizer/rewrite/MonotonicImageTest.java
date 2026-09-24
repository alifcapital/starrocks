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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MonotonicImageTest {

    private final ColumnRefOperator dtCol = new ColumnRefOperator(1, DateType.DATETIME, "ts", true);
    private final ColumnRefOperator epochCol = new ColumnRefOperator(2, IntegerType.BIGINT, "ep", true);
    private ConnectContext ctx;

    @BeforeEach
    public void setUp() {
        ctx = new ConnectContext();
        ctx.getSessionVariable().setTimeZone("+08:00");
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        ConnectContext.remove();
    }

    private MinMax datetimeDomain(LocalDateTime min, LocalDateTime max) {
        return MinMax.of(Range.closed(ConstantOperator.createDatetime(min), ConstantOperator.createDatetime(max)));
    }

    private MinMax epochDomain(long min, long max) {
        return MinMax.of(Range.closed(ConstantOperator.createBigint(min), ConstantOperator.createBigint(max)));
    }

    @Test
    public void testUnixTimestampClampEndpointRefused() {
        Function fn = new Function(new FunctionName("unix_timestamp"),
                new Type[] {DateType.DATETIME}, IntegerType.BIGINT, false);
        CallOperator expr = new CallOperator("unix_timestamp", IntegerType.BIGINT, ImmutableList.of(dtCol), fn);
        // 9999-12-31 23:59:59 at +08:00 is past MAX_UNIX_TIMESTAMP: the fold clamps to 0,
        // the endpoint sits on the clamp plateau and bounds nothing
        assertFalse(MonotonicImage.imageRange(expr, dtCol,
                datetimeDomain(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(9999, 12, 31, 23, 59, 59)))
                .isPresent());
        // a domain inside the valid epoch range has an image
        assertTrue(MonotonicImage.imageRange(expr, dtCol,
                datetimeDomain(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2024, 6, 1, 0, 0)))
                .isPresent());
    }

    @Test
    public void testFromUnixTimeTransitionWindowRefused() {
        Function fn = new Function(new FunctionName("from_unixtime"),
                new Type[] {IntegerType.BIGINT, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        CallOperator expr = new CallOperator("from_unixtime", VarcharType.VARCHAR,
                ImmutableList.of(epochCol, ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")), fn);
        // a transition-free window in a DST zone keeps the image: New York, June 2024
        ctx.getSessionVariable().setTimeZone("America/New_York");
        assertTrue(MonotonicImage.imageRange(expr, epochCol,
                epochDomain(1717200000L, 1719800000L)).isPresent());
        // the same zone across the 2024-11-03 fall-back: the wall clock repeats an hour
        // inside the window and the rendering is not monotonic there
        assertFalse(MonotonicImage.imageRange(expr, epochCol,
                epochDomain(1730592000L, 1730678400L)).isPresent());
        // a fixed-offset zone has no transitions at all
        ctx.getSessionVariable().setTimeZone("+08:00");
        assertTrue(MonotonicImage.imageRange(expr, epochCol,
                epochDomain(1730592000L, 1730678400L)).isPresent());
    }
    @Test
    public void testNestedRenderingCannotHideUnixClamp() {
        Function epochFn = new Function(new FunctionName("unix_timestamp"),
                new Type[] {DateType.DATETIME}, IntegerType.BIGINT, false);
        CallOperator epoch = new CallOperator("unix_timestamp", IntegerType.BIGINT, ImmutableList.of(dtCol), epochFn);
        Function renderFn = new Function(new FunctionName("from_unixtime"),
                new Type[] {IntegerType.BIGINT, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        CallOperator render = new CallOperator("from_unixtime", VarcharType.VARCHAR,
                ImmutableList.of(epoch, ConstantOperator.createVarchar("%Y-%m-%d")), renderFn);
        assertFalse(MonotonicImage.imageRange(render, dtCol,
                datetimeDomain(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(9999, 12, 31, 23, 59, 59)))
                .isPresent());
        // Safe nested calls remain useful; do not disable the whole family to fix the clamp.
        assertTrue(MonotonicImage.imageRange(render, dtCol,
                datetimeDomain(LocalDateTime.of(2024, 1, 1, 0, 0), LocalDateTime.of(2024, 6, 1, 0, 0)))
                .isPresent());
    }

    @Test
    public void testUnixImageRefusesSpringGap() {
        ctx.getSessionVariable().setTimeZone("America/New_York");
        Function epochFn = new Function(new FunctionName("unix_timestamp"),
                new Type[] {DateType.DATETIME}, IntegerType.BIGINT, false);
        CallOperator epoch = new CallOperator("unix_timestamp", IntegerType.BIGINT, ImmutableList.of(dtCol), epochFn);
        assertFalse(MonotonicImage.imageRange(epoch, dtCol,
                datetimeDomain(LocalDateTime.of(2024, 3, 10, 1, 30), LocalDateTime.of(2024, 3, 10, 3, 30)))
                .isPresent());
    }

    private MinMax stringDomain(String lo, String hi) {
        return MinMax.of(Range.closed(ConstantOperator.createVarchar(lo), ConstantOperator.createVarchar(hi)));
    }

    @Test
    public void testCanonicalStringAssertionIsOptIn() {
        ColumnRefOperator text = new ColumnRefOperator(3, VarcharType.VARCHAR, "ds", true);
        CastOperator cast = new CastOperator(DateType.DATE, text);
        MinMax domain = stringDomain("20120701", "20120731");
        assertFalse(ctx.getSessionVariable().isEnableStringDateJoinPruning());
        assertFalse(MonotonicImage.imageRange(cast, text, domain).isPresent());
        ctx.getSessionVariable().setEnableStringDateJoinPruning(true);
        Range<ConstantOperator> image = MonotonicImage.imageRange(cast, text, domain).orElseThrow();
        assertEquals(LocalDateTime.of(2012, 7, 1, 0, 0), image.lowerEndpoint().getDatetime());
        assertEquals(LocalDateTime.of(2012, 7, 31, 0, 0), image.upperEndpoint().getDatetime());
    }

    @Test
    public void testDeclaredStringFormatSupportsMicrosAndRejectsMismatchedEndpoints() {
        ctx.getSessionVariable().setEnableStringDateJoinPruning(true);
        ColumnRefOperator text = new ColumnRefOperator(3, VarcharType.VARCHAR, "ds", true);
        CastOperator cast = new CastOperator(DateType.DATETIME, text);
        MinMax micros = stringDomain("2024-02-29T00:00:00.000001", "2024-03-01T00:00:00.999999");
        assertFalse(MonotonicImage.imageRange(cast, text, micros).isPresent());
        ctx.getSessionVariable().setEnableStringDatePredicatePushdown(true);
        ctx.getSessionVariable().setStringDatePredicateFormat("%Y-%m-%dT%H:%i:%s.%f");
        assertTrue(MonotonicImage.imageRange(cast, text, micros).isPresent());
        assertFalse(MonotonicImage.imageRange(cast, text, stringDomain("20240229", "20240301")).isPresent());
        ctx.getSessionVariable().setEnableStringDateJoinPruning(false);
        assertFalse(MonotonicImage.imageRange(cast, text, micros).isPresent());
    }

    @Test
    public void testCanonicalStringEndpointFormats() {
        ctx.getSessionVariable().setEnableStringDateJoinPruning(true);
        ColumnRefOperator text = new ColumnRefOperator(3, VarcharType.VARCHAR, "ds", true);
        CastOperator cast = new CastOperator(DateType.DATETIME, text);
        for (String[] bounds : new String[][] {
                {"20120701000000", "20120731235959"},
                {"2012-07-01", "2012-07-31"},
                {"2012-07-01 00:00:00", "2012-07-31 23:59:59"},
                {"2012-07-01T00:00:00", "2012-07-31T23:59:59"}}) {
            assertTrue(MonotonicImage.imageRange(cast, text, stringDomain(bounds[0], bounds[1])).isPresent(), bounds[0]);
        }
        for (String[] bounds : new String[][] {
                {"2012072", "2012073"}, {"201207061530", "201207071530"},
                {"2012-07-01", "20120731"}, {"2012-7-01", "2012-7-31"},
                {"20120230", "20120301"}}) {
            assertFalse(MonotonicImage.imageRange(cast, text, stringDomain(bounds[0], bounds[1])).isPresent(), bounds[0]);
        }
    }
}
