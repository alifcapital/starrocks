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
}
