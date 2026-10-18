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
import com.starrocks.common.util.DateFormatModel;
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.TimeZone;

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
    public void testMonthShiftOnDatetimeCoversWholeDays() {
        Function fn = new Function(new FunctionName("months_add"),
                new Type[] {DateType.DATETIME, IntegerType.INT}, DateType.DATETIME, false);
        CallOperator expr = new CallOperator("months_add", DateType.DATETIME,
                ImmutableList.of(dtCol, ConstantOperator.createInt(1)), fn);
        // 2024-01-29 23:00 is inside the domain and maps to 2024-02-29 23:00, above the images
        // of both endpoints (2024-02-29 12:00 and 2024-02-29 00:00)
        Range<ConstantOperator> image = MonotonicImage.imageRange(expr, dtCol,
                datetimeDomain(LocalDateTime.of(2024, 1, 29, 12, 0), LocalDateTime.of(2024, 1, 30, 0, 0))).get();
        assertTrue(image.contains(ConstantOperator.createDatetime(LocalDateTime.of(2024, 2, 29, 23, 0))),
                image.toString());
    }

    @Test
    public void testFloatCastHasNoImage() {
        Function fn = new Function(new FunctionName("date_format"),
                new Type[] {DateType.DATETIME, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        CallOperator rendered = new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(dtCol, ConstantOperator.createVarchar("%Y%m%d")), fn);
        MinMax domain = datetimeDomain(LocalDateTime.of(2024, 3, 5, 0, 0), LocalDateTime.of(2024, 3, 7, 0, 0));
        // FLOAT rounds these 8-digit dates, so we expect no image. DOUBLE holds them exactly.
        assertFalse(MonotonicImage.imageRange(new CastOperator(FloatType.FLOAT, rendered), dtCol, domain)
                .isPresent());
        Range<ConstantOperator> image = MonotonicImage.imageRange(new CastOperator(FloatType.DOUBLE, rendered),
                dtCol, domain).orElseThrow();
        assertEquals(20240305.0, image.lowerEndpoint().getDouble());
        assertEquals(20240307.0, image.upperEndpoint().getDouble());
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

    private static StringDateFormat format(String pattern) {
        return StringDateFormat.parse(pattern);
    }

    @Test
    public void testCanonicalStringNeedsDeclaredFormat() {
        ColumnRefOperator text = new ColumnRefOperator(3, VarcharType.VARCHAR, "ds", true);
        CastOperator cast = new CastOperator(DateType.DATE, text);
        MinMax domain = stringDomain("20120701", "20120731");
        assertFalse(MonotonicImage.imageRange(cast, text, domain).isPresent());
        assertFalse(MonotonicImage.imageRange(cast, text, domain, null).isPresent());
        Range<ConstantOperator> image = MonotonicImage.imageRange(cast, text, domain, format("%Y%m%d")).orElseThrow();
        assertEquals(LocalDateTime.of(2012, 7, 1, 0, 0), image.lowerEndpoint().getDatetime());
        assertEquals(LocalDateTime.of(2012, 7, 31, 0, 0), image.upperEndpoint().getDatetime());
    }

    @Test
    public void testDeclaredStringFormatSupportsMicrosAndRejectsMismatchedEndpoints() {
        ColumnRefOperator text = new ColumnRefOperator(3, VarcharType.VARCHAR, "ds", true);
        CastOperator cast = new CastOperator(DateType.DATETIME, text);
        MinMax micros = stringDomain("2024-02-29T00:00:00.000001", "2024-03-01T00:00:00.999999");
        assertFalse(MonotonicImage.imageRange(cast, text, micros, null).isPresent());
        StringDateFormat declared = format("%Y-%m-%dT%H:%i:%s.%f");
        assertTrue(MonotonicImage.imageRange(cast, text, micros, declared).isPresent());
        assertFalse(MonotonicImage.imageRange(cast, text, stringDomain("20240229", "20240301"), declared).isPresent());
    }

    @Test
    public void testCanonicalStringEndpointFormats() {
        ColumnRefOperator text = new ColumnRefOperator(3, VarcharType.VARCHAR, "ds", true);
        CastOperator cast = new CastOperator(DateType.DATETIME, text);
        for (String[] bounds : new String[][] {
                {"%Y%m%d%H%i%s", "20120701000000", "20120731235959"},
                {"%Y-%m-%d", "2012-07-01", "2012-07-31"},
                {"%Y-%m-%d %H:%i:%s", "2012-07-01 00:00:00", "2012-07-31 23:59:59"},
                {"%Y-%m-%dT%H:%i:%s", "2012-07-01T00:00:00", "2012-07-31T23:59:59"}}) {
            assertTrue(MonotonicImage.imageRange(cast, text, stringDomain(bounds[1], bounds[2]), format(bounds[0]))
                    .isPresent(), bounds[0]);
        }
        for (String[] bounds : new String[][] {
                {"2012072", "2012073"}, {"201207061530", "201207071530"},
                {"2012-07-01", "20120731"}, {"2012-7-01", "2012-7-31"},
                {"20120230", "20120301"}}) {
            assertFalse(MonotonicImage.imageRange(cast, text, stringDomain(bounds[0], bounds[1]), format("%Y%m%d"))
                    .isPresent(), bounds[0]);
        }
    }

    private CallOperator dateFormat(String format) {
        Function fn = new Function(new FunctionName("date_format"),
                new Type[] {DateType.DATETIME, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        return new CallOperator("date_format", VarcharType.VARCHAR,
                ImmutableList.of(dtCol, ConstantOperator.createVarchar(format)), fn);
    }

    // The strings of an ordered format sort as their dates, so the image of a range of dates is the range of the strings of
    // its ends; %S is %s for the BE
    @Test
    public void testOrderedFormatsOfDateFormatHaveAnImage() {
        LocalDateTime min = LocalDateTime.of(2024, 3, 5, 6, 30, 15);
        LocalDateTime max = LocalDateTime.of(2024, 3, 7, 10, 20, 30);
        for (String format : new String[] {"%Y", "%Y-%m", "%Y%m%d", "%Y-%m-%d %H", "%Y%m%d%H", "%Y-%m-%dT%H:%i:%s",
                "%Y-%m-%d %H:%i:%S", "%Y-%m-01 00:00:00", "%Y-%m-%d %H:00:00", "yyyy-MM-dd HH:mm:ss"}) {
            Range<ConstantOperator> image = MonotonicImage.imageRange(dateFormat(format), dtCol,
                    datetimeDomain(min, max)).orElseThrow();
            DateFormatModel model = DateFormatModel.of(format);
            assertEquals(model.render(min), image.lowerEndpoint().getVarchar(), format);
            assertEquals(model.render(max), image.upperEndpoint().getVarchar(), format);
        }
    }

    @Test
    public void testFormatsThatAreNotOrderedHaveNoImage() {
        MinMax domain = datetimeDomain(LocalDateTime.of(2024, 3, 5, 6, 30, 15), LocalDateTime.of(2024, 3, 7, 10, 20, 30));
        for (String format : new String[] {"%d.%m.%Y", "%y-%m", "%Y-%c", "%m/%d/%Y %h", "%e%c%Y", "%Y%m%d%i", "%W", "%Y-%q"}) {
            assertFalse(MonotonicImage.imageRange(dateFormat(format), dtCol, domain).isPresent(), format);
        }
    }

    // The cast of such a string to a date is the date it holds, cut to the period of the format, so the image is the cut of
    // the ends of the range. The BE casts '%Y-%m' and '%Y' to NULL and reads ten digits as a year of two digits, and no
    // order of the dates is left there.
    @Test
    public void testCastToADateOfAFormatThatTheBackendReadsAsThePeriodStartHasAnImage() {
        LocalDateTime min = LocalDateTime.of(2024, 3, 5, 6, 30, 15);
        LocalDateTime max = LocalDateTime.of(2024, 5, 7, 10, 20, 30);
        for (String format : new String[] {"%Y-%m-01 00:00:00", "%Y-%m-%d", "%Y-%m-%d %H:00:00", "%Y-%m-%d %H:%i:00",
                "%Y-%m-%dT%H:%i:%s", "%Y-%m-%d %H:%i:%S"}) {
            DateFormatModel model = DateFormatModel.of(format);
            Range<ConstantOperator> image = MonotonicImage.imageRange(new CastOperator(DateType.DATETIME, dateFormat(format)),
                    dtCol, datetimeDomain(min, max)).orElseThrow(() -> new AssertionError(format));
            assertEquals(StringDateFormat.truncate(min, model.precision()), image.lowerEndpoint().getDatetime(), format);
            assertEquals(StringDateFormat.truncate(max, model.precision()), image.upperEndpoint().getDatetime(), format);
            // the FE may refuse to fold the cast of a string with a time to a DATE; if it folds, it keeps the date
            MonotonicImage.imageRange(new CastOperator(DateType.DATE, dateFormat(format)), dtCol,
                    datetimeDomain(min, max)).ifPresent(days -> {
                        assertEquals(StringDateFormat.truncate(min, model.precision()).toLocalDate(),
                                days.lowerEndpoint().getDatetime().toLocalDate(), format);
                        assertEquals(StringDateFormat.truncate(max, model.precision()).toLocalDate(),
                                days.upperEndpoint().getDatetime().toLocalDate(), format);
                    });
        }
        MinMax domain = datetimeDomain(min, max);
        for (String format : new String[] {"%Y-%m", "%Y", "%Y%m", "%Y%m%d%H", "%Y%m%d%H%i", "%d.%m.%Y", "%y-%m", "%W"}) {
            assertFalse(MonotonicImage.imageRange(new CastOperator(DateType.DATETIME, dateFormat(format)), dtCol, domain)
                    .isPresent(), format);
        }
        // Only the strings of date_format are admitted: from_unixtime writes the same strings, and the cast of them is not
        Function unixtimeFn = new Function(new FunctionName("from_unixtime"),
                new Type[] {IntegerType.BIGINT, VarcharType.VARCHAR}, VarcharType.VARCHAR, false);
        CallOperator unixtime = new CallOperator("from_unixtime", VarcharType.VARCHAR,
                ImmutableList.of(epochCol, ConstantOperator.createVarchar("%Y-%m-%d %H:%i:%s")), unixtimeFn);
        assertFalse(MonotonicImage.imageRange(new CastOperator(DateType.DATETIME, unixtime), epochCol,
                epochDomain(1717200000L, 1719800000L)).isPresent());
    }

    // The BE keeps the fields of a string with a Z, while the FE folds it into the zone of the JVM, so the image of its cast
    // is the range of the strings only where that zone is UTC
    @Test
    public void testCastOfAStringWithAZHasAnImageOnlyInUtc() {
        MinMax domain = datetimeDomain(LocalDateTime.of(2024, 3, 5, 6, 30, 15), LocalDateTime.of(2024, 5, 7, 10, 20, 30));
        CastOperator cast = new CastOperator(DateType.DATETIME, dateFormat("%Y-%m-%dT%H:%i:%sZ"));
        TimeZone saved = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
            assertFalse(MonotonicImage.imageRange(cast, dtCol, domain).isPresent());
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
            assertTrue(MonotonicImage.imageRange(cast, dtCol, domain).isPresent());
        } finally {
            TimeZone.setDefault(saved);
        }
    }

    // A cast of the digits to a number keeps the order where every string fits the type: 8 digits fit an INT and a
    // DOUBLE, 10 digits a BIGINT and a DOUBLE and not an INT, and 20 digits no BIGINT and no DOUBLE
    @Test
    public void testNumericCastOfDigitsKeepsOrderWhereTheDigitsFit() {
        MinMax domain = datetimeDomain(LocalDateTime.of(2024, 3, 5, 6, 30, 15), LocalDateTime.of(2024, 3, 7, 10, 20, 30));
        Object[][] cases = {
                {"%Y%m%d", 8, true, true, true},
                {"%Y%m%d%H", 10, false, true, true},
                {"%Y%m%d%H%i%s", 14, false, true, true},
                {"%Y%m%d%H%i%s%f", 20, false, false, false}};
        for (Object[] row : cases) {
            String format = (String) row[0];
            assertEquals(row[2], MonotonicImage.imageRange(new CastOperator(IntegerType.INT, dateFormat(format)), dtCol,
                    domain).isPresent(), format + " as INT");
            assertEquals(row[3], MonotonicImage.imageRange(new CastOperator(IntegerType.BIGINT, dateFormat(format)),
                    dtCol, domain).isPresent(), format + " as BIGINT");
            assertEquals(row[4], MonotonicImage.imageRange(new CastOperator(FloatType.DOUBLE, dateFormat(format)), dtCol,
                    domain).isPresent(), format + " as DOUBLE");
        }
        // a format with text does not cast to a number at all
        assertFalse(MonotonicImage.imageRange(new CastOperator(IntegerType.BIGINT, dateFormat("%Y-%m-%d")), dtCol, domain)
                .isPresent());
    }
}
