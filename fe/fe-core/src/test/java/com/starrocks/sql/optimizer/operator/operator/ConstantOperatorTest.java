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

package com.starrocks.sql.optimizer.operator.operator;

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarbinaryType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

public class ConstantOperatorTest {
    @Test
    public void testCastToDateValid() throws Exception {
        String[][] testCases = {
                // YYYY-MM-dd or yy-MM-dd
                {"1997-10-07", "1997-10-07T00:00", "1997-10-07T00:00"},
                {"0097-10-07", "0097-10-07T00:00", "0097-10-07T00:00"},
                {"2020-02-29", "2020-02-29T00:00", "2020-02-29T00:00"}, // Leap year.
                {"97-10-07", "1997-10-07T00:00", "1997-10-07T00:00"},
                {"99-10-07", "1999-10-07T00:00", "1999-10-07T00:00"},
                {"70-10-07", "1970-10-07T00:00", "1970-10-07T00:00"},
                {"69-10-07", "2069-10-07T00:00", "2069-10-07T00:00"},
                {"00-10-07", "2000-10-07T00:00", "2000-10-07T00:00"},
                {"1997-1-07", "1997-01-07T00:00", "1997-01-07T00:00"},
                {"1997-1-7", "1997-01-07T00:00", "1997-01-07T00:00"},
                {"19971117", "1997-11-17T00:00", "1997-11-17T00:00"},

                // YYYY-MM-dd HH:mm:ss or yy-MM-dd HH:mm:ss
                {"1997-10-07 10:11:12", "1997-10-07T00:00", "1997-10-07T10:11:12"},
                {"0097-10-07 10:11:12", "0097-10-07T00:00", "0097-10-07T10:11:12"},
                {"2020-02-29 10:11:12", "2020-02-29T00:00", "2020-02-29T10:11:12"}, // Leap year.
                {"97-10-07 10:11:12", "1997-10-07T00:00", "1997-10-07T10:11:12"},
                {"99-10-07 10:11:12", "1999-10-07T00:00", "1999-10-07T10:11:12"},
                {"70-10-07 10:11:12", "1970-10-07T00:00", "1970-10-07T10:11:12"},
                {"69-10-07 10:11:12", "2069-10-07T00:00", "2069-10-07T10:11:12"},
                {"00-10-07 10:11:12", "2000-10-07T00:00", "2000-10-07T10:11:12"},
                // YYYY-MM-dd HH:mm:ss.SSS
                {"1997-10-07 10:11:12.123", "1997-10-07T00:00", "1997-10-07T10:11:12.123"},
                {"0097-10-07 10:11:12.123", "0097-10-07T00:00", "0097-10-07T10:11:12.123"},
                {"2020-02-29 10:11:12.123", "2020-02-29T00:00", "2020-02-29T10:11:12.123"}, // Leap year.
        };

        for (String[] c : testCases) {
            ConstantOperator in = ConstantOperator.createVarchar(c[0]);
            Assertions.assertEquals(c[1], in.castTo(DateType.DATE).get().getDate().toString());
            Assertions.assertEquals(c[2], in.castTo(DateType.DATETIME).get().getDate().toString());
        }
    }

    @Test
    public void testCastToDateInvalid() {
        String[] testCases = {
                // Invalid year.
                "20190-05-31",
                "20190-05-31 10:11:12",
                "20190-05-31 10:11:12.123",
                "1-05-31",
                "1-05-31 10:11:12",
                "1-05-31 10:11:12.123",
                // Invalid month.
                "2019-16-31",
                "2019-16-31 10:11:12",
                "2019-16-31 10:11:12.123",
                "1997117",
                "199711",
                // Invalid day.
                "2019-02-29",
                "2019-02-29 10:11:12",
                "2019-02-29 10:11:12.123",
                "2019-04-31",
                "2019-04-31 10:11:12",
                "2019-04-31 10:11:12.123",
                "2019-05-32",
                "2019-05-32 10:11:12",
                "2019-05-32 10:11:12.123",
                // Invalid hour, minute, or second.
                "2019-05-31 25:11:12",
                "2019-05-31 25:11:12.123",
                "2019-05-31 10:61:12",
                "2019-05-31 10:61:12.123",
                "2019-05-31 10:11:61",
                "2019-05-31 10:11:61.123",
                "2019-05-31 10:11:12.1234567",

                // Other invalid formats.
                "2019-05-31-1",
                "2019-05-31-1 10:11:12",
                "2019-05-31-1 10:11:12.123",
                "not-date",
        };
        for (String c : testCases) {
            ConstantOperator in = ConstantOperator.createVarchar(c);
            Assertions.assertFalse(in.castTo(DateType.DATE).isPresent());
            Assertions.assertFalse(in.castTo(DateType.DATETIME).isPresent());
        }
    }

    @Test
    public void testCastDateToNumber() throws Exception {
        ConstantOperator date = ConstantOperator.createDate(LocalDateTime.of(2023, 01, 01, 0, 0));
        ConstantOperator datetime = ConstantOperator.createDatetime(LocalDateTime.of(2023, 01, 01, 0, 0, 0));

        ConstantOperator intNumber = ConstantOperator.createInt(20230101);
        Assertions.assertEquals(intNumber, date.castTo(IntegerType.INT).get());

        ConstantOperator dateBigintNumber = ConstantOperator.createBigint(20230101L);
        Assertions.assertEquals(dateBigintNumber, date.castTo(IntegerType.BIGINT).get());

        ConstantOperator datetimeBigintNumber = ConstantOperator.createBigint(20230101000000L);
        Assertions.assertEquals(datetimeBigintNumber, datetime.castTo(IntegerType.BIGINT).get());

        ConstantOperator dateLargeintNumber = ConstantOperator.createLargeInt(new BigInteger("20230101"));
        Assertions.assertEquals(dateLargeintNumber, date.castTo(IntegerType.LARGEINT).get());

        ConstantOperator datetimeLargeintNumber = ConstantOperator.createLargeInt(new BigInteger("20230101000000"));
        Assertions.assertEquals(datetimeLargeintNumber, datetime.castTo(IntegerType.LARGEINT).get());

        ConstantOperator dateFloatNumber = ConstantOperator.createFloat(20230101);
        Assertions.assertEquals(dateFloatNumber, date.castTo(FloatType.FLOAT).get());

        ConstantOperator datetimeFloatNumber = ConstantOperator.createFloat(20230101000000L);
        Assertions.assertEquals(datetimeFloatNumber, datetime.castTo(FloatType.FLOAT).get());

        ConstantOperator dateDoubleNumber = ConstantOperator.createDouble(20230101);
        Assertions.assertEquals(dateDoubleNumber, date.castTo(FloatType.DOUBLE).get());

        ConstantOperator datetimeDoubleNumber = ConstantOperator.createDouble(20230101000000L);
        Assertions.assertEquals(datetimeDoubleNumber, datetime.castTo(FloatType.DOUBLE).get());
    }

    @Test
    public void testCastTimeToDateTime() {
        LocalDateTime now = LocalDateTime.now().withNano(0);
        ConstantOperator time = ConstantOperator.createTime(now.getHour() * 3600D + now.getMinute() * 60D + now.getSecond());
        ConstantOperator datetime = ConstantOperator.createDatetime(now);
        Assertions.assertEquals(datetime, time.castTo(DateType.DATETIME).get());
    }

    @Test
    public void testCastBinaryToStringKeepsTheBytes() {
        byte[] bytes = "zz".getBytes(StandardCharsets.UTF_8);
        ConstantOperator binary = ConstantOperator.createBinary(bytes, VarbinaryType.VARBINARY);

        // The BE casts binary to string by handing the underlying bytes over untouched, folding must agree.
        String folded = binary.castTo(VarcharType.VARCHAR).get().getVarchar();
        Assertions.assertEquals("zz", folded);
        Assertions.assertFalse(folded.startsWith("[B@"), folded);

        // The folded value used to be the identity hash code of the byte[], so it differed on every call.
        Assertions.assertEquals(folded, binary.castTo(VarcharType.VARCHAR).get().getVarchar());
        Assertions.assertEquals(binary.castTo(VarcharType.VARCHAR).get(), binary.castTo(VarcharType.VARCHAR).get());

        // An equal but distinct constant, i.e. a different byte[] instance, folds to the same value.
        ConstantOperator otherInstance =
                ConstantOperator.createBinary("zz".getBytes(StandardCharsets.UTF_8), VarbinaryType.VARBINARY);
        Assertions.assertEquals(folded, otherInstance.castTo(VarcharType.VARCHAR).get().getVarchar());

        // CHAR and sized VARCHAR take the same path, the BE does not truncate to the target length either.
        Assertions.assertEquals("zz", binary.castTo(CharType.CHAR).get().getVarchar());
        Assertions.assertEquals("zz", binary.castTo(TypeFactory.createVarcharType(1)).get().getVarchar());
    }

    @Test
    public void testCastBinaryToBinaryKeepsTheBytes() {
        byte[] bytes = "zz".getBytes(StandardCharsets.UTF_8);
        ConstantOperator binary = ConstantOperator.createBinary(bytes, VarbinaryType.VARBINARY);

        Assertions.assertArrayEquals(bytes, binary.castTo(VarbinaryType.VARBINARY).get().getBinary());
        Assertions.assertArrayEquals(bytes, binary.castTo(TypeFactory.createVarbinary(10)).get().getBinary());
    }

    @Test
    public void testCastNonUtf8BinaryToStringIsNotFolded() {
        // 0xFF is not valid UTF-8: decoding it would substitute U+FFFD and the folded constant would no
        // longer carry the bytes the BE would produce, so the cast has to be left to the BE.
        ConstantOperator binary = ConstantOperator.createBinary(new byte[] {(byte) 0xFF}, VarbinaryType.VARBINARY);
        Assertions.assertEquals(Optional.empty(), binary.castTo(VarcharType.VARCHAR));
        Assertions.assertEquals(Optional.empty(), binary.castTo(CharType.CHAR));
    }

    @Test
    public void testCastBinaryToOtherTypesIsNotFolded() {
        ConstantOperator binary =
                ConstantOperator.createBinary("12".getBytes(StandardCharsets.UTF_8), VarbinaryType.VARBINARY);

        // The BE only supports casting a binary value to a string, folding anything else would make the
        // constant path disagree with the non-folded one.
        Assertions.assertEquals(Optional.empty(), binary.castTo(IntegerType.INT));
        Assertions.assertEquals(Optional.empty(), binary.castTo(IntegerType.BIGINT));
        Assertions.assertEquals(Optional.empty(), binary.castTo(FloatType.DOUBLE));
        Assertions.assertEquals(Optional.empty(), binary.castTo(DateType.DATE));

        // A null binary constant used to be folded into the literal string "null".
        Assertions.assertEquals(Optional.empty(),
                ConstantOperator.createNull(VarbinaryType.VARBINARY).castTo(VarcharType.VARCHAR));
    }

    @Test
    public void testDecimalToStringPadsToTheScale() {
        List<ScalarType> types = List.of(
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 0),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL32, 9, 2),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 18, 3),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL128, 38, 10),
                TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL256, 76, 20),
                DecimalType.DECIMALV2);
        String[] values = {"0", "0.0", "-0.50", "1E+3", "244731.20", "244736.106", "-7.5", "1.005",
                "12345678901234567890123456789012345678.12345678901234567890"};
        for (ScalarType type : types) {
            int scale = type.getScalarScale();
            DecimalFormat format = new DecimalFormat((scale == 0 ? "0" : "0.") + "0".repeat(scale),
                    DecimalFormatSymbols.getInstance(Locale.ROOT));
            for (String value : values) {
                BigDecimal decimal = new BigDecimal(value);
                String expected = type.isDecimalV2() ? decimal.stripTrailingZeros().toPlainString()
                        : format.format(decimal);
                Assertions.assertEquals(expected, ConstantOperator.createDecimal(decimal, type).toString(),
                        value + " as " + type);
            }
        }
    }

    @Test
    public void testDistance() {
        {
            // tinyint
            ConstantOperator var1 = ConstantOperator.createTinyInt((byte) 10);
            ConstantOperator var2 = ConstantOperator.createTinyInt((byte) 20);
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // smallint
            ConstantOperator var1 = ConstantOperator.createSmallInt((short) 10);
            ConstantOperator var2 = ConstantOperator.createSmallInt((short) 20);
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // int
            ConstantOperator var1 = ConstantOperator.createInt(10);
            ConstantOperator var2 = ConstantOperator.createInt(20);
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // long
            ConstantOperator var1 = ConstantOperator.createBigint(10);
            ConstantOperator var2 = ConstantOperator.createBigint(20);
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // large int
            ConstantOperator var1 = ConstantOperator.createLargeInt(BigInteger.valueOf(10));
            ConstantOperator var2 = ConstantOperator.createLargeInt(BigInteger.valueOf(20));
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // date
            ConstantOperator var1 = ConstantOperator.createDate(LocalDateTime.of(2023, 10, 5, 0, 0, 0));
            ConstantOperator var2 = ConstantOperator.createDate(LocalDateTime.of(2023, 10, 15, 0, 0, 0));
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }

        {
            // datetime
            ConstantOperator var1 = ConstantOperator.createDatetime(LocalDateTime.of(2023, 10, 5, 0, 0, 0));
            ConstantOperator var2 = ConstantOperator.createDatetime(LocalDateTime.of(2023, 10, 5, 0, 0, 10));
            Assertions.assertEquals(10, var1.distance(var2));
            Assertions.assertEquals(-10, var2.distance(var1));
        }
    }

    private static int expectedHash(Object value, PrimitiveType type, boolean isNull) {
        return 31 * (31 * (31 + Objects.hashCode(value)) + Objects.hashCode(type)) + Boolean.hashCode(isNull);
    }

    @Test
    public void testHashIsKeptAndFollowsTheType() {
        ConstantOperator seven = ConstantOperator.createInt(7);
        int expected = expectedHash(7, PrimitiveType.INT, false);
        Assertions.assertEquals(expected, seven.hashCode());
        Assertions.assertEquals(expected, seven.hashCode());
        Assertions.assertEquals(expected, seven.clone().hashCode());

        // A hash read before the type changes is not kept after it.
        seven.setType(IntegerType.BIGINT);
        Assertions.assertEquals(expectedHash(7, PrimitiveType.BIGINT, false), seven.hashCode());

        // A clone that changes its type leaves the hash of the original as it was.
        ConstantOperator bigint = ConstantOperator.createBigint(7L);
        Assertions.assertEquals(expectedHash(7L, PrimitiveType.BIGINT, false), bigint.hashCode());
        ScalarOperator copy = bigint.clone();
        copy.setType(IntegerType.INT);
        Assertions.assertEquals(expectedHash(7L, PrimitiveType.INT, false), copy.hashCode());
        Assertions.assertEquals(expectedHash(7L, PrimitiveType.BIGINT, false), bigint.hashCode());

        Assertions.assertEquals(expectedHash(null, PrimitiveType.VARCHAR, true),
                ConstantOperator.createNull(VarcharType.VARCHAR).hashCode());
        Assertions.assertEquals(expectedHash("abc", PrimitiveType.VARCHAR, false),
                ConstantOperator.createVarchar("abc").hashCode());
        Assertions.assertEquals(expectedHash("", PrimitiveType.VARCHAR, false),
                ConstantOperator.createVarchar("").hashCode());
        LocalDateTime time = LocalDateTime.of(2026, 10, 5, 1, 2, 3);
        Assertions.assertEquals(expectedHash(time, PrimitiveType.DATETIME, false),
                ConstantOperator.createDatetime(time).hashCode());
    }
}
