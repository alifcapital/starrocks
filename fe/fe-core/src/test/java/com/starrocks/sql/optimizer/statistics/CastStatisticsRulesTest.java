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

import com.starrocks.sql.optimizer.statistics.CastStatisticsRules.Order;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CastStatisticsRulesTest {
    private static final Type DECIMAL_10_2 = new DecimalType(PrimitiveType.DECIMAL64, 10, 2);
    private static final Type DECIMAL_12_3 = new DecimalType(PrimitiveType.DECIMAL64, 12, 3);
    private static final Type DECIMAL_9_0 = new DecimalType(PrimitiveType.DECIMAL32, 9, 0);

    private static void assertOrder(Order expected, Type from, Type to) {
        Assertions.assertEquals(expected, CastStatisticsRules.order(from, to), from + " -> " + to);
    }

    @Test
    public void testNumbers() {
        assertOrder(Order.PRESERVED, IntegerType.INT, IntegerType.BIGINT);
        assertOrder(Order.PRESERVED, IntegerType.INT, FloatType.DOUBLE);
        assertOrder(Order.PRESERVED, IntegerType.SMALLINT, FloatType.FLOAT);
        assertOrder(Order.PRESERVED, FloatType.FLOAT, FloatType.DOUBLE);
        assertOrder(Order.PRESERVED, DECIMAL_10_2, DECIMAL_12_3);
        assertOrder(Order.NON_DECREASING, IntegerType.BIGINT, FloatType.DOUBLE);
        assertOrder(Order.NON_DECREASING, IntegerType.LARGEINT, FloatType.DOUBLE);
        assertOrder(Order.NON_DECREASING, IntegerType.INT, FloatType.FLOAT);
        assertOrder(Order.NON_DECREASING, DECIMAL_10_2, FloatType.DOUBLE);
        assertOrder(Order.NON_DECREASING, FloatType.DOUBLE, FloatType.FLOAT);
        assertOrder(Order.STRICT, IntegerType.BIGINT, IntegerType.TINYINT);
        assertOrder(Order.STRICT, IntegerType.BIGINT, DECIMAL_9_0);
        assertOrder(Order.NON_DECREASING, FloatType.DOUBLE, IntegerType.INT);
        assertOrder(Order.NON_DECREASING, DECIMAL_10_2, IntegerType.INT);
        assertOrder(Order.NON_DECREASING, DECIMAL_12_3, DECIMAL_10_2);
        assertOrder(Order.NON_DECREASING, FloatType.DOUBLE, DECIMAL_10_2);
        assertOrder(Order.SEGMENTED, IntegerType.INT, DateType.DATE);
        assertOrder(Order.SEGMENTED, FloatType.DOUBLE, DateType.DATETIME);
        assertOrder(Order.SEGMENTED, DECIMAL_10_2, DateType.DATE);
        assertOrder(Order.NONE, IntegerType.INT, VarcharType.VARCHAR);
        assertOrder(Order.NONE, FloatType.DOUBLE, VarcharType.VARCHAR);
        assertOrder(Order.NONE, DECIMAL_10_2, VarcharType.VARCHAR);
    }

    @Test
    public void testDates() {
        assertOrder(Order.STRICT, DateType.DATE, DateType.DATETIME);
        assertOrder(Order.NON_DECREASING, DateType.DATETIME, DateType.DATE);
        assertOrder(Order.STRICT, DateType.DATE, IntegerType.INT);
        assertOrder(Order.STRICT, DateType.DATE, IntegerType.LARGEINT);
        assertOrder(Order.STRICT, DateType.DATE, FloatType.DOUBLE);
        assertOrder(Order.STRICT, DateType.DATE, DECIMAL_10_2);
        assertOrder(Order.NON_DECREASING, DateType.DATE, FloatType.FLOAT);
        assertOrder(Order.NONE, DateType.DATE, IntegerType.SMALLINT);
        assertOrder(Order.NON_DECREASING, DateType.DATETIME, IntegerType.BIGINT);
        assertOrder(Order.NON_DECREASING, DateType.DATETIME, FloatType.DOUBLE);
        assertOrder(Order.NON_DECREASING, DateType.DATETIME, DECIMAL_9_0);
        assertOrder(Order.NONE, DateType.DATETIME, IntegerType.INT);
        assertOrder(Order.STRICT, DateType.DATE, VarcharType.VARCHAR);
        assertOrder(Order.STRICT, DateType.DATETIME, VarcharType.VARCHAR);
    }

    @Test
    public void testStringsAndOthers() {
        assertOrder(Order.LAYOUT, VarcharType.VARCHAR, DateType.DATE);
        assertOrder(Order.LAYOUT, VarcharType.VARCHAR, DateType.DATETIME);
        assertOrder(Order.NONE, VarcharType.VARCHAR, IntegerType.INT);
        assertOrder(Order.NONE, VarcharType.VARCHAR, FloatType.DOUBLE);
        assertOrder(Order.NONE, VarcharType.VARCHAR, DECIMAL_10_2);
        assertOrder(Order.PRESERVED, TypeFactory.createVarcharType(10), TypeFactory.createVarcharType(5));
        assertOrder(Order.PRESERVED, CharType.CHAR, VarcharType.VARCHAR);
        assertOrder(Order.UNSUPPORTED, BooleanType.BOOLEAN, IntegerType.INT);
        assertOrder(Order.UNSUPPORTED, IntegerType.INT, BooleanType.BOOLEAN);
        assertOrder(Order.UNSUPPORTED, VarcharType.VARCHAR, BooleanType.BOOLEAN);
        assertOrder(Order.UNSUPPORTED, DateType.TIME, IntegerType.INT);
        Assertions.assertTrue(CastStatisticsRules.preservesValues(IntegerType.INT, IntegerType.BIGINT));
        Assertions.assertFalse(CastStatisticsRules.preservesValues(IntegerType.BIGINT, FloatType.DOUBLE));
    }
}
