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

package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.thrift.TDictQueryExpr;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

// equivalent must not call two expressions of different meaning the same: the Iceberg planning caches and the MV
// rewrite share results between equivalent predicates.
public class ScalarOperatorEquivalentTest {

    @Test
    public void testCaseWhenShape() {
        ColumnRefOperator x = new ColumnRefOperator(1, IntegerType.INT, "x", true);
        ColumnRefOperator y = new ColumnRefOperator(2, IntegerType.INT, "y", true);
        ColumnRefOperator z = new ColumnRefOperator(3, IntegerType.INT, "z", true);
        // CASE x WHEN y THEN z END and CASE WHEN x THEN y ELSE z END have the same arguments x, y, z
        CaseWhenOperator simpleCase = new CaseWhenOperator(IntegerType.INT, x, null, List.of(y, z));
        CaseWhenOperator searchedCase = new CaseWhenOperator(IntegerType.INT, null, z, List.of(x, y));
        Assertions.assertEquals(simpleCase.getChildren(), searchedCase.getChildren());
        Assertions.assertFalse(simpleCase.equivalent(searchedCase));

        ColumnRefOperator otherX = new ColumnRefOperator(11, IntegerType.INT, "x", true);
        ColumnRefOperator otherY = new ColumnRefOperator(12, IntegerType.INT, "y", true);
        ColumnRefOperator otherZ = new ColumnRefOperator(13, IntegerType.INT, "z", true);
        Assertions.assertTrue(simpleCase.equivalent(
                new CaseWhenOperator(IntegerType.INT, otherX, null, List.of(otherY, otherZ))));
    }

    @Test
    public void testIgnoreNulls() {
        ColumnRefOperator x = new ColumnRefOperator(1, IntegerType.INT, "x", true);
        CallOperator ignoreNulls = new CallOperator("first_value", IntegerType.INT, List.of(x));
        ignoreNulls.setIgnoreNulls(true);
        CallOperator keepNulls = new CallOperator("first_value", IntegerType.INT, List.of(x));
        Assertions.assertFalse(ignoreNulls.equivalent(keepNulls));
        Assertions.assertTrue(keepNulls.equivalent(new CallOperator("first_value", IntegerType.INT,
                List.of(new ColumnRefOperator(9, IntegerType.INT, "x", true)))));
    }

    private static DictQueryOperator dictMapping(String db, Map<Long, Long> partitionVersion) {
        TDictQueryExpr expr = new TDictQueryExpr();
        expr.setDb_name(db);
        expr.setTbl_name("dict");
        expr.setPartition_version(partitionVersion);
        expr.setKey_fields(List.of("k"));
        expr.setValue_field("v");
        expr.setStrict_mode(false);
        List<ScalarOperator> arguments = List.of(ConstantOperator.createVarchar("dict"),
                new ColumnRefOperator(1, VarcharType.VARCHAR, "k", true));
        return new DictQueryOperator(arguments, expr, null, IntegerType.BIGINT);
    }

    // `dict_mapping('dict', k)` resolves the database from the session, so equal arguments may name different
    // dictionaries; the partition versions only record the dictionary data at analysis time.
    @Test
    public void testDictQueryDictionary() {
        Assertions.assertFalse(dictMapping("db1", Map.of(1L, 1L)).equivalent(dictMapping("db2", Map.of(1L, 1L))));
        Assertions.assertTrue(dictMapping("db1", Map.of(1L, 1L)).equivalent(dictMapping("db1", Map.of(1L, 2L))));
    }
}
