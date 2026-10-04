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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.TableName;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RelationFieldsTest {
    @Test
    public void testStructRelationResolvesLikeAFullScan() {
        TableName t = new TableName("default_catalog", "db", "t");
        TableName u = new TableName("default_catalog", "db", "u");
        StructType inner = new StructType(new ArrayList<>(List.of(new StructField("b", IntegerType.INT))));
        StructType struct = new StructType(new ArrayList<>(List.of(new StructField("a", inner),
                new StructField("Id", IntegerType.INT))));
        // Names that differ only in case, a name with the Kelvin sign that equalsIgnoreCase matches to "k", and
        // a field named like a struct path.
        List<Field> fields = List.of(
                new Field("id", IntegerType.INT, t, null),
                new Field("s", struct, t, null),
                new Field("ID", IntegerType.INT, u, null),
                new Field("k", VarcharType.VARCHAR, t, null),
                new Field("s", struct, u, null),
                new Field("s.a.b", IntegerType.INT, null, null),
                new Field("name", VarcharType.VARCHAR, null, null));
        RelationFields relation = new RelationFields(fields);
        List<List<String>> names = List.of(List.of("id"), List.of("Id"), List.of("t", "id"), List.of("u", "ID"),
                List.of("db", "t", "id"), List.of("default_catalog", "db", "u", "id"), List.of("K"),
                List.of("s"), List.of("t", "s"), List.of("s", "a"), List.of("s", "a", "b"), List.of("t", "s", "a", "b"),
                List.of("db", "t", "s", "a", "b"), List.of("s", "id"), List.of("s.a.b"), List.of("name"),
                List.of("missing"), List.of("x", "id"));
        for (List<String> parts : names) {
            SlotRef slot = new SlotRef(QualifiedName.of(parts));
            List<Field> expected = fields.stream().filter(field -> field.canResolve(slot)).collect(Collectors.toList());
            Assertions.assertEquals(expected, relation.resolveFields(slot), parts.toString());
        }
        Assertions.assertEquals(List.of(fields.get(0), fields.get(2)),
                relation.resolveFields(new SlotRef(QualifiedName.of(List.of("id")))));
        Assertions.assertEquals(List.of(fields.get(1)),
                relation.resolveFields(new SlotRef(QualifiedName.of(List.of("db", "t", "s", "a", "b")))));
        Assertions.assertEquals(List.of(), relation.resolveFields(new SlotRef(QualifiedName.of(List.of("x", "id")))));
    }

    @Test
    public void testStructPathRecordsTheFieldPositions() {
        TableName t = new TableName("default_catalog", "db", "t");
        StructType inner = new StructType(new ArrayList<>(List.of(new StructField("b", IntegerType.INT))));
        StructType struct = new StructType(new ArrayList<>(List.of(new StructField("a", inner),
                new StructField("Id", IntegerType.INT))));
        Field field = new Field("s", struct, t, null);
        Assertions.assertTrue(field.getTmpUsedStructFieldPos().isEmpty());
        Assertions.assertTrue(field.canResolve(new SlotRef(QualifiedName.of(List.of("t", "S", "A", "B")))));
        Assertions.assertEquals(List.of(0, 0), field.getTmpUsedStructFieldPos());
        Assertions.assertTrue(field.canResolve(new SlotRef(QualifiedName.of(List.of("DB", "t", "s", "id")))));
        Assertions.assertEquals(List.of(1), field.getTmpUsedStructFieldPos());
        // Only the table name is case sensitive.
        Assertions.assertFalse(field.canResolve(new SlotRef(QualifiedName.of(List.of("T", "s", "a")))));
        Assertions.assertTrue(field.getTmpUsedStructFieldPos().isEmpty());
    }
}
