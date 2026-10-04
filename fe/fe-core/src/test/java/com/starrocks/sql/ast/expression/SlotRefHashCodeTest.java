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

package com.starrocks.sql.ast.expression;

import com.google.common.base.Objects;
import com.starrocks.catalog.TableName;
import com.starrocks.planner.SlotId;
import com.starrocks.sql.ast.QualifiedName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

// The planner iterates hash maps keyed by expressions, and that order decides column ref ids in plans.
// We compare SlotRef.hashCode() with the string based formula to make sure the value stays the same.
public class SlotRefHashCodeTest {

    private static final String[] NAMES = {
            null, "", "a", "Z", "abc", "ABC", "MiXeD_Case_123", "t1", "_", "__LAMBDA_TABLE", "0123456789",
            "`", "``", "a`b", "`quoted`", ".", "a.b", "null", "NULL", " ", "with space", "@#$%^&*()[]{}",
            "default_catalog", "DEFAULT_CATALOG", "Default_Catalog", "hive_catalog", "Iceberg",
            "\u0000", "\u007f", "\t\n",
            // Non-ASCII names take the string based path.
            "\u00c0\u00e9", "\u00df", "\u0130stanbul", "I\u0307", // latin letters with marks
            "\u03a3\u0391\u03a3", "\u03c3\u03c2", // greek sigma
            "\u8868\u540d", "\u0414\u0430\u043d\u043d\u044b\u0435", // chinese and cyrillic
            "\ud83d\ude00", "\ud83d", "\u212a", "ABC\u0080", // surrogates, kelvin sign, C1 control
    };

    private static final String[] CATALOG_AND_DB_NAMES = {
            null, "", "default_catalog", "DEFAULT_CATALOG", "Hive_1", "a`b",
            "\u00c0\u00e9", "\u0130stanbul", // non-ASCII names
    };

    private static final String[] LOCALES = {"", "en", "en-US", "de", "fr", "ru", "zh-CN", "tr", "tr-TR", "az", "lt",
            "el", "ja"};

    private static int referenceHashCode(SlotRef ref) {
        if (ref.getDesc() != null) {
            return ref.getDesc().getId().hashCode();
        }
        TableName tblName = ref.getTblName();
        String label = ref.getLabel();
        if (ref.getUsedStructFieldPos() != null) {
            return Objects.hashCode((tblName == null ? "" : tblName.toSql() + "." + label).toLowerCase(),
                    ref.getUsedStructFieldPos());
        } else {
            return Objects.hashCode((tblName == null ? "" : tblName.toSql() + "." + label).toLowerCase());
        }
    }

    private static List<TableName> tableNames() {
        List<TableName> result = new ArrayList<>();
        result.add(null);
        result.add(new TableName());
        for (String catalog : CATALOG_AND_DB_NAMES) {
            for (String db : CATALOG_AND_DB_NAMES) {
                for (String tbl : NAMES) {
                    result.add(new TableName(catalog, db, tbl));
                }
            }
        }
        return result;
    }

    private static void check(SlotRef ref) {
        Assertions.assertEquals(referenceHashCode(ref), ref.hashCode(),
                () -> "tblName=" + (ref.getTblName() == null ? "null" : ref.getTblName().toSql()) + " label="
                        + ref.getLabel() + " struct=" + ref.getUsedStructFieldPos() + " locale=" + Locale.getDefault());
    }

    private static void checkAll(List<TableName> tableNames) {
        for (TableName tableName : tableNames) {
            for (String label : NAMES) {
                SlotRef ref = new SlotRef(tableName, "col", label);
                check(ref);
                ref.setUsedStructFieldPos(Arrays.asList(1, 0));
                check(ref);
            }
            check(new SlotRef(tableName, "MyCol"));
        }
    }

    @Test
    public void testNameCombinationsInLocales() {
        List<TableName> tableNames = tableNames();
        Locale saved = Locale.getDefault();
        try {
            for (String tag : LOCALES) {
                Locale.setDefault(Locale.forLanguageTag(tag));
                checkAll(tableNames);
            }
        } finally {
            Locale.setDefault(saved);
        }
    }

    @Test
    public void testRandomAsciiNames() {
        Random random = new Random(20261004L);
        Locale saved = Locale.getDefault();
        try {
            for (String tag : new String[] {"en-US", "tr"}) {
                Locale.setDefault(Locale.forLanguageTag(tag));
                for (int i = 0; i < 20000; i++) {
                    TableName tableName = new TableName(randomName(random), randomName(random), randomName(random));
                    SlotRef ref = new SlotRef(tableName, "c", randomName(random));
                    check(ref);
                    ref.setUsedStructFieldPos(List.of(random.nextInt(4)));
                    check(ref);
                }
            }
        } finally {
            Locale.setDefault(saved);
        }
    }

    private static String randomName(Random random) {
        int kind = random.nextInt(10);
        if (kind == 0) {
            return null;
        }
        if (kind == 1) {
            return "default_catalog";
        }
        int length = random.nextInt(24);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if (kind == 2 && i == length - 1) {
                // One non-ASCII char at the end.
                sb.append((char) (0x80 + random.nextInt(0x3000)));
            } else {
                sb.append((char) random.nextInt(0x80));
            }
        }
        return sb.toString();
    }

    @Test
    public void testParsedAndModifiedSlotRefs() {
        List<SlotRef> refs = new ArrayList<>();
        refs.add(new SlotRef(QualifiedName.of("Col")));
        refs.add(new SlotRef(QualifiedName.of("T1", "Col")));
        refs.add(new SlotRef(QualifiedName.of("Db", "T1", "Col")));
        refs.add(new SlotRef(QualifiedName.of("Hive", "Db", "T1", "Col")));
        refs.add(new SlotRef(QualifiedName.of("default_catalog", "Db", "T1", "Col")));
        refs.add(new SlotRef(QualifiedName.of("Hive", "Db", "T1", "Col", "Field")));
        refs.add(new SlotRef(QualifiedName.of("\u8868", "\u5217"))); // non-ASCII names
        for (SlotRef ref : refs) {
            check(ref);
            check((SlotRef) ref.clone());
        }

        SlotRef ref = new SlotRef(new TableName("Hive", "Db", "T1"), "Col");
        check(ref);
        ref.getTblName().setCatalog("default_catalog");
        check(ref);
        ref.getTblName().setDb(null);
        check(ref);
        ref.getTblName().setTbl("\u00c4"); // non-ASCII names
        check(ref);
        ref.setTblName(new TableName(null, "X", "Y"));
        check(ref);
        ref.setLabel(null);
        check(ref);
        ref.setTblName(null);
        check(ref);

        SlotRef withDesc = new SlotRef(new SlotId(7));
        check(withDesc);
        withDesc.setTblName(new TableName("Hive", "Db", "T1"));
        withDesc.setLabel("Label");
        check(withDesc);
    }
}
