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

package com.starrocks.type;

import com.google.common.base.Objects;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.*;

@ResourceLock("java.util.Locale.default")
public class StructFieldHashContractTest {
    private static StructField field(String name) {
        return new StructField(name, 7, "PhysicalI", IntegerType.INT, null);
    }
    private static void locales(Runnable test) {
        Locale previous = Locale.getDefault();
        try {
            for (Locale locale : new Locale[] {Locale.US, Locale.forLanguageTag("tr-TR")}) {
                Locale.setDefault(locale);
                test.run();
            }
        } finally {
            Locale.setDefault(previous);
        }
    }
    private static void equivalenceClass(String... names) {
        for (String left : names) {
            HashSet<StructField> set = new HashSet<>();
            HashMap<StructField, String> map = new HashMap<>();
            StructField first = field(left);
            set.add(first);
            map.put(first, "value");
            for (String right : names) {
                assertTrue(StringUtils.equalsIgnoreCase(left, right), left + "/" + right);
                assertEquals(first, field(right));
                assertEquals(first.hashCode(), field(right).hashCode());
                assertTrue(set.contains(field(right)));
                assertEquals("value", map.get(field(right)));
                set.add(field(right));
                assertEquals(1, set.size());
            }
            assertEquals(left, first.getName());
        }
    }
    @Test
    public void unicodeEqualityAndHashCollections() {
        locales(() -> {
            equivalenceClass("I", "i", "\u0130", "\u0131");
            equivalenceClass("\u03a3", "\u03c3", "\u03c2");
            equivalenceClass("\ud801\udc00", "\ud801\udc28");
            equivalenceClass("x\ud801\udc00I", "X\ud801\udc28\u0131");
            equivalenceClass("\u00df", "\u1e9e");
            assertFalse(StringUtils.equalsIgnoreCase("\u00df", "SS"));
            assertNotEquals(field("\u00df"), field("SS"));
            assertNotEquals(field("\u0130"), field("i\u0307"));
        });
    }
    @Test
    public void asciiHashMatchesPreviousRootStringHash() {
        locales(() -> {
            for (String name : new String[] {"", "I", "FIELD_I", "ASCII_123$", "MixedCase"}) {
                StructField field = field(name);
                assertEquals(Objects.hashCode(name.toLowerCase(Locale.ROOT), IntegerType.INT, 7, "PhysicalI"),
                        field.hashCode());
                assertEquals(name, field.getName());
            }
        });
    }
    @Test
    public void otherFieldsAndIgnoredMetadataRemainUnchanged() {
        locales(() -> {
            StructField original = field("I");
            StructField comment = new StructField("i", 7, "PhysicalI", IntegerType.INT, "comment");
            comment.setPosition(42);
            assertEquals(original, comment);
            assertEquals(original.hashCode(), comment.hashCode());
            assertNotEquals(original, new StructField("i", 8, "PhysicalI", IntegerType.INT, null));
            assertNotEquals(original, new StructField("i", 7, "physicali", IntegerType.INT, null));
            assertNotEquals(original, new StructField("i", 7, "PhysicalI", IntegerType.BIGINT, null));
            StructField nullableOthers = new StructField("I", 7, null, null, null);
            assertEquals(Objects.hashCode("i", null, 7, null), nullableOthers.hashCode());
        });
    }
    @Test
    public void unpairedSurrogatesAndNullNamesRetainBoundary() {
        locales(() -> {
            equivalenceClass("\ud800I", "\ud800i");
            equivalenceClass("\udc00I", "\udc00i");
            StructField left = new StructField();
            StructField right = new StructField();
            assertEquals(left, right);
            assertThrows(NullPointerException.class, left::hashCode);
            assertThrows(NullPointerException.class, () -> field(null).hashCode());
            assertNotEquals(field(null), field(""));
        });
    }
}
