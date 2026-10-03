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

package com.starrocks.sql.optimizer.base;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ColumnRefSetTest {
    private static void check(ColumnRefSet actual, RoaringBitmap expected) {
        assertArrayEquals(expected.toArray(), actual.getColumnIds());
        assertArrayEquals(expected.toArray(), actual.getStream().mapToInt(Integer::intValue).toArray());
        assertEquals(expected.getCardinality(), actual.size());
        assertEquals(expected.getCardinality(), actual.cardinality());
        assertEquals(expected.isEmpty(), actual.isEmpty());
        assertEquals(expected.hashCode(), actual.hashCode());
        assertEquals(expected.toString(), actual.toString());
        if (!expected.isEmpty()) {
            assertEquals(expected.first(), actual.getFirstId());
        } else {
            Throwable reference = assertThrows(RuntimeException.class, expected::first);
            assertEquals(reference.getClass(), assertThrows(RuntimeException.class, actual::getFirstId).getClass());
        }
        ColumnRefSet reconstructed = ColumnRefSet.createByIds(Arrays.stream(expected.toArray()).boxed().toList());
        assertEquals(reconstructed, actual);
        assertEquals(actual, reconstructed);
        assertTrue(actual.isSame(reconstructed));
        assertEquals(actual.hashCode(), reconstructed.hashCode());
    }

    @Test
    void operationsMatchRoaringAcrossRepresentations() {
        Random random = new Random(20261001);
        int[] boundaries = {0, 1, 65535, 65536, Integer.MAX_VALUE, Integer.MIN_VALUE, -1};
        ColumnRefSet[] sets = new ColumnRefSet[8];
        RoaringBitmap[] refs = new RoaringBitmap[8];
        for (int i = 0; i < sets.length; i++) {
            sets[i] = new ColumnRefSet();
            refs[i] = new RoaringBitmap();
        }
        for (int step = 0; step < 30000; step++) {
            int a = random.nextInt(sets.length);
            int b = random.nextInt(sets.length);
            int id = random.nextBoolean() ? boundaries[random.nextInt(boundaries.length)] : random.nextInt();
            switch (random.nextInt(9)) {
                case 0:
                    sets[a].union(id);
                    refs[a].add(id);
                    break;
                case 1:
                    sets[a].union(sets[b]);
                    refs[a].or(refs[b]);
                    break;
                case 2:
                    sets[a].except(sets[b]);
                    refs[a].andNot(refs[b]);
                    break;
                case 3:
                    sets[a].intersect(sets[b]);
                    refs[a].and(refs[b]);
                    break;
                case 4:
                    sets[a].clear();
                    refs[a].clear();
                    break;
                case 5:
                    sets[a] = sets[b].clone();
                    refs[a] = refs[b].clone();
                    break;
                case 6:
                    sets[a].intersect(id);
                    refs[a].and(RoaringBitmap.bitmapOf(id));
                    break;
                case 7:
                    sets[a].and(sets[b]);
                    refs[a].and(refs[b]);
                    break;
                default:
                    sets[a] = new ColumnRefSet(id);
                    refs[a] = RoaringBitmap.bitmapOf(id);
            }
            assertEquals(refs[a].contains(id), sets[a].contains(id));
            assertEquals(refs[a].contains(refs[b]), sets[a].containsAll(sets[b]));
            assertEquals(RoaringBitmap.intersects(refs[a], refs[b]), sets[a].isIntersect(sets[b]));
            assertEquals(refs[a].equals(refs[b]), sets[a].equals(sets[b]));
            for (int i = 0; i < sets.length; i++) {
                check(sets[i], refs[i]);
            }
        }
    }

    @Test
    void collectionOverloadsAndMixedRepresentations() {
        ColumnRefOperator one = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator two = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        ColumnRefSet duplicate = new ColumnRefSet(List.of(one, one));
        ColumnRefSet inline = new ColumnRefSet(1);
        assertEquals(inline, duplicate);
        assertEquals(duplicate, inline);
        assertEquals(inline.hashCode(), duplicate.hashCode());
        assertTrue(inline.containsAll(duplicate));
        assertTrue(duplicate.containsAll(inline));
        duplicate.union(List.of(one, two));
        check(duplicate, RoaringBitmap.bitmapOf(1, 2));
        assertTrue(duplicate.containsAny(List.of(two)));
        assertTrue(duplicate.containsAll(List.of(1, 2)));
        duplicate.except(List.of(one));
        check(duplicate, RoaringBitmap.bitmapOf(2));
        duplicate.intersect(List.of(two));
        check(duplicate, RoaringBitmap.bitmapOf(2));
        duplicate.intersect(one);
        check(duplicate, new RoaringBitmap());
        check(ColumnRefSet.of(one, two, one), RoaringBitmap.bitmapOf(1, 2));
        inline.union(two);
        assertFalse(inline.equals(new ColumnRefSet(1)));
        assertFalse(new ColumnRefSet(1).equals(inline));
    }

    @Test
    void denseAndSparseSetsKeepOrderingAndCloneOwnership() {
        for (int stride : new int[] {1, 65537}) {
            ColumnRefSet actual = new ColumnRefSet();
            RoaringBitmap expected = new RoaringBitmap();
            for (int i = 0; i < 10000; i++) {
                actual.union(i * stride);
                expected.add(i * stride);
            }
            check(actual, expected);
            ColumnRefSet copy = actual.clone();
            copy.intersect(65537);
            check(actual, expected);
            ColumnRefSet union = new ColumnRefSet(-1);
            union.union(actual);
            union.clear();
            check(actual, expected);
            actual.except(actual);
            check(actual, new RoaringBitmap());
        }
    }
}
