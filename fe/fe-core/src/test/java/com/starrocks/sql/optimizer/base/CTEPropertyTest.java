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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CTEPropertyTest {
    @Test
    public void memoKeySurvivesMutationOfSourceSet() {
        // A CTEProperty is part of a memo key. We expect it to keep its own read-only copy of the ids,
        // so changing the source set after the lookup key is stored must not move the key.
        Set<Integer> ids = new HashSet<>(Set.of(7, 19));
        CTEProperty property = CTEProperty.createProperty(ids);
        PhysicalPropertySet key = new PhysicalPropertySet();
        key.setCteProperty(property);
        Map<PhysicalPropertySet, String> memo = new HashMap<>();
        memo.put(key, "plan");
        ids.clear();
        ids.add(33);
        PhysicalPropertySet equivalentKey = key.copy();
        equivalentKey.setCteProperty(CTEProperty.createProperty(Set.of(19, 7)));
        assertEquals("plan", memo.get(equivalentKey));
        assertEquals(Set.of(7, 19), property.getCteIds());
        assertThrows(UnsupportedOperationException.class, () -> property.getCteIds().add(33));
        assertThrows(UnsupportedOperationException.class, () -> property.getCteIds().remove(7));
        var iterator = property.getCteIds().iterator();
        iterator.next();
        assertThrows(UnsupportedOperationException.class, iterator::remove);
    }

    @Test
    public void setOperationsMatchHashSetAndDoNotChangeOperands() {
        // union, intersect, withCTE and withoutCTE return new values. We compare them with the same
        // operations on HashSet and check that both operands keep their ids.
        Random random = new Random(4156);
        for (int round = 0; round < 3000; round++) {
            Set<Integer> leftIds = new HashSet<>();
            Set<Integer> rightIds = new HashSet<>();
            for (int i = 0; i < 16; i++) {
                if (random.nextBoolean()) {
                    leftIds.add(random.nextInt(24) - 12);
                }
                if (random.nextBoolean()) {
                    rightIds.add(random.nextInt(24) - 12);
                }
            }
            CTEProperty left = CTEProperty.createProperty(leftIds);
            CTEProperty right = CTEProperty.createProperty(rightIds);
            int id = random.nextInt(24) - 12;
            Set<Integer> union = new HashSet<>(leftIds);
            union.addAll(rightIds);
            Set<Integer> intersection = new HashSet<>(leftIds);
            intersection.retainAll(rightIds);
            Set<Integer> added = new HashSet<>(leftIds);
            added.add(id);
            Set<Integer> removed = new HashSet<>(leftIds);
            removed.remove(id);
            assertEquals(CTEProperty.createProperty(union), left.union(right));
            assertEquals(CTEProperty.createProperty(intersection), left.intersect(right));
            assertEquals(CTEProperty.createProperty(added), left.withCTE(id));
            assertEquals(CTEProperty.createProperty(removed), left.withoutCTE(id));
            assertEquals(leftIds, left.getCteIds());
            assertEquals(rightIds, right.getCteIds());
            assertEquals(left, left.union(left));
            assertEquals(left, left.intersect(left));
            assertEquals(left, left.union(EmptyCTEProperty.INSTANCE));
            assertEquals(left, left.withoutCTE(100));
        }
    }

    @Test
    public void anchorRemoveAndUnionOrderIsSignificant() {
        CTEProperty producer = new CTEProperty(7);
        CTEProperty consumer = CTEProperty.createProperty(Set.of(7, 9));
        // The logical anchor removes its definition after it merges both branches.
        assertEquals(new CTEProperty(9), producer.union(consumer).withoutCTE(7));
        // The physical anchor removes the definition from the consumer side before it merges the producer side.
        assertEquals(consumer, consumer.withoutCTE(7).union(producer));
        assertSame(EmptyCTEProperty.INSTANCE, producer.withoutCTE(7));
        assertEquals(producer, EmptyCTEProperty.INSTANCE.union(producer));
        assertEquals(producer, producer.intersect(consumer));
        assertEquals(producer, producer.withCTE(7));
        CTEProperty merged = producer.union(new CTEProperty(9));
        assertThrows(UnsupportedOperationException.class, () -> merged.getCteIds().clear());
    }

    @Test
    public void boundaryIdsAndNullsKeepSetSemantics() {
        // Ids 0..63 and other ids can take different storage paths. We expect the same set semantics
        // and the same hash as a HashSet on both sides of that boundary.
        CTEProperty compact = CTEProperty.createProperty(Set.of(0, 31, 32, 63));
        for (Integer extra : Arrays.asList(-1, 64, 65, Integer.MIN_VALUE, Integer.MAX_VALUE, null)) {
            Set<Integer> expected = new HashSet<>(compact.getCteIds());
            expected.add(extra);
            CTEProperty general = compact.union(CTEProperty.createProperty(new HashSet<>(Arrays.asList(extra))));
            assertEquals(expected, general.getCteIds());
            assertEquals(Objects.hash(expected), general.hashCode());
            assertEquals(compact, general.intersect(compact));
            if (extra != null) {
                assertEquals(general, compact.withCTE(extra));
                assertEquals(compact, general.withoutCTE(extra));
            }
            assertEquals(Set.of(0, 31, 32, 63), compact.getCteIds());
        }
        CTEProperty signBit = new CTEProperty(63);
        assertEquals(Set.of(63), signBit.getCteIds());
        assertEquals(Set.of(0, 63), signBit.withCTE(0).getCteIds());
        assertSame(EmptyCTEProperty.INSTANCE, signBit.withoutCTE(63));
        var iterator = signBit.getCteIds().iterator();
        assertEquals(63, iterator.next());
        assertThrows(NoSuchElementException.class, iterator::next);
        assertThrows(UnsupportedOperationException.class, iterator::remove);
        assertThrows(UnsupportedOperationException.class, () -> signBit.getCteIds().clear());
        assertFalse(signBit.getCteIds().contains(63L));
        assertFalse(signBit.getCteIds().contains(null));
    }
}
