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

import com.starrocks.common.util.UnionFind;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EquivalentDescriptorTest {
    private static DistributionCol col(int id, boolean strict) {
        return new DistributionCol(id, strict, false);
    }

    @Test
    public void copiesAreIndependentBeforeAndAfterTheUnionFindIsBuilt() {
        // The union-find sets are built on first read. We expect a copy taken before or after that point
        // to keep its own state, so changes to the copy or to the source must not leak into the other one.
        EquivalentDescriptor source = new EquivalentDescriptor(7, List.of(11L));
        source.initDistributionUnionFind(List.of(col(1, true)));
        EquivalentDescriptor unbuiltCopy = source.copy();
        source.initDistributionUnionFind(List.of(col(2, true)));
        unbuiltCopy.initDistributionUnionFind(List.of(col(3, true)));
        assertFalse(unbuiltCopy.getNullStrictUnionFind().find(col(2, true)));
        assertFalse(source.getNullStrictUnionFind().find(col(3, true)));

        EquivalentDescriptor builtCopy = source.copy();
        builtCopy.unionDistributionCols(col(1, true), col(2, true));
        assertFalse(source.isConnected(col(1, true), col(2, true)));
        assertTrue(builtCopy.isConnected(col(1, true), col(2, true)));

        // Clearing the null-strict set keeps the instance that callers already hold and keeps the
        // null-relax set.
        UnionFind<DistributionCol> live = source.getNullStrictUnionFind();
        source.clearNullStrictUnionFind();
        assertSame(live, source.getNullStrictUnionFind());
        assertTrue(live.getAllGroups().isEmpty());
        assertTrue(source.getNullRelaxUnionFind().find(col(1, false)));
        assertEquals(7L, unbuiltCopy.getTableId());
        assertEquals(List.of(11L), unbuiltCopy.getPartitionIds());
    }

    @Test
    public void hashDistributionSatisfactionFollowsEquivalenceAndNullStrictness() {
        HashDistributionDesc.SourceType source = HashDistributionDesc.SourceType.SHUFFLE_JOIN;
        HashDistributionSpec actual = new HashDistributionSpec(new HashDistributionDesc(List.of(col(1, true)), source));
        HashDistributionSpec required = new HashDistributionSpec(
                new HashDistributionDesc(List.of(col(2, true)), source));
        HashDistributionSpec relaxed = new HashDistributionSpec(
                new HashDistributionDesc(List.of(col(2, false)), source));
        assertFalse(actual.isSatisfy(required));
        EquivalentDescriptor independent = actual.getEquivDesc().copy();
        actual.getEquivDesc().unionDistributionCols(col(1, true), col(2, true));
        assertTrue(actual.isSatisfy(required));
        assertFalse(independent.isConnected(col(2, true), col(1, true)));
        actual.getEquivDesc().clearNullStrictUnionFind();
        assertFalse(actual.isSatisfy(required));
        assertTrue(actual.isSatisfy(relaxed));
    }
}
