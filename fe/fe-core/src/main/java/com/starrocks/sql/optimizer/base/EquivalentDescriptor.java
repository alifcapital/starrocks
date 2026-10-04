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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

// Property information for hash distribution desc, it used for check DistributionSpec satisfy condition.
// it uses disjoint-set data structure to record derived distribution cols from equalPredicate in on clause.
// Given the following example：
//      select * from A JOIN B ON A.a = B.b
//      JOIN C ON B.b = C.c
//      JOIN D ON C.c = D.d
//      JOIN E ON D.d = E.e
// After A JOIN B, we can use A.a and B.b to describe the distribution info.
// After A JOIN B Join C, we can use A.a and B.b to describe the distribution info.
// The `distributionCols` in `HashDistributionDesc` can only record one possibility, while Equivalence-related
// information is not preserved. When performing the next JOIN operation JOIN D ON C.c = D.d, the output property
// of the left child may A.a, and the required property is C.c. We need determine if A.a is equivalent to C.c.
// The operation of checking if the output of the child satisfies the join condition is like a "find" operation.
// After the join, the operation of establishing equivalence between the corresponding columns of the left and right
// child is like a "union" operation.
public final class EquivalentDescriptor {
    private static final int MAX_PENDING_COLUMNS = 8;

    private final long tableId;

    private final List<Long> partitionIds;

    private UnionFind<DistributionCol> nullStrictUnionFind;
    private DistributionCol pendingStrict;
    private List<DistributionCol> moreStrict;

    private UnionFind<DistributionCol> nullRelaxUnionFind;
    private DistributionCol pendingRelax;
    private List<DistributionCol> moreRelax;

    public EquivalentDescriptor() {
        this(-1, Collections.emptyList());
    }

    public EquivalentDescriptor(long tableId, List<Long> partitionIds) {
        this.tableId = tableId;
        this.partitionIds = partitionIds;
    }

    public long getTableId() {
        return tableId;
    }

    public List<Long> getPartitionIds() {
        return partitionIds;
    }

    public boolean isSinglePartition() {
        return partitionIds.size() == 1;
    }

    public boolean isEmptyPartition() {
        return partitionIds.isEmpty();
    }

    public void initDistributionUnionFind(List<DistributionCol> distributionCols) {
        for (DistributionCol col : distributionCols) {
            if (col.isNullStrict()) {
                addStrictSeed(col);
                addRelaxSeed(new DistributionCol(col.getColId(), false));
            } else {
                addRelaxSeed(col);
            }
        }
    }

    // Most distribution candidates initially contain only singleton equivalence classes.
    // Keep their insertion order without allocating the maps/sets until they are queried,
    // merged, or exposed through a getter. A materialized getter retains its live identity.
    private void addStrictSeed(DistributionCol col) {
        if (nullStrictUnionFind != null) {
            nullStrictUnionFind.add(col);
        } else if (pendingStrict == null) {
            pendingStrict = col;
        } else if (!pendingStrict.equals(col) && (moreStrict == null || !moreStrict.contains(col))) {
            if (moreStrict == null) {
                moreStrict = new ArrayList<>(1);
            }
            moreStrict.add(col);
            // Bound linear duplicate checks and pending storage for wide distributions.
            if (moreStrict.size() + 1 >= MAX_PENDING_COLUMNS) {
                getNullStrictUnionFind();
            }
        }
    }

    private void addRelaxSeed(DistributionCol col) {
        if (nullRelaxUnionFind != null) {
            nullRelaxUnionFind.add(col);
        } else if (pendingRelax == null) {
            pendingRelax = col;
        } else if (!pendingRelax.equals(col) && (moreRelax == null || !moreRelax.contains(col))) {
            if (moreRelax == null) {
                moreRelax = new ArrayList<>(1);
            }
            moreRelax.add(col);
            // Bound linear duplicate checks and pending storage for wide distributions.
            if (moreRelax.size() + 1 >= MAX_PENDING_COLUMNS) {
                getNullRelaxUnionFind();
            }
        }
    }

    public UnionFind<DistributionCol> getNullStrictUnionFind() {
        if (nullStrictUnionFind == null) {
            nullStrictUnionFind = new UnionFind<>();
            if (pendingStrict != null) {
                nullStrictUnionFind.add(pendingStrict);
                pendingStrict = null;
            }
            if (moreStrict != null) {
                for (DistributionCol col : moreStrict) {
                    nullStrictUnionFind.add(col);
                }
                moreStrict = null;
            }
        }
        return nullStrictUnionFind;
    }

    public UnionFind<DistributionCol> getNullRelaxUnionFind() {
        if (nullRelaxUnionFind == null) {
            nullRelaxUnionFind = new UnionFind<>();
            if (pendingRelax != null) {
                nullRelaxUnionFind.add(pendingRelax);
                pendingRelax = null;
            }
            if (moreRelax != null) {
                for (DistributionCol col : moreRelax) {
                    nullRelaxUnionFind.add(col);
                }
                moreRelax = null;
            }
        }
        return nullRelaxUnionFind;
    }

    public void unionDistributionCols(DistributionCol leftCol, DistributionCol rightCol) {
        checkState(leftCol.isNullStrict() == rightCol.isNullStrict(),
                "%s and %s should have same nullStrict value", leftCol, rightCol);
        if (leftCol.isNullStrict()) {
            getNullStrictUnionFind().union(leftCol, rightCol);
            getNullRelaxUnionFind().union(leftCol.getNullRelaxCol(), rightCol.getNullRelaxCol());
        } else {
            getNullRelaxUnionFind().union(leftCol, rightCol);
            getNullStrictUnionFind().union(leftCol.getNullStrictCol(), rightCol.getNullStrictCol());
        }
    }

    public void unionNullRelaxCols(DistributionCol leftCol, DistributionCol rightCol) {
        getNullRelaxUnionFind().union(leftCol.getNullRelaxCol(), rightCol.getNullRelaxCol());
    }

    public void clearNullStrictUnionFind() {
        pendingStrict = null;
        moreStrict = null;
        if (nullStrictUnionFind != null) {
            nullStrictUnionFind.clear();
        }
    }

    public boolean isConnected(DistributionCol requiredCol, DistributionCol existDistributionCol) {
        if (requiredCol.isNullStrict() && !existDistributionCol.isNullStrict()) {
            return false;
        }

        if (requiredCol.isNullStrict()) {
            return isEquivalent(getNullStrictUnionFind(), requiredCol, existDistributionCol);
        } else {
            return isEquivalent(getNullRelaxUnionFind(), requiredCol, existDistributionCol.getNullRelaxCol());
        }
    }

    public EquivalentDescriptor copy() {
        EquivalentDescriptor copy = new EquivalentDescriptor(tableId, partitionIds);
        copy.nullRelaxUnionFind = nullRelaxUnionFind == null ? null : nullRelaxUnionFind.copy();
        copy.nullStrictUnionFind = nullStrictUnionFind == null ? null : nullStrictUnionFind.copy();
        copy.pendingRelax = pendingRelax;
        copy.moreRelax = moreRelax == null ? null : new ArrayList<>(moreRelax);
        copy.pendingStrict = pendingStrict;
        copy.moreStrict = moreStrict == null ? null : new ArrayList<>(moreStrict);
        return copy;
    }

    private boolean isEquivalent(UnionFind<DistributionCol> unionFind, DistributionCol requiredCol,
                                 DistributionCol existDistributionCol) {
        unionFind.add(requiredCol);
        unionFind.add(requiredCol);
        Optional<Integer> leftGroupId = unionFind.getGroupId(requiredCol);
        Optional<Integer> rightGroupId = unionFind.getGroupId(existDistributionCol);
        return leftGroupId.isPresent() && rightGroupId.isPresent() && leftGroupId.get().equals(rightGroupId.get());

    }
}
