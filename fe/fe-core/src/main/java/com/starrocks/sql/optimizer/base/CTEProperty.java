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

import com.google.common.base.Objects;
import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;
import org.apache.commons.collections4.CollectionUtils;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

public class CTEProperty implements PhysicalProperty {
    // All cteID will be passed from top to bottom, and prune plan when meet CTENoOp node with same CTEid 
    private final Set<Integer> cteIds;
    private final int hashCode;

    public static CTEProperty createProperty(Set<Integer> cteIds) {
        if (CollectionUtils.isEmpty(cteIds)) {
            return EmptyCTEProperty.INSTANCE;
        } else {
            return new CTEProperty(cteIds);
        }
    }

    protected CTEProperty(Set<Integer> cteIds) {
        this(cteIds, true);
    }

    // copy=false is reserved for sets created here and never exposed before ownership transfer.
    private CTEProperty(Set<Integer> ids, boolean copy) {
        SmallCteSet small = ids instanceof SmallCteSet ? (SmallCteSet) ids : SmallCteSet.copyOf(ids);
        if (small != null) {
            cteIds = small;
        } else if (ids.size() == 1) {
            cteIds = Collections.singleton(ids.iterator().next());
        } else {
            cteIds = Collections.unmodifiableSet(copy ? new HashSet<>(ids) : ids);
        }
        hashCode = 31 + cteIds.hashCode();
    }

    private static CTEProperty fromOwned(Set<Integer> ownedIds) {
        return ownedIds.isEmpty() ? EmptyCTEProperty.INSTANCE : new CTEProperty(ownedIds, false);
    }

    public CTEProperty(int cteId) {
        cteIds = fitsMask(cteId) ? new SmallCteSet(1L << cteId) : Collections.singleton(cteId);
        hashCode = 31 + cteId;
    }

    // Properties are immutable memo keys. No-op operations reuse the existing property;
    // changes allocate one owned set, without a temporary set followed by a snapshot copy.
    public CTEProperty union(CTEProperty other) {
        if (cteIds instanceof SmallCteSet && other.cteIds instanceof SmallCteSet) {
            long left = ((SmallCteSet) cteIds).mask;
            long right = ((SmallCteSet) other.cteIds).mask;
            long union = left | right;
            return union == left ? this : union == right ? other : fromMask(union);
        }
        if (this == other || cteIds.containsAll(other.cteIds)) {
            return this;
        }
        if (other.cteIds.containsAll(cteIds)) {
            return other;
        }
        Set<Integer> ids = new HashSet<>(cteIds);
        ids.addAll(other.cteIds);
        return fromOwned(ids);
    }

    public CTEProperty intersect(CTEProperty other) {
        if (cteIds instanceof SmallCteSet && other.cteIds instanceof SmallCteSet) {
            long left = ((SmallCteSet) cteIds).mask;
            long right = ((SmallCteSet) other.cteIds).mask;
            long intersection = left & right;
            return intersection == left ? this : intersection == right ? other : fromMask(intersection);
        }
        if (this == other || other.cteIds.containsAll(cteIds)) {
            return this;
        }
        if (cteIds.containsAll(other.cteIds)) {
            return other;
        }
        Set<Integer> smaller = cteIds.size() <= other.cteIds.size() ? cteIds : other.cteIds;
        Set<Integer> larger = smaller == cteIds ? other.cteIds : cteIds;
        Set<Integer> ids = new HashSet<>();
        for (Integer id : smaller) {
            if (larger.contains(id)) {
                ids.add(id);
            }
        }
        return fromOwned(ids);
    }

    public CTEProperty withCTE(int id) {
        if (cteIds instanceof SmallCteSet && fitsMask(id)) {
            long mask = ((SmallCteSet) cteIds).mask;
            long added = mask | (1L << id);
            return added == mask ? this : fromMask(added);
        }
        if (cteIds.contains(id)) {
            return this;
        }
        if (cteIds.isEmpty()) {
            return new CTEProperty(id);
        }
        Set<Integer> ids = new HashSet<>(cteIds);
        ids.add(id);
        return fromOwned(ids);
    }

    public CTEProperty withoutCTE(int id) {
        if (cteIds instanceof SmallCteSet && fitsMask(id)) {
            long mask = ((SmallCteSet) cteIds).mask;
            long removed = mask & ~(1L << id);
            return removed == mask ? this : fromMask(removed);
        }
        if (!cteIds.contains(id)) {
            return this;
        }
        if (cteIds.size() == 1) {
            return EmptyCTEProperty.INSTANCE;
        }
        Set<Integer> ids = new HashSet<>(cteIds);
        ids.remove(id);
        return fromOwned(ids);
    }

    private static boolean fitsMask(int id) {
        return id >= 0 && id < Long.SIZE;
    }

    private static CTEProperty fromMask(long mask) {
        return mask == 0 ? EmptyCTEProperty.INSTANCE : new CTEProperty(new SmallCteSet(mask), false);
    }

    // CTE IDs normally start at one per transformation. Keep small sets as a word, while
    // the general Set path preserves arbitrary integer IDs and nullable legacy inputs.
    // This immutable Set also keeps getCteIds() consumers on the compact representation.
    private static final class SmallCteSet extends AbstractSet<Integer> {
        private final long mask;

        private SmallCteSet(long mask) {
            this.mask = mask;
        }

        private static SmallCteSet copyOf(Set<Integer> ids) {
            long mask = 0;
            for (Integer id : ids) {
                if (id == null || !fitsMask(id)) {
                    return null;
                }
                mask |= 1L << id;
            }
            return new SmallCteSet(mask);
        }

        @Override
        public int size() {
            return Long.bitCount(mask);
        }

        @Override
        public boolean contains(Object value) {
            return value instanceof Integer && fitsMask((Integer) value) && (mask & (1L << (Integer) value)) != 0;
        }

        @Override
        public boolean containsAll(Collection<?> values) {
            if (values instanceof SmallCteSet) {
                long other = ((SmallCteSet) values).mask;
                return (mask & other) == other;
            }
            return super.containsAll(values);
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof SmallCteSet ? mask == ((SmallCteSet) other).mask : super.equals(other);
        }

        @Override
        public int hashCode() {
            int hash = 0;
            for (long bits = mask; bits != 0; bits &= bits - 1) {
                hash += Long.numberOfTrailingZeros(bits);
            }
            return hash;
        }

        @Override
        public Iterator<Integer> iterator() {
            return new Iterator<>() {
                private long remaining = mask;

                @Override
                public boolean hasNext() {
                    return remaining != 0;
                }

                @Override
                public Integer next() {
                    if (remaining == 0) {
                        throw new NoSuchElementException();
                    }
                    int id = Long.numberOfTrailingZeros(remaining);
                    remaining &= remaining - 1;
                    return id;
                }
            };
        }
    }

    // Read-only. Use the value operations above to change CTE requirements.
    public Set<Integer> getCteIds() {
        return cteIds;
    }

    public boolean isEmpty() {
        return cteIds.isEmpty();
    }

    @Override
    public boolean isSatisfy(PhysicalProperty other) {
        return true;
    }

    @Override
    public GroupExpression appendEnforcers(Group child) {
        throw new StarRocksPlannerException("cannot enforce cte property", ErrorType.INTERNAL_ERROR);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CTEProperty that = (CTEProperty) o;
        return Objects.equal(cteIds, that.cteIds);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        return "[" + cteIds.stream().map(String::valueOf).collect(Collectors.joining(", ")) + "]";
    }
}
