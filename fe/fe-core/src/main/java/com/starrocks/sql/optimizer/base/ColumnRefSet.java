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
import org.roaringbitmap.RoaringBitmap;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

// Small sets keep their only ID inline; larger sets retain Roaring's unsigned-ID representation.
public class ColumnRefSet implements Cloneable {
    private static final RoaringBitmap EMPTY_BITMAP = new RoaringBitmap();
    private RoaringBitmap bitSet;
    // Used only when bitSet is null. No ID is reserved as a sentinel.
    private int inlineSize;
    private int singleId;

    public ColumnRefSet() {
    }

    public ColumnRefSet(int id) {
        inlineSize = 1;
        singleId = id;
    }

    public ColumnRefSet(Collection<ColumnRefOperator> refs) {
        if (refs.size() > 1) {
            bitSet = new RoaringBitmap();
            for (ColumnRefOperator ref : refs) {
                bitSet.add(ref.getId());
            }
        } else {
            for (ColumnRefOperator ref : refs) {
                union(ref.getId());
            }
        }
    }

    public static ColumnRefSet createByIds(Collection<Integer> colIds) {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        colIds.forEach(columnRefSet::union);
        return columnRefSet;
    }

    public static ColumnRefSet of(ColumnRefOperator... columnRefs) {
        ColumnRefSet columnRefSet = new ColumnRefSet();
        for (ColumnRefOperator colRef : columnRefs) {
            columnRefSet.union(colRef);
        }
        return columnRefSet;
    }

    public int[] getColumnIds() {
        if (bitSet != null) {
            return bitSet.toArray();
        }
        return inlineSize == 0 ? new int[0] : new int[] {singleId};
    }

    public Stream<Integer> getStream() {
        if (bitSet == null) {
            return inlineSize == 0 ? Stream.empty() : Stream.of(singleId);
        }
        Spliterator<Integer> spliterator = Spliterators.spliteratorUnknownSize(bitSet.iterator(), Spliterator.ORDERED);
        return StreamSupport.stream(spliterator, false);
    }

    public int getFirstId() {
        if (bitSet != null) {
            return bitSet.first();
        }
        return inlineSize == 1 ? singleId : EMPTY_BITMAP.first();
    }

    @Override
    public ColumnRefSet clone() {
        try {
            ColumnRefSet result = (ColumnRefSet) super.clone();
            if (bitSet != null) {
                result.bitSet = bitSet.clone();
            }
            return result;
        } catch (CloneNotSupportedException e) {
            throw new InternalError(e);
        }
    }

    // Keep exact library hashing/formatting rather than duplicating container-specific formulas.
    // Reads do not change representation, and the shared empty bitmap is never mutated.
    private RoaringBitmap bitmapForRead() {
        if (bitSet != null) {
            return bitSet;
        }
        if (inlineSize == 0) {
            return EMPTY_BITMAP;
        }
        RoaringBitmap singleton = new RoaringBitmap();
        singleton.add(singleId);
        return singleton;
    }

    @Override
    public int hashCode() {
        return bitSet != null ? bitSet.hashCode() : bitmapForRead().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ColumnRefSet)) {
            return false;
        }
        ColumnRefSet rhs = (ColumnRefSet) obj;
        if (bitSet != null && rhs.bitSet != null) {
            return bitSet.equals(rhs.bitSet);
        }
        return size() == rhs.size() && (isEmpty() || contains(rhs.getFirstId()));
    }

    public int size() {
        return bitSet == null ? inlineSize : bitSet.getCardinality();
    }

    // The meaning is same with SQL Union Operation
    public void union(int id) {
        if (bitSet != null) {
            bitSet.add(id);
        } else {
            unionInline(id);
        }
    }

    private void unionInline(int id) {
        if (inlineSize == 0) {
            singleId = id;
            inlineSize = 1;
        } else if (singleId != id) {
            bitSet = RoaringBitmap.bitmapOf(singleId, id);
        }
    }

    public void union(ColumnRefOperator ref) {
        union(ref.getId());
    }

    public void union(Collection<ColumnRefOperator> refs) {
        if (bitSet != null) {
            for (ColumnRefOperator ref : refs) {
                bitSet.add(ref.getId());
            }
        } else {
            for (ColumnRefOperator ref : refs) {
                union(ref.getId());
            }
        }
    }

    public void union(ColumnRefSet set) {
        if (set.bitSet == null) {
            if (set.inlineSize == 1) {
                union(set.singleId);
            }
        } else if (bitSet != null) {
            bitSet.or(set.bitSet);
        } else {
            bitSet = set.bitSet.clone();
            if (inlineSize == 1) {
                bitSet.add(singleId);
            }
        }
    }

    // The meaning is same with SQL Except Operation
    public void except(Collection<ColumnRefOperator> refs) {
        except(new ColumnRefSet(refs));
    }

    public void except(ColumnRefSet set) {
        if (bitSet == null) {
            if (inlineSize == 1 && set.contains(singleId)) {
                clear();
            }
        } else if (set.bitSet != null) {
            bitSet.andNot(set.bitSet);
        } else if (set.inlineSize == 1) {
            bitSet.remove(set.singleId);
        }
    }

    // The meaning is same with SQL Intersect Operation
    public void intersect(List<ColumnRefOperator> refs) {
        intersect(new ColumnRefSet(refs));
    }

    public void intersect(ColumnRefOperator column) {
        intersect(column.getId());
    }

    public void intersect(int id) {
        boolean present = contains(id);
        bitSet = null;
        inlineSize = present ? 1 : 0;
        singleId = id;
    }

    public void intersect(ColumnRefSet set) {
        if (set.bitSet == null) {
            if (set.inlineSize == 0) {
                clear();
            } else {
                intersect(set.singleId);
            }
        } else if (bitSet != null) {
            bitSet.and(set.bitSet);
        } else if (inlineSize == 1 && !set.contains(singleId)) {
            clear();
        }
    }

    public boolean isIntersect(ColumnRefSet other) {
        if (bitSet == null) {
            return inlineSize == 1 && other.contains(singleId);
        }
        if (other.bitSet == null) {
            return other.inlineSize == 1 && contains(other.singleId);
        }
        return RoaringBitmap.intersects(bitSet, other.bitSet);
    }

    public int cardinality() {
        return size();
    }

    public boolean isEmpty() {
        return bitSet == null ? inlineSize == 0 : bitSet.isEmpty();
    }

    public void and(ColumnRefSet set) {
        intersect(set);
    }

    public boolean isSame(ColumnRefSet columnRefSet) {
        return equals(Objects.requireNonNull(columnRefSet));
    }

    public void clear() {
        bitSet = null;
        inlineSize = 0;
    }

    public boolean contains(ColumnRefOperator ref) {
        return contains(ref.getId());
    }

    public boolean contains(int id) {
        return bitSet == null ? inlineSize == 1 && singleId == id : bitSet.contains(id);
    }

    public boolean containsAll(ColumnRefSet rhs) {
        if (rhs.bitSet == null) {
            return rhs.inlineSize == 0 || contains(rhs.singleId);
        }
        if (bitSet != null) {
            return bitSet.contains(rhs.bitSet);
        }
        return rhs.isEmpty() || (inlineSize == 1 && rhs.size() == 1 && rhs.contains(singleId));
    }

    public boolean containsAny(ColumnRefSet rhs) {
        return isIntersect(rhs);
    }

    public boolean containsAny(Collection<ColumnRefOperator> rhs) {
        for (ColumnRefOperator ref : rhs) {
            if (contains(ref)) {
                return true;
            }
        }
        return false;
    }

    public boolean containsAll(Collection<Integer> rhs) {
        for (Integer id : rhs) {
            if (!contains(id)) {
                return false;
            }
        }
        return true;
    }

    public List<ColumnRefOperator> getColumnRefOperators(ColumnRefFactory columnRefFactory) {
        return getStream().map(columnRefFactory::getColumnRef).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return bitSet != null ? bitSet.toString() : bitmapForRead().toString();
    }
}
