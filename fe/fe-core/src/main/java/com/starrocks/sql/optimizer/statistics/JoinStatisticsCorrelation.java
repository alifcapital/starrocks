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

package com.starrocks.sql.optimizer.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * A coordinated Corr-d head and bucketed Holder tail. All sides share one key dictionary,
 * head selection, hash family, power and collection generation. A missing head entry is zero
 * only within this tensor; heads from independently collected objects cannot be aligned by ordinal.
 */
public final class JoinStatisticsCorrelation {
    public static final int HEAD_BUDGET = 16384;
    public static final int TAIL_BUCKETS = JoinStatisticsBasis.TAIL_BUCKETS;
    public static final int TAIL_LAYOUTS = 3;

    public static final class Slice {
        private final CompactDegreeVector head;
        private final double tailNorm;
        private final double[] buckets;
        private final JoinStatisticsBasis.Slice shared;
        private final int arity;
        private final int power;
        private final boolean presence;
        private JoinStatisticsExtrapolation extrapolated;

        static Slice extrapolated(JoinStatisticsExtrapolation mixture, CompactDegreeVector shape,
                                 int arity, boolean presence) {
            Slice result = new Slice(shape, 0, EMPTY_BUCKETS, true);
            result.extrapolated = mixture;
            result.extrapolatedArity = arity;
            result.extrapolatedPresence = presence;
            return result;
        }

        private int extrapolatedArity;
        private boolean extrapolatedPresence;

        private double norm() {
            return extrapolated == null ? tailNorm : extrapolated.root(extrapolatedArity, extrapolatedPresence, -1);
        }

        /** Buckets contain the L_k norms of frequency^power, or presence for a membership side. */
        public Slice(CompactDegreeVector head, double tailNorm, double[] buckets) {
            this(head, tailNorm, buckets, false);
        }

        private Slice(CompactDegreeVector head, double tailNorm, double[] buckets, boolean owned) {
            if (head.size() > HEAD_BUDGET || !Double.isFinite(tailNorm) || tailNorm < 0
                    || (buckets.length != 0 && buckets.length != TAIL_LAYOUTS * TAIL_BUCKETS)) {
                throw new IllegalArgumentException("Invalid correlation slice");
            }
            boolean nonzero = false;
            for (double value : buckets) {
                if (!Double.isFinite(value) || value < 0 || value > tailNorm * (1 + 1e-9)) {
                    throw new IllegalArgumentException("Invalid correlation tail bucket");
                }
                nonzero |= value != 0;
            }
            if ((tailNorm > 0) != nonzero) {
                throw new IllegalArgumentException("Inconsistent correlation tail");
            }
            this.head = head;
            this.tailNorm = tailNorm;
            this.buckets = nonzero ? owned ? buckets : buckets.clone() : EMPTY_BUCKETS;
            this.shared = null;
            this.arity = 0;
            this.power = 0;
            this.presence = false;
        }

        private Slice(JoinStatisticsBasis.Slice shared, int arity, int power, boolean presence) {
            this.head = shared.getHead();
            this.tailNorm = shared.projectedRoot(arity, power, presence, -1);
            if (!Double.isFinite(tailNorm) || tailNorm < 0) {
                throw new IllegalArgumentException("Invalid projected correlation norm");
            }
            this.buckets = EMPTY_BUCKETS;
            this.shared = shared;
            this.arity = arity;
            this.power = power;
            this.presence = presence;
        }

        /** Immutable view: read the prepared source roots without allocating or copying a projected tail. */
        static Slice projected(JoinStatisticsBasis.Slice shared, int arity, int power, boolean presence) {
            if (arity < 2 || arity > 4 || power < 1 || power > 3) {
                throw new IllegalArgumentException("Invalid shared correlation projection");
            }
            return new Slice(shared, arity, power, presence);
        }

        static Slice preparedProjection(JoinStatisticsBasis.Slice shared, int arity, int power, boolean presence) {
            double[] buckets = new double[TAIL_LAYOUTS * TAIL_BUCKETS];
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = shared.projectedRoot(arity, power, presence, i);
            }
            return new Slice(shared.getHead(), shared.projectedRoot(arity, power, presence, -1), buckets, true);
        }

        private double bucket(int index) {
            if (extrapolated != null) {
                return extrapolated.root(extrapolatedArity, extrapolatedPresence, index);
            }
            return shared != null ? shared.projectedRoot(arity, power, presence, index)
                    : buckets.length == 0 ? 0 : buckets[index];
        }

        public CompactDegreeVector getHead() {
            return head;
        }

        public double getTailNorm() {
            return norm();
        }

        public double getBucket(int layout, int bucket) {
            if (layout < 0 || layout >= TAIL_LAYOUTS || bucket < 0 || bucket >= TAIL_BUCKETS) {
                throw new IllegalArgumentException("Invalid tail bucket index");
            }
            return bucket(layout * TAIL_BUCKETS + bucket);
        }

        public long estimatedSize() {
            return 64 + head.estimatedSize() + 8L * buckets.length;
        }

    }

    private static final double[] EMPTY_BUCKETS = new double[0];

    private final List<List<Slice>> sides;
    private final int presenceMask;
    private final int power;
    private final int headSize;

    public JoinStatisticsCorrelation(List<List<Slice>> sides, int presenceMask, int power) {
        if (sides.size() < 2 || sides.size() > 4 || presenceMask < 0 || presenceMask >= (1 << sides.size())
                || power < 1 || power > 3) {
            throw new IllegalArgumentException("Invalid correlation shape");
        }
        int size = -1;
        List<List<Slice>> copied = new ArrayList<>();
        for (List<Slice> side : sides) {
            if (side.isEmpty()) {
                throw new IllegalArgumentException("Missing correlation slice dictionary");
            }
            for (Slice slice : side) {
                if (size < 0) {
                    size = slice.head.size();
                } else if (size != slice.head.size()) {
                    throw new IllegalArgumentException("Incompatible correlation key dictionaries");
                }
            }
            copied.add(List.copyOf(side));
        }
        this.sides = List.copyOf(copied);
        this.presenceMask = presenceMask;
        this.power = power;
        this.headSize = size;
    }

    public int getSideCount() {
        return sides.size();
    }

    public int getSliceCount(int side) {
        return sides.get(side).size();
    }

    /** Reuses a tensor whose affected roles have unit frequencies in every collected slice. */
    public JoinStatisticsCorrelation withUnitPresenceRoles(int presence) {
        return withUnitPresenceRoles(presence, power);
    }

    /** The caller may reuse a different power only if all affected frequency slices contain 0/1 counts. */
    public JoinStatisticsCorrelation withUnitPresenceRoles(int presence, int momentPower) {
        return new JoinStatisticsCorrelation(sides, presence, momentPower);
    }

    public int getPresenceMask() {
        return presenceMask;
    }

    public int getPower() {
        return power;
    }

    public Slice getSlice(int side, int slice) {
        return sides.get(side).get(slice);
    }

    /** Bounded query-local memo shared by JOIN enumeration and RF projection; never retained in Caffeine. */
    static final class Evaluation {
        private record LayoutKey(int sources, int mask, List<Integer> domains, boolean reduceCommonKey) { }
        private final Map<LayoutKey, JoinStatisticsEstimate.Layout> layouts = new java.util.HashMap<>();

        JoinStatisticsEstimate.Layout layout(int sources, int mask, int[] domains, boolean reduceCommonKey) {
            LayoutKey key = new LayoutKey(sources, mask, Arrays.stream(domains).boxed().toList(), reduceCommonKey);
            JoinStatisticsEstimate.Layout prepared = layouts.get(key);
            if (prepared == null) {
                prepared = new JoinStatisticsEstimate.Layout(sources, mask, domains, reduceCommonKey);
                // At most 4 sources and 3 domains: <128KiB including primitive arrays and keys.
                if (layouts.size() < 256) {
                    layouts.put(key, prepared);
                }
            }
            return prepared;
        }

        private final JoinStatisticsEntropyModel.ShapeCache entropyShapes = new JoinStatisticsEntropyModel.ShapeCache();

        JoinStatisticsEntropyModel model(int attributes, boolean commonKeyStar) {
            return new JoinStatisticsEntropyModel(attributes, commonKeyStar, entropyShapes);
        }

        private static final long MEMO_BUDGET = 8L * 1024 * 1024 - 24 - 8L * HEAD_BUDGET;
        private record ProductKey(List<CompactDegreeVector> vectors, int presence) {
        }

        private record BasisKey(JoinStatisticsBasis basis, int side, JoinStatisticsSliceSet ids) {
        }

        private final Map<BasisKey, JoinStatisticsBasis.Slice> sharedSelections = new java.util.HashMap<>();

        JoinStatisticsBasis.Slice select(JoinStatisticsBasis basis, int side, int[] ids) {
            return select(basis, side, JoinStatisticsSliceSet.copyOf(ids));
        }

        JoinStatisticsBasis.Slice select(JoinStatisticsBasis basis, int side, JoinStatisticsSliceSet sliceSet) {
            int[] ids = sliceSet.ids();
            if (ids.length == 1) {
                return basis.getSlices(side).get(ids[0]);
            }
            BasisKey key = new BasisKey(basis, side, sliceSet);
            JoinStatisticsBasis.Slice selected = sharedSelections.get(key);
            if (selected == null) {
                selected = basis.union(side, ids);
                long bytes = 192L + sliceSet.estimatedSize() + selected.estimatedSize();
                if (sharedSelections.size() < 1024 && boundBytes + bytes <= MEMO_BUDGET) {
                    sharedSelections.put(key, selected);
                    boundBytes += bytes;
                }
            }
            return selected;
        }

        private record ExtrapolationKey(JoinStatisticsBasis basis, int side, JoinStatisticsSliceSet known,
                                        JoinStatisticsSliceSet remaining, double weight) { }
        private final Map<ExtrapolationKey, JoinStatisticsExtrapolation> extrapolations = new java.util.HashMap<>();

        JoinStatisticsExtrapolation extrapolate(JoinStatisticsBasis basis, int side, JoinStatisticsSliceSet known,
                                                JoinStatisticsSliceSet remaining, double weight) {
            ExtrapolationKey key = new ExtrapolationKey(basis, side, known, remaining, weight);
            JoinStatisticsExtrapolation prepared = extrapolations.get(key);
            if (prepared != null) {
                return prepared;
            }
            var a = select(basis, side, known);
            var b = select(basis, side, remaining);
            prepared = new JoinStatisticsExtrapolation(a, b, weight);
            long bytes = 512L + 16L * a.getHead().size() + a.estimatedSize() + b.estimatedSize()
                    + known.estimatedSize() + remaining.estimatedSize();
            if (extrapolations.size() < 1024 && boundBytes + bytes <= MEMO_BUDGET) {
                extrapolations.put(key, prepared);
                boundBytes += bytes;
            }
            return prepared;
        }

        private record ProjectionKey(JoinStatisticsBasis.Slice slice, int arity, int power, boolean presence) { }
        private final Map<ProjectionKey, Slice> projections = new java.util.HashMap<>();

        Slice project(JoinStatisticsBasis.Slice slice, int arity, int power, boolean presence) {
            if (!presence && !slice.hasUnitTail()) {
                return slice.project(arity, power, false);
            }
            ProjectionKey key = new ProjectionKey(slice, arity, power, presence);
            Slice prepared = projections.get(key);
            if (prepared != null) {
                return prepared;
            }
            long bytes = 256L + 8L * TAIL_LAYOUTS * TAIL_BUCKETS;
            if (projections.size() >= 1024 || boundBytes + bytes > MEMO_BUDGET) {
                return slice.project(arity, power, presence);
            }
            prepared = Slice.preparedProjection(slice, arity, power, presence);
            projections.put(key, prepared);
            boundBytes += bytes;
            return prepared;
        }

        private final Map<ProductKey, double[]> products = new java.util.HashMap<>();
        private long boundBytes;
        private double[] work;

        double estimateShared(JoinStatisticsCorrelation distribution, Slice[] selected) {
            if (Arrays.stream(selected).anyMatch(slice -> slice.extrapolated != null)) {
                int size = selected[0].head.size();
                if (work == null) {
                    work = new double[HEAD_BUDGET];
                }
                Arrays.fill(work, 0, size, 1);
                for (int side = 0; side < selected.length; side++) {
                    Slice slice = selected[side];
                    if (slice.extrapolated != null) {
                        slice.extrapolated.multiplyInto(work, slice.extrapolatedPresence);
                    } else {
                        slice.head.multiplyInto(work, (distribution.presenceMask & (1 << side)) != 0);
                    }
                }
                double head = 0;
                for (int key = 0; key < size; key++) {
                    head += work[key];
                }
                return distribution.estimateSlices(head, selected);
            }
            List<CompactDegreeVector> vectors = Arrays.stream(selected).map(Slice::getHead).toList();
            int presence = distribution.presenceMask;
            for (int i = 0; i < selected.length; i++) {
                if (selected[i].head.hasUnitFrequencies()) {
                    presence &= ~(1 << i);
                }
            }
            ProductKey key = new ProductKey(vectors, presence);
            double[] moments = products.get(key);
            if (moments == null) {
                if (work == null) {
                    work = new double[HEAD_BUDGET];
                }
                moments = CompactDegreeVector.products(vectors.toArray(CompactDegreeVector[]::new), presence, work);
                long bytes = 256 + vectors.stream().mapToLong(CompactDegreeVector::estimatedSize).sum();
                if (products.size() < 4096 && boundBytes + bytes <= MEMO_BUDGET) {
                    products.put(key, moments);
                    boundBytes += bytes;
                }
            }
            // Ordinary projections borrow prepared source roots. Unit/presence projections are
            // separately admitted by project() under the same bounded query-local budget.
            return distribution.estimateSlices(moments[distribution.power - 1], selected);
        }

        void clear() {
            sharedSelections.clear();
            entropyShapes.clear();
            layouts.clear();
            products.clear();
            projections.clear();
            extrapolations.clear();
            boundBytes = 0;
            work = null;
        }

        long entropyShapeBytes() {
            return entropyShapes.estimatedSize();
        }

        long estimatedSize() {
            return boundBytes + (work == null ? 0 : 24L + 8L * work.length);
        }
    }

    public double estimate(int leftSlice, int rightSlice) {
        if (sides.size() != 2) {
            throw new IllegalArgumentException("Not a two-sided correlation");
        }
        Slice left = getSlice(0, leftSlice);
        Slice right = getSlice(1, rightSlice);
        if (left.extrapolated != null || right.extrapolated != null) {
            return estimateSlices(left, right);
        }
        double head = left.head.product(right.head, (presenceMask & 1) != 0, (presenceMask & 2) != 0, power);
        double tail = left.tailNorm * right.tailNorm;
        if (tail != 0) {
            for (int layout = 0; layout < TAIL_LAYOUTS; layout++) {
                double bound = 0;
                for (int bucket = 0; bucket < TAIL_BUCKETS; bucket++) {
                    int index = layout * TAIL_BUCKETS + bucket;
                    bound += left.bucket(index) * right.bucket(index);
                }
                tail = Math.min(tail, bound);
            }
        }
        double result = head + tail;
        return result == 0 ? 0 : Math.nextUp(result * (1 + 1e-12));
    }

    public double estimate(int... sliceIds) {
        if (sliceIds.length != sides.size()) {
            throw new IllegalArgumentException("Invalid number of correlation slices");
        }
        Slice[] selected = new Slice[sliceIds.length];
        for (int i = 0; i < sliceIds.length; i++) {
            selected[i] = sides.get(i).get(sliceIds[i]);
        }
        return estimateSlices(selected);
    }

    /** Partial predicates union disjoint full tuples; no independent grouping-set statistics are required. */
    public Slice unionSlices(int side, int... sliceIds) {
        return unionSlices(side, sliceIds, null);
    }

    private Slice unionSlices(int side, int[] sliceIds, CompactDegreeVector preparedHead) {
        if (sliceIds.length == 1) {
            return getSlice(side, sliceIds[0]);
        }
        boolean[] seen = new boolean[getSliceCount(side)];
        long[] head = preparedHead == null ? new long[headSize] : null;
        double[] bucketRoots = new double[TAIL_LAYOUTS * TAIL_BUCKETS];
        double normRoot = 0;
        boolean presence = (presenceMask & (1 << side)) != 0;
        int rootPower = presence ? 1 : power;
        for (int id : sliceIds) {
            if (id < 0 || id >= seen.length || seen[id]) {
                throw new IllegalArgumentException("Invalid or duplicate slice ID");
            }
            seen[id] = true;
            Slice slice = getSlice(side, id);
            if (head != null) {
                slice.head.addTo(head);
            }
            // Minkowski for L_(k*p); summing raw p-th-power norms would understate cross terms.
            if (rootPower == 1) {
                normRoot += slice.tailNorm;
                for (int i = 0; i < bucketRoots.length; i++) {
                    bucketRoots[i] += slice.bucket(i);
                }
            } else if (rootPower == 2) {
                normRoot += Math.sqrt(slice.tailNorm);
                for (int i = 0; i < bucketRoots.length; i++) {
                    bucketRoots[i] += Math.sqrt(slice.bucket(i));
                }
            } else {
                normRoot += Math.cbrt(slice.tailNorm);
                for (int i = 0; i < bucketRoots.length; i++) {
                    bucketRoots[i] += Math.cbrt(slice.bucket(i));
                }
            }
        }
        if (!presence && power != 1) {
            normRoot = power == 2 ? normRoot * normRoot : normRoot * normRoot * normRoot;
            for (int i = 0; i < bucketRoots.length; i++) {
                double root = bucketRoots[i];
                bucketRoots[i] = power == 2 ? root * root : root * root * root;
            }
        }
        return new Slice(preparedHead == null ? CompactDegreeVector.copyOf(head) : preparedHead, normRoot,
                normRoot == 0 ? EMPTY_BUCKETS : bucketRoots, true);
    }

    public double estimateSlices(Slice... selected) {
        if (selected.length != sides.size()) {
            throw new IllegalArgumentException("Invalid number of correlation slices");
        }
        if (Arrays.stream(selected).anyMatch(slice -> slice.extrapolated != null)) {
            return new Evaluation().estimateShared(this, selected);
        }
        double head;
        if (selected.length == 2) {
            head = selected[0].head.product(selected[1].head, (presenceMask & 1) != 0,
                    (presenceMask & 2) != 0, power);
        } else {
            head = multipleHeadProduct(selected);
        }
        return estimateSlices(head, selected);
    }

    private double estimateSlices(double head, Slice[] selected) {
        if (selected.length != sides.size()) {
            throw new IllegalArgumentException("Invalid number of correlation slices");
        }
        double tail = 1;
        for (Slice slice : selected) {
            if (slice.head.size() != headSize) {
                throw new IllegalArgumentException("Incompatible correlation key dictionary");
            }
            tail *= slice.norm();
        }
        if (tail != 0) {
            for (int layout = 0; layout < TAIL_LAYOUTS; layout++) {
                double bound = 0;
                for (int bucket = 0; bucket < TAIL_BUCKETS; bucket++) {
                    double product = 1;
                    for (Slice slice : selected) {
                        product *= slice.bucket(layout * TAIL_BUCKETS + bucket);
                    }
                    bound += product;
                }
                tail = Math.min(tail, bound);
            }
        }
        double result = head + tail;
        return result == 0 ? 0 : Math.nextUp(result * (1 + 1e-12));
    }

    private double multipleHeadProduct(Slice[] selected) {
        CompactDegreeVector[] vectors = new CompactDegreeVector[selected.length];
        for (int i = 0; i < selected.length; i++) {
            vectors[i] = selected[i].head;
        }
        return CompactDegreeVector.product(vectors, presenceMask, power);
    }
}
