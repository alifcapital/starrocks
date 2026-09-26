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

import java.util.Arrays;
import java.util.List;
import java.util.OptionalDouble;

/** One aligned key dictionary and reusable per-source summaries for an equality domain. */
public final class JoinStatisticsBasis {
    // Count for membership; L_r norms needed for 2..4 sides and powers 1..3. This set does not
    // grow with the number of table subsets or presence masks. Norms are prepared during load.
    private static final int[] ORDERS = {0, 2, 3, 4, 6, 8, 9, 12};
    public static final int TAIL_BUCKETS = 256;
    public static final int TAIL_LAYOUTS = 3;
    public static final int WIDTH = TAIL_BUCKETS * TAIL_LAYOUTS;

    OptionalDouble weightedPairEstimate(int left, int right, double[] a, double[] b, int presence, long budgetNanos) {
        long deadline = System.nanoTime() + Math.max(0, budgetNanos);
        for (Pair pair : pairs) {
            if (pair.left == left && pair.right == right) {
                double result = 0;
                // Linear in the existing matrix size, no new Cartesian state or cached role matrices.
                for (int i = 0; i < a.length; i++) {
                    if (a[i] == 0) {
                        continue;
                    }
                    if (System.nanoTime() >= deadline) {
                        return OptionalDouble.empty();
                    }
                    for (int j = 0; j < b.length; j++) {
                        result += a[i] * b[j] * pair.products[(i * pair.rightSize + j) * 4 + presence];
                    }
                }
                return OptionalDouble.of(result);
            }
        }
        return OptionalDouble.empty();
    }

    public static int[] momentOrders() {
        return ORDERS.clone();
    }

    public static final class Slice {
        private final CompactDegreeVector head;
        private final double[][] roots;
        private final double[] totals;
        private final boolean unit;
        private final double maximumFrequencyBound;
        // Zero means absent; otherwise the shared position in every moment row plus one.
        // A direct index keeps sparse bucket access O(1), including partial-slice unions.
        private final short[] bucketOffsets;

        /** Raw sums of frequency^r per bucket; r=0 counts distinct keys. Empty tails have no rows. */
        public Slice(CompactDegreeVector head, double[][] moments, boolean unit) {
            if (head.size() > JoinStatisticsCorrelation.HEAD_BUDGET
                    || (moments.length != 0 && moments.length != (unit ? 1 : ORDERS.length))) {
                throw new IllegalArgumentException("Invalid shared JOIN tail");
            }
            this.head = head;
            this.unit = unit;
            double[][] preparedRoots = new double[moments.length][];
            this.totals = new double[moments.length];
            for (int i = 0; i < moments.length; i++) {
                if (moments[i].length != WIDTH) {
                    throw new IllegalArgumentException("Invalid shared JOIN bucket count");
                }
                preparedRoots[i] = new double[WIDTH];
                for (int b = 0; b < WIDTH; b++) {
                    double value = moments[i][b];
                    if (!Double.isFinite(value) || value < 0) {
                        throw new IllegalArgumentException("Invalid shared JOIN moment");
                    }
                    if (i > 0 && ((value == 0) != (moments[0][b] == 0)
                            || value + Math.ulp(value) < moments[i - 1][b])) {
                        throw new IllegalArgumentException("Non-monotone shared JOIN moments");
                    }
                    preparedRoots[i][b] = i == 0 ? value : Math.pow(value, 1.0 / ORDERS[i]);
                    if (b < TAIL_BUCKETS) {
                        totals[i] += value;
                    }
                }
                if (!Double.isFinite(totals[i])) {
                    throw new IllegalArgumentException("Shared JOIN moment overflow");
                }
                if (i > 0) {
                    totals[i] = Math.pow(totals[i], 1.0 / ORDERS[i]);
                }
            }
            this.bucketOffsets = sparseOffsets(preparedRoots);
            this.roots = compact(preparedRoots, bucketOffsets);
            this.maximumFrequencyBound = computeMaximumFrequencyBound();
        }

        private Slice(CompactDegreeVector head, double[][] roots, double[] totals, boolean unit) {
            this.head = head;
            this.bucketOffsets = sparseOffsets(roots);
            this.roots = compact(roots, bucketOffsets);
            this.totals = totals;
            this.unit = unit;
            this.maximumFrequencyBound = computeMaximumFrequencyBound();
        }

        // Exact head identities survive a union of predicate slices. In the tail, each bucket's
        // L12 norm bounds every individual frequency, even after a Minkowski slice union.
        private double computeMaximumFrequencyBound() {
            double tail = 0;
            if (roots.length != 0) {
                tail = Double.POSITIVE_INFINITY;
                for (int layout = 0; layout < TAIL_LAYOUTS; layout++) {
                    double maximum = 0;
                    for (int bucket = 0; bucket < TAIL_BUCKETS; bucket++) {
                        maximum = Math.max(maximum, root(12, layout * TAIL_BUCKETS + bucket));
                    }
                    tail = Math.min(tail, maximum);
                }
                if (unit) {
                    tail = Math.min(tail, 1);
                }
            }
            return Math.max(head.maximumFrequency(), tail);
        }

        double maximumFrequencyBound() {
            return maximumFrequencyBound;
        }

        /** Bounds fanout on the intersection, without assuming uniqueness outside that intersection. */
        double maximumOnSupport(Slice support) {
            double tail = 0;
            if (roots.length != 0 && support.roots.length != 0) {
                tail = Double.POSITIVE_INFINITY;
                for (int layout = 0; layout < TAIL_LAYOUTS; layout++) {
                    double maximum = 0;
                    for (int bucket = 0; bucket < TAIL_BUCKETS; bucket++) {
                        int index = layout * TAIL_BUCKETS + bucket;
                        if (support.root(0, index) != 0) {
                            maximum = Math.max(maximum, root(12, index));
                        }
                    }
                    tail = Math.min(tail, maximum);
                }
                if (unit) {
                    tail = Math.min(tail, 1);
                }
            }
            return Math.max(head.maximumOnSupport(support.head), tail);
        }

        private static short[] sparseOffsets(double[][] roots) {
            if (roots.length == 0) {
                return null;
            }
            int nonzero = 0;
            for (double value : roots[0]) {
                nonzero += value != 0 ? 1 : 0;
            }
            if ((long) (WIDTH - nonzero) * roots.length * Double.BYTES <= 16L + WIDTH * Short.BYTES) {
                return null;
            }
            short[] offsets = new short[WIDTH];
            short next = 0;
            for (int bucket = 0; bucket < WIDTH; bucket++) {
                if (roots[0][bucket] != 0) {
                    offsets[bucket] = ++next;
                }
            }
            return offsets;
        }

        private static double[][] compact(double[][] roots, short[] offsets) {
            if (offsets == null) {
                return roots;
            }
            int count = 0;
            for (short offset : offsets) {
                count = Math.max(count, offset);
            }
            double[][] compact = new double[roots.length][count];
            for (int bucket = 0; bucket < WIDTH; bucket++) {
                int offset = offsets[bucket];
                if (offset != 0) {
                    for (int order = 0; order < roots.length; order++) {
                        compact[order][offset - 1] = roots[order][bucket];
                    }
                }
            }
            return compact;
        }

        public CompactDegreeVector getHead() {
            return head;
        }

        int storedOrders() {
            return roots.length;
        }

        boolean hasUnitTail() {
            return unit;
        }

        double storedRoot(int order, int bucket) {
            if (bucketOffsets == null) {
                return roots[order][bucket];
            }
            int offset = bucketOffsets[bucket];
            return offset == 0 ? 0 : roots[order][offset - 1];
        }

        double storedTotal(int order) {
            return totals[order];
        }

        /** Decode already prepared roots without repeating pow() or deriving totals from rounded roots. */
        static Slice prepared(CompactDegreeVector head, double[][] roots, double[] totals, boolean unit) {
            if (roots.length != totals.length || (roots.length != 0 && roots.length != (unit ? 1 : ORDERS.length))) {
                throw new IllegalArgumentException("Invalid prepared JOIN tail");
            }
            for (int i = 0; i < roots.length; i++) {
                if (roots[i].length != WIDTH || !Double.isFinite(totals[i]) || totals[i] < 0) {
                    throw new IllegalArgumentException("Invalid prepared JOIN tail shape");
                }
                boolean[] nonzero = new boolean[TAIL_LAYOUTS];
                for (int bucket = 0; bucket < WIDTH; bucket++) {
                    double value = roots[i][bucket];
                    if (!Double.isFinite(value) || value < 0 || value > totals[i] * (1 + 1e-12)
                            || (value == 0) != (roots[0][bucket] == 0)) {
                        throw new IllegalArgumentException("Invalid prepared JOIN tail root");
                    }
                    nonzero[bucket / TAIL_BUCKETS] |= value > 0;
                }
                for (boolean layout : nonzero) {
                    if (layout != (totals[i] > 0)) {
                        throw new IllegalArgumentException("Inconsistent prepared JOIN tail layout");
                    }
                }
            }
            return new Slice(head, roots, totals, unit);
        }

        private double root(int order, int bucket) {
            if (roots.length == 0) {
                return 0;
            }
            int slot = Arrays.binarySearch(ORDERS, order);
            if (slot < 0) {
                throw new IllegalArgumentException("Unsupported shared JOIN moment");
            }
            if (unit && slot != 0) {
                return Math.pow(bucket < 0 ? totals[0] : storedRoot(0, bucket), 1.0 / order);
            }
            return bucket < 0 ? totals[slot] : storedRoot(slot, bucket);
        }

        double projectedRoot(int arity, int power, boolean presence, int bucket) {
            return presence ? Math.pow(root(0, bucket), 1.0 / arity) : pow(root(arity * power, bucket), power);
        }

        JoinStatisticsCorrelation.Slice project(int arity, int power, boolean presence) {
            return JoinStatisticsCorrelation.Slice.projected(this, arity, power, presence);
        }

        public long estimatedSize() {
            long bytes = 112 + head.estimatedSize() + 24L + 8L * totals.length + 24L + 8L * roots.length;
            for (double[] row : roots) {
                bytes += 24L + 8L * row.length;
            }
            return bytes + (bucketOffsets == null ? 0 : 24L + 2L * bucketOffsets.length);
        }
    }

    /** Lossless pairwise inner products of frequency and support vectors; no powers or subset tails. */
    public static final class Pair {
        private final int left;
        private final int right;
        private final int leftSize;
        private final int rightSize;
        private final double[] products;

        public Pair(int left, int right, int leftSize, int rightSize, double[] products) {
            if (left < 0 || right <= left || right >= 4 || leftSize < 1 || rightSize < 1
                    || leftSize > JoinStatisticsData.MAX_SLICES || rightSize > JoinStatisticsData.MAX_SLICES
                    || products.length != Math.multiplyExact(Math.multiplyExact(leftSize, rightSize), 4)) {
                throw new IllegalArgumentException("Invalid pairwise JOIN matrix");
            }
            this.left = left;
            this.right = right;
            this.leftSize = leftSize;
            this.rightSize = rightSize;
            this.products = products.clone();
            for (int i = 0; i < products.length; i += 4) {
                for (int role = 0; role < 4; role++) {
                    double value = products[i + role];
                    if (!Double.isFinite(value) || value < 0 || value > products[i] * (1 + 1e-12)) {
                        throw new IllegalArgumentException("Invalid pairwise JOIN product");
                    }
                }
            }
        }

        int left() {
            return left;
        }

        int right() {
            return right;
        }

        int leftSize() {
            return leftSize;
        }

        int rightSize() {
            return rightSize;
        }

        int size() {
            return products.length;
        }

        double value(int index) {
            return products[index];
        }

        private double estimate(int[] leftIds, int[] rightIds, int presence) {
            double sum = 0;
            for (int l : leftIds) {
                for (int r : rightIds) {
                    sum += products[(l * rightSize + r) * 4 + presence];
                }
            }
            // Frequency union is linear. Support union is at most the sum of supports, never assumed disjoint.
            return sum;
        }

        long estimatedSize() {
            return 64L + 8L * products.length;
        }
    }

    private final JoinStatisticsHeadKeys headKeys;
    private final int domain;
    private final List<Integer> sources;
    private final List<List<Slice>> sides;
    private final List<Pair> pairs;

    public JoinStatisticsBasis(int domain, List<Integer> sources, List<List<Slice>> sides) {
        this(domain, sources, sides, List.of());
    }

    public JoinStatisticsBasis(int domain, List<Integer> sources, List<List<Slice>> sides, List<Pair> pairs) {
        this(domain, sources, sides, pairs, null);
    }

    public JoinStatisticsBasis(int domain, List<Integer> sources, List<List<Slice>> sides, List<Pair> pairs,
                               JoinStatisticsHeadKeys headKeys) {
        this.headKeys = headKeys;
        if (domain < 0 || domain >= com.starrocks.statistic.JoinStatisticsDefinition.MAX_DOMAINS
                || sources.size() < 2 || sources.size() > 4
                || sources.size() != sides.size() || sources.stream().distinct().count() != sources.size()) {
            throw new IllegalArgumentException("Invalid shared JOIN basis");
        }
        this.pairs = List.copyOf(pairs);
        boolean[][] seen = new boolean[sources.size()][sources.size()];
        for (Pair pair : pairs) {
            if (pair.right >= sources.size() || seen[pair.left][pair.right]
                    || pair.leftSize != sides.get(pair.left).size() || pair.rightSize != sides.get(pair.right).size()) {
                throw new IllegalArgumentException("Mismatched pairwise JOIN dictionary");
            }
            seen[pair.left][pair.right] = true;
        }
        this.domain = domain;
        this.sources = List.copyOf(sources);
        this.sides = sides.stream().map(List::copyOf).toList();
        int headSize = headKeys == null ? -1 : headKeys.size();
        for (List<Slice> side : sides) {
            if (side.isEmpty() || side.size() > JoinStatisticsData.MAX_SLICES) {
                throw new IllegalArgumentException("Invalid shared JOIN slices");
            }
            for (Slice slice : side) {
                if (headSize < 0) {
                    headSize = slice.head.size();
                }
                if (slice.head.size() != headSize) {
                    throw new IllegalArgumentException("Unaligned shared JOIN heads");
                }
            }
        }
    }

    public JoinStatisticsHeadKeys getHeadKeys() {
        return headKeys;
    }

    public List<Pair> getPairs() {
        return pairs;
    }

    OptionalDouble pairEstimate(int left, int right, int[] leftIds, int[] rightIds, int presence) {
        for (Pair pair : pairs) {
            if (pair.left == left && pair.right == right) {
                return OptionalDouble.of(pair.estimate(leftIds, rightIds, presence));
            }
        }
        return OptionalDouble.empty();
    }

    public int getDomain() {
        return domain;
    }

    public List<Integer> getSources() {
        return sources;
    }

    public List<Slice> getSlices(int side) {
        return sides.get(side);
    }

    public Slice union(int side, int[] ids) {
        if (ids.length == 1) {
            return sides.get(side).get(ids[0]);
        }
        long[] head = new long[sides.get(side).get(0).head.size()];
        double[][] roots = new double[ORDERS.length][WIDTH];
        double[] totals = new double[ORDERS.length];
        boolean[] seen = new boolean[sides.get(side).size()];
        boolean nonempty = false;
        for (int id : ids) {
            if (id < 0 || id >= seen.length || seen[id]) {
                throw new IllegalArgumentException("Invalid shared JOIN slice union");
            }
            seen[id] = true;
            Slice slice = sides.get(side).get(id);
            slice.head.addTo(head);
            nonempty |= slice.roots.length > 0;
            for (int p = 0; p < ORDERS.length; p++) {
                // Minkowski for frequency moments. Presence union is bounded by the sum of distinct counts;
                // counts must never be interpreted as an exact union when keys occur in multiple slices.
                totals[p] += slice.root(ORDERS[p], -1);
                for (int b = 0; b < WIDTH; b++) {
                    roots[p][b] += slice.root(ORDERS[p], b);
                }
            }
        }
        return new Slice(CompactDegreeVector.copyOf(head), nonempty ? roots : new double[0][],
                nonempty ? totals : new double[0], false);
    }

    public long estimatedSize() {
        long bytes = 160 + 8L * pairs.size() + (headKeys == null ? 0 : headKeys.estimatedSize());
        for (Pair pair : pairs) {
            bytes += pair.estimatedSize();
        }
        for (List<Slice> side : sides) {
            bytes += 48 + 8L * side.size();
            for (Slice slice : side) {
                bytes += slice.estimatedSize();
            }
        }
        return bytes;
    }

    private static double pow(double x, int p) {
        return p == 1 ? x : p == 2 ? x * x : x * x * x;
    }
}
