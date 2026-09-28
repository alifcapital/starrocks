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

/** Query-local best effort: measured slices plus a sample of the remaining observed key distribution. */
final class JoinStatisticsExtrapolation {
    private final JoinStatisticsBasis.Slice known;
    private final JoinStatisticsBasis.Slice remaining;
    private final double[] factors = new double[5];
    private final double[] frequencies;
    private final double[] membership;

    JoinStatisticsExtrapolation(JoinStatisticsBasis.Slice known, JoinStatisticsBasis.Slice remaining, double weight) {
        this.known = known;
        this.remaining = remaining;
        for (int arity = 2; arity <= 4; arity++) {
            factors[arity] = weight < 1 ? Math.pow(weight, 1.0 / arity) : weight;
        }
        double logMiss = weight < 1 ? Math.log1p(-weight) : Double.NEGATIVE_INFINITY;
        long[] a = new long[known.getHead().size()];
        long[] b = new long[a.length];
        known.getHead().addTo(a);
        remaining.getHead().addTo(b);
        frequencies = new double[a.length];
        membership = new double[a.length];
        for (int i = 0; i < a.length; i++) {
            frequencies[i] = a[i] + weight * b[i];
            // Row sampling: duplicate build rows improve membership probability, never multiply RF output.
            membership[i] = a[i] > 0 ? 1 : b[i] == 0 ? 0 : weight >= 1 ? 1
                    : -Math.expm1(b[i] * logMiss);
        }
    }

    void multiplyInto(double[] work, boolean presence) {
        double[] values = presence ? membership : frequencies;
        for (int i = 0; i < values.length; i++) {
            work[i] *= values[i];
        }
    }

    double root(int arity, boolean presence, int bucket) {
        double a = known.projectedRoot(arity, 1, presence, bucket);
        double b = remaining.projectedRoot(arity, 1, presence, bucket);
        // Without per-key tail degrees we cannot infer sampled support. Keep its support bound.
        // E[Binomial(d,q)^r] <= q*d^r for q<=1, rather than the invalid q^r*d^r.
        double factor = presence ? 1 : factors[arity];
        return a + factor * b;
    }

    JoinStatisticsCorrelation.Slice project(int arity, int power, boolean presence) {
        if (power != 1) {
            throw new IllegalArgumentException("Extrapolation does not provide higher correlation moments");
        }
        return JoinStatisticsCorrelation.Slice.extrapolated(this, known.getHead(), arity, presence);
    }
}
