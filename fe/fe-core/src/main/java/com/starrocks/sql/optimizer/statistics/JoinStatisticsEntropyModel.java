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

import org.apache.commons.math3.exception.MathIllegalStateException;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.optim.MaxIter;
import org.apache.commons.math3.optim.PointValuePair;
import org.apache.commons.math3.optim.linear.LinearConstraint;
import org.apache.commons.math3.optim.linear.LinearConstraintSet;
import org.apache.commons.math3.optim.linear.LinearObjectiveFunction;
import org.apache.commons.math3.optim.linear.NonNegativeConstraint;
import org.apache.commons.math3.optim.linear.PivotSelectionRule;
import org.apache.commons.math3.optim.linear.Relationship;
import org.apache.commons.math3.optim.linear.SimplexSolver;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Joint entropy constraints for one connected inner-join subplan. Each relation has a private
 * row-identity attribute so that duplicate rows contribute to JOIN, but not to RF membership.
 * Masks refer to attributes of this model, never to table or column IDs.
 */
public final class JoinStatisticsEntropyModel {
    // The Shannon cone is exponential. Larger subplans must use the ordinary estimator.
    public static final int MAX_ATTRIBUTES = 7;
    private static final int MAX_STATISTIC_CONSTRAINTS = 1024;
    private static final double LOG_2 = Math.log(2);
    private static final double TOLERANCE = 1e-8;
    private static final Map<Integer, List<LinearConstraint>> SHANNON_CONSTRAINTS = new ConcurrentHashMap<>();

    private final int attributes;
    private final int fullMask;
    private final boolean commonKeyStar;
    private final ShapeCache shapes;
    private final java.util.Set<LinearConstraint> constraints = new java.util.LinkedHashSet<>();
    private final List<int[]> dependencies = new ArrayList<>();
    private boolean empty;

    public JoinStatisticsEntropyModel(int attributes) {
        this(attributes, false);
    }

    private JoinStatisticsEntropyModel(int attributes, boolean commonKeyStar) {
        this(attributes, commonKeyStar, null);
    }

    JoinStatisticsEntropyModel(int attributes, boolean commonKeyStar, ShapeCache shapes) {
        if (attributes < 1 || attributes > MAX_ATTRIBUTES) {
            throw new IllegalArgumentException("Unsupported entropy model size");
        }
        this.attributes = attributes;
        this.fullMask = (1 << attributes) - 1;
        this.commonKeyStar = commonKeyStar;
        this.shapes = shapes;
    }

    /**
     * Exact reduction when attribute 0 is the common key and each other attribute is a row identity
     * determining that key. Set h=H(key), c_i=H(row_i|key). Every stored constraint has the form
     * h + sum(p_i*c_i) <= bound. Conditional independence attains h + sum(c_i) for the output rows,
     * and defines a feasible Shannon polymatroid for every nonnegative h,c_i. Thus this smaller LP
     * has exactly the same optimum as the full Shannon LP, including RF projections.
     */
    static JoinStatisticsEntropyModel commonKeyStar(int attributes) {
        return new JoinStatisticsEntropyModel(attributes, true);
    }

    public void addCardinality(int projection, double rows) {
        checkMask(projection);
        double[] coefficients = new double[fullMask];
        coefficients[projection - 1] = 1;
        addBound(coefficients, rows);
    }

    public void addFunctionalDependency(int determinant, int dependent) {
        checkMask(determinant);
        checkMask(dependent);
        dependencies.add(new int[] {determinant, dependent});
        double[] coefficients = new double[fullMask];
        coefficients[(determinant | dependent) - 1] += 1;
        coefficients[determinant - 1] -= 1;
        addBound(coefficients, 1);
    }

    public void addDegree(int key, int relation, DegreeStatistics statistics) {
        checkMask(key);
        checkMask(relation);
        if ((key & relation) != key) {
            throw new IllegalArgumentException("Join key must belong to the relation");
        }
        addCardinality(key, statistics.getDistinctCount());
        addMaximumFrequency(key, relation, statistics.getMaximumFrequency());
        for (int power = 1; power <= DegreeStatistics.MOMENT_COUNT; power++) {
            addMoment(key, relation, power, statistics.getMoment(power));
        }
    }

    void addMaximumFrequency(int key, int relation, double maximumFrequency) {
        checkMask(key);
        checkMask(relation);
        if ((key & relation) != key) {
            throw new IllegalArgumentException("Join key must belong to the relation");
        }
        double[] maximum = new double[fullMask];
        maximum[relation - 1] += 1;
        maximum[key - 1] -= 1;
        addBound(maximum, maximumFrequency);
        if (maximumFrequency == 1) {
            // H(relation | key) <= 0 is exactly a functional dependency, including a fanout
            // bound established only on the participating JOIN-key intersection.
            dependencies.add(new int[] {key, relation});
        }
    }

    public void addMoment(int key, int relation, int power, double moment) {
        checkMask(key);
        checkMask(relation);
        if (power < 1 || power > DegreeStatistics.MOMENT_COUNT || (key & relation) != key) {
            throw new IllegalArgumentException("Invalid frequency moment constraint");
        }
        double[] coefficients = new double[fullMask];
        coefficients[key - 1] += 1 - power;
        coefficients[relation - 1] += power;
        addBound(coefficients, moment);
    }

    /**
     * H(X) + sum p_i (H(X_i,Y_i) - H(X_i)) <= log2(correlation).
     * Presence is represented by relation_i == key_i. The same constraint supports inter-table
     * correlations and correlations of different keys inside one relation.
     */
    public void addCorrelation(int projection, int[] keys, int[] relations, int[] powers, double correlation) {
        checkMask(projection);
        if (keys.length < 2 || keys.length > 4 || keys.length != relations.length || keys.length != powers.length) {
            throw new IllegalArgumentException("Invalid correlation dimensions");
        }
        double[] coefficients = new double[fullMask];
        coefficients[projection - 1] = 1;
        for (int i = 0; i < keys.length; i++) {
            checkMask(keys[i]);
            checkMask(relations[i]);
            if ((keys[i] & projection) != keys[i] || (keys[i] & relations[i]) != keys[i]
                    || powers[i] < 1 || powers[i] > DegreeStatistics.MOMENT_COUNT) {
                throw new IllegalArgumentException("Invalid correlation projection");
            }
            coefficients[relations[i] - 1] += powers[i];
            coefficients[keys[i] - 1] -= powers[i];
        }
        addBound(coefficients, correlation);
    }

    private void addBound(double[] coefficients, double value) {
        if (!Double.isFinite(value) || value < 0 || (value > 0 && value < 1)) {
            throw new IllegalArgumentException("Invalid cardinality bound");
        }
        if (constraints.size() >= MAX_STATISTIC_CONSTRAINTS) {
            throw new IllegalArgumentException("Too many entropy constraints");
        }
        if (value == 0) {
            empty = true;
        } else {
            if (commonKeyStar) {
                coefficients = shapes == null ? starCoefficients(attributes, coefficients)
                        : shapes.reduceStar(attributes, coefficients);
                if (java.util.Arrays.stream(coefficients).allMatch(coefficient -> coefficient == 0)) {
                    return;
                }
            }
            constraints.add(new LinearConstraint(coefficients, Relationship.LEQ, Math.log(value) / LOG_2));
        }
    }

    private static double[] starCoefficients(int attributes, double[] coefficients) {
        double[] reduced = new double[attributes];
        for (int mask = 1; mask <= coefficients.length; mask++) {
            reduced[0] += coefficients[mask - 1];
            for (int row = 1; row < attributes; row++) {
                if ((mask & (1 << row)) != 0) {
                    reduced[row] += coefficients[mask - 1];
                }
            }
        }
        return reduced;
    }

    /** Import constraints, mapping shared row/key identities only once. Never add cardinalities together. */
    void include(JoinStatisticsEntropyModel other, int[] attributeMasks) {
        if (other.commonKeyStar || attributeMasks.length != other.attributes) {
            throw new IllegalArgumentException("Composition requires unreduced entropy coordinates");
        }
        for (int mask : attributeMasks) {
            checkMask(mask);
        }
        empty |= other.empty;
        for (int[] dependency : other.dependencies) {
            dependencies.add(new int[] {mapMask(dependency[0], attributeMasks),
                    mapMask(dependency[1], attributeMasks)});
        }
        for (LinearConstraint constraint : other.constraints) {
            double[] coefficients = new double[fullMask];
            for (int mask = 1; mask <= other.fullMask; mask++) {
                double coefficient = constraint.getCoefficients().getEntry(mask - 1);
                if (coefficient == 0) {
                    continue;
                }
                int mapped = 0;
                for (int bit = 0; bit < attributeMasks.length; bit++) {
                    if ((mask & (1 << bit)) != 0) {
                        checkMask(attributeMasks[bit]);
                        mapped |= attributeMasks[bit];
                    }
                }
                coefficients[mapped - 1] += coefficient;
            }
            if (commonKeyStar) {
                coefficients = shapes == null ? starCoefficients(attributes, coefficients)
                        : shapes.reduceStar(attributes, coefficients);
            }
            constraints.add(new LinearConstraint(coefficients, Relationship.LEQ, constraint.getValue()));
            if (constraints.size() > MAX_STATISTIC_CONSTRAINTS) {
                throw new IllegalArgumentException("Too many composed entropy constraints");
            }
        }
    }

    /** Query-local structural templates. Bounds and simplex tableaux are never shared. */
    static final class ShapeCache {
        private static final long MAX_BYTES = 4L * 1024 * 1024;
        private record Key(int attributes, List<Long> dependencies) { }
        private final Map<Key, Shape> shapes = new LinkedHashMap<>();
        private record StarKey(int attributes, RealVector row) { }
        private final Map<StarKey, double[]> starRows = new LinkedHashMap<>();

        double[] reduceStar(int attributes, double[] coefficients) {
            StarKey key = new StarKey(attributes, new ArrayRealVector(coefficients, false));
            double[] reduced = starRows.get(key);
            if (reduced != null) {
                return reduced;
            }
            reduced = starCoefficients(attributes, coefficients);
            long charge = 192L + 8L * (coefficients.length + reduced.length);
            if (starRows.size() < 1024 && bytes + charge <= MAX_BYTES) {
                starRows.put(new StarKey(attributes, key.row().copy()), reduced);
                bytes += charge;
            }
            return reduced;
        }

        private long bytes;

        private Shape get(JoinStatisticsEntropyModel model) {
            List<Long> dependencies = model.dependencies.stream()
                    .map(d -> ((long) d[0] << 32) | (d[1] & 0xffffffffL)).distinct().sorted().toList();
            Key key = new Key(model.attributes, dependencies);
            Shape shape = shapes.get(key);
            if (shape != null) {
                return shape;
            }
            int[] coordinates = model.closureCoordinates();
            int variables = Arrays.stream(coordinates).max().orElseThrow() + 1;
            List<LinearConstraint> shannon = quotientConstraints(SHANNON_CONSTRAINTS.computeIfAbsent(model.attributes,
                    JoinStatisticsEntropyModel::shannonConstraints), coordinates, variables);
            shape = new Shape(coordinates, variables, shannon);
            long charge = 256L + 32L * dependencies.size() + 4L * coordinates.length
                    + (128L + 8L * variables) * shannon.size();
            if (shapes.size() < 64 && bytes + charge <= MAX_BYTES) {
                shapes.put(key, shape);
                bytes += charge;
                shape.owner = this;
            }
            return shape;
        }

        long estimatedSize() {
            return bytes;
        }

        void clear() {
            shapes.clear();
            starRows.clear();
            bytes = 0;
        }
    }

    private static final class Shape {
        private final int[] coordinates;
        private final int variables;
        private final List<LinearConstraint> shannon;
        private final Map<RealVector, RealVector> rows = new LinkedHashMap<>();
        private ShapeCache owner;

        private Shape(int[] coordinates, int variables, List<LinearConstraint> shannon) {
            this.coordinates = coordinates;
            this.variables = variables;
            this.shannon = shannon;
        }

        private RealVector reduce(RealVector original) {
            RealVector reduced = rows.get(original);
            if (reduced != null) {
                return reduced;
            }
            double[] values = new double[variables];
            for (int i = 0; i < coordinates.length; i++) {
                values[coordinates[i]] += original.getEntry(i);
            }
            reduced = new ArrayRealVector(values, false);
            long charge = 192L + 8L * (coordinates.length + variables);
            if (owner != null && rows.size() < 1024 && owner.bytes + charge <= ShapeCache.MAX_BYTES) {
                rows.put(original.copy(), reduced);
                owner.bytes += charge;
            }
            return reduced;
        }

        private List<LinearConstraint> constraints(java.util.Collection<LinearConstraint> dynamic) {
            Map<RealVector, Double> bounds = new LinkedHashMap<>();
            for (LinearConstraint constraint : shannon) {
                bounds.put(constraint.getCoefficients(), constraint.getValue());
            }
            for (LinearConstraint constraint : dynamic) {
                RealVector reduced = reduce(constraint.getCoefficients());
                boolean nonzero = false;
                for (int i = 0; i < variables; i++) {
                    nonzero |= reduced.getEntry(i) != 0;
                }
                if (nonzero) {
                    bounds.merge(reduced, constraint.getValue(), Math::min);
                }
            }
            List<LinearConstraint> result = new ArrayList<>(bounds.size());
            bounds.forEach((row, bound) -> result.add(new LinearConstraint(row, Relationship.LEQ, bound)));
            return result;
        }
    }

    private void checkMask(int mask) {
        if (mask <= 0 || mask > fullMask) {
            throw new IllegalArgumentException("Invalid entropy attribute mask");
        }
    }

    private static int mapMask(int mask, int[] attributeMasks) {
        int result = 0;
        for (int bit = 0; bit < attributeMasks.length; bit++) {
            if ((mask & (1 << bit)) != 0) {
                result |= attributeMasks[bit];
            }
        }
        return result;
    }

    /**
     * A functional dependency makes H(S) equal to H(closure(S)). Substitute one variable per
     * closure into ALL Shannon and statistic inequalities; no inequality is relaxed. This is an
     * exact quotient of the original LP, also for projections used by SEMI JOIN and RF.
     */
    private int[] closureCoordinates() {
        int[] coordinates = new int[fullMask];
        int[] indexes = new int[fullMask + 1];
        Arrays.fill(indexes, -1);
        int variables = 0;
        for (int mask = 1; mask <= fullMask; mask++) {
            int closure = mask;
            int previous;
            do {
                previous = closure;
                for (int[] dependency : dependencies) {
                    if ((closure & dependency[0]) == dependency[0]) {
                        closure |= dependency[1];
                    }
                }
            } while (closure != previous);
            if (indexes[closure] < 0) {
                indexes[closure] = variables++;
            }
            coordinates[mask - 1] = indexes[closure];
        }
        return coordinates;
    }

    private static List<LinearConstraint> quotientConstraints(List<LinearConstraint> original,
                                                              int[] coordinates, int variables) {
        Map<RealVector, Double> bounds = new LinkedHashMap<>();
        for (LinearConstraint constraint : original) {
            double[] reduced = new double[variables];
            RealVector coefficients = constraint.getCoefficients();
            for (int i = 0; i < coordinates.length; i++) {
                reduced[coordinates[i]] += coefficients.getEntry(i);
            }
            if (Arrays.stream(reduced).anyMatch(value -> value != 0)) {
                bounds.merge(new ArrayRealVector(reduced, false), constraint.getValue(), Math::min);
            }
        }
        List<LinearConstraint> result = new ArrayList<>(bounds.size());
        bounds.forEach((coefficients, bound) -> result.add(new LinearConstraint(coefficients, Relationship.LEQ, bound)));
        return result;
    }

    /** An empty result means no usable estimate, not an empty JOIN. */
    public OptionalDouble estimate(int objective, long budgetNanos) {
        return estimate(objective, budgetNanos, true);
    }

    // The unreduced formulation is retained as a test oracle for the exact substitution.
    OptionalDouble estimate(int objective, long budgetNanos, boolean reduceDependencies) {
        checkMask(objective);
        if (empty) {
            return OptionalDouble.of(0);
        }
        if (budgetNanos <= 0 || Thread.currentThread().isInterrupted()) {
            return OptionalDouble.empty();
        }
        long started = System.nanoTime();
        List<LinearConstraint> all;
        int[] coordinates = null;
        int variables = commonKeyStar ? attributes : fullMask;
        if (!commonKeyStar && reduceDependencies && !dependencies.isEmpty() && shapes != null) {
            Shape shape = shapes.get(this);
            coordinates = shape.coordinates;
            variables = shape.variables;
            all = shape.constraints(constraints);
        } else {
            all = commonKeyStar ? new ArrayList<>()
                    : new ArrayList<>(SHANNON_CONSTRAINTS.computeIfAbsent(attributes,
                            JoinStatisticsEntropyModel::shannonConstraints));
            all.addAll(constraints);
            coordinates = !commonKeyStar && reduceDependencies && !dependencies.isEmpty()
                    ? closureCoordinates() : null;
            if (coordinates != null) {
                variables = Arrays.stream(coordinates).max().orElseThrow() + 1;
                all = quotientConstraints(all, coordinates, variables);
            }
        }
        double[] coefficients = new double[variables];
        if (commonKeyStar) {
            coefficients[0] = 1;
            for (int row = 1; row < attributes; row++) {
                if ((objective & (1 << row)) != 0) {
                    coefficients[row] = 1;
                }
            }
        } else {
            coefficients[coordinates == null ? objective - 1 : coordinates[objective - 1]] = 1;
        }
        SimplexSolver solver = new SimplexSolver(TOLERANCE, 10, 1e-12) {
            @Override
            protected void incrementIterationCount() {
                if (Thread.currentThread().isInterrupted() || System.nanoTime() - started >= budgetNanos) {
                    throw new BudgetExceededException();
                }
                super.incrementIterationCount();
            }
        };
        try {
            PointValuePair optimum = solver.optimize(new MaxIter(4096), new LinearObjectiveFunction(coefficients, 0),
                    new LinearConstraintSet(all), GoalType.MAXIMIZE, new NonNegativeConstraint(true),
                    PivotSelectionRule.BLAND);
            if (System.nanoTime() - started >= budgetNanos || !validSolution(optimum, all)) {
                return OptionalDouble.empty();
            }
            double rows = Math.pow(2, optimum.getValue() + TOLERANCE);
            return Double.isFinite(rows) ? OptionalDouble.of(rows) : OptionalDouble.empty();
        } catch (MathIllegalStateException | BudgetExceededException e) {
            return OptionalDouble.empty();
        }
    }

    private static boolean validSolution(PointValuePair optimum, List<LinearConstraint> constraints) {
        if (!Double.isFinite(optimum.getValue()) || optimum.getValue() < -TOLERANCE) {
            return false;
        }
        double[] point = optimum.getPointRef();
        for (double value : point) {
            if (!Double.isFinite(value) || value < -TOLERANCE) {
                return false;
            }
        }
        for (LinearConstraint constraint : constraints) {
            double actual = 0;
            for (int i = 0; i < point.length; i++) {
                actual += constraint.getCoefficients().getEntry(i) * point[i];
            }
            if (actual > constraint.getValue() + 1e-6) {
                return false;
            }
        }
        return true;
    }

    private static List<LinearConstraint> shannonConstraints(int attributes) {
        int all = (1 << attributes) - 1;
        List<LinearConstraint> result = new ArrayList<>();
        for (int i = 0; i < attributes; i++) {
            double[] coefficients = new double[all];
            coefficients[all - 1] = -1;
            int rest = all ^ (1 << i);
            if (rest != 0) {
                coefficients[rest - 1] += 1;
            }
            result.add(new LinearConstraint(coefficients, Relationship.LEQ, 0));
        }
        for (int i = 0; i < attributes; i++) {
            for (int j = i + 1; j < attributes; j++) {
                int pair = (1 << i) | (1 << j);
                int rest = all ^ pair;
                int subset = rest;
                while (true) {
                    double[] coefficients = new double[all];
                    if (subset != 0) {
                        coefficients[subset - 1] += 1;
                    }
                    coefficients[(subset | pair) - 1] += 1;
                    coefficients[(subset | (1 << i)) - 1] -= 1;
                    coefficients[(subset | (1 << j)) - 1] -= 1;
                    result.add(new LinearConstraint(coefficients, Relationship.LEQ, 0));
                    if (subset == 0) {
                        break;
                    }
                    subset = (subset - 1) & rest;
                }
            }
        }
        return List.copyOf(result);
    }

    private static class BudgetExceededException extends RuntimeException {
    }
}
