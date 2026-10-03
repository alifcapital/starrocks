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

import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** One immutable, prepared collection generation. Slice IDs are local to each source. */
public final class JoinStatisticsData {
    public static final int MAX_SLICES = 4096;

    public static final class Source {
        private final String tableUuid;
        private final long snapshot;
        private final long rows;
        private final List<String> columns;
        private final List<Type> types;
        private final List<List<String>> tuples;
        private final ConstantOperator[][] constants;
        private final long[] tupleRows;
        private final long coveredRows;
        private final JoinStatisticsSliceSet allSlices;
        private final Map<Integer, List<DegreeStatistics>> degrees;

        private Source(Source source, String role) {
            tableUuid = role;
            snapshot = source.snapshot;
            rows = source.rows;
            columns = source.columns;
            types = source.types;
            tuples = source.tuples;
            constants = source.constants;
            tupleRows = source.tupleRows;
            coveredRows = source.coveredRows;
            allSlices = source.allSlices;
            degrees = source.degrees;
        }

        public Source(String tableUuid, long snapshot, long rows, List<String> columns, List<Type> types,
                      List<List<String>> tuples, long[] tupleRows, Map<Integer, List<DegreeStatistics>> degrees) {
            if (tableUuid == null || rows < 0 || columns.size() != types.size() || tuples.size() != tupleRows.length
                    || tuples.size() > MAX_SLICES || columns.size() > 32) {
                throw new IllegalArgumentException("Invalid JOIN statistics source");
            }
            this.tableUuid = tableUuid;
            this.snapshot = snapshot;
            this.rows = rows;
            this.columns = List.copyOf(columns);
            this.types = List.copyOf(types);
            this.tupleRows = tupleRows.clone();
            this.constants = new ConstantOperator[tuples.size()][columns.size()];
            List<List<String>> copied = new ArrayList<>();
            long coveredRows = 0;
            Set<List<String>> unique = new java.util.HashSet<>();
            for (int i = 0; i < tuples.size(); i++) {
                List<String> tuple = tuples.get(i);
                if (tuple.size() != columns.size() || tupleRows[i] < 0 || !unique.add(tuple)) {
                    throw new IllegalArgumentException("Invalid JOIN statistics predicate tuple");
                }
                coveredRows = Math.addExact(coveredRows, tupleRows[i]);
                if (coveredRows > rows) {
                    throw new IllegalArgumentException("Predicate tuples exceed source row count");
                }
                copied.add(Collections.unmodifiableList(new ArrayList<>(tuple)));
                for (int column = 0; column < tuple.size(); column++) {
                    String value = tuple.get(column);
                    Type type = types.get(column);
                    constants[i][column] = value == null ? ConstantOperator.createNull(type)
                            : ConstantOperator.createVarchar(value).castTo(type).filter(literal -> !literal.isNull())
                            .orElseThrow(() -> new IllegalArgumentException("Invalid JOIN statistics predicate value"));
                }
            }
            this.tuples = List.copyOf(copied);
            this.coveredRows = coveredRows;
            this.allSlices = JoinStatisticsSliceSet.ordered(java.util.stream.IntStream.range(0, tuples.size()).toArray());
            Map<Integer, List<DegreeStatistics>> copiedDegrees = new HashMap<>();
            degrees.forEach((domain, distributions) -> {
                if (domain < 0 || distributions.size() != tuples.size()) {
                    throw new IllegalArgumentException("Invalid JOIN key distributions");
                }
                for (int i = 0; i < distributions.size(); i++) {
                    if (distributions.get(i).getRowCount() != tupleRows[i]) {
                        throw new IllegalArgumentException("Inconsistent JOIN key distribution row count");
                    }
                }
                copiedDegrees.put(domain, List.copyOf(distributions));
            });
            this.degrees = Map.copyOf(copiedDegrees);
        }

        public String getTableUuid() {
            return tableUuid;
        }

        public long getSnapshot() {
            return snapshot;
        }

        public long getRows() {
            return rows;
        }

        public List<String> getColumns() {
            return columns;
        }

        public List<Type> getTypes() {
            return types;
        }

        public List<List<String>> getTuples() {
            return tuples;
        }

        long coveredRows() {
            return coveredRows;
        }

        JoinStatisticsSliceSet allSlices() {
            return allSlices;
        }

        public long getTupleRows(int slice) {
            return tupleRows[slice];
        }

        ConstantOperator predicateValue(int slice, int column) {
            return constants[slice][column];
        }

        public Map<Integer, List<DegreeStatistics>> getDegrees() {
            return degrees;
        }

        private long estimatedSize() {
            long size = 528 + allSlices.estimatedSize() + 2L * tableUuid.length() + 48L * tuples.size();
            for (String column : columns) {
                size += 256 + 2L * column.length();
            }
            for (List<String> tuple : tuples) {
                size += 64 + 16L * tuple.size();
                for (String value : tuple) {
                    size += 256 + (value == null ? 0 : 4L * value.length());
                }
            }
            for (List<DegreeStatistics> group : degrees.values()) {
                size += 128;
                for (DegreeStatistics degree : group) {
                    size += 16 + degree.estimatedSize();
                }
            }
            return size;
        }
    }

    public static final class Correlation {
        private final int domain;
        private final List<Integer> sources;
        private final JoinStatisticsCorrelation distribution;

        public Correlation(int domain, List<Integer> sources, JoinStatisticsCorrelation distribution) {
            if (domain < 0 || sources.size() != distribution.getSideCount()
                    || new java.util.HashSet<>(sources).size() != sources.size()) {
                throw new IllegalArgumentException("Invalid correlation sources");
            }
            this.domain = domain;
            this.sources = List.copyOf(sources);
            this.distribution = distribution;
        }

        public int getDomain() {
            return domain;
        }

        public List<Integer> getSources() {
            return sources;
        }

        public JoinStatisticsCorrelation getDistribution() {
            return distribution;
        }
    }

    /** Correlation between two different keys of one source, over its observed key pairs. */
    public static final class IntraCorrelation {
        private final int source;
        private final int leftDomain;
        private final int rightDomain;
        private final long[] support;
        private final double[][] moments;

        public IntraCorrelation(int source, int leftDomain, int rightDomain, long[] support, double[][] moments) {
            if (source < 0 || leftDomain < 0 || rightDomain <= leftDomain || support.length != moments.length
                    || support.length > MAX_SLICES) {
                throw new IllegalArgumentException("Invalid intra-source correlation");
            }
            this.source = source;
            this.leftDomain = leftDomain;
            this.rightDomain = rightDomain;
            this.support = support.clone();
            this.moments = new double[moments.length][];
            for (int i = 0; i < support.length; i++) {
                if (support[i] < 0 || moments[i].length != 3) {
                    throw new IllegalArgumentException("Invalid intra-source correlation slice");
                }
                this.moments[i] = moments[i].clone();
                double previous = support[i];
                for (double moment : moments[i]) {
                    if (!Double.isFinite(moment) || moment < previous || (support[i] == 0) != (moment == 0)) {
                        throw new IllegalArgumentException("Invalid intra-source correlation moment");
                    }
                    previous = moment;
                }
            }
        }

        public int getSource() {
            return source;
        }

        public int getLeftDomain() {
            return leftDomain;
        }

        public int getRightDomain() {
            return rightDomain;
        }

        public int getSliceCount() {
            return support.length;
        }

        public long getSupport(int slice) {
            return support[slice];
        }

        public double getMoment(int slice, int power) {
            return moments[slice][power - 1];
        }

        private long estimatedSize() {
            return 144L + 64L * support.length;
        }
    }

    private final long objectId;
    private final long generation;
    private final List<Source> sources;
    private final List<JoinStatisticsBasis> bases;
    private final List<IntraCorrelation> intraCorrelations;
    private final long estimatedSize;

    public JoinStatisticsData(long objectId, long generation, List<Source> sources, List<JoinStatisticsBasis> bases) {
        this(objectId, generation, sources, bases, List.of());
    }

    public JoinStatisticsData(long objectId, long generation, List<Source> sources, List<JoinStatisticsBasis> bases,
                              List<IntraCorrelation> intraCorrelations) {
        if (objectId <= 0 || generation <= 0 || sources.size() < 2 || sources.size() > 4) {
            throw new IllegalArgumentException("Invalid JOIN statistics generation");
        }
        this.objectId = objectId;
        this.generation = generation;
        this.sources = List.copyOf(sources);
        long size = 256;
        for (Source source : sources) {
            size += source.estimatedSize();
        }
        if (bases.size() > com.starrocks.statistic.JoinStatisticsDefinition.MAX_DOMAINS) {
            throw new IllegalArgumentException("Too many JOIN statistics key bases");
        }
        Set<Integer> domains = new java.util.HashSet<>();
        for (JoinStatisticsBasis basis : bases) {
            if (!domains.add(basis.getDomain())) {
                throw new IllegalArgumentException("Duplicate JOIN statistics key basis");
            }
            for (int side = 0; side < basis.getSources().size(); side++) {
                int source = basis.getSources().get(side);
                if (source < 0 || source >= sources.size() || !sources.get(source).degrees.containsKey(basis.getDomain())
                        || sources.get(source).tuples.size() != basis.getSlices(side).size()) {
                    throw new IllegalArgumentException("JOIN basis does not match its source dictionary");
                }
            }
            size += basis.estimatedSize();
        }
        this.bases = List.copyOf(bases);
        for (IntraCorrelation intra : intraCorrelations) {
            if (intra.source >= sources.size() || sources.get(intra.source).tuples.size() != intra.getSliceCount()
                    || !sources.get(intra.source).degrees.keySet().containsAll(
                            List.of(intra.leftDomain, intra.rightDomain))) {
                throw new IllegalArgumentException("Intra-source correlation does not match its source dictionary");
            }
            for (int i = 0; i < intra.getSliceCount(); i++) {
                if (intra.support[i] > sources.get(intra.source).tupleRows[i]) {
                    throw new IllegalArgumentException("Joint key support exceeds its source row count");
                }
            }
            size += intra.estimatedSize();
        }
        this.intraCorrelations = List.copyOf(intraCorrelations);
        this.estimatedSize = size;
    }

    private JoinStatisticsData(JoinStatisticsData data, List<String> roles) {
        objectId = data.objectId;
        generation = data.generation;
        List<Source> mapped = new ArrayList<>();
        for (int i = 0; i < data.sources.size(); i++) {
            mapped.add(new Source(data.sources.get(i), roles.get(i)));
        }
        sources = List.copyOf(mapped);
        bases = data.bases;
        intraCorrelations = data.intraCorrelations;
        estimatedSize = data.estimatedSize;
    }

    public JoinStatisticsData withSourceRoles(List<String> roles) {
        if (roles.size() != sources.size()) {
            throw new IllegalArgumentException("Mismatched JOIN source role count");
        }
        for (int i = 0; i < roles.size(); i++) {
            if (!roles.get(i).equals(sources.get(i).tableUuid)) {
                return new JoinStatisticsData(this, roles);
            }
        }
        return this;
    }

    public long getObjectId() {
        return objectId;
    }

    public long getGeneration() {
        return generation;
    }

    public List<Source> getSources() {
        return sources;
    }

    public List<JoinStatisticsBasis> getBases() {
        return bases;
    }

    public List<IntraCorrelation> getIntraCorrelations() {
        return intraCorrelations;
    }

    public long estimatedSize() {
        return estimatedSize;
    }
}
