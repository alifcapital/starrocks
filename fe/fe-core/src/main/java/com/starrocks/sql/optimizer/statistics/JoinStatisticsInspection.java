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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.catalog.Column;
import com.starrocks.qe.ShowResultSet;
import com.starrocks.qe.ShowResultSetMetaData;
import com.starrocks.statistic.JoinStatisticsMeta;
import com.starrocks.type.TypeFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Read-only, paginated rendering of a collected generation, outside the optimizer path. */
public final class JoinStatisticsInspection {
    private static final Gson JSON = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();
    private static final int MAX_OUTPUT_BYTES = 32 * 1024 * 1024;

    private JoinStatisticsInspection() {
    }

    public static ShowResultSet show(JoinStatisticsMeta meta, JoinStatisticsData data, long offset, long limit) {
        if (offset < 0 || limit < 0 || limit > 1000) {
            throw new IllegalArgumentException("Inspection LIMIT must be between 0 and 1000 and OFFSET non-negative");
        }
        if (meta.getId() != data.getObjectId() || meta.getGeneration() != data.getGeneration()) {
            throw new IllegalArgumentException("JOIN statistics inspection generation mismatch");
        }
        Page page = new Page(data.getGeneration(), offset, limit);
        var definition = meta.getDefinition();
        page.add("OBJECT", -1, -1, -1, fields("name", definition.getName(), "object_id", meta.getId(),
                "collected_at_ms", meta.getCollectedAt(), "stored_bytes", meta.getPayloadBytes(),
                "prepared_bytes", data.estimatedSize()));
        for (int source = 0; source < data.getSources().size() && !page.full(); source++) {
            var value = data.getSources().get(source);
            page.add("SOURCE", source, -1, -1, fields("table", definition.getSources().get(source).getTableName().toString(),
                    "role", value.getTableUuid(), "snapshot", value.getSnapshot(), "rows", value.getRows(),
                    "predicate_columns", value.getColumns(), "predicate_types", value.getTypes().stream()
                            .map(type -> type.toSql()).toList(), "slices", value.getTuples().size()));
            for (int slice = 0; slice < value.getTuples().size() && !page.full(); slice++) {
                page.add("SLICE", source, -1, slice,
                        fields("values", value.getTuples().get(slice), "rows", value.getTupleRows(slice)));
            }
            for (int domain : value.getDegrees().keySet().stream().sorted().toList()) {
                List<DegreeStatistics> degrees = value.getDegrees().get(domain);
                if (page.skip(degrees.size())) {
                    continue;
                }
                for (int slice = 0; slice < degrees.size() && !page.full(); slice++) {
                    if (page.skip(1)) {
                        continue;
                    }
                    var degree = degrees.get(slice);
                    Map<Integer, Double> moments = new LinkedHashMap<>();
                    for (int power = 1; power <= DegreeStatistics.MOMENT_COUNT; power++) {
                        moments.put(power, degree.getMoment(power));
                    }
                    page.add("DEGREE", source, domain, slice, fields("rows", degree.getRowCount(),
                            "null_rows", degree.getNullCount(), "ndv", degree.getDistinctCount(),
                            "maximum_frequency", degree.getMaximumFrequency(), "frequency_moments", moments));
                }
            }
        }
        for (int domain = 0; domain < definition.getDomains().size() && !page.full(); domain++) {
            var key = definition.getDomains().get(domain);
            page.add("DOMAIN", -1, domain, -1, fields("source_columns", key.getColumns(), "types", key.getTypes()));
        }
        for (JoinStatisticsBasis basis : data.getBases()) {
            if (page.full()) {
                break;
            }
            int domain = basis.getDomain();
            int headSize = basis.getSlices(0).isEmpty() ? 0 : basis.getSlices(0).get(0).getHead().size();
            page.add("BASIS", -1, domain, -1, fields("sources", basis.getSources(), "head_positions", headSize,
                    "tail_layouts", JoinStatisticsBasis.TAIL_LAYOUTS, "buckets_per_layout", JoinStatisticsBasis.TAIL_BUCKETS));
            if (!page.skip(headSize)) {
                int arity = definition.getDomains().get(domain).getTypes().size();
                for (int index = 0; index < headSize && !page.full(); index++) {
                    if (page.skip(1)) {
                        continue;
                    }
                    var keys = basis.getHeadKeys();
                    List<String> values = keys == null ? List.of() : keys.tuple(index, arity);
                    page.add("HEAD_KEY", -1, domain, -1, fields("key_index", index,
                            "label_retained", !values.isEmpty(), "values", values.isEmpty() ? null : values));
                }
            }
            for (int side = 0; side < basis.getSources().size() && !page.full(); side++) {
                int source = basis.getSources().get(side);
                var slices = basis.getSlices(side);
                for (int slice = 0; slice < slices.size() && !page.full(); slice++) {
                    var value = slices.get(slice);
                    if (!page.skip(value.getHead().nonZeroCount())) {
                        long[] frequencies = new long[value.getHead().size()];
                        value.getHead().addTo(frequencies);
                        for (int key = 0; key < frequencies.length && !page.full(); key++) {
                            if (frequencies[key] != 0) {
                                page.add("HEAD", source, domain, slice,
                                        fields("key_index", key, "frequency", frequencies[key]));
                            }
                        }
                    }
                    for (int bucket = 0; bucket < JoinStatisticsBasis.WIDTH && !page.full(); bucket++) {
                        if (value.storedOrders() == 0 || value.storedRoot(0, bucket) == 0 || page.skip(1)) {
                            continue;
                        }
                        Map<Integer, Double> norms = new LinkedHashMap<>();
                        int[] orders = JoinStatisticsBasis.momentOrders();
                        for (int order = 0; order < value.storedOrders(); order++) {
                            norms.put(orders[order], value.storedRoot(order, bucket));
                        }
                        page.add("TAIL", source, domain, slice, fields("layout", bucket / JoinStatisticsBasis.TAIL_BUCKETS,
                                "bucket", bucket % JoinStatisticsBasis.TAIL_BUCKETS,
                                "unit_frequencies", value.hasUnitTail(), "stored_lp_norms", norms));
                    }
                }
            }
            for (var pair : basis.getPairs()) {
                if (page.skip((long) pair.leftSize() * pair.rightSize())) {
                    continue;
                }
                for (int left = 0; left < pair.leftSize() && !page.full(); left++) {
                    for (int right = 0; right < pair.rightSize() && !page.full(); right++) {
                        if (page.skip(1)) {
                            continue;
                        }
                        int index = (left * pair.rightSize() + right) * 4;
                        page.add("PAIR", -1, domain, -1, fields("left_source", basis.getSources().get(pair.left()),
                                "right_source", basis.getSources().get(pair.right()), "left_slice", left, "right_slice", right,
                                "products_by_presence_mask", List.of(pair.value(index), pair.value(index + 1),
                                        pair.value(index + 2), pair.value(index + 3))));
                    }
                }
            }
        }
        for (var intra : data.getIntraCorrelations()) {
            for (int slice = 0; slice < intra.getSliceCount() && !page.full(); slice++) {
                if (page.skip(1)) {
                    continue;
                }
                page.add("INTRA", intra.getSource(), -1, slice, fields("left_domain", intra.getLeftDomain(),
                        "right_domain", intra.getRightDomain(), "support", intra.getSupport(slice),
                        "moments", List.of(intra.getMoment(slice, 1), intra.getMoment(slice, 2), intra.getMoment(slice, 3))));
            }
        }
        return page.result();
    }

    private static Map<String, Object> fields(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) {
            result.put((String) values[i], values[i + 1]);
        }
        return result;
    }

    private static final class Page {
        private final long generation;
        private final long offset;
        private final long limit;
        private final List<List<String>> rows = new ArrayList<>();
        private long position;
        private long bytes;

        private Page(long generation, long offset, long limit) {
            this.generation = generation;
            this.offset = offset;
            this.limit = limit;
        }

        private boolean full() {
            return rows.size() >= limit;
        }

        private boolean skip(long count) {
            if (full()) {
                return true;
            }
            if (count <= offset - position) {
                position += count;
                return true;
            }
            return false;
        }

        private void add(String section, int source, int domain, int slice, Map<String, Object> value) {
            if (skip(1)) {
                return;
            }
            String json = JSON.toJson(value);
            bytes += json.getBytes(StandardCharsets.UTF_8).length + 128;
            if (bytes > MAX_OUTPUT_BYTES) {
                throw new IllegalArgumentException("JOIN statistics inspection page exceeds 32 MiB; use a smaller LIMIT");
            }
            rows.add(List.of(Long.toString(generation), Long.toString(position++), section,
                    source < 0 ? "" : Integer.toString(source), domain < 0 ? "" : Integer.toString(domain),
                    slice < 0 ? "" : Integer.toString(slice), json));
        }

        private ShowResultSet result() {
            var metadata = ShowResultSetMetaData.builder();
            for (String name : List.of("Generation", "Row", "Section", "Source", "Domain", "Slice", "Details")) {
                metadata.addColumn(new Column(name, TypeFactory.createVarcharType(65533)));
            }
            return new ShowResultSet(metadata.build(), rows);
        }
    }
}
