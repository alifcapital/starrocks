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

package com.starrocks.statistic;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.TableName;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Journaled collection scope; source snapshots and slice dictionaries belong to a collected generation. */
public final class JoinStatisticsDefinition {
    public static final int MAX_DOMAINS = 3;
    public static final class Source {
        @SerializedName("catalog")
        private final String catalog;
        @SerializedName("database")
        private final String database;
        @SerializedName("table")
        private final String table;
        @SerializedName("uuid")
        private final String uuid;
        @SerializedName("tableUuid")
        private final String tableUuid;
        @SerializedName("predicates")
        private final List<String> predicates;

        public Source(String catalog, String database, String table, String uuid, List<String> predicates) {
            this(catalog, database, table, uuid, uuid, predicates);
        }

        public Source(String catalog, String database, String table, String role, String tableUuid, List<String> predicates) {
            this.catalog = Objects.requireNonNull(catalog);
            this.database = Objects.requireNonNull(database);
            this.table = Objects.requireNonNull(table);
            this.uuid = Objects.requireNonNull(role);
            this.tableUuid = Objects.requireNonNull(tableUuid);
            this.predicates = List.copyOf(predicates);
        }

        public String getCatalogName() {
            return catalog;
        }

        public TableName getTableName() {
            return new TableName(catalog, database, table);
        }

        /** Relation role within the definition; legacy definitions use the physical UUID directly. */
        public String getUuid() {
            return uuid;
        }

        /** Physical identity for metadata resolution and collection version tracking. */
        public String getTableUuid() {
            return tableUuid == null ? uuid : tableUuid;
        }

        public Source withRole(String role) {
            return new Source(catalog, database, table, role, getTableUuid(), predicates);
        }

        public List<String> getPredicates() {
            return predicates;
        }
    }

    /** One equality-key domain, possibly shared by more than two relations. */
    public static final class KeyDomain {
        @SerializedName("columns")
        private final Map<Integer, List<String>> columns;
        @SerializedName("types")
        private final List<String> types;

        public KeyDomain(Map<Integer, List<String>> columns, List<String> types) {
            if (columns.size() < 2 || columns.size() > 4 || types.isEmpty()) {
                throw new IllegalArgumentException("Invalid JOIN statistics key domain");
            }
            for (Map.Entry<Integer, List<String>> entry : columns.entrySet()) {
                if (entry.getKey() < 0 || entry.getValue().size() != types.size()) {
                    throw new IllegalArgumentException("Invalid JOIN statistics key columns");
                }
            }
            this.columns = columns.entrySet().stream().collect(java.util.stream.Collectors.toUnmodifiableMap(
                    Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
            this.types = List.copyOf(types);
        }

        public Map<Integer, List<String>> getColumns() {
            return columns;
        }

        public List<String> getTypes() {
            return types;
        }
    }

    /** Equalities with the same participants form one typed tuple key, not independent LP attributes. */
    public static List<KeyDomain> tupleDomains(List<KeyDomain> scalarDomains) {
        Map<Set<Integer>, List<KeyDomain>> groups = new LinkedHashMap<>();
        for (KeyDomain domain : scalarDomains) {
            groups.computeIfAbsent(domain.columns.keySet(), ignored -> new ArrayList<>()).add(domain);
        }
        List<KeyDomain> result = new ArrayList<>();
        for (var group : groups.values()) {
            int first = group.get(0).columns.keySet().stream().min(Integer::compare).orElseThrow();
            group.sort(Comparator.comparing(d -> String.join("\u0001", d.columns.get(first))));
            Map<Integer, List<String>> columns = new LinkedHashMap<>();
            List<String> types = new ArrayList<>();
            for (KeyDomain domain : group) {
                domain.columns.forEach((source, names) ->
                        columns.computeIfAbsent(source, ignored -> new ArrayList<>()).addAll(names));
                types.addAll(domain.types);
            }
            result.add(new KeyDomain(columns, types));
        }
        return List.copyOf(result);
    }

    @SerializedName("name")
    private final String name;
    @SerializedName("sources")
    private final List<Source> sources;
    @SerializedName("domains")
    private final List<KeyDomain> domains;
    @SerializedName("properties")
    private final Map<String, String> properties;

    public JoinStatisticsDefinition(String name, List<Source> sources, List<KeyDomain> domains,
                                    Map<String, String> properties) {
        if (name == null || name.isEmpty() || sources.size() < 2 || sources.size() > 4 || domains.isEmpty()
                || domains.size() > MAX_DOMAINS) {
            throw new IllegalArgumentException("Invalid JOIN statistics definition");
        }
        for (KeyDomain domain : domains) {
            for (int source : domain.columns.keySet()) {
                if (source >= sources.size()) {
                    throw new IllegalArgumentException("Unknown JOIN statistics source");
                }
            }
        }
        this.name = name.toLowerCase(Locale.ROOT);
        this.sources = List.copyOf(sources);
        this.domains = List.copyOf(domains);
        this.properties = Map.copyOf(properties);
    }

    /** Only lossless integer widening and VARCHAR length changes preserve this encoded key domain. */
    public static boolean matchesKeyType(Type actual, String declared) {
        String sql = declared.toUpperCase(Locale.ROOT);
        String primitive = sql.replaceFirst("\\(.*\\)$", "");
        if (actual.isIntegerType()) {
            List<String> integers = List.of("TINYINT", "SMALLINT", "INT", "BIGINT", "LARGEINT");
            int from = integers.indexOf(actual.toSql().toUpperCase(Locale.ROOT).replaceFirst("\\(.*\\)$", ""));
            int to = integers.indexOf(primitive);
            return from >= 0 && to >= from;
        }
        return actual.toSql().equalsIgnoreCase(declared)
                || (actual.isVarchar() && primitive.equals("VARCHAR"));
    }

    public String getName() {
        return name;
    }

    public JoinStatisticsDefinition immutableCopy() {
        List<Source> sourceCopies = sources.stream().map(source -> new Source(source.catalog, source.database,
                source.table, source.uuid, source.getTableUuid(), source.predicates)).toList();
        List<KeyDomain> domainCopies = domains.stream().map(domain -> new KeyDomain(domain.columns, domain.types)).toList();
        return new JoinStatisticsDefinition(name, sourceCopies, domainCopies, properties);
    }

    public List<Source> getSources() {
        return sources;
    }

    public List<KeyDomain> getDomains() {
        return domains;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
