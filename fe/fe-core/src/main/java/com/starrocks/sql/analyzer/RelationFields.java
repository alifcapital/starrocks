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

package com.starrocks.sql.analyzer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.SlotRef;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class RelationFields {
    private final List<Field> allFields;

    // NOTE: sort fields by name to speedup resolve performance
    // Built on the first lookup: many relations, such as the order scope of every select, never resolve a name.
    private ImmutableListMultimap<String, Field> names;
    private final boolean resolveStruct;
    
    // Track if this RelationFields comes from FULL OUTER JOIN USING
    // Used to handle unqualified USING columns specially in resolveFields
    private final boolean fromFullOuterJoinUsing;

    public RelationFields(Field... fields) {
        this(ImmutableList.copyOf(fields));
    }

    public RelationFields(List<Field> fields) {
        this(fields, false);
    }
    
    public RelationFields(List<Field> fields, boolean fromFullOuterJoinUsing) {
        requireNonNull(fields, "fields is null");
        this.allFields = ImmutableList.copyOf(fields);
        boolean hasStruct = false;
        for (Field field : allFields) {
            hasStruct |= field.getType().isStructType();
        }
        this.resolveStruct = hasStruct;
        this.fromFullOuterJoinUsing = fromFullOuterJoinUsing;
    }
    
    private static boolean isRelationAliasCaseInsensitive() {
        ConnectContext context = ConnectContext.get();
        return context != null && context.isRelationAliasCaseInsensitive();
    }

    public boolean isFromFullOuterJoinUsing() {
        return fromFullOuterJoinUsing;
    }

    /**
     * Gets the index of the specified field.
     */
    public int indexOf(Field field) {
        return allFields.indexOf(field);
    }

    /**
     * Gets the field at the specified index.
     */
    public Field getFieldByIndex(int fieldIndex) {
        checkElementIndex(fieldIndex, allFields.size(), "fieldIndex");
        return allFields.get(fieldIndex);
    }

    public List<Field> getAllFields() {
        return allFields;
    }

    public List<Field> getAllVisibleFields() {
        return allFields.stream().filter(Field::isVisible).collect(Collectors.toList());
    }

    /**
     * Gets the index of all columns matching the specified name
     */
    public List<Field> resolveFields(SlotRef name) {
        if (resolveStruct) {
            // A struct field also resolves names that go into the struct, so every field is checked. The session
            // flag for the case of relation aliases is the same for all of them.
            boolean aliasCaseInsensitive = isRelationAliasCaseInsensitive();
            List<Field> resolved = new ArrayList<>();
            for (int i = 0; i < allFields.size(); i++) {
                Field field = allFields.get(i);
                if (field.canResolve(name, aliasCaseInsensitive)) {
                    resolved.add(field);
                }
            }
            return resolved;
        }
        // Resolve the slot based on column name first, then table name
        // For the case a table with thousands of columns, resolve by table name could not reduce the cardinality,
        // but resolve by column name first could reduce it a lot
        if (names == null) {
            ImmutableListMultimap.Builder<String, Field> builder = ImmutableListMultimap.builder();
            for (Field field : allFields) {
                builder.put(field.getName().toLowerCase(), field);
            }
            names = builder.build();
        }
        ImmutableList<Field> resolved = names.get(name.getColumnName().toLowerCase());
        
        if (name.getTblNameWithoutAnalyzed() == null) {
            // For unqualified column references in FULL OUTER JOIN USING scope,
            // only return unqualified fields to avoid ambiguity
            if (fromFullOuterJoinUsing && resolved.size() > 1) {
                return resolved.stream()
                        .filter(field -> field.getRelationAlias() == null)
                        .collect(toImmutableList());
            }
            return resolved;
        } else {
            boolean aliasCaseInsensitive = isRelationAliasCaseInsensitive();
            return resolved.stream().filter(input -> input.canResolve(name, aliasCaseInsensitive))
                    .collect(toImmutableList());
        }
    }

    public RelationFields joinWith(RelationFields other) {
        List<Field> fields = ImmutableList.<Field>builder()
                .addAll(this.allFields)
                .addAll(other.allFields)
                .build();

        boolean preserveFlag = this.fromFullOuterJoinUsing || other.fromFullOuterJoinUsing;
        return new RelationFields(fields, preserveFlag);
    }

    public List<Field> resolveFieldsWithPrefix(TableName prefix) {
        return allFields.stream()
                .filter(input -> input.matchesPrefix(prefix))
                .collect(toImmutableList());
    }

    public int size() {
        return allFields.size();
    }

    @Override
    public String toString() {
        return allFields.toString();
    }
}
