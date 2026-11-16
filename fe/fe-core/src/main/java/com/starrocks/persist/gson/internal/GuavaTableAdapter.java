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


package com.starrocks.persist.gson.internal;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
 * The json adapter for Guava Table. Current support: HashBasedTable.
 *
 * A Table<R, C, V> is written as
 * {
 *   "clazz": "HashBasedTable",
 *   "rowKeys": [rowKey1, rowKey2, ...],
 *   "columnKeys": [colKey1, colKey2, ...],
 *   "cells": [0, 0, value1, 0, 1, value2, ...]
 * }
 * where each cell is the index of its row key, the index of its column key and the value, so a key repeated in
 * many cells is written once.
 *
 * We expect tables with many small cells, like the per-fragment counters of a load job, which the journal and
 * the image hold for every load. So the adapter reads and writes the stream directly. A key or a value is written
 * with the adapter of its runtime class and read with the adapter of the declared type, as Gson does for a
 * JsonSerializationContext and a JsonDeserializationContext, so the JSON is the same as theirs.
 */
public class GuavaTableAdapter implements TypeAdapterFactory {
    private static final String CLAZZ = "clazz";
    private static final String ROW_KEYS = "rowKeys";
    private static final String COLUMN_KEYS = "columnKeys";
    private static final String CELLS = "cells";
    private static final String HASH_BASED_TABLE = "HashBasedTable";

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
        if (!Table.class.isAssignableFrom(typeToken.getRawType())) {
            return null;
        }
        Type type = typeToken.getType();
        Type[] arguments = type instanceof ParameterizedType
                ? ((ParameterizedType) type).getActualTypeArguments()
                : new Type[] {Object.class, Object.class, Object.class};
        return (TypeAdapter<T>) new Adapter(gson, arguments[0], arguments[1], arguments[2]).nullSafe();
    }

    private static final class Adapter<R, C, V> extends TypeAdapter<Table<R, C, V>> {
        private final Gson gson;
        private final TypeAdapter<R> rowKeyAdapter;
        private final TypeAdapter<C> columnKeyAdapter;
        private final TypeAdapter<V> valueAdapter;

        @SuppressWarnings("unchecked")
        Adapter(Gson gson, Type rowKeyType, Type columnKeyType, Type valueType) {
            this.gson = gson;
            this.rowKeyAdapter = (TypeAdapter<R>) gson.getAdapter(TypeToken.get(rowKeyType));
            this.columnKeyAdapter = (TypeAdapter<C>) gson.getAdapter(TypeToken.get(columnKeyType));
            this.valueAdapter = (TypeAdapter<V>) gson.getAdapter(TypeToken.get(valueType));
        }

        @Override
        public void write(JsonWriter out, Table<R, C, V> table) throws IOException {
            RuntimeClassWriter writer = new RuntimeClassWriter(gson);
            out.beginObject();
            out.name(CLAZZ).value(table.getClass().getSimpleName());

            out.name(ROW_KEYS).beginArray();
            Map<R, Integer> rowKeyToIndex = new HashMap<>();
            for (R rowKey : table.rowKeySet()) {
                rowKeyToIndex.put(rowKey, rowKeyToIndex.size());
                writer.write(out, rowKey);
            }
            out.endArray();

            out.name(COLUMN_KEYS).beginArray();
            Map<C, Integer> columnKeyToIndex = new HashMap<>();
            for (C columnKey : table.columnKeySet()) {
                columnKeyToIndex.put(columnKey, columnKeyToIndex.size());
                writer.write(out, columnKey);
            }
            out.endArray();

            out.name(CELLS).beginArray();
            for (Table.Cell<R, C, V> cell : table.cellSet()) {
                out.value(rowKeyToIndex.get(cell.getRowKey()));
                out.value(columnKeyToIndex.get(cell.getColumnKey()));
                writer.write(out, cell.getValue());
            }
            out.endArray();
            out.endObject();
        }

        @Override
        public Table<R, C, V> read(JsonReader in) throws IOException {
            String clazz = null;
            List<R> rowKeys = null;
            List<C> columnKeys = null;
            // [rowIndex, columnIndex] of each cell; the keys may come after the cells
            List<Integer> cellKeyIndexes = null;
            List<V> cellValues = null;

            in.beginObject();
            while (in.hasNext()) {
                switch (in.nextName()) {
                    case CLAZZ:
                        clazz = in.nextString();
                        break;
                    case ROW_KEYS:
                        rowKeys = readArray(in, rowKeyAdapter);
                        break;
                    case COLUMN_KEYS:
                        columnKeys = readArray(in, columnKeyAdapter);
                        break;
                    case CELLS:
                        cellKeyIndexes = new ArrayList<>();
                        cellValues = new ArrayList<>();
                        in.beginArray();
                        while (in.hasNext()) {
                            cellKeyIndexes.add(in.nextInt());
                            cellKeyIndexes.add(in.nextInt());
                            cellValues.add(valueAdapter.read(in));
                        }
                        in.endArray();
                        break;
                    default:
                        in.skipValue();
                        break;
                }
            }
            in.endObject();

            if (clazz == null || rowKeys == null || columnKeys == null || cellValues == null) {
                throw new JsonParseException("a Guava table needs " + CLAZZ + ", " + ROW_KEYS + ", " + COLUMN_KEYS
                        + " and " + CELLS);
            }
            Preconditions.checkState(HASH_BASED_TABLE.equals(clazz), "unknown guava table class: " + clazz);
            Table<R, C, V> table = HashBasedTable.create();
            for (int i = 0; i < cellValues.size(); i++) {
                table.put(rowKeys.get(cellKeyIndexes.get(2 * i)), columnKeys.get(cellKeyIndexes.get(2 * i + 1)),
                        cellValues.get(i));
            }
            return table;
        }

        private static <E> List<E> readArray(JsonReader in, TypeAdapter<E> adapter) throws IOException {
            List<E> elements = new ArrayList<>();
            in.beginArray();
            while (in.hasNext()) {
                elements.add(adapter.read(in));
            }
            in.endArray();
            return elements;
        }
    }

    // Writes an object with the adapter of its runtime class, as JsonSerializationContext.serialize does. The keys
    // and the values of one table are usually of one class, so the last adapter is kept.
    private static final class RuntimeClassWriter {
        private final Gson gson;
        private Class<?> lastClass;
        private TypeAdapter<Object> lastAdapter;

        RuntimeClassWriter(Gson gson) {
            this.gson = gson;
        }

        @SuppressWarnings("unchecked")
        void write(JsonWriter out, Object value) throws IOException {
            if (value == null) {
                out.nullValue();
                return;
            }
            if (value.getClass() != lastClass) {
                lastClass = value.getClass();
                lastAdapter = (TypeAdapter<Object>) gson.getAdapter(lastClass);
            }
            lastAdapter.write(out, value);
        }
    }
}
