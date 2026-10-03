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

import com.github.luben.zstd.Zstd;
import com.starrocks.thrift.TStatisticData;
import com.starrocks.type.Type;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** One immutable partition row, prepared once on load and charged to the shared BASIC cache. */
public final class ExternalPartitionStatistics implements ExternalColumnStatistics {
    private static final int MAGIC = 0x50535432;
    public static final int MAX_PAYLOAD_BYTES = 1024 * 1024;
    private static final int MAX_RAW_BYTES = 16 * 1024 * 1024;
    private static final int MAX_COLUMNS = 10000;
    public final Map<String, Partition> columns;
    private final int bytes;

    ExternalPartitionStatistics(Map<String, Partition> columns) {
        this.columns = Map.copyOf(columns);
        long retained = 128;
        for (var entry : columns.entrySet()) {
            retained += 96L + 2L * entry.getKey().length() + entry.getValue().retainedBytes();
        }
        bytes = (int) Math.min(Integer.MAX_VALUE, retained);
    }

    // Oversized rows use the existing per-column/block store; collection must not fail just because
    // the optional packed representation is too wide. No source-table rescan is necessary.
    public static Optional<byte[]> encode(List<TStatisticData> rows, Map<String, Type> types) {
        if (rows.size() > MAX_COLUMNS) {
            return Optional.empty();
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(rows.size());
            for (TStatisticData row : rows) {
                Partition value = new Partition(row, types.get(row.columnName));
                writeString(out, row.columnName);
                writeString(out, value.getSourceType());
                out.writeLong(value.getRowCount());
                out.writeDouble(value.getDataSize());
                out.writeLong(value.getNullCount());
                out.writeDouble(value.getMinValue());
                out.writeDouble(value.getMaxValue());
                byte[] hll = row.getHll();
                out.writeInt(hll.length);
                out.write(hll);
                if (bytes.size() > MAX_RAW_BYTES) {
                    return Optional.empty();
                }
            }
            byte[] compressed = Zstd.compress(bytes.toByteArray(), 1);
            return compressed.length <= MAX_PAYLOAD_BYTES ? Optional.of(compressed) : Optional.empty();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    public static ExternalPartitionStatistics decode(byte[] payload) {
        if (payload == null || payload.length == 0 || payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("Invalid external partition statistics payload size");
        }
        long size = Zstd.decompressedSize(payload);
        if (size < 8 || size > MAX_RAW_BYTES) {
            throw new IllegalArgumentException("Invalid external partition statistics decoded size");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(Zstd.decompress(payload, (int) size)));
            if (in.readInt() != MAGIC) {
                throw new IllegalArgumentException("Unsupported external partition statistics version");
            }
            int count = in.readInt();
            if (count < 0 || count > MAX_COLUMNS) {
                throw new IllegalArgumentException("Invalid external partition statistics column count");
            }
            Map<String, Partition> columns = new HashMap<>();
            for (int i = 0; i < count; i++) {
                String name = readString(in);
                String type = readString(in);
                long rows = in.readLong();
                double dataSize = in.readDouble();
                long nulls = in.readLong();
                double min = in.readDouble();
                double max = in.readDouble();
                int length = in.readInt();
                if (rows < 0 || !Double.isFinite(dataSize) || dataSize < 0 || nulls < 0 || nulls > rows
                        || Double.isNaN(min) || Double.isNaN(max) || length < 1 || length > 16385
                        || length > in.available()) {
                    throw new IllegalArgumentException("Invalid external partition statistics column");
                }
                Partition value = new Partition(type, rows, dataSize, nulls,
                        StatisticsHll.fromSerialized(in.readNBytes(length)), min, max);
                if (columns.put(name, value) != null) {
                    throw new IllegalArgumentException("Duplicate external partition statistics column");
                }
            }
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing external partition statistics bytes");
            }
            return new ExternalPartitionStatistics(columns);
        } catch (IOException e) {
            throw new IllegalArgumentException("Truncated external partition statistics", e);
        }
    }

    private static void writeString(DataOutputStream out, String text) throws IOException {
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length <= 0 || length > MAX_PAYLOAD_BYTES || length > in.available()) {
            throw new IllegalArgumentException("Invalid external partition statistics string");
        }
        return new String(in.readNBytes(length), StandardCharsets.UTF_8);
    }

    @Override
    public String getSourceType() {
        return "";
    }

    @Override
    public int retainedBytes() {
        return bytes;
    }
}
