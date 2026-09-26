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

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Versioned, bounded binary payload. Frequencies never pass through JSON or double counters. */
public final class JoinStatisticsCodec {
    private static final int MAGIC = 0x53524a53;
    private static final int VERSION = 5;
    private static final int MAX_STRING_BYTES = 1 << 20;

    private JoinStatisticsCodec() {
    }

    public static byte[] encode(JoinStatisticsData data, int maxBytes) throws IOException {
        if (maxBytes <= 0 || data.estimatedSize() > maxBytes) {
            throw new IOException("JOIN statistics exceed the object memory limit");
        }
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        LimitedOutput compressed = new LimitedOutput(buffer, maxBytes);
        DataOutputStream header = new DataOutputStream(compressed);
        header.writeInt(MAGIC);
        header.writeInt(VERSION);
        try (DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new LimitedOutput(
                new ZstdOutputStream(compressed, 3).setChecksum(true), maxBytes), 64 * 1024))) {
            writeBody(data, out);
        }
        return buffer.toByteArray();
    }

    public static JoinStatisticsData decode(byte[] payload, int maxBytes, long objectId, long generation) throws IOException {
        if (payload.length > maxBytes || maxBytes <= 0) {
            throw new IOException("JOIN statistics payload exceeds the size limit");
        }
        ByteArrayInputStream buffer = new ByteArrayInputStream(payload);
        DataInputStream header = new DataInputStream(buffer);
        int magic = header.readInt();
        int version = header.readInt();
        if (magic != MAGIC || (version != 4 && version != VERSION)) {
            throw new IOException("Unsupported JOIN statistics payload format");
        }
        // DataInputStream reads primitive values a byte at a time. Buffer above the decoder to
        // avoid a JNI decompression call for every byte of a long or double.
        try (DataInputStream in = new DataInputStream(new BufferedInputStream(new LimitedInput(
                new ZstdInputStream(buffer).setLongMax(23), maxBytes), 64 * 1024))) {
            JoinStatisticsData data = readBody(in, maxBytes, version);
            if (data.getObjectId() != objectId || data.getGeneration() != generation || data.estimatedSize() > maxBytes) {
                throw new IOException("JOIN statistics payload identity or size mismatch");
            }
            if (in.read() != -1 || buffer.available() != 0) {
                throw new IOException("Trailing JOIN statistics payload data");
            }
            return data;
        } catch (IllegalArgumentException | ArithmeticException e) {
            throw new IOException("Invalid JOIN statistics payload", e);
        }
    }

    private static void writeBody(JoinStatisticsData data, DataOutput out) throws IOException {
        out.writeLong(data.getObjectId());
        out.writeLong(data.getGeneration());
        out.writeInt(data.getSources().size());
        for (JoinStatisticsData.Source source : data.getSources()) {
            writeString(out, source.getTableUuid());
            out.writeLong(source.getSnapshot());
            out.writeLong(source.getRows());
            out.writeInt(source.getColumns().size());
            for (int i = 0; i < source.getColumns().size(); i++) {
                writeString(out, source.getColumns().get(i));
                writeType(out, source.getTypes().get(i));
            }
            out.writeInt(source.getTuples().size());
            for (int i = 0; i < source.getTuples().size(); i++) {
                for (String value : source.getTuples().get(i)) {
                    writeString(out, value);
                }
                out.writeLong(source.getTupleRows(i));
            }
            out.writeInt(source.getDegrees().size());
            for (Map.Entry<Integer, List<DegreeStatistics>> entry : source.getDegrees().entrySet()) {
                out.writeInt(entry.getKey());
                for (DegreeStatistics degree : entry.getValue()) {
                    out.writeLong(degree.getRowCount());
                    out.writeLong(degree.getNullCount());
                    out.writeLong(degree.getDistinctCount());
                    out.writeLong(degree.getMaximumFrequency());
                    for (int p = 1; p <= DegreeStatistics.MOMENT_COUNT; p++) {
                        out.writeDouble(degree.getMoment(p));
                    }
                }
            }
        }
        out.writeInt(data.getBases().size());
        for (JoinStatisticsBasis basis : data.getBases()) {
            out.writeInt(basis.getDomain());
            JoinStatisticsHeadKeys keys = basis.getHeadKeys();
            out.writeByte(keys == null ? 0 : keys.isInteger() ? 1 : 2);
            if (keys != null) {
                out.writeInt(keys.size());
                for (int i = 0; i < keys.size(); i++) {
                    if (keys.isInteger()) {
                        out.writeLong(keys.integer(i));
                    } else {
                        writeString(out, keys.text(i));
                    }
                }
            }
            out.writeInt(basis.getSources().size());
            for (int side = 0; side < basis.getSources().size(); side++) {
                out.writeInt(basis.getSources().get(side));
                out.writeInt(basis.getSlices(side).size());
                for (JoinStatisticsBasis.Slice slice : basis.getSlices(side)) {
                    slice.getHead().write(out);
                    out.writeBoolean(slice.hasUnitTail());
                    out.writeInt(slice.storedOrders());
                    for (int order = 0; order < slice.storedOrders(); order++) {
                        out.writeDouble(slice.storedTotal(order));
                        for (int bucket = 0; bucket < JoinStatisticsBasis.WIDTH; bucket++) {
                            out.writeDouble(slice.storedRoot(order, bucket));
                        }
                    }
                }
            }
            out.writeInt(basis.getPairs().size());
            for (JoinStatisticsBasis.Pair pair : basis.getPairs()) {
                out.writeInt(pair.left());
                out.writeInt(pair.right());
                for (int i = 0; i < pair.size(); i++) {
                    out.writeDouble(pair.value(i));
                }
            }
        }
        writeIntra(out, data.getIntraCorrelations());
    }

    private static void writeIntra(DataOutput out, List<JoinStatisticsData.IntraCorrelation> correlations) throws IOException {
        out.writeInt(correlations.size());
        for (JoinStatisticsData.IntraCorrelation intra : correlations) {
            out.writeInt(intra.getSource());
            out.writeInt(intra.getLeftDomain());
            out.writeInt(intra.getRightDomain());
            out.writeInt(intra.getSliceCount());
            for (int i = 0; i < intra.getSliceCount(); i++) {
                out.writeLong(intra.getSupport(i));
                for (int p = 1; p <= 3; p++) {
                    out.writeDouble(intra.getMoment(i, p));
                }
            }
        }
    }

    private static void writeType(DataOutput out, Type type) throws IOException {
        if (!(type instanceof ScalarType scalar)) {
            throw new IOException("JOIN statistics require scalar predicate types");
        }
        writeString(out, type.getPrimitiveType().name());
        out.writeInt(scalar.getLength());
        out.writeInt(scalar.getScalarPrecision());
        out.writeInt(scalar.getScalarScale());
    }

    private static Type readType(DataInput in) throws IOException {
        String name = readString(in);
        if (name == null) {
            throw new IOException("Missing JOIN statistics predicate type");
        }
        PrimitiveType primitive = PrimitiveType.valueOf(name);
        int length = in.readInt();
        int precision = in.readInt();
        int scale = in.readInt();
        if (length < -1 || length > TypeFactory.CATALOG_MAX_VARCHAR_LENGTH) {
            throw new IOException("Invalid JOIN statistics type length");
        }
        if (primitive.isDecimalV3Type()) {
            if (precision < 1 || precision > PrimitiveType.getMaxPrecisionOfDecimal(primitive)
                    || scale < 0 || scale > precision) {
                throw new IOException("Invalid JOIN statistics decimal type");
            }
            return TypeFactory.createDecimalV3Type(primitive, precision, scale);
        }
        return switch (primitive) {
            case CHAR -> TypeFactory.createCharType(length);
            case VARCHAR -> TypeFactory.createVarcharType(length);
            case DECIMALV2 -> TypeFactory.createDecimalV2Type(precision, scale);
            default -> TypeFactory.createType(primitive);
        };
    }

    private static JoinStatisticsData readBody(DataInputStream in, int maxBytes, int version) throws IOException {
        long objectId = in.readLong();
        long generation = in.readLong();
        int sourceCount = count(in, 4);
        List<JoinStatisticsData.Source> sources = new ArrayList<>();
        for (int s = 0; s < sourceCount; s++) {
            String uuid = readString(in);
            long snapshot = in.readLong();
            long rows = in.readLong();
            int columns = count(in, 32);
            List<String> names = new ArrayList<>();
            List<Type> types = new ArrayList<>();
            for (int column = 0; column < columns; column++) {
                names.add(readString(in));
                Type type = readType(in);
                if (type == null || type.isComplexType() || type.isOnlyMetricType()
                        || type.isJsonType() || !type.canStatistic()) {
                    throw new IOException("Invalid JOIN statistics predicate type");
                }
                types.add(type);
            }
            int sliceCount = count(in, JoinStatisticsData.MAX_SLICES);
            List<List<String>> tuples = new ArrayList<>();
            long[] tupleRows = new long[sliceCount];
            for (int slice = 0; slice < sliceCount; slice++) {
                List<String> tuple = new ArrayList<>();
                for (int column = 0; column < columns; column++) {
                    tuple.add(readString(in));
                }
                tuples.add(tuple);
                tupleRows[slice] = in.readLong();
            }
            int domains = count(in, com.starrocks.statistic.JoinStatisticsDefinition.MAX_DOMAINS);
            Map<Integer, List<DegreeStatistics>> degrees = new HashMap<>();
            for (int d = 0; d < domains; d++) {
                int domain = count(in, com.starrocks.statistic.JoinStatisticsDefinition.MAX_DOMAINS - 1);
                List<DegreeStatistics> group = new ArrayList<>();
                for (int slice = 0; slice < sliceCount; slice++) {
                    long total = in.readLong();
                    long nulls = in.readLong();
                    long distinct = in.readLong();
                    long maximum = in.readLong();
                    double[] moments = new double[DegreeStatistics.MOMENT_COUNT];
                    for (int p = 0; p < moments.length; p++) {
                        moments[p] = in.readDouble();
                    }
                    group.add(new DegreeStatistics(total, nulls, distinct, maximum, moments));
                }
                if (degrees.put(domain, group) != null) {
                    throw new IOException("Duplicate JOIN statistics key domain");
                }
            }
            sources.add(new JoinStatisticsData.Source(uuid, snapshot, rows, names, types, tuples, tupleRows, degrees));
        }
        int basisCount = count(in, com.starrocks.statistic.JoinStatisticsDefinition.MAX_DOMAINS);
        List<JoinStatisticsBasis> bases = new ArrayList<>();
        long preparedBytes = 0;
        for (int c = 0; c < basisCount; c++) {
            int domain = count(in, com.starrocks.statistic.JoinStatisticsDefinition.MAX_DOMAINS - 1);
            JoinStatisticsHeadKeys keys = null;
            int encoding = version >= 5 ? in.readUnsignedByte() : 0;
            if (encoding == 1) {
                long[] values = new long[count(in, JoinStatisticsCorrelation.HEAD_BUDGET)];
                for (int i = 0; i < values.length; i++) {
                    values[i] = in.readLong();
                }
                keys = new JoinStatisticsHeadKeys(values);
            } else if (encoding == 2) {
                String[] values = new String[count(in, JoinStatisticsCorrelation.HEAD_BUDGET)];
                for (int i = 0; i < values.length; i++) {
                    values[i] = readString(in);
                }
                keys = new JoinStatisticsHeadKeys(values);
            } else if (encoding != 0) {
                throw new IOException("Invalid JOIN head key encoding");
            }
            preparedBytes += keys == null ? 0 : keys.estimatedSize();
            if (preparedBytes > maxBytes) {
                throw new IOException("JOIN head keys exceed the object memory limit");
            }
            int sides = count(in, 4);
            List<Integer> ids = new ArrayList<>();
            List<List<JoinStatisticsBasis.Slice>> slices = new ArrayList<>();
            for (int side = 0; side < sides; side++) {
                int id = count(in, sourceCount - 1);
                ids.add(id);
                int size = count(in, JoinStatisticsData.MAX_SLICES);
                if (size != sources.get(id).getTuples().size()) {
                    throw new IOException("Mismatched JOIN statistics slice dictionary");
                }
                List<JoinStatisticsBasis.Slice> values = new ArrayList<>();
                for (int slice = 0; slice < size; slice++) {
                    CompactDegreeVector head = CompactDegreeVector.read(in, JoinStatisticsCorrelation.HEAD_BUDGET);
                    boolean unit = in.readBoolean();
                    int orders = count(in, unit ? 1 : JoinStatisticsBasis.momentOrders().length);
                    if (orders != 0 && orders != (unit ? 1 : JoinStatisticsBasis.momentOrders().length)) {
                        throw new IOException("Invalid shared JOIN tail shape");
                    }
                    // At most one dense slice is temporary (8 x 768 doubles). Account for
                    // retained sparse slices, not the sum of discarded decode workspaces.
                    double[][] roots = new double[orders][JoinStatisticsBasis.WIDTH];
                    double[] totals = new double[orders];
                    for (int order = 0; order < orders; order++) {
                        totals[order] = in.readDouble();
                        for (int bucket = 0; bucket < JoinStatisticsBasis.WIDTH; bucket++) {
                            roots[order][bucket] = in.readDouble();
                        }
                    }
                    var prepared = JoinStatisticsBasis.Slice.prepared(head, roots, totals, unit);
                    preparedBytes += prepared.estimatedSize();
                    if (preparedBytes > maxBytes) {
                        throw new IOException("JOIN statistics bases exceed the object memory limit");
                    }
                    values.add(prepared);
                }
                slices.add(values);
            }
            int pairCount = count(in, sides * (sides - 1) / 2);
            List<JoinStatisticsBasis.Pair> pairs = new ArrayList<>();
            for (int p = 0; p < pairCount; p++) {
                int left = count(in, sides - 1);
                int right = count(in, sides - 1);
                int leftSize = slices.get(left).size();
                int rightSize = slices.get(right).size();
                int cells = Math.multiplyExact(Math.multiplyExact(leftSize, rightSize), 4);
                preparedBytes += 64L + 8L * cells;
                if (preparedBytes > maxBytes) {
                    throw new IOException("Pairwise JOIN statistics exceed the object memory limit");
                }
                double[] products = new double[cells];
                for (int i = 0; i < cells; i++) {
                    products[i] = in.readDouble();
                }
                pairs.add(new JoinStatisticsBasis.Pair(left, right, leftSize, rightSize, products));
            }
            bases.add(new JoinStatisticsBasis(domain, ids, slices, pairs, keys));
        }
        int intraCount = count(in, 12);
        List<JoinStatisticsData.IntraCorrelation> intra = new ArrayList<>();
        for (int i = 0; i < intraCount; i++) {
            int source = count(in, sourceCount - 1);
            int left = count(in, com.starrocks.statistic.JoinStatisticsDefinition.MAX_DOMAINS - 1);
            int right = count(in, com.starrocks.statistic.JoinStatisticsDefinition.MAX_DOMAINS - 1);
            int size = count(in, JoinStatisticsData.MAX_SLICES);
            if (size != sources.get(source).getTuples().size()) {
                throw new IOException("Mismatched intra-source slice dictionary");
            }
            long[] support = new long[size];
            double[][] moments = new double[size][3];
            for (int slice = 0; slice < size; slice++) {
                support[slice] = in.readLong();
                for (int p = 0; p < 3; p++) {
                    moments[slice][p] = in.readDouble();
                }
            }
            intra.add(new JoinStatisticsData.IntraCorrelation(source, left, right, support, moments));
        }
        return new JoinStatisticsData(objectId, generation, sources, bases, intra);
    }

    private static int count(DataInput in, int maximum) throws IOException {
        int value = in.readInt();
        if (value < 0 || value > maximum) {
            throw new IOException("Invalid JOIN statistics array length");
        }
        return value;
    }

    private static void writeString(DataOutput out, String value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_STRING_BYTES) {
            throw new IOException("JOIN statistics string exceeds the length limit");
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInput in) throws IOException {
        int length = in.readInt();
        if (length == -1) {
            return null;
        }
        if (length < 0 || length > MAX_STRING_BYTES) {
            throw new IOException("Invalid JOIN statistics string length");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
    }

    private static final class LimitedOutput extends FilterOutputStream {
        private long remaining;

        private LimitedOutput(OutputStream out, long limit) {
            super(out);
            remaining = limit;
        }

        @Override
        public void write(int value) throws IOException {
            consume(1);
            out.write(value);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            consume(length);
            out.write(bytes, offset, length);
        }

        private void consume(int length) throws IOException {
            if (length > remaining) {
                throw new IOException("JOIN statistics payload exceeds the size limit");
            }
            remaining -= length;
        }
    }

    private static final class LimitedInput extends FilterInputStream {
        private long remaining;

        private LimitedInput(InputStream in, long limit) {
            super(in);
            remaining = limit;
        }

        @Override
        public int read() throws IOException {
            int value = in.read();
            if (value >= 0) {
                consume(1);
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int actual = in.read(bytes, offset, (int) Math.min(length, remaining + 1));
            if (actual > 0) {
                consume(actual);
            }
            return actual;
        }

        private void consume(int length) throws IOException {
            if (length > remaining) {
                throw new IOException("Decoded JOIN statistics payload exceeds the size limit");
            }
            remaining -= length;
        }
    }
}
