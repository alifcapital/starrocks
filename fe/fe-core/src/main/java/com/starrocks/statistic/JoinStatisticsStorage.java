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

import com.google.common.hash.Hashing;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsCodec;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsData;
import com.starrocks.sql.parser.SqlParser;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Writes bounded payload parts first; the registry publishes their manifest only after every write succeeds. */
public class JoinStatisticsStorage {
    public static final int PART_BYTES = 128 * 1024;
    static final String TABLE = "default_catalog." + StatsConstants.STATISTICS_DB_NAME + "."
            + StatsConstants.JOIN_STATISTICS_TABLE_NAME;

    public record Manifest(int parts, long bytes, String checksum) {
    }

    public Manifest write(ConnectContext context, JoinStatisticsData data, Runnable checkCancelled) throws Exception {
        byte[] payload = JoinStatisticsCodec.encode(data, Config.statistic_join_object_max_bytes);
        int parts = (int) ((payload.length + (long) PART_BYTES - 1) / PART_BYTES);
        for (int start = 0; start < parts; start += 16) {
            checkCancelled.run();
            StringBuilder sql = new StringBuilder("INSERT INTO ").append(TABLE).append(" VALUES ");
            for (int part = start; part < Math.min(parts, start + 16); part++) {
                if (part != start) {
                    sql.append(',');
                }
                int offset = part * PART_BYTES;
                ByteBuffer encoded = Base64.getEncoder().encode(ByteBuffer.wrap(payload, offset,
                        Math.min(PART_BYTES, payload.length - offset)));
                String base64 = java.nio.charset.StandardCharsets.US_ASCII.decode(encoded).toString();
                sql.append('(').append(data.getObjectId()).append(',').append(data.getGeneration()).append(',')
                        .append(part).append(",from_base64('").append(base64).append("'),now())");
            }
            execute(context, sql.toString());
        }
        return new Manifest(parts, payload.length, Hashing.sha256().hashBytes(payload).toString());
    }

    public JoinStatisticsData load(ConnectContext context, JoinStatisticsMeta meta) throws IOException {
        int maximum = Config.statistic_join_object_max_bytes;
        validateManifest(meta, maximum);
        byte[] payload = new byte[(int) meta.getPayloadBytes()];
        long started = System.nanoTime();
        long budget = TimeUnit.SECONDS.toNanos(context.getSessionVariable().getQueryTimeoutS());
        for (int start = 0; start < meta.getParts(); start += 16) {
            long remaining = budget - (System.nanoTime() - started);
            if (remaining <= 0 || Thread.currentThread().isInterrupted()) {
                throw new IOException("JOIN statistics load timed out or was interrupted");
            }
            context.getSessionVariable().setQueryTimeoutS((int) Math.max(1, TimeUnit.NANOSECONDS.toSeconds(remaining)));
            int end = Math.min(meta.getParts(), start + 16);
            List<List<String>> rows = new StatisticExecutor().executeStatisticJsonDQL(context,
                    "SELECT part_id, to_base64(payload) FROM " + TABLE + " WHERE object_id = " + meta.getId()
                            + " AND generation = " + meta.getGeneration() + " AND part_id >= " + start
                            + " AND part_id < " + end + " ORDER BY part_id LIMIT " + (end - start + 1));
            appendParts(payload, rows, start, end);
        }
        return decodePayload(meta, payload, maximum);
    }

    private static void validateManifest(JoinStatisticsMeta meta, int maximum) throws IOException {
        if (meta.getGeneration() <= 0 || meta.getPayloadBytes() <= 0 || meta.getPayloadBytes() > maximum
                || meta.getParts() != (meta.getPayloadBytes() + PART_BYTES - 1) / PART_BYTES) {
            throw new IOException("Invalid JOIN statistics manifest");
        }
    }

    static JoinStatisticsData decodeParts(JoinStatisticsMeta meta, List<List<String>> rows, int maximum) throws IOException {
        validateManifest(meta, maximum);
        byte[] payload = new byte[(int) meta.getPayloadBytes()];
        appendParts(payload, rows, 0, meta.getParts());
        return decodePayload(meta, payload, maximum);
    }

    private static void appendParts(byte[] payload, List<List<String>> rows, int start, int end) throws IOException {
        if (rows.size() != end - start) {
            throw new IOException("Incomplete JOIN statistics generation");
        }
        try {
            for (int i = start; i < end; i++) {
                List<String> row = rows.get(i - start);
                if (row.size() != 2 || row.get(0) == null || row.get(1) == null || Integer.parseInt(row.get(0)) != i
                        || row.get(1).length() > 4 * ((PART_BYTES + 2) / 3)) {
                    throw new IOException("Invalid JOIN statistics part");
                }
                byte[] part = Base64.getDecoder().decode(row.get(1));
                int offset = i * PART_BYTES;
                int expected = Math.min(PART_BYTES, payload.length - offset);
                if (part.length != expected) {
                    throw new IOException("Invalid JOIN statistics part length");
                }
                System.arraycopy(part, 0, payload, offset, part.length);
            }
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid JOIN statistics part encoding", e);
        }
    }

    private static JoinStatisticsData decodePayload(JoinStatisticsMeta meta, byte[] bytes, int maximum) throws IOException {
        if (bytes.length != meta.getPayloadBytes() || !Hashing.sha256().hashBytes(bytes).toString().equals(meta.getChecksum())) {
            throw new IOException("JOIN statistics manifest checksum mismatch");
        }
        return JoinStatisticsCodec.decode(bytes, maximum, meta.getId(), meta.getGeneration());
    }

    public void delete(ConnectContext context, long objectId, Long generation) throws Exception {
        if (objectId <= 0 || (generation != null && generation <= 0)) {
            throw new IllegalArgumentException("Invalid JOIN statistics data identity");
        }
        execute(context, "DELETE FROM " + TABLE + " WHERE object_id = " + objectId
                + (generation == null ? "" : " AND generation = " + generation));
    }

    static void execute(ConnectContext context, String sql) throws Exception {
        context.setQueryId(UUIDUtil.genUUID());
        context.getState().reset();
        StatementBase statement = SqlParser.parseOneWithStarRocksDialect(sql, context.getSessionVariable());
        StmtExecutor executor = StmtExecutor.newInternalExecutor(context, statement);
        context.setExecutor(executor);
        executor.execute();
        if (context.getState().isError()) {
            throw new DdlException(context.getState().getErrorMessage());
        }
    }
}
