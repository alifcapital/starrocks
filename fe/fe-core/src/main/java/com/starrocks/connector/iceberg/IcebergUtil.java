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

package com.starrocks.connector.iceberg;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.credential.CloudConfiguration;
import com.starrocks.credential.CloudConfigurationFactory;
import com.starrocks.credential.CloudType;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TExprMinMaxValue;
import com.starrocks.thrift.TExprNodeType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.InternalData;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LocationUtil;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class IcebergUtil {
    public static String fileName(String path) {
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private static final Schema MANIFEST_PROJECTION =
            ManifestFile.schema().select(
                    "manifest_path",
                    "manifest_length",
                    "content",
                    "partition_spec_id",
                    "added_snapshot_id",
                    "deleted_data_files_count");

    /**
     * Streams the manifest files of a snapshot. When the snapshot has a manifest list file,
     * reads it directly (AVRO) with a narrow projection instead of materializing the full
     * manifest list via snapshot.allManifests(); otherwise falls back to allManifests().
     * Aligned with Iceberg FileCleanupStrategy.readManifests().
     */
    public static CloseableIterable<ManifestFile> readManifests(Snapshot snapshot, FileIO fileIO) {
        if (snapshot.manifestListLocation() != null) {
            return InternalData.read(
                            FileFormat.AVRO, fileIO.newInputFile(snapshot.manifestListLocation()))
                    .setRootType(GenericManifestFile.class)
                    .project(MANIFEST_PROJECTION)
                    .reuseContainers()
                    .build();
        } else {
            return CloseableIterable.withNoopClose(snapshot.allManifests(fileIO));
        }
    }

    public static class MinMaxValue {
        Object minValue;
        Object maxValue;
        long nullValueCount;
        long valueCount;

        private boolean toThrift(SlotDescriptor slot, TExprMinMaxValue texpr) {
            texpr.setHas_null((nullValueCount > 0));
            texpr.setAll_null((valueCount == nullValueCount));
            if (valueCount == nullValueCount) {
                texpr.setType(TExprNodeType.NULL_LITERAL);
                return true;
            }
            if (minValue == null || maxValue == null) {
                return false;
            }
            switch (slot.getType().getPrimitiveType()) {
                case BOOLEAN:
                    texpr.setType(TExprNodeType.BOOL_LITERAL);
                    texpr.setMin_int_value((Boolean) minValue ? 1 : 0);
                    texpr.setMax_int_value((Boolean) maxValue ? 1 : 0);
                    break;
                case TINYINT:
                case SMALLINT:
                case INT:
                case DATE:
                    texpr.setType(TExprNodeType.INT_LITERAL);
                    texpr.setMin_int_value((Integer) minValue);
                    texpr.setMax_int_value((Integer) maxValue);
                    break;
                case BIGINT:
                case TIME:
                case DATETIME:
                    texpr.setType(TExprNodeType.INT_LITERAL);
                    if (minValue instanceof Integer) {
                        texpr.setMin_int_value(((Integer) minValue).longValue());
                    } else {
                        texpr.setMin_int_value((Long) minValue);
                    }
                    if (maxValue instanceof Integer) {
                        texpr.setMax_int_value(((Integer) maxValue).longValue());
                    } else {
                        texpr.setMax_int_value((Long) maxValue);
                    }
                    break;
                case FLOAT:
                    texpr.setType(TExprNodeType.FLOAT_LITERAL);
                    texpr.setMin_float_value((Float) minValue);
                    texpr.setMax_float_value((Float) maxValue);
                    break;
                case DOUBLE:
                    texpr.setType(TExprNodeType.FLOAT_LITERAL);
                    texpr.setMin_float_value((Double) minValue);
                    texpr.setMax_float_value((Double) maxValue);
                    break;
                default:
                    // Unsupported type for min/max optimization
                    return false;
            }
            return true;
        }

        public void toThrift(Map<Integer, TExprMinMaxValue> tExprMinMaxValueMap, SlotDescriptor slot) {
            TExprMinMaxValue texpr = new TExprMinMaxValue();
            if (toThrift(slot, texpr)) {
                tExprMinMaxValueMap.put(slot.getId().asInt(), texpr);
            }
        }
    }

    private static final Set<Type.TypeID> MIN_MAX_SUPPORTED_TYPES = Set.of(
            // TODO(yanz): to support more types.
            // datetime and timestamp: need to consider timezone.
            // decimal: need to consider precision and scale.
            // binary: min/max is not accurate for binary type.
            Type.TypeID.BOOLEAN,
            Type.TypeID.INTEGER,
            Type.TypeID.LONG,
            Type.TypeID.FLOAT,
            Type.TypeID.DOUBLE,
            Type.TypeID.DATE,
            Type.TypeID.TIME,
            Type.TypeID.TIMESTAMP
    );

    @VisibleForTesting
    public static Map<Integer, MinMaxValue> parseMinMaxValueBySlots(Schema schema,
                                                                    Map<Integer, ByteBuffer> lowerBounds,
                                                                    Map<Integer, ByteBuffer> upperBounds,
                                                                    Map<Integer, Long> nullValueCounts,
                                                                    Map<Integer, Long> valueCounts,
                                                                    List<SlotDescriptor> slots) {

        Preconditions.checkArgument(nullValueCounts != null && valueCounts != null,
                "nullValueCounts and valueCounts cannot be null");
        lowerBounds = lowerBounds == null ? Map.of() : lowerBounds;
        upperBounds = upperBounds == null ? Map.of() : upperBounds;
        Map<Integer, MinMaxValue> minMaxValues = new HashMap<>();
        for (SlotDescriptor slot : slots) {
            // has to be a scalar type
            if (!slot.getType().isScalarType()) {
                continue;
            }
            Types.NestedField field = schema.findField(slot.getColumn().getName());
            if (field == null) {
                continue;
            }
            Type type = field.type();
            // Skip unsupported types
            if (!MIN_MAX_SUPPORTED_TYPES.contains(type.typeId())) {
                continue;
            }
            if (!nullValueCounts.containsKey(field.fieldId()) || !valueCounts.containsKey(field.fieldId())) {
                continue;
            }
            // create the min/max value object to put into map
            MinMaxValue minMaxValue = new MinMaxValue();
            minMaxValues.put(field.fieldId(), minMaxValue);
            minMaxValue.nullValueCount = nullValueCounts.get(field.fieldId());
            minMaxValue.valueCount = valueCounts.get(field.fieldId());
            // parse lower and upper bounds
            Object low = Conversions.fromByteBuffer(field.type(), lowerBounds.get(field.fieldId()));
            Object high = Conversions.fromByteBuffer(field.type(), upperBounds.get(field.fieldId()));
            minMaxValue.minValue = low;
            minMaxValue.maxValue = high;
            if (type.typeId() == Type.TypeID.TIMESTAMP) {
                Types.TimestampType timestampType = (Types.TimestampType) type;
                if (timestampType.shouldAdjustToUTC() && low instanceof Long && high instanceof Long) {
                    // Iceberg TIMESTAMP WITH TIME ZONE stores instants in UTC, while StarRocks compares DATETIME
                    // values in the session timezone, so file-level bounds are converted into session-local micros
                    // before being sent to BE.
                    //
                    // This is an endpoint-only conversion: it maps just the file's min and max instants. It is exact
                    // only while UTC->local is monotonic across the file's [low, high] range, i.e. local = utc + offset
                    // never folds back on itself. A spring-forward transition (offset jumps up, e.g. -08:00 -> -07:00)
                    // keeps local time strictly increasing, so the endpoints stay the true local min/max. A fall-back
                    // transition (offset drops, e.g. -07:00 -> -08:00) rewinds local time, so the real min/max can sit
                    // strictly inside the file and is unrecoverable from the endpoints alone. Only that case is
                    // rejected: the file-level min/max is dropped and BE reads this file directly (a per-file
                    // fallback) instead of trusting an inexact bound.
                    ZoneId zoneId = TimeUtils.getTimeZone().toZoneId();
                    if (isEndpointConversionExact(zoneId, (Long) low, (Long) high)) {
                        minMaxValue.minValue = adjustTimestampMicrosToSessionTz((Long) low);
                        minMaxValue.maxValue = adjustTimestampMicrosToSessionTz((Long) high);
                    } else {
                        minMaxValues.remove(field.fieldId());
                    }
                }
            }
        }
        return minMaxValues;
    }

    private static long adjustTimestampMicrosToSessionTz(long micros) {
        long seconds = Math.floorDiv(micros, 1_000_000L);
        long microsRemainder = Math.floorMod(micros, 1_000_000L);
        int nanos = (int) (microsRemainder * 1000L);
        ZoneId zoneId = TimeUtils.getTimeZone().toZoneId();
        ZoneOffset offset = zoneId.getRules().getOffset(Instant.ofEpochSecond(seconds, nanos));
        return micros + offset.getTotalSeconds() * 1_000_000L;
    }

    /**
     * Returns true when the endpoint UTC-&gt;local conversion in {@link #adjustTimestampMicrosToSessionTz(long)}
     * produces the exact local min/max for a file covering the instant range {@code [lowMicros, highMicros]} in
     * {@code zoneId}.
     *
     * <p>{@code local = utc + offset(utc)} preserves order as long as the offset never decreases across the range, so
     * the file's earliest/latest instants stay its earliest/latest local times. A spring-forward jump (offset
     * increases) only widens the gap and keeps that order; a fall-back jump (offset decreases) rewinds local time and
     * can move the true min/max onto an interior row that the endpoints no longer represent. We therefore reject the
     * range only if it contains an offset-decreasing transition; constant-offset and spring-forward ranges stay exact.
     */
    private static boolean isEndpointConversionExact(ZoneId zoneId, long lowMicros, long highMicros) {
        Instant low = Instant.ofEpochSecond(Math.floorDiv(lowMicros, 1_000_000L));
        Instant high = Instant.ofEpochSecond(Math.floorDiv(highMicros, 1_000_000L));
        ZoneRules rules = zoneId.getRules();
        // nextTransition returns the first transition strictly after its argument; zone transitions never land on
        // sub-second boundaries, so truncating micros to whole seconds cannot skip one. Walk every transition in
        // (low, high] and reject as soon as one decreases the UTC offset (a fall-back).
        for (ZoneOffsetTransition t = rules.nextTransition(low);
                t != null && !t.getInstant().isAfter(high);
                t = rules.nextTransition(t.getInstant())) {
            if (t.getOffsetAfter().getTotalSeconds() < t.getOffsetBefore().getTotalSeconds()) {
                return false;
            }
        }
        return true;
    }

    public static Map<Integer, TExprMinMaxValue> toThriftMinMaxValueBySlots(Schema schema,
                                                                            Map<Integer, ByteBuffer> lowerBounds,
                                                                            Map<Integer, ByteBuffer> upperBounds,
                                                                            Map<Integer, Long> nullValueCounts,
                                                                            Map<Integer, Long> valueCounts,
                                                                            List<SlotDescriptor> slots) {
        Map<Integer, TExprMinMaxValue> result = new HashMap<>();
        Map<Integer, MinMaxValue> minMaxValues =
                parseMinMaxValueBySlots(schema, lowerBounds, upperBounds, nullValueCounts, valueCounts, slots);
        for (SlotDescriptor slot : slots) {
            Types.NestedField field = schema.findField(slot.getColumn().getName());
            if (field == null) {
                continue;
            }
            int fieldId = field.fieldId();
            MinMaxValue minMaxValue = minMaxValues.get(fieldId);
            if (minMaxValue == null) {
                continue; // No min/max value for this slot
            }
            minMaxValue.toThrift(result, slot);
        }
        return result;
    }

    /**
     * Builds the per-file bound of a VARCHAR TopN reorder key, or returns null when the file has no
     * usable bound. BE then never skips the file and orders it after the files with a bound.
     *
     * <p>We want BE to order and skip files in the order the sort uses: unsigned bytes, a shorter prefix
     * first. Iceberg string bounds keep that order for characters inside the BMP, and a truncated bound
     * only gets wider (the lower bound is a prefix, the upper bound is a prefix with its last character
     * incremented), so skipping by it stays safe. What we fear, and what we do:
     * <ul>
     *   <li>A writer that truncates by bytes can leave invalid UTF-8, and decoding it into a Java String
     *   would change the bytes. So we copy the bytes as they are.</li>
     *   <li>Outside the BMP, UTF-16 order can differ from byte order. So we drop the bounds of a file
     *   when either bound has a byte &gt;= 0xF0, the first byte of a 4-byte UTF-8 character.</li>
     *   <li>A missing bound does not limit its side. So we drop the bounds of a file that misses one.</li>
     * </ul>
     * The result has type STRING_LITERAL, and BE never uses such a value as an exact min/max.
     */
    public static TExprMinMaxValue toThriftTopnStringBounds(Schema schema,
                                                            Map<Integer, ByteBuffer> lowerBounds,
                                                            Map<Integer, ByteBuffer> upperBounds,
                                                            Map<Integer, Long> nullValueCounts,
                                                            Map<Integer, Long> valueCounts,
                                                            SlotDescriptor slot) {
        if (!slot.getType().isVarchar() && !slot.getType().isChar()) {
            return null;
        }
        Types.NestedField field = schema.findField(slot.getColumn().getName());
        if (field == null || field.type().typeId() != Type.TypeID.STRING) {
            return null;
        }
        int fieldId = field.fieldId();
        if (nullValueCounts == null || valueCounts == null) {
            return null;
        }
        Long nullValueCount = nullValueCounts.get(fieldId);
        Long valueCount = valueCounts.get(fieldId);
        if (nullValueCount == null || valueCount == null) {
            return null;
        }
        TExprMinMaxValue texpr = new TExprMinMaxValue();
        texpr.setType(TExprNodeType.STRING_LITERAL);
        texpr.setHas_null(nullValueCount > 0);
        texpr.setAll_null(valueCount.longValue() == nullValueCount.longValue());
        if (texpr.isAll_null()) {
            return texpr;
        }
        byte[] lower = rawBoundBytes(lowerBounds, fieldId);
        byte[] upper = rawBoundBytes(upperBounds, fieldId);
        if (lower == null || upper == null || hasNonBmpLeadByte(lower) || hasNonBmpLeadByte(upper)) {
            return null;
        }
        texpr.setMin_string_value(lower);
        texpr.setMax_string_value(upper);
        return texpr;
    }

    private static byte[] rawBoundBytes(Map<Integer, ByteBuffer> bounds, int fieldId) {
        if (bounds == null) {
            return null;
        }
        ByteBuffer buffer = bounds.get(fieldId);
        if (buffer == null) {
            return null;
        }
        // duplicate() so that reading the bytes does not move the position of the shared buffer.
        ByteBuffer copy = buffer.duplicate();
        byte[] bytes = new byte[copy.remaining()];
        copy.get(bytes);
        return bytes;
    }

    private static boolean hasNonBmpLeadByte(byte[] bytes) {
        for (byte b : bytes) {
            if ((b & 0xFF) >= 0xF0) {
                return true;
            }
        }
        return false;
    }

    public static String tableDataLocation(Table table) {
        Preconditions.checkArgument(table != null, "table is null");
        String tableLocation = table.location();
        return table.properties().getOrDefault(TableProperties.WRITE_DATA_LOCATION,
                String.format("%s/data", LocationUtil.stripTrailingSlash(tableLocation)));
    }

    public static CloudConfiguration getVendedCloudConfiguration(String catalogName, IcebergTable icebergTable) {
        CatalogConnector connector = GlobalStateMgr.getCurrentState().getConnectorMgr().getConnector(catalogName);
        Preconditions.checkState(connector != null, "connector of catalog %s should not be null", catalogName);

        // Try to get vended credentials from loadTable response
        CloudConfiguration vendedCredentialsCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForVendedCredentials(icebergTable.getNativeTable().io().properties(),
                        icebergTable.getNativeTable().location());
        if (vendedCredentialsCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return vendedCredentialsCloudConfiguration;
        }

        // Try to get credentials from catalog config (/v1/config response).
        // This is used as fallback when STS is unavailable (e.g., Apache Polaris without STS).
        // getMetadata builds the metadata objects of the connector on every call, so this asks once.
        ConnectorMetadata metadata = connector.getMetadata();
        CloudConfiguration catalogConfigCloudConfiguration = CloudConfigurationFactory.
                buildCloudConfigurationForVendedCredentials(metadata.getCatalogProperties(),
                        icebergTable.getNativeTable().location());
        if (catalogConfigCloudConfiguration.getCloudType() != CloudType.DEFAULT) {
            return catalogConfigCloudConfiguration;
        }

        // Fall back to user-provided catalog credentials
        CloudConfiguration cloudConfiguration = metadata.getCloudConfiguration();
        Preconditions.checkState(cloudConfiguration != null,
                "cloudConfiguration of catalog %s should not be null", catalogName);
        return cloudConfiguration;
    }

    public static void checkFileFormatSupportedDelete(FileScanTask fileScanTask, boolean uedForDelete) {
        // Check file format for DELETE operations
        // Only Parquet format is supported for Iceberg DELETE operations now
        if (uedForDelete && fileScanTask.file().format() != FileFormat.PARQUET) {
            throw new StarRocksConnectorException(
                    String.format("Delete operations on Iceberg tables are only supported for " +
                                    "Parquet format files. Found %s format file: %s",
                            fileScanTask.file().format(), fileScanTask.file().location()));

        }
    }
}
