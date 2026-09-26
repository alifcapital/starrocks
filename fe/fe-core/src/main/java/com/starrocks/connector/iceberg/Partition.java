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

import com.starrocks.connector.PartitionInfo;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

import java.util.concurrent.TimeUnit;

public class Partition implements PartitionInfo {
    private final long modifiedTime;
    private final long version;
    private int specId;
    // Typed values in this partition's own spec order. No DataFile or manifest is retained.
    private IcebergPartitionData values;

    public void clearValues() {
        values = null;
    }

    public StructLike getValues() {
        return values;
    }

    // Metadata table rows can reuse their backing storage. Copy once into the weighted partition cache.
    public void setValues(PartitionSpec spec, Types.StructType unionType, StructLike unionValues) {
        IcebergPartitionData copy = new IcebergPartitionData(spec.fields().size());
        for (int i = 0; i < spec.fields().size(); i++) {
            int id = spec.fields().get(i).fieldId();
            boolean found = false;
            for (int j = 0; j < unionType.fields().size(); j++) {
                if (unionType.fields().get(j).fieldId() == id) {
                    Object value = unionValues.get(j, Object.class);
                    Class<?> expected = spec.javaClasses()[i];
                    // The union partition type can promote an old int field to long.
                    if (value instanceof Long number && expected == Integer.class) {
                        if (number < Integer.MIN_VALUE || number > Integer.MAX_VALUE) {
                            values = null;
                            return;
                        }
                        value = number.intValue();
                    } else if (value instanceof Integer number && expected == Long.class) {
                        value = number.longValue();
                    }
                    if (value instanceof byte[] bytes) {
                        value = bytes.clone();
                    }
                    copy.set(i, value instanceof CharSequence ? value.toString() : value);
                    found = true;
                    break;
                }
            }
            if (!found) {
                values = null;
                return;
            }
        }
        values = copy;
    }

    // Row counts from the Iceberg PARTITIONS metadata table. recordCount is the pre-delete live-data-file row
    // count; the delete counts track rows logically removed by MOR delete files. -1 means unknown (metadata
    // did not carry it). Used by bounded-cost statistics collection to extrapolate a truncated sample back to
    // the full partition, and to gate that extrapolation when deletes make record_count unreliable as a
    // live-row total (see IcebergPartitionTraits#getPartitionRowCounts).
    private long recordCount = -1;
    private long positionDeleteRecordCount = -1;
    private long equalityDeleteRecordCount = -1;

    @Override
    public long getModifiedTime() {
        return modifiedTime;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public long getPositionDeleteRecordCount() {
        return positionDeleteRecordCount;
    }

    public void setPositionDeleteRecordCount(long positionDeleteRecordCount) {
        this.positionDeleteRecordCount = positionDeleteRecordCount;
    }

    public long getEqualityDeleteRecordCount() {
        return equalityDeleteRecordCount;
    }

    public void setEqualityDeleteRecordCount(long equalityDeleteRecordCount) {
        this.equalityDeleteRecordCount = equalityDeleteRecordCount;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public TimeUnit getModifiedTimeUnit() {
        return TimeUnit.MICROSECONDS;
    }

    public int getSpecId() {
        return specId;
    }

    public Partition(long modifiedTime) {
        this(modifiedTime, modifiedTime, -1);
    }

    public Partition(long modifiedTime, int specId) {
        this(modifiedTime, modifiedTime, specId);
    }

    public Partition(long modifiedTime, long version) {
        this(modifiedTime, version, -1);
    }

    public Partition(long modifiedTime, long version, int specId) {
        this.modifiedTime = modifiedTime;
        this.version = version;
        this.specId = specId;
    }
}
