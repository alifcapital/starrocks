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
package org.apache.iceberg;

// copy from https://github.com/apache/iceberg/blob/apache-iceberg-1.9.0/core/src/main/java/org/apache/iceberg/PartitionStats.java

import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class PartitionStats implements StructLike {

    private static final int STATS_COUNT = 13;

    private StructLike partition;
    private int specId;
    private long dataRecordCount;
    private int dataFileCount;
    private long totalDataFileSizeInBytes;
    private long positionDeleteRecordCount;
    private int positionDeleteFileCount;
    private long equalityDeleteRecordCount;
    private int equalityDeleteFileCount;
    private Long totalRecordCount;
    private Long lastUpdatedAt;
    private Long lastUpdatedSnapshotId;
    private int dvCount;

    public PartitionStats(StructLike partition, int specId) {
        this.partition = partition;
        this.specId = specId;
    }

    public StructLike partition() {
        return partition;
    }

    public int specId() {
        return specId;
    }

    public long dataRecordCount() {
        return dataRecordCount;
    }

    public int dataFileCount() {
        return dataFileCount;
    }

    public long totalDataFileSizeInBytes() {
        return totalDataFileSizeInBytes;
    }

    public long positionDeleteRecordCount() {
        return positionDeleteRecordCount;
    }

    public int positionDeleteFileCount() {
        return positionDeleteFileCount;
    }

    public long equalityDeleteRecordCount() {
        return equalityDeleteRecordCount;
    }

    public int equalityDeleteFileCount() {
        return equalityDeleteFileCount;
    }

    public Long totalRecords() {
        return totalRecordCount;
    }

    public Long lastUpdatedAt() {
        return lastUpdatedAt;
    }

    public Long lastUpdatedSnapshotId() {
        return lastUpdatedSnapshotId;
    }

    public int dvCount() {
        return dvCount;
    }

    public void liveEntry(ContentFile<?> file, Snapshot snapshot) {
        Preconditions.checkArgument(specId == file.specId(), "Spec IDs must match");

        switch (file.content()) {
            case DATA:
                this.dataRecordCount += file.recordCount();
                this.dataFileCount += 1;
                this.totalDataFileSizeInBytes += file.fileSizeInBytes();
                break;
            case POSITION_DELETES:
                this.positionDeleteRecordCount += file.recordCount();
                if (file.format() == FileFormat.PUFFIN) {
                    this.dvCount += 1;
                } else {
                    this.positionDeleteFileCount += 1;
                }
                break;
            case EQUALITY_DELETES:
                this.equalityDeleteRecordCount += file.recordCount();
                this.equalityDeleteFileCount += 1;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported file content type: " + file.content());
        }

        if (snapshot != null) {
            updateSnapshotInfo(snapshot.snapshotId(), snapshot.timestampMillis());
        }
    }

    public void deletedEntry(Snapshot snapshot) {
        if (snapshot != null) {
            updateSnapshotInfo(snapshot.snapshotId(), snapshot.timestampMillis());
        }
    }

    void deletedEntryForIncrementalCompute(ContentFile<?> file, Snapshot snapshot) {
        Preconditions.checkArgument(specId == file.specId(), "Spec IDs must match");

        switch (file.content()) {
            case DATA:
                this.dataRecordCount -= file.recordCount();
                this.dataFileCount -= 1;
                this.totalDataFileSizeInBytes -= file.fileSizeInBytes();
                break;
            case POSITION_DELETES:
                this.positionDeleteRecordCount -= file.recordCount();
                if (file.format() == FileFormat.PUFFIN) {
                    this.dvCount -= 1;
                } else {
                    this.positionDeleteFileCount -= 1;
                }
                break;
            case EQUALITY_DELETES:
                this.equalityDeleteRecordCount -= file.recordCount();
                this.equalityDeleteFileCount -= 1;
                break;
            default:
                throw new UnsupportedOperationException("Unsupported file content type: " + file.content());
        }

        if (snapshot != null) {
            updateSnapshotInfo(snapshot.snapshotId(), snapshot.timestampMillis());
        }
    }

    public void appendStats(PartitionStats entry) {
        Preconditions.checkArgument(specId == entry.specId(), "Spec IDs must match");

        this.dataRecordCount += entry.dataRecordCount;
        this.dataFileCount += entry.dataFileCount;
        this.totalDataFileSizeInBytes += entry.totalDataFileSizeInBytes;
        this.positionDeleteRecordCount += entry.positionDeleteRecordCount;
        this.positionDeleteFileCount += entry.positionDeleteFileCount;
        this.equalityDeleteRecordCount += entry.equalityDeleteRecordCount;
        this.equalityDeleteFileCount += entry.equalityDeleteFileCount;
        this.dvCount += entry.dvCount;

        if (entry.totalRecordCount != null) {
            if (totalRecordCount == null) {
                this.totalRecordCount = entry.totalRecordCount;
            } else {
                this.totalRecordCount += entry.totalRecordCount;
            }
        }

        if (entry.lastUpdatedAt != null) {
            updateSnapshotInfo(entry.lastUpdatedSnapshotId, entry.lastUpdatedAt);
        }
    }

    private void updateSnapshotInfo(long snapshotId, long updatedAt) {
        if (lastUpdatedAt == null || lastUpdatedAt < updatedAt) {
            this.lastUpdatedAt = updatedAt;
            this.lastUpdatedSnapshotId = snapshotId;
        }
    }

    @Override
    public int size() {
        return STATS_COUNT;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        switch (pos) {
            case 0:
                return javaClass.cast(partition);
            case 1:
                return javaClass.cast(specId);
            case 2:
                return javaClass.cast(dataRecordCount);
            case 3:
                return javaClass.cast(dataFileCount);
            case 4:
                return javaClass.cast(totalDataFileSizeInBytes);
            case 5:
                return javaClass.cast(positionDeleteRecordCount);
            case 6:
                return javaClass.cast(positionDeleteFileCount);
            case 7:
                return javaClass.cast(equalityDeleteRecordCount);
            case 8:
                return javaClass.cast(equalityDeleteFileCount);
            case 9:
                return javaClass.cast(totalRecordCount);
            case 10:
                return javaClass.cast(lastUpdatedAt);
            case 11:
                return javaClass.cast(lastUpdatedSnapshotId);
            case 12:
                return javaClass.cast(dvCount);
            default:
                throw new UnsupportedOperationException("Unknown position: " + pos);
        }
    }

    @Override
    public <T> void set(int pos, T value) {
        switch (pos) {
            case 0:
                this.partition = (StructLike) value;
                break;
            case 1:
                this.specId = (int) value;
                break;
            case 2:
                this.dataRecordCount = (long) value;
                break;
            case 3:
                this.dataFileCount = (int) value;
                break;
            case 4:
                this.totalDataFileSizeInBytes = (long) value;
                break;
            case 5:
                this.positionDeleteRecordCount = value == null ? 0L : (long) value;
                break;
            case 6:
                this.positionDeleteFileCount = value == null ? 0 : (int) value;
                break;
            case 7:
                this.equalityDeleteRecordCount = value == null ? 0L : (long) value;
                break;
            case 8:
                this.equalityDeleteFileCount = value == null ? 0 : (int) value;
                break;
            case 9:
                this.totalRecordCount = (Long) value;
                break;
            case 10:
                this.lastUpdatedAt = (Long) value;
                break;
            case 11:
                this.lastUpdatedSnapshotId = (Long) value;
                break;
            case 12:
                this.dvCount = value == null ? 0 : (int) value;
                break;
            default:
                throw new UnsupportedOperationException("Unknown position: " + pos);
        }
    }
}
