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

import com.starrocks.connector.RemoteFileInfoSource;
import com.starrocks.planner.PartitionIdGenerator;
import com.starrocks.thrift.THdfsScanRange;
import com.starrocks.thrift.TScanRangeLocations;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

public class IcebergScanRangePositionDeletesTest {

    private static FileScanTask taskWithPositionDelete() {
        DataFile dataFile = Mockito.mock(DataFile.class);
        DeleteFile posDelete = Mockito.mock(DeleteFile.class);
        Mockito.when(posDelete.content()).thenReturn(FileContent.POSITION_DELETES);
        Mockito.when(posDelete.format()).thenReturn(FileFormat.PARQUET);
        Mockito.when(posDelete.path()).thenReturn("s3://bucket/t/data/pos-deletes.parquet");
        Mockito.when(posDelete.location()).thenReturn("s3://bucket/t/data/pos-deletes.parquet");
        Mockito.when(posDelete.fileSizeInBytes()).thenReturn(10L);

        FileScanTask task = Mockito.mock(FileScanTask.class);
        Mockito.when(task.file()).thenReturn(dataFile);
        Mockito.when(task.deletes()).thenReturn(List.of(posDelete));
        return task;
    }

    private static IcebergConnectorScanRangeSource newSource() {
        return new IcebergConnectorScanRangeSource(null, Mockito.mock(RemoteFileInfoSource.class),
                IcebergMORParams.EMPTY, null, Optional.empty(), PartitionIdGenerator.of(), false, false) {
            @Override
            public long addPartition(FileScanTask task) {
                return 0;
            }

            @Override
            protected THdfsScanRange buildScanRange(FileScanTask task, ContentFile<?> file, Long partitionId) {
                return new THdfsScanRange();
            }
        };
    }

    @Test
    public void testScanRangeCarriesPositionDeletes() {
        IcebergConnectorScanRangeSource source = newSource();
        List<TScanRangeLocations> ranges = source.toScanRanges(taskWithPositionDelete());

        Assertions.assertEquals(1, ranges.size());
        THdfsScanRange range = ranges.get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertTrue(range.isSetDelete_files());
        Assertions.assertEquals(1, range.getDelete_files().size());
    }

    @Test
    public void testScanRangeIgnoresPositionDeletes() {
        IcebergConnectorScanRangeSource source = newSource();
        source.setIgnorePositionDeletes(true);
        List<TScanRangeLocations> ranges = source.toScanRanges(taskWithPositionDelete());

        Assertions.assertEquals(1, ranges.size());
        THdfsScanRange range = ranges.get(0).getScan_range().getHdfs_scan_range();
        Assertions.assertFalse(range.isSetDelete_files());
    }
}
