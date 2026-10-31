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

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Map;

public class IcebergMetadataScannerPartitionTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "p", Types.StringType.get()));

    // A table partitioned by p that later dropped the field: a file of the older spec still sends its partition,
    // and a file of the current, unpartitioned spec sends none.
    @Test
    public void testFileOfOlderPartitionedSpecHasPartitionData(@TempDir Path dir) {
        PartitionSpec byP = PartitionSpec.builderFor(SCHEMA).identity("p").build();
        Table table = new HadoopTables(new Configuration()).create(SCHEMA, byP,
                Map.of(TableProperties.FORMAT_VERSION, "2"), dir.resolve("t").toString());
        table.updateSpec().removeField("p").commit();
        Assertions.assertTrue(table.spec().isUnpartitioned());

        DataFile oldFile = DataFiles.builder(table.specs().get(byP.specId()))
                .withPath(dir.resolve("a.parquet").toString())
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .withPartitionPath("p=a")
                .build();
        DataFile newFile = DataFiles.builder(table.spec())
                .withPath(dir.resolve("b.parquet").toString())
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();

        Assertions.assertTrue(IcebergMetadataScanner.hasPartitionData(table, oldFile));
        Assertions.assertFalse(IcebergMetadataScanner.hasPartitionData(table, newFile));
    }
}
