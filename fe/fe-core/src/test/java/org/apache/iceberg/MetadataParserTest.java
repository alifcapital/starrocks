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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.starrocks.connector.iceberg.TestTables;
import com.starrocks.connector.share.iceberg.CommonMetadataBean;
import com.starrocks.connector.share.iceberg.IcebergMetricsBean;
import com.starrocks.thrift.TIcebergMetadata;
import com.starrocks.thrift.TMetadataEntry;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;

public class MetadataParserTest {
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "p", Types.StringType.get()));

    private static byte[] partitionData(Object... values) {
        Kryo kryo = new Kryo();
        kryo.register(CommonMetadataBean.class);
        kryo.register(IcebergMetricsBean.class);
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try (Output output = new Output(stream)) {
            CommonMetadataBean bean = new CommonMetadataBean();
            bean.setValues(values);
            kryo.writeObject(output, bean);
        }
        return stream.toByteArray();
    }

    private static TMetadataEntry entry(int specId, byte[] partitionData) {
        TIcebergMetadata metadata = new TIcebergMetadata();
        metadata.setContent(FileContent.DATA.id());
        metadata.setFile_path("/tmp/data/" + specId + ".parquet");
        metadata.setFile_format("PARQUET");
        metadata.setSpec_id(specId);
        metadata.setRecord_count(1);
        metadata.setFile_size_in_bytes(10);
        if (partitionData != null) {
            metadata.setPartition_data(partitionData);
        }
        TMetadataEntry entry = new TMetadataEntry();
        entry.setIceberg_metadata(metadata);
        return entry;
    }

    // A table partitioned by p that later dropped the field: a file of the older spec keeps its partition, which
    // its identity column and its partitioned deletes depend on, and a file of the current spec has none.
    @Test
    public void testFileKeepsPartitionOfItsSpecAfterTheSpecDroppedAllFields(@TempDir Path dir) {
        TestTables.TestTable table = TestTables.create(dir.toFile(), "dropped_field", SCHEMA,
                PartitionSpec.builderFor(SCHEMA).identity("p").build(), 2);
        int oldSpecId = table.spec().specId();
        table.updateSpec().removeField("p").commit();
        Assertions.assertTrue(table.spec().isUnpartitioned());

        MetadataParser parser = new MetadataParser(table, null, null, null, null, null, null, null, 0);

        DataFile oldFile = (DataFile) parser.parseThriftToIcebergDataFile(entry(oldSpecId, partitionData("a")));
        Assertions.assertEquals(oldSpecId, oldFile.specId());
        Assertions.assertEquals(1, oldFile.partition().size());
        Assertions.assertEquals("a", oldFile.partition().get(0, String.class));

        DataFile newFile = (DataFile) parser.parseThriftToIcebergDataFile(entry(table.spec().specId(), null));
        Assertions.assertEquals(0, newFile.partition().size());
    }
}
