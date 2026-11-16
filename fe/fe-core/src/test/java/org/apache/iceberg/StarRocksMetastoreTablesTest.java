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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class StarRocksMetastoreTablesTest {
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

    private static DataFile dataFile(String name) {
        return DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/tbl/data/" + name + ".parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();
    }

    private static String metadataLocation(Table table) {
        return ((BaseTable) table).operations().current().metadataFileLocation();
    }

    @Test
    public void tableAtLocationMatchesLoadedTableAndCommits() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("cat", Map.of());
        TableIdentifier id = TableIdentifier.of(Namespace.of("db"), "tbl");
        catalog.createNamespace(Namespace.of("db"));
        Table created = catalog.createTable(id, SCHEMA);
        created.newAppend().appendFile(dataFile("a")).commit();
        String firstLocation = metadataLocation(created);
        long firstSnapshotId = created.currentSnapshot().snapshotId();
        created.newAppend().appendFile(dataFile("b")).commit();

        Table loaded = catalog.loadTable(id);
        Table atLocation = StarRocksMetastoreTables.loadAt(catalog, id, metadataLocation(loaded));
        Assertions.assertEquals(loaded.name(), atLocation.name());
        Assertions.assertEquals(metadataLocation(loaded), metadataLocation(atLocation));
        Assertions.assertEquals(loaded.currentSnapshot().snapshotId(), atLocation.currentSnapshot().snapshotId());
        Assertions.assertTrue(loaded.schema().sameSchema(atLocation.schema()));

        // the table is read at the given location, not at the one the catalog holds now
        Table older = StarRocksMetastoreTables.loadAt(catalog, id, firstLocation);
        Assertions.assertEquals(firstLocation, metadataLocation(older));
        Assertions.assertEquals(firstSnapshotId, older.currentSnapshot().snapshotId());

        // a commit through the table reads the catalog itself, so it lands on top of the newest metadata
        atLocation.newAppend().appendFile(dataFile("c")).commit();
        Table reloaded = catalog.loadTable(id);
        Assertions.assertEquals(atLocation.currentSnapshot().snapshotId(), reloaded.currentSnapshot().snapshotId());
        Assertions.assertEquals(3, Iterables.size(reloaded.snapshots()));
    }

    @Test
    public void nullLocationIsRejected() {
        InMemoryCatalog catalog = new InMemoryCatalog();
        catalog.initialize("cat", Map.of());
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> StarRocksMetastoreTables.loadAt(catalog, TableIdentifier.of("db", "tbl"), null));
    }
}
