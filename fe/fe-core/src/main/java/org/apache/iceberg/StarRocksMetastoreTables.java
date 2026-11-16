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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

// Lives in org.apache.iceberg for the protected members of BaseMetastoreCatalog and BaseMetastoreTableOperations.
public final class StarRocksMetastoreTables {
    private StarRocksMetastoreTables() {
    }

    // We want the table that BaseMetastoreCatalog.loadTable returns, but the caller has just read the metadata
    // location from the metastore, and loadTable would ask the metastore for it again when it refreshes the new
    // operations. So the operations read the metadata file at the given location. They end in the same state as
    // after loadTable: the current metadata and its location are set and no refresh is pending, and a commit
    // reads the metastore itself.
    public static Table loadAt(BaseMetastoreCatalog catalog, TableIdentifier identifier, String metadataLocation) {
        Preconditions.checkArgument(metadataLocation != null, "Metadata location of %s is null", identifier);
        TableOperations ops = catalog.newTableOps(identifier);
        Preconditions.checkState(ops instanceof BaseMetastoreTableOperations,
                "Unexpected table operations %s of catalog %s", ops.getClass().getName(), catalog.name());
        ((BaseMetastoreTableOperations) ops).refreshFromMetadataLocation(metadataLocation);
        return new BaseTable(ops, BaseMetastoreCatalog.fullTableName(catalog.name(), identifier),
                catalog.metricsReporter());
    }
}
