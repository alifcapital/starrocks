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

import com.google.common.collect.ImmutableList;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.glue.IcebergGlueCatalog;
import com.starrocks.connector.share.iceberg.IcebergAwsClientFactory;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mocked;
import mockit.Verifications;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergGlueCatalogTest {
    public static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        connectContext = UtFrameUtils.createDefaultCtx();
    }

    @Test
    public void testListAllDatabases(@Mocked GlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.listNamespaces();
                result = ImmutableList.of(Namespace.of("db1"), Namespace.of("db2"));
                times = 1;
            }
        };

        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);
        List<String> dbs = icebergGlueCatalog.listAllDatabases(connectContext);
        Assertions.assertEquals(Arrays.asList("db1", "db2"), dbs);
    }

    @Test
    public void testTableExists(@Mocked GlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);
        Assertions.assertTrue(icebergGlueCatalog.tableExists(connectContext, "db1", "tbl1"));
    }

    @Test
    public void testRenameTable(@Mocked GlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.tableExists((TableIdentifier) any);
                result = true;
            }
        };
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);
        icebergGlueCatalog.renameTable(connectContext, "db", "tb1", "tb2");
        boolean exists = icebergGlueCatalog.tableExists(connectContext, "db", "tbl2");
        Assertions.assertTrue(exists);
    }

    @Test
    public void testCreateDBWithInvalidLocationURI(@Mocked GlueCatalog glueCatalog) {
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);

        // An unsupported scheme causes FileSystem.get() to throw, which should surface via fromExternalException
        Map<String, String> dbProperties = new HashMap<>();
        dbProperties.put("location", "unsupportedscheme://bucket/path");
        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Invalid location URI: unsupportedscheme://bucket/path",
                () -> icebergGlueCatalog.createDB(connectContext, "testdb", dbProperties));
    }

    @Test
    public void testDropDBWhenGetDBThrows(@Mocked GlueCatalog glueCatalog) {
        new Expectations() {
            {
                glueCatalog.loadNamespaceMetadata((Namespace) any);
                result = new RuntimeException("Glue access denied");
            }
        };
        Map<String, String> icebergProperties = new HashMap<>();
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), icebergProperties);

        ExceptionChecker.expectThrowsWithMsg(StarRocksConnectorException.class,
                "Failed to access database testdb",
                () -> icebergGlueCatalog.dropDB(connectContext, "testdb"));
    }

    private static GetTableResponse glueTableResponse(String tableType, String metadataLocation) {
        Map<String, String> parameters = new HashMap<>();
        if (tableType != null) {
            parameters.put(BaseMetastoreTableOperations.TABLE_TYPE_PROP, tableType);
        }
        parameters.put(BaseMetastoreTableOperations.METADATA_LOCATION_PROP, metadataLocation);
        return GetTableResponse.builder()
                .table(software.amazon.awssdk.services.glue.model.Table.builder().parameters(parameters).build())
                .build();
    }

    // The metadata location comes from one Glue GetTable with the catalog id, database and table of the wrapped
    // GlueCatalog, and only an Iceberg table that Glue has gives a location. The Glue client is built once.
    @Test
    public void testGetCurrentMetadataLocation(@Mocked GlueCatalog glueCatalog) {
        GlueClient glueClient = Mockito.mock(GlueClient.class);
        AwsClientFactory clientFactory = Mockito.mock(AwsClientFactory.class);
        Mockito.when(clientFactory.glue()).thenReturn(glueClient);
        Mockito.when(glueClient.getTable(Mockito.any(GetTableRequest.class))).thenReturn(
                glueTableResponse("ICEBERG", "s3://bucket/db/tbl/metadata/00002.metadata.json"),
                glueTableResponse("HIVE", "s3://bucket/db/tbl/metadata/00002.metadata.json"),
                glueTableResponse(null, "s3://bucket/db/tbl/metadata/00002.metadata.json"))
                .thenThrow(EntityNotFoundException.builder().message("missing").build());
        Map<String, String> properties = new HashMap<>();
        properties.put(AwsProperties.GLUE_CATALOG_ID, "123456789012");
        try (MockedStatic<AwsClientFactories> factories = Mockito.mockStatic(AwsClientFactories.class)) {
            factories.when(() -> AwsClientFactories.from(Mockito.anyMap())).thenReturn(clientFactory);
            IcebergGlueCatalog catalog = new IcebergGlueCatalog("glue_native_catalog", new Configuration(), properties);

            Assertions.assertEquals("s3://bucket/db/tbl/metadata/00002.metadata.json",
                    catalog.getCurrentMetadataLocation("db", "tbl"));
            Assertions.assertNull(catalog.getCurrentMetadataLocation("db", "tbl"));
            Assertions.assertNull(catalog.getCurrentMetadataLocation("db", "tbl"));
            Assertions.assertNull(catalog.getCurrentMetadataLocation("db", "tbl"));

            ArgumentCaptor<GetTableRequest> requests = ArgumentCaptor.forClass(GetTableRequest.class);
            Mockito.verify(glueClient, Mockito.times(4)).getTable(requests.capture());
            GetTableRequest request = requests.getAllValues().get(0);
            Assertions.assertEquals("123456789012", request.catalogId());
            Assertions.assertEquals("db", request.databaseName());
            Assertions.assertEquals("tbl", request.name());
            @SuppressWarnings("unchecked")
            ArgumentCaptor<Map<String, String>> clientProperties = ArgumentCaptor.forClass(Map.class);
            factories.verify(() -> AwsClientFactories.from(clientProperties.capture()), Mockito.times(1));
            Assertions.assertEquals(IcebergAwsClientFactory.class.getName(),
                    clientProperties.getValue().get(AwsProperties.CLIENT_FACTORY));
            Assertions.assertEquals("123456789012", clientProperties.getValue().get(AwsProperties.GLUE_CATALOG_ID));

            catalog.shutdown();
            Mockito.verify(glueClient).close();
            Assertions.assertThrows(IllegalStateException.class, () -> catalog.getCurrentMetadataLocation("db", "tbl"));
        }
    }

    // The connector shuts the catalog down when the catalog is dropped or replaced and when a checkpoint discards
    // its copy of the metadata, and the wrapped Iceberg catalog releases what it holds only when it is closed.
    @Test
    public void testShutdownClosesIcebergCatalog(@Mocked GlueCatalog glueCatalog) throws Exception {
        IcebergGlueCatalog catalog = new IcebergGlueCatalog("glue_native_catalog", new Configuration(), new HashMap<>());
        catalog.shutdown();
        new Verifications() {
            {
                glueCatalog.close();
                times = 1;
            }
        };
    }

    // A failure to close the Iceberg catalog does not fail the shutdown of the connector.
    @Test
    public void testShutdownToleratesCloseFailure(@Mocked GlueCatalog glueCatalog) throws Exception {
        new Expectations() {
            {
                glueCatalog.close();
                result = new IOException("close failed");
            }
        };
        IcebergGlueCatalog icebergGlueCatalog = new IcebergGlueCatalog(
                "glue_native_catalog", new Configuration(), new HashMap<>());
        Assertions.assertDoesNotThrow(icebergGlueCatalog::shutdown);
    }
}
