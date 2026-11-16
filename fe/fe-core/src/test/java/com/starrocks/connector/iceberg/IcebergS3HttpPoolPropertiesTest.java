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

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.share.iceberg.SharedS3HttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

// A catalog is created only with valid settings of its S3 HTTP pool, and a user cannot name the pool.
public class IcebergS3HttpPoolPropertiesTest {

    private static IcebergCatalogProperties catalog(String key, String value) {
        Map<String, String> properties = new HashMap<>();
        properties.put(IcebergCatalogProperties.ICEBERG_CATALOG_TYPE, "rest");
        properties.put(key, value);
        return new IcebergCatalogProperties(properties);
    }

    @Test
    public void validSettingsAreAccepted() {
        catalog(SharedS3HttpClient.MAX_CONNECTIONS, "512");
        catalog(SharedS3HttpClient.CONNECTION_TIME_TO_LIVE_MS, "300000");
        catalog("ICEBERG.CATALOG." + SharedS3HttpClient.MAX_CONNECTIONS, "64");
    }

    @Test
    public void badSettingsAreRejected() {
        for (String key : new String[] {SharedS3HttpClient.MAX_CONNECTIONS,
                IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX + SharedS3HttpClient.MAX_CONNECTIONS}) {
            for (String value : new String[] {"0", "4097", "many", "100%"}) {
                StarRocksConnectorException e = Assertions.assertThrows(StarRocksConnectorException.class,
                        () -> catalog(key, value), key + "=" + value);
                Assertions.assertTrue(e.getMessage().contains(value), e.getMessage());
            }
        }
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> catalog(SharedS3HttpClient.CONNECTION_TIME_TO_LIVE_MS, "-1"));
    }

    @Test
    public void thePoolCannotBeNamedByAUser() {
        for (String key : new String[] {SharedS3HttpClient.POOL_CATALOG, SharedS3HttpClient.POOL_ID,
                IcebergCatalogProperties.ICEBERG_CUSTOM_PROPERTIES_PREFIX + SharedS3HttpClient.POOL_ID}) {
            Assertions.assertThrows(StarRocksConnectorException.class, () -> catalog(key, "other"), key);
        }
    }
}
