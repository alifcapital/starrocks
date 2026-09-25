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

package com.starrocks.warehouse.multi;

import com.starrocks.common.Config;
import com.starrocks.extension.ExtensionContext;
import com.starrocks.extension.SRModule;
import com.starrocks.extension.StarRocksExtension;
import com.starrocks.server.WarehouseManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Registers {@link MultiWarehouseManager} when multi-warehouse execution is enabled.
 */
@SRModule(name = "multi_warehouse")
public class MultiWarehouseExtension implements StarRocksExtension {
    private static final Logger LOG = LogManager.getLogger(MultiWarehouseExtension.class);

    @Override
    public void onLoad(ExtensionContext ctx) {
        if (!Config.enable_multi_warehouse) {
            LOG.info("multi_warehouse extension is disabled by enable_multi_warehouse");
            return;
        }
        ctx.register(WarehouseManager.class, new MultiWarehouseManager());
        LOG.info("multi_warehouse extension loaded, WarehouseManager -> {}",
                MultiWarehouseManager.class.getName());
    }
}
