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

package com.starrocks.sql.optimizer.rule.transformation.partition;

import com.starrocks.catalog.Table;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.thrift.TResultBatch;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class PartitionSelectorExternalStatsTest {
    @Test
    public void testEachPartitionNameIsItsOwnLiteral() {
        AtomicReference<String> captured = new AtomicReference<>();
        new MockUp<SimpleExecutor>() {
            @Mock
            public List<TResultBatch> executeDQL(String sql) {
                captured.set(sql);
                return List.of();
            }
        };
        Table table = Mockito.mock(Table.class);
        Mockito.when(table.getUUID()).thenReturn("uuid-1");
        PartitionSelector.getExternalTablePartitionStats(table, new LinkedHashSet<>(List.of("a", "b", "x'y")));
        Assertions.assertTrue(captured.get().contains("PARTITION_NAME in ('a', 'b', 'x''y')"), captured.get());
    }
}
