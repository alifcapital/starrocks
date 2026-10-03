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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class RuntimeFilterPreparedMcvTest {
    @Test
    void cachedCanonicalKeysAreInvalidatedByTypeAndNeverScaledInPlace() {
        var a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        var stats = new MultiColumnCombinedStats(2, 100, List.of(a), List.of(
                new MultiColumnCombinedStats.McvEntry(List.of("01"), 60),
                new MultiColumnCombinedStats.McvEntry(List.of("1"), 40)), List.of(0L));
        var basic = ColumnStatistic.builder().setDistinctValuesCount(2).setNullsFraction(0).build();
        var prepared = stats.getRuntimeFilterHead(0, a.getType());
        assertSame(prepared, stats.getRuntimeFilterHead(0, a.getType()));
        var numeric = RuntimeFilterStatistics.from(a, basic, List.of(stats), 100);
        assertEquals(1, numeric.knownMembership(a.getType(), "001", false).orElseThrow());
        a.setType(VarcharType.VARCHAR);
        assertNotSame(prepared, stats.getRuntimeFilterHead(0, a.getType()));
        var text = RuntimeFilterStatistics.from(a, basic, List.of(stats), 100);
        assertEquals(0, text.knownMembership(a.getType(), "001", false).orElseThrow());
        RuntimeFilterStatistics.from(a, basic, List.of(stats), 0);
        assertEquals(1, RuntimeFilterStatistics.from(a, basic, List.of(stats), 100)
                .knownMembership(a.getType(), "01", false).orElseThrow());
    }
}
