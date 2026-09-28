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

package com.starrocks.statistic;

import com.google.common.hash.Hashing;
import com.starrocks.sql.optimizer.statistics.JoinStatisticsCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

class JoinStatisticsStorageTest {
    @Test
    void completeManifestRestoresGeneration() throws Exception {
        byte[] bytes = JoinStatisticsCodec.encode(JoinStatisticsCacheTest.data(1, 2), 1 << 20);
        JoinStatisticsMeta meta = new JoinStatisticsMeta(1, JoinStatisticsCacheTest.definition(), 2, 1, bytes.length,
                Hashing.sha256().hashBytes(bytes).toString(), 1);
        var rows = List.of(List.of("0", Base64.getEncoder().encodeToString(bytes)));
        Assertions.assertEquals(2, JoinStatisticsStorage.decodeParts(meta, rows, 1 << 20).getGeneration());
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsStorage.decodeParts(meta, List.of(), 1 << 20));
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsStorage.decodeParts(meta,
                List.of(List.of("1", rows.get(0).get(1))), 1 << 20));
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsStorage.decodeParts(meta,
                List.of(List.of("0", "!")), 1 << 20));
        bytes[bytes.length - 1] ^= 1;
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsStorage.decodeParts(meta,
                List.of(List.of("0", Base64.getEncoder().encodeToString(bytes))), 1 << 20));
    }
}
