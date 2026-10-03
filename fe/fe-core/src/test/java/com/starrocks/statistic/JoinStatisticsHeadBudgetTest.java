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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

class JoinStatisticsHeadBudgetTest {
    @Test
    void integerDisplayWidthsDoNotTurnNumericHeadsIntoText() {
        for (String type : List.of("TINYINT", "SMALLINT(6)", "int(11)", "BIGINT(20)")) {
            Assertions.assertTrue(JoinStatisticsCollector.scalarIntegerKey(List.of(type)));
        }
        Assertions.assertFalse(JoinStatisticsCollector.scalarIntegerKey(List.of("VARCHAR(36)")));
        Assertions.assertFalse(JoinStatisticsCollector.scalarIntegerKey(List.of("BIGINT", "BIGINT")));
    }

    @Test
    void threeFullUuidHeadsAreNeverDropped() {
        int[] uuid = new int[16384];
        Arrays.fill(uuid, 36);
        var selected = JoinStatisticsHeadBudget.select(List.of(uuid, uuid, uuid));
        Assertions.assertTrue(selected.stream().allMatch(bits -> bits.cardinality() == 16384));
        Assertions.assertEquals(1769472, 3 * 16384 * 36);
    }

    @Test
    void shortLabelsSurviveEvenWhenTheyExceedTheBudget() {
        int[] lengths = new int[16384];
        Arrays.fill(lengths, 256);
        lengths[16383] = 257;
        var selected = JoinStatisticsHeadBudget.select(List.of(lengths, lengths, lengths));
        Assertions.assertTrue(selected.stream().allMatch(bits -> bits.cardinality() == 16383 && !bits.get(16383)));
    }

    @Test
    void hugeLabelsDoNotBlockAffordableLabelsAndDomainsShareBudget() {
        var selected = JoinStatisticsHeadBudget.select(List.of(new int[] {9000, 600, 400, 0},
                new int[] {600, 400}), 2000);
        Assertions.assertFalse(selected.get(0).get(0));
        Assertions.assertEquals(3, selected.get(0).cardinality());
        Assertions.assertEquals(2, selected.get(1).cardinality());
        var donated = JoinStatisticsHeadBudget.select(List.of(new int[] {1500}, new int[] {300}), 1800);
        Assertions.assertEquals(1, donated.get(0).cardinality());
        Assertions.assertEquals(1, donated.get(1).cardinality());
    }

    @Test
    void selectionAlwaysHonorsByteAccountingAndShortGuarantee() {
        Random random = new Random(37);
        for (int run = 0; run < 100; run++) {
            List<int[]> lengths = List.of(new int[200], new int[300], new int[400]);
            lengths.forEach(domain -> Arrays.setAll(domain, i -> random.nextInt(2048)));
            long budget = random.nextInt(100000);
            var selected = JoinStatisticsHeadBudget.select(lengths, budget);
            long used = 0;
            long mandatory = 0;
            for (int d = 0; d < lengths.size(); d++) {
                for (int i = 0; i < lengths.get(d).length; i++) {
                    int bytes = lengths.get(d)[i];
                    if (bytes <= 256) {
                        mandatory += bytes;
                        Assertions.assertTrue(selected.get(d).get(i));
                    }
                    if (selected.get(d).get(i)) {
                        used += bytes;
                    }
                }
            }
            Assertions.assertTrue(used <= Math.max(budget, mandatory));
        }
    }
}
