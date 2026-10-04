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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

class ExternalStatisticsRequestTest {
    private static List<String> viaStream(Collection<String> values) {
        return values.stream().distinct().sorted().collect(Collectors.toUnmodifiableList());
    }

    @Test
    void namesAreSortedAndDistinctLikeTheStreamGivesThem() {
        List<List<String>> cases = List.of(
                List.of(),
                List.of("a"),
                List.of("b", "a"),
                List.of("a", "a"),
                List.of("b", "a", "b", "c", "a"),
                List.of("p=2", "p=10", "p=1", "p=2"),
                List.of("Z", "a", "B", "b", "z", "A"),
                List.of("", "a", ""),
                List.of("другая\"колонка", "first", "другая\"колонка"));
        for (List<String> names : cases) {
            ExternalStatisticsRequest request = new ExternalStatisticsRequest("uuid", names, names, false);
            Assertions.assertEquals(viaStream(names), request.partitions, names.toString());
            Assertions.assertEquals(viaStream(names), request.columns, names.toString());
            // Both lists are unmodifiable.
            Assertions.assertThrows(UnsupportedOperationException.class, () -> request.partitions.add("x"));
            Assertions.assertThrows(UnsupportedOperationException.class, () -> request.columns.add("x"));
        }
    }

    @Test
    void anyCollectionOfNamesGivesTheSameRequest() {
        Random random = new Random(11);
        for (int round = 0; round < 200; round++) {
            List<String> names = new ArrayList<>();
            for (int i = random.nextInt(40); i > 0; i--) {
                names.add("c" + random.nextInt(25));
            }
            ExternalStatisticsRequest fromList = new ExternalStatisticsRequest("uuid", names, names, true, false);
            ExternalStatisticsRequest fromSet = new ExternalStatisticsRequest("uuid", new LinkedHashSet<>(names),
                    new LinkedHashSet<>(names), true, false);
            Assertions.assertEquals(viaStream(names), fromList.columns);
            Assertions.assertEquals(viaStream(names), fromList.partitions);
            Assertions.assertEquals(fromList, fromSet);
            Assertions.assertEquals(fromList.hashCode(), fromSet.hashCode());
        }
    }
}
