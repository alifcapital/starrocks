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

package com.starrocks.system;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

class AliveNodeCountTest {
    @Test
    void countsMatchIdsAndFollowLivenessAndMembershipChanges() {
        SystemInfoService service = new SystemInfoService();
        Assertions.assertEquals(0, service.getAliveBackendNumber());
        Assertions.assertEquals(0, service.getAliveComputeNodeNumber());
        for (int i = 0; i < 128; i++) {
            Backend backend = new Backend(i, "127.0.0.1", 9000 + i);
            backend.setAlive(i % 3 == 0);
            backend.setDecommissioned(i % 5 == 0);
            service.idToBackendRef.put((long) i, backend);
            ComputeNode node = new ComputeNode(i, "127.0.0.1", 10000 + i);
            node.setAlive(i % 2 == 0);
            service.idToComputeNodeRef.put((long) i, node);
        }
        Assertions.assertEquals(43, service.getAliveBackendNumber());
        Assertions.assertEquals(64, service.getAliveComputeNodeNumber());
        for (int i = 0; i < 128; i++) {
            service.idToBackendRef.get((long) i).setAlive(i % 2 == 0);
            service.idToComputeNodeRef.get((long) i).setAlive(i % 3 == 0);
            Assertions.assertEquals(service.getBackendIds(true).size(), service.getAliveBackendNumber());
            Assertions.assertEquals(service.getComputeNodeIds(true).size(), service.getAliveComputeNodeNumber());
        }
        for (int i = 0; i < 128; i += 2) {
            service.idToBackendRef.remove((long) i);
            service.idToComputeNodeRef.remove((long) i);
        }
        Assertions.assertEquals(service.getBackendIds(true).size(), service.getAliveBackendNumber());
        Assertions.assertEquals(service.getComputeNodeIds(true).size(), service.getAliveComputeNodeNumber());
        service.idToBackendRef = new ConcurrentHashMap<>();
        service.idToComputeNodeRef = new ConcurrentHashMap<>();
        Assertions.assertEquals(0, service.getAliveBackendNumber());
        Assertions.assertEquals(0, service.getAliveComputeNodeNumber());
    }
}
