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

package com.starrocks.rpc;

import com.starrocks.planner.PlanFragment;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TPlanFragment;
import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ByteArrayTSerializerTest extends PlanTestBase {
    @Test
    public void testSameBytesAsTheStockSerializer() throws Exception {
        ExecPlan plan = getExecPlan("select v1, sum(v2) from t0 join t1 on v1 = v4 " +
                "where v3 in (1, 2, 3, 4, 5) and v5 > 10 group by v1 order by v1 limit 10");
        List<TBase<?, ?>> messages = new ArrayList<>();
        for (PlanFragment fragment : plan.getFragments()) {
            TPlanFragment thrift = fragment.toThrift();
            messages.add(thrift);
        }
        messages.add(plan.getDescTbl().toThrift());
        for (ConfigurableSerDesFactory.Protocol protocol : ConfigurableSerDesFactory.Protocol.values()) {
            TSerializer stock = new TSerializer(ConfigurableTProtocolFactory.getTProtocolFactory(protocol));
            // One serializer for all the messages, so every message after the first reuses the buffer.
            TSerializer serializer = ConfigurableSerDesFactory.getTSerializer(protocol.name());
            Assertions.assertInstanceOf(ByteArrayTSerializer.class, serializer);
            for (TBase<?, ?> message : messages) {
                Assertions.assertArrayEquals(stock.serialize(message), serializer.serialize(message),
                        protocol + " " + message.getClass().getSimpleName());
            }
        }
    }
}
