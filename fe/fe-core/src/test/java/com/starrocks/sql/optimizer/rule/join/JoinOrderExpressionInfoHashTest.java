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

package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JoinOrderExpressionInfoHashTest {
    @Test
    public void hashEqualsObjectsHashOfOperatorAndChildren() {
        // The greedy reorder keeps its top-K expressions in a queue that breaks cost ties by hashCode.
        // We expect ExpressionInfo.hashCode to equal Objects.hash(operator hash, left child, right child),
        // including null children, negative hashes and int overflow, so that tie order and plans stay the same.
        int[] values = {0, 1, -1, 127, -128, Integer.MIN_VALUE, Integer.MAX_VALUE};
        for (int op : values) {
            for (int left = -1; left < values.length; left++) {
                for (int right = -1; right < values.length; right++) {
                    JoinOrder.ExpressionInfo info = new JoinOrder.ExpressionInfo(
                            OptExpression.create(new HashValues(op)), left < 0 ? null : new HashGroup(values[left]),
                            right < 0 ? null : new HashGroup(values[right]));
                    assertEquals(Objects.hash(info.expr.getOp().hashCode(), info.leftChildExpr, info.rightChildExpr),
                            info.hashCode());
                }
            }
        }
    }

    private static final class HashValues extends LogicalValuesOperator {
        private final int value;

        HashValues(int value) {
            super(List.of());
            this.value = value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }

    private static final class HashGroup extends JoinOrder.GroupInfo {
        private final int value;

        HashGroup(int value) {
            super(new BitSet());
            this.value = value;
        }

        @Override
        public int hashCode() {
            return value;
        }
    }
}
