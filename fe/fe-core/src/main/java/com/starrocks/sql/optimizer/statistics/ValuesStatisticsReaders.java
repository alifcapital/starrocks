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

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEConsumeOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalCTEProduceOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalLimitOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTreeAnchorOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;

import java.util.HashSet;
import java.util.Set;

/**
 * Whether the statistics of the columns of a VALUES operator have a reader in a query.
 *
 * We expect most VALUES lists to be the rows of an INSERT, with only a projection above them, and nothing reads
 * the distribution of their columns. Counting the distinct values of a list of a million rows then costs time that
 * the plan never uses. So the statistics of VALUES are computed only when an operator above some VALUES can read
 * them, see OptimizerContext#isValuesStatisticsRead.
 *
 * An operator that passes its input on and only derives its output statistics from it does not read the
 * distribution of the columns: a VALUES, a projection, a limit, the operators of a CTE, a UNION ALL, a scan and the
 * anchor that the rewrite rules put above the tree. Every other operator is a reader, including the operators that
 * we do not know yet. We fear a wrong class, and the cost of each is small: a reader that we take for a
 * pass-through leaves the statistics as they were without this work, and a pass-through that we take for a reader
 * only costs the time of the count.
 *
 * A CTE with one reference is inlined into the tree. A CTE with more references is a producer and its consumers, and
 * the consumers can have no input; the body of the producer is read where its consumers are read. So the ancestors
 * of an operator in the body of a producer include the ancestors of every consumer of it.
 */
public final class ValuesStatisticsReaders {
    private ValuesStatisticsReaders() {
    }

    /**
     * True when some VALUES in the tree has a reader among its ancestors. The result does not depend on the order
     * of the children of an operator: the CTEs that are read are found first, over the whole tree.
     */
    public static boolean hasReader(OptExpression root) {
        Set<Integer> readCtes = new HashSet<>();
        // A consumer in the body of a producer is read when its producer is read, and the producer can come after
        // the consumer in the walk, so we repeat the walk until it marks no more CTEs.
        boolean marked;
        do {
            marked = markReadCtes(root, false, readCtes);
        } while (marked);
        return valuesRead(root, false, readCtes);
    }

    // Adds the CTEs with a consumer that has a reader among its ancestors. The result is true when it added any.
    private static boolean markReadCtes(OptExpression node, boolean read, Set<Integer> readCtes) {
        Operator operator = node.getOp();
        boolean marked = operator instanceof LogicalCTEConsumeOperator consume && read
                && readCtes.add(consume.getCteId());
        boolean readBelow = readBelow(node, read, readCtes);
        for (OptExpression input : node.getInputs()) {
            marked |= markReadCtes(input, readBelow, readCtes);
        }
        return marked;
    }

    private static boolean valuesRead(OptExpression node, boolean read, Set<Integer> readCtes) {
        if (node.getOp() instanceof LogicalValuesOperator) {
            return read;
        }
        boolean readBelow = readBelow(node, read, readCtes);
        for (OptExpression input : node.getInputs()) {
            if (valuesRead(input, readBelow, readCtes)) {
                return true;
            }
        }
        return false;
    }

    // Whether the operators below the node have a reader among their ancestors.
    private static boolean readBelow(OptExpression node, boolean read, Set<Integer> readCtes) {
        Operator operator = node.getOp();
        return read || isReader(operator)
                || operator instanceof LogicalCTEProduceOperator produce && readCtes.contains(produce.getCteId());
    }

    private static boolean isReader(Operator operator) {
        boolean passesInput = operator instanceof LogicalValuesOperator
                || operator instanceof LogicalProjectOperator
                || operator instanceof LogicalLimitOperator
                || operator instanceof LogicalCTEAnchorOperator
                || operator instanceof LogicalCTEProduceOperator
                || operator instanceof LogicalCTEConsumeOperator
                || operator instanceof LogicalUnionOperator union && union.isUnionAll()
                || operator instanceof LogicalScanOperator
                || operator instanceof LogicalTreeAnchorOperator;
        return !passesInput;
    }
}
