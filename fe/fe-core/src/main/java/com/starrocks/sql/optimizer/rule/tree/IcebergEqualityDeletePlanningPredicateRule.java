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

package com.starrocks.sql.optimizer.rule.tree;

import com.starrocks.connector.iceberg.IcebergMORParams;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergEqualityDeleteScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalIcebergScanOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The equality delete scans of a block plan Iceberg with their own predicate, the data scan with its final
 * predicate. Conditions pushed into the data scan after IcebergEqualityDeleteRewriteRule (for example derived
 * from a join) make the two differ, and Iceberg then plans the table once for the data scan and once more for
 * the delete scans. We want one planning per block, and the delete scans need exactly the deletes of the data
 * files that the data scan with deletes reads, so they plan with its final predicate. This runs on the final
 * plan, after every rule that can change a scan predicate.
 */
public class IcebergEqualityDeletePlanningPredicateRule implements TreeRewriteRule {
    private static final Logger LOG = LogManager.getLogger(IcebergEqualityDeletePlanningPredicateRule.class);

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        Map<Long, List<PhysicalIcebergScanOperator>> dataScansWithDeletes = new HashMap<>();
        Map<Long, List<PhysicalIcebergEqualityDeleteScanOperator>> deleteScans = new HashMap<>();
        collect(root, dataScansWithDeletes, deleteScans);

        for (Map.Entry<Long, List<PhysicalIcebergEqualityDeleteScanOperator>> entry : deleteScans.entrySet()) {
            List<PhysicalIcebergScanOperator> dataScans = dataScansWithDeletes.get(entry.getKey());
            // A block has exactly one data scan with deletes. Anything else is left as it is: the delete scans
            // then plan with their own predicate, which costs another planning but reads every needed delete.
            if (dataScans == null || dataScans.size() != 1) {
                LOG.info("Skip aligning the planning predicate of iceberg equality delete scans of block {}: "
                        + "{} data scans with deletes", entry.getKey(), dataScans == null ? 0 : dataScans.size());
                continue;
            }
            for (PhysicalIcebergEqualityDeleteScanOperator deleteScan : entry.getValue()) {
                deleteScan.setOriginPredicate(dataScans.get(0).getPredicate());
            }
        }
        return root;
    }

    private static void collect(OptExpression expression,
                                Map<Long, List<PhysicalIcebergScanOperator>> dataScansWithDeletes,
                                Map<Long, List<PhysicalIcebergEqualityDeleteScanOperator>> deleteScans) {
        if (expression.getOp() instanceof PhysicalIcebergScanOperator scan
                && scan.getMORParams().getScanTaskType() == IcebergMORParams.ScanTaskType.DATA_FILE_WITH_EQ_DELETE
                && !scan.getTableFullMORParams().isEmpty()) {
            dataScansWithDeletes.computeIfAbsent(scan.getTableFullMORParams().getMORId(), k -> new ArrayList<>())
                    .add(scan);
        } else if (expression.getOp() instanceof PhysicalIcebergEqualityDeleteScanOperator scan
                && !scan.getTableFullMORParams().isEmpty()) {
            deleteScans.computeIfAbsent(scan.getTableFullMORParams().getMORId(), k -> new ArrayList<>()).add(scan);
        }
        for (OptExpression input : expression.getInputs()) {
            collect(input, dataScansWithDeletes, deleteScans);
        }
    }
}
