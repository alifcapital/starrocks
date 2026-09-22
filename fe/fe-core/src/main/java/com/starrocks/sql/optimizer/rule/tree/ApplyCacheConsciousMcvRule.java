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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Pair;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalHashAggregateOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.Histogram;
import com.starrocks.sql.optimizer.task.TaskContext;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Mirrors {@link ApplyMinMaxStatisticRule}, but for the cache-conscious top-n operator: for a
 * cc-marked aggregation it pulls the (single) group-by column's most-common values from the
 * histogram statistics and attaches them to the agg, so the backend can seed the frozen FA with the
 * known hot keys instead of discovering them online.
 *
 * <p>Runs right after {@link MarkCacheConsciousTopnRule} (the cc flag must be set). Like min/max, the
 * histogram is read from the statistic storage, not the OptExpression tree -- tree statistics are not
 * maintained through physicalRuleRewrite. The cc operator targets high-cardinality integral keys,
 * which the low-cardinality dict rewrite never touches, so the key value domain is stable here.
 */
public class ApplyCacheConsciousMcvRule implements TreeRewriteRule {

    // Defensive cap on the carried list; histogram MCV collection is already bounded far below this.
    private static final int MAX_MCV = 1024;

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        List<PhysicalHashAggregateOperator> aggLists = Lists.newArrayList();
        Utils.extractOperator(root, aggLists, op -> OperatorType.PHYSICAL_HASH_AGG.equals(op.getOpType()));

        ColumnRefSet ccGroupByRefs = new ColumnRefSet();
        boolean hasCc = false;
        for (PhysicalHashAggregateOperator agg : aggLists) {
            if (agg.isCacheConsciousTopn() && agg.getGroupBys().size() == 1) {
                ccGroupByRefs.union(agg.getGroupBys().get(0).getId());
                hasCc = true;
            }
        }
        if (!hasCc) {
            return root;
        }

        // colId -> hot (value, frequency) pairs, descending by frequency.
        Map<Integer, List<Pair<ConstantOperator, Long>>> mcvByColId = Maps.newHashMap();
        for (PhysicalOlapScanOperator scan : Utils.extractPhysicalOlapScanOperator(root)) {
            if (!(scan.getTable() instanceof OlapTable)) {
                continue;
            }
            OlapTable table = (OlapTable) scan.getTable();
            for (ColumnRefOperator column : scan.getColRefToColumnMetaMap().keySet()) {
                if (!ccGroupByRefs.contains(column.getId()) || !column.getType().isIntegerType()) {
                    continue;
                }
                Column c = table.getColumn(column.getName());
                if (c == null) {
                    continue;
                }
                Map<String, Histogram> histograms = GlobalStateMgr.getCurrentState().getStatisticStorage()
                        .getHistogramStatistics(table, List.of(column.getName()));
                Histogram histogram = histograms.get(column.getName());
                if (histogram == null || histogram.getMCV() == null || histogram.getMCV().isEmpty()) {
                    continue;
                }
                List<Pair<ConstantOperator, Long>> mcvs = Lists.newArrayList();
                histogram.getMCV().entrySet().stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
                        .limit(MAX_MCV)
                        .forEach(e -> mcvs.add(Pair.create(ConstantOperator.createVarchar(e.getKey()), e.getValue())));
                mcvByColId.put(column.getId(), mcvs);
            }
        }
        if (mcvByColId.isEmpty()) {
            return root;
        }

        for (PhysicalHashAggregateOperator agg : aggLists) {
            if (agg.isCacheConsciousTopn() && agg.getGroupBys().size() == 1) {
                List<Pair<ConstantOperator, Long>> mcvs = mcvByColId.get(agg.getGroupBys().get(0).getId());
                if (mcvs != null && !mcvs.isEmpty()) {
                    agg.setCacheConsciousMcv(mcvs);
                }
            }
        }
        return root;
    }
}
