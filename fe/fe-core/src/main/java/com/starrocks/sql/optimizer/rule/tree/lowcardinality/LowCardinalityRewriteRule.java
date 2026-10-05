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

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;

public class LowCardinalityRewriteRule implements TreeRewriteRule {

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable session = taskContext.getOptimizerContext().getSessionVariable();
        ConnectContext connectContext = taskContext.getOptimizerContext().getConnectContext();
        boolean isQuery = connectContext.getState().isQuery();
        // The query that collects a lake dict and the queries of ANALYZE read the lake files to build dicts and
        // statistics. We do not want them to start another dict collection or to read through a dict that may
        // miss values, so they plan lake scans without global dicts.
        boolean lakeDictAllowed = !connectContext.isLakeDictCollection() && !connectContext.isStatisticsConnection();
        if (!session.isEnableLowCardinalityOptimize() || !session.isUseLowCardinalityOptimizeV2()) {
            return root;
        }

        ColumnRefFactory factory = taskContext.getOptimizerContext().getColumnRefFactory();
        DecodeContext context = new DecodeContext(factory);
        {
            DecodeCollector collector = new DecodeCollector(session, isQuery, lakeDictAllowed);
            collector.collect(root, context);
            if (!collector.isValidMatchChildren()) {
                return root;
            }
        }
        DecodeRewriter rewriter = new DecodeRewriter(factory, context, session);
        return rewriter.rewrite(root);
    }
}