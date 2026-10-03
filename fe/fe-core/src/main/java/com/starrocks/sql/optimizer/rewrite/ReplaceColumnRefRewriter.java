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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

// Replace the corresponding ColumnRef with ScalarOperator
public class ReplaceColumnRefRewriter {
    // We skip an optional rewrite, such as a merge of two projections, when it copies more nodes than this: we expect
    // a separate projection that computes the value once to cost less than that many copies. See isTooLarge.
    public static final long MAX_COPIED_NODES = 10000;

    private final Rewriter rewriter = new Rewriter();
    private final Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap;

    private final boolean isRecursively;

    public ReplaceColumnRefRewriter(Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap) {
        this.operatorMap = operatorMap;
        this.isRecursively = false;
    }

    public ReplaceColumnRefRewriter(Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap,
                                    boolean isRecursively) {
        this.operatorMap = operatorMap;
        this.isRecursively = isRecursively;
    }

    public ScalarOperator rewrite(ScalarOperator origin) {
        if (origin == null) {
            return null;
        }

        ScalarOperator result = origin.clone().accept(rewriter, null);
        // Check expression complexity after column reference replacement
        // Use cached value (true) since clone() has already cleared the cache
        result.checkMaxFlatChildren(true);
        return result;
    }

    /**
     * We want to know how much the replacement of the column refs of the operators with their values in the map
     * grows the plan. A plan that computes a value once and refers to it has the value once. After the replacement
     * every use of the column ref has its own copy, so we count the nodes of the value for each use after the first.
     */
    public static long copiedNodes(Collection<? extends ScalarOperator> operators,
                                   Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap) {
        Map<ColumnRefOperator, Integer> uses = Maps.newHashMap();
        for (ScalarOperator operator : operators) {
            countUses(operator, operatorMap, uses);
        }
        long copied = 0;
        for (Map.Entry<ColumnRefOperator, Integer> entry : uses.entrySet()) {
            // A value of one node only takes the place of the column ref, so we count the nodes after the first
            copied += (long) (entry.getValue() - 1) * (operatorMap.get(entry.getKey()).getNumFlatChildren() - 1);
        }
        return copied;
    }

    /**
     * The nodes of the operator after replacing its column refs with their values in the map.
     */
    public static long replacedNodes(ScalarOperator operator,
                                     Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap) {
        if (operator instanceof ColumnRefOperator && operatorMap.containsKey(operator)) {
            return operatorMap.get(operator).getNumFlatChildren();
        }
        long nodes = 1;
        for (ScalarOperator child : operator.getChildren()) {
            nodes += replacedNodes(child, operatorMap);
        }
        return nodes;
    }

    /**
     * We use this to skip an optional rewrite, such as a merge of two projections. We skip it when it copies more
     * than MAX_COPIED_NODES nodes, or when it makes an operator larger than max_scalar_operator_flat_children: the
     * rewrite would fail the query with "Expression too complex", while the plan without the rewrite is correct.
     */
    public static boolean isTooLarge(Collection<? extends ScalarOperator> operators,
                                     Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap) {
        ReplacementSize size = new ReplacementSize(operatorMap, Config.max_scalar_operator_flat_children);
        for (ScalarOperator operator : operators) {
            if (size.count(operator) > size.maxNodes || size.copied > MAX_COPIED_NODES) {
                return true;
            }
        }
        return false;
    }

    // Count result size and additional copies together. Only a repeated non-leaf replacement
    // grows the plan, so leaf mappings need neither a use counter nor a set entry.
    private static final class ReplacementSize {
        private final Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap;
        private final long maxNodes;
        private Set<ColumnRefOperator> seen;
        private long copied;

        private ReplacementSize(Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap, int maxNodes) {
            this.operatorMap = operatorMap;
            this.maxNodes = maxNodes > 0 ? maxNodes : Long.MAX_VALUE;
        }

        private long count(ScalarOperator operator) {
            if (operator instanceof ColumnRefOperator column) {
                ScalarOperator replacement = operatorMap.get(column);
                if (replacement == null) {
                    return 1;
                }
                int nodes = replacement.getNumFlatChildren();
                if (nodes > 1) {
                    if (seen == null) {
                        seen = Sets.newHashSet();
                    }
                    if (!seen.add(column)) {
                        copied += nodes - 1;
                    }
                }
                return nodes;
            }
            long nodes = 1;
            for (ScalarOperator child : operator.getChildren()) {
                nodes += count(child);
                if (nodes > maxNodes || copied > MAX_COPIED_NODES) {
                    break;
                }
            }
            return nodes;
        }
    }

    private static void countUses(ScalarOperator operator, Map<ColumnRefOperator, ? extends ScalarOperator> operatorMap,
                                  Map<ColumnRefOperator, Integer> uses) {
        if (operator instanceof ColumnRefOperator) {
            if (operatorMap.containsKey(operator)) {
                uses.merge((ColumnRefOperator) operator, 1, Integer::sum);
            }
            return;
        }
        for (ScalarOperator child : operator.getChildren()) {
            countUses(child, operatorMap, uses);
        }
    }

    public ScalarOperator rewriteWithoutClone(ScalarOperator origin) {
        if (origin == null) {
            return null;
        }

        ScalarOperator result = origin.accept(rewriter, null);
        // Check expression complexity after column reference replacement
        // Force recalculation (false) since the expression structure has been modified without cloning
        result.checkMaxFlatChildren(false);
        return result;
    }

    private class Rewriter extends ScalarOperatorVisitor<ScalarOperator, Void> {
        // Track keys that are temporarily excluded from replacement during recursive rewriting
        // to prevent cycles when a replaced expression contains the original column reference
        private final Set<ColumnRefOperator> excludedKeys = Sets.newHashSet();

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
            int childCount = scalarOperator.getChildren().size();
            for (int i = 0; i < childCount; ++i) {
                scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
            }
            return scalarOperator;
        }

        @Override
        public ScalarOperator visitVariableReference(ColumnRefOperator column, Void context) {
            // If this column is excluded, don't replace it (prevents cycles)
            if (excludedKeys.contains(column)) {
                return column;
            }
            if (!operatorMap.containsKey(column)) {
                return column;
            }
            // Must clone here because
            // The rewritten predicate will be rewritten continually,
            // Rewiring predicate shouldn't change the origin project columnRefMap

            ScalarOperator mapperOperator = operatorMap.get(column);
            if (column.equals(mapperOperator)) {
                return column;
            }
            if (!isRecursively) {
                return mapperOperator.clone();
            } else {
                while (mapperOperator instanceof ColumnRefOperator && operatorMap.containsKey(mapperOperator)) {
                    ScalarOperator mapped = operatorMap.get(mapperOperator);
                    if (mapped.equals(mapperOperator)) {
                        break;
                    }
                    mapperOperator = mapped;
                }
                mapperOperator = mapperOperator.clone();
                
                // Temporarily exclude this key from replacement when recursively rewriting children
                // to prevent cycles when the replacement contains the original column reference
                excludedKeys.add(column);
                try {
                    for (int i = 0; i < mapperOperator.getChildren().size(); ++i) {
                        mapperOperator.setChild(i, mapperOperator.getChild(i).accept(this, null));
                    }
                } finally {
                    // Restore the key after rewriting children
                    excludedKeys.remove(column);
                }
            }
            return mapperOperator;
        }
    }
}
