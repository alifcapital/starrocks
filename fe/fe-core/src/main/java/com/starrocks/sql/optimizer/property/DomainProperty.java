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

package com.starrocks.sql.optimizer.property;

import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.MonotonicImage;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * DomainProperty is used to store the domain of a column or expr.
 * For a sql like `select col_a, col_b, col_c from table where col_a > 10 and col_b + col_c = 20`,
 * we can get the domain property of col_a, col_b + col_c from the where clause like
 * col_a -> {predicateDesc : col_a > 10, rangeDesc : [10, +inf)}
 * col_b + col_c -> {predicateDesc : col_b + col_c = 20, rangeDesc : [20, 20]}
 */
public class DomainProperty {

    private Map<ScalarOperator, DomainWrapper> domainMap;


    public DomainProperty(Map<ScalarOperator, DomainWrapper> domainMap) {
        this.domainMap = domainMap;
    }

    public boolean contains(ScalarOperator scalarOperator) {
        return domainMap.containsKey(scalarOperator);
    }

    public Map<ScalarOperator, DomainWrapper> getDomainMap() {
        return domainMap;
    }

    public DomainWrapper getValueWrapper(ScalarOperator scalarOperator) {
        return domainMap.get(scalarOperator);
    }
    public ScalarOperator getPredicateDesc(ScalarOperator scalarOperator) {
        return domainMap.get(scalarOperator).getPredicateDesc();
    }

    public DomainProperty projectDomainProperty(Map<ColumnRefOperator, ScalarOperator> columnRefMap) {
        Map<ScalarOperator, DomainWrapper> newDomainMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            if (domainMap.containsKey(entry.getValue()) && !entry.getValue().equals(entry.getKey())) {
                DomainWrapper sourceWrapper = domainMap.get(entry.getValue());
                ReplaceShuttle shuttle = new ReplaceShuttle(Map.of(entry.getValue(), entry.getKey()));
                ScalarOperator rewriteResult = shuttle.rewrite(sourceWrapper.getPredicateDesc());
                if (rewriteResult != null) {
                    // keep the monotonicDerived mark: a column rename must not turn a computed
                    // domain into a user-written one
                    newDomainMap.put(entry.getKey(), new DomainWrapper(rewriteResult,
                            sourceWrapper.isNeedDeriveRange(), sourceWrapper.isMonotonicDerived()));
                }
            } else if (!entry.getValue().equals(entry.getKey())) {
                // The projected expression has no domain entry of its own (domains are keyed
                // by exact structure, and scans fill only bare-column domains). But its range
                // can still be computed from the range of its column.
                //
                // Example: PushDownJoinOnExpressionToChildProject rewrites the join key
                //   ... ON f.dmonth = date_trunc('month', e.datadate)
                // into a child projection slot7 := date_trunc('month', datadate). The scan
                // below has datadate BETWEEN '2024-03-05' AND '2024-04-10', so slot7 gets the
                // domain ['2024-03-01', '2024-04-01'], and predicate move-around can transfer
                // it across slot7 = dmonth.
                DomainWrapper wrapper = deriveMonotonicImageDomain(entry.getKey(), entry.getValue());
                if (wrapper != null) {
                    newDomainMap.put(entry.getKey(), wrapper);
                }
            }
        }
        ColumnRefSet outputCols = new ColumnRefSet(columnRefMap.keySet());
        for (Map.Entry<ScalarOperator, DomainWrapper> entry : domainMap.entrySet()) {
            if (!newDomainMap.containsKey(entry.getKey()) && outputCols.containsAll(entry.getKey().getUsedColumns())) {
                newDomainMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new DomainProperty(newDomainMap);
    }

    /**
     * Domain for a projection slot defined by a monotonic expression over a column with a
     * known range. The expression is folded at the range endpoints ({@link MonotonicImage})
     * and the slot gets the resulting interval:
     * <pre>
     *   slot7 := date_trunc('month', datadate),  domain(datadate) = ['2024-03-05','2024-04-10']
     *   =&gt; domain(slot7) = { slot7 &gt;= '2024-03-01' AND slot7 &lt;= '2024-04-01' }
     * </pre>
     * Returns null when the image cannot be computed (non-monotonic expression, unbounded
     * column range, fold failure) or when session variable
     * enable_monotonic_predicate_move_around is off.
     */
    private DomainWrapper deriveMonotonicImageDomain(ColumnRefOperator outputColumn, ScalarOperator expr) {
        ConnectContext context = ConnectContext.get();
        if (context == null || !context.getSessionVariable().isEnableMonotonicPredicateMoveAround()) {
            return null;
        }
        List<ColumnRefOperator> usedColumns = expr.getColumnRefs();
        Set<ColumnRefOperator> distinctColumns = new HashSet<>(usedColumns);
        if (distinctColumns.size() != 1) {
            return null;
        }
        ColumnRefOperator column = distinctColumns.iterator().next();
        DomainWrapper columnWrapper = domainMap.get(column);
        if (columnWrapper == null || columnWrapper.getRangeDesc() == null
                || columnWrapper.getRangeDesc().getRange() == null) {
            return null;
        }
        Range<ConstantOperator> image =
                MonotonicImage.imageRange(expr, column, columnWrapper.getRangeDesc().getRange()).orElse(null);
        if (image == null) {
            return null;
        }
        // a point image [v, v] becomes an equality, an interval becomes two bounds
        ScalarOperator predicateDesc;
        if (image.lowerEndpoint().equals(image.upperEndpoint())) {
            predicateDesc = new BinaryPredicateOperator(BinaryType.EQ, outputColumn, image.lowerEndpoint());
        } else {
            predicateDesc = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                    new BinaryPredicateOperator(BinaryType.GE, outputColumn, image.lowerEndpoint()),
                    new BinaryPredicateOperator(BinaryType.LE, outputColumn, image.upperEndpoint()));
        }
        return new DomainWrapper(predicateDesc, true, true);
    }

    public DomainProperty filterDomainProperty(DomainProperty filterDomainProperty) {
        Map<ScalarOperator, DomainWrapper> newDomainMap = Maps.newHashMap(domainMap);

        for (Map.Entry<ScalarOperator, DomainWrapper> entry : filterDomainProperty.domainMap.entrySet()) {
            if (domainMap.containsKey(entry.getKey())) {
                newDomainMap.put(entry.getKey(), domainMap.get(entry.getKey()).andValueWrapper(entry.getValue()));
            } else {
                newDomainMap.put(entry.getKey(), entry.getValue());
            }
        }
        return new DomainProperty(newDomainMap);
    }

    public static DomainProperty mergeDomainProperty(List<DomainProperty> domainPropertyList) {
        Map<ScalarOperator, DomainWrapper> newDomainMap = Maps.newHashMap();
        for (DomainProperty domainProperty : domainPropertyList) {
            for (Map.Entry<ScalarOperator, DomainWrapper> entry : domainProperty.domainMap.entrySet()) {
                if (newDomainMap.containsKey(entry.getKey())) {
                    newDomainMap.put(entry.getKey(), newDomainMap.get(entry.getKey()).orValueWrapper(entry.getValue()));
                } else {
                    newDomainMap.put(entry.getKey(), entry.getValue());
                }
            }
        }
        return new DomainProperty(newDomainMap);
    }

    public static class DomainWrapper {
        private final ScalarOperator predicateDesc;

        private final boolean needDeriveRange;

        // true when this domain was computed by mapping a column range through a monotonic
        // projected expression, not taken from a user predicate. A consumer that moves such a
        // predicate onto another expression must check that target first (see
        // OnPredicateMoveAroundRule.isSafeDeriveTarget).
        private final boolean monotonicDerived;

        @Nullable
        private final RangeExtractor.RangeDescriptor rangeDesc;


        public DomainWrapper(ScalarOperator predicateDesc, boolean needDeriveRange) {
            this(predicateDesc, needDeriveRange, false);
        }

        public DomainWrapper(ScalarOperator predicateDesc, boolean needDeriveRange, boolean monotonicDerived) {
            this.predicateDesc = predicateDesc;
            this.needDeriveRange = needDeriveRange;
            this.monotonicDerived = monotonicDerived;
            if (needDeriveRange) {
                this.rangeDesc = deriveRange(predicateDesc);
            } else {
                this.rangeDesc = new RangeExtractor.RangeDescriptor(ConstantOperator.FALSE);
            }
        }

        public boolean isMonotonicDerived() {
            return monotonicDerived;
        }


        public ScalarOperator getPredicateDesc() {
            return predicateDesc;
        }

        public RangeExtractor.RangeDescriptor getRangeDesc() {
            return rangeDesc;
        }

        public boolean isNeedDeriveRange() {
            return needDeriveRange;
        }

        public DomainWrapper andValueWrapper(DomainWrapper domainWrapper) {
            ScalarOperator newScalarOperator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                    predicateDesc, domainWrapper.predicateDesc);
            return new DomainWrapper(newScalarOperator, needDeriveRange || domainWrapper.needDeriveRange,
                    monotonicDerived || domainWrapper.monotonicDerived);
        }

        public DomainWrapper orValueWrapper(DomainWrapper domainWrapper) {
            ScalarOperator newScalarOperator = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                    predicateDesc, domainWrapper.predicateDesc);
            return new DomainWrapper(newScalarOperator, needDeriveRange && domainWrapper.needDeriveRange,
                    monotonicDerived || domainWrapper.monotonicDerived);
        }

        private RangeExtractor.RangeDescriptor deriveRange(ScalarOperator scalarOperator) {
            RangeExtractor extractor = new RangeExtractor();

            Map<ScalarOperator, RangeExtractor.ValueDescriptor> res = extractor.apply(scalarOperator, null);

            if (res.size() != 1) {
                return new RangeExtractor.RangeDescriptor(ConstantOperator.FALSE);
            } else {
                RangeExtractor.ValueDescriptor desc = res.values().stream().findFirst().get();
                if (desc instanceof RangeExtractor.MultiValuesDescriptor) {
                    RangeExtractor.MultiValuesDescriptor multiValues = (RangeExtractor.MultiValuesDescriptor) desc;
                    RangeExtractor.RangeDescriptor rangeDesc = new RangeExtractor.RangeDescriptor(multiValues.columnRef);
                    rangeDesc = (RangeExtractor.RangeDescriptor) rangeDesc.union(multiValues);
                    return rangeDesc;
                } else {
                    return (RangeExtractor.RangeDescriptor) desc;
                }
            }
        }
    }
}
