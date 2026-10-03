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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.TreeRangeSet;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.DateTruncEquivalent;
import com.starrocks.sql.optimizer.rule.transformation.materialization.equivalent.TimeSliceRewriteEquivalent;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class PredicateExtractor extends ScalarOperatorVisitor<RangePredicate, PredicateExtractor.PredicateExtractorContext> {
    private final List<ScalarOperator> columnEqualityPredicates = Lists.newArrayList();
    private final List<ScalarOperator> residualPredicates = Lists.newArrayList();

    public List<ScalarOperator> getColumnEqualityPredicates() {
        return columnEqualityPredicates;
    }

    public List<ScalarOperator> getResidualPredicates() {
        return residualPredicates;
    }

    public static class PredicateExtractorContext {
        private boolean isAnd = true;

        public boolean isAnd() {
            return isAnd;
        }

        public PredicateExtractorContext setAnd(boolean and) {
            isAnd = and;
            return this;
        }
    }

    @Override
    public RangePredicate visit(ScalarOperator scalarOperator, PredicateExtractorContext context) {
        return null;
    }

    @Override
    public RangePredicate visitBinaryPredicate(BinaryPredicateOperator predicate, PredicateExtractorContext context) {
        RangePredicate rangePredicate = rewriteBinaryPredicate(predicate);
        if (rangePredicate != null) {
            return rangePredicate;
        }

        ScalarOperator left = predicate.getChild(0);
        ScalarOperator right = predicate.getChild(1);
        if (left.isColumnRef() && right.isColumnRef() && context.isAnd()) {
            if (predicate.getBinaryType().isEqual()) {
                columnEqualityPredicates.add(predicate);
            } else {
                residualPredicates.add(predicate);
            }
        } else if (context.isAnd()) {
            residualPredicates.add(predicate);
        }
        return null;
    }

    private boolean isSupportedRangeExpr(ScalarOperator op) {
        if (op instanceof ColumnRefOperator) {
            return true;
        }
        if (!op.isVariable()) {
            return false;
        }
        return Utils.collect(op, ColumnRefOperator.class).size() == 1;
    }

    private RangePredicate rewriteBinaryPredicate(BinaryPredicateOperator predicate) {
        ScalarOperator left = predicate.getChild(0);
        ScalarOperator right = predicate.getChild(1);
        ScalarOperator op1 = null;
        ConstantOperator op2 = null;
        if (right instanceof ConstantOperator && isSupportedRangeExpr(left)) {
            op1 = left;
            op2 = (ConstantOperator) right;
        } else if (left instanceof ConstantOperator && isSupportedRangeExpr(right)) {
            op1 = right;
            op2 = (ConstantOperator) left;
        } else {
            return null;
        }

        // rewrite to column ref by equivalent
        if (!(op1 instanceof ColumnRefOperator)) {
            RangePredicate rangePredicate = rewriteByEquivalent(predicate);
            if (rangePredicate != null) {
                return rangePredicate;
            }
        }

        // by default
        TreeRangeSet<ConstantOperator> rangeSet = range(predicate.getBinaryType(), op2);
        if (rangeSet == null) {
            return null;
        }
        return new ColumnRangePredicate(op1, rangeSet);
    }

    private static RangePredicate rewriteByEquivalent(BinaryPredicateOperator predicate) {
        ScalarOperator left = predicate.getChild(0);
        ScalarOperator right = predicate.getChild(1);
        ScalarOperator op1 = null;
        ConstantOperator op2 = null;
        if (left instanceof CallOperator && right instanceof ConstantOperator) {
            op1 = left;
            op2 = (ConstantOperator) right;
        } else if (right instanceof CallOperator && left instanceof ConstantOperator) {
            op1 = right;
            op2 = (ConstantOperator) left;
        } else {
            return null;
        }

        TreeRangeSet<ConstantOperator> range = range(predicate.getBinaryType(), op2);
        if (DateTruncEquivalent.isSupportedBinaryType(predicate.getBinaryType()) &&
                DateTruncEquivalent.INSTANCE.isEquivalent(op1, op2)) {
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.addAll(range);
            return new ColumnRangePredicate(op1.getChild(1).cast(), rangeSet);
        } else if (TimeSliceRewriteEquivalent.INSTANCE.isEquivalent(op1, op2)) {
            TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
            rangeSet.addAll(range);
            return new ColumnRangePredicate(op1.getChild(0).cast(), rangeSet);
        } else {
            return null;
        }
    }

    @Override
    public RangePredicate visitCompoundPredicate(
            CompoundPredicateOperator predicate, PredicateExtractorContext context) {

        if (predicate.isNot()) {
            // try to remove not
            ScalarOperator canonized = MvUtils.canonizePredicate(predicate);
            if (canonized == null || ((canonized instanceof CompoundPredicateOperator)
                    && ((CompoundPredicateOperator) canonized).isNot())) {
                return null;
            }
            return canonized.accept(this, context);
        }

        List<RangePredicate> rangePredicates = Lists.newArrayList();
        for (ScalarOperator child : predicate.getChildren()) {
            boolean isAndOrigin = context.isAnd();
            if (!predicate.isAnd()) {
                context.setAnd(false);
            }
            RangePredicate childRange = child.accept(this, context);
            context.setAnd(isAndOrigin);
            if (childRange == null) {
                if (!context.isAnd() || !predicate.isAnd()) {
                    return null;
                } else {
                    if (!(child instanceof BinaryPredicateOperator)) {
                        residualPredicates.add(child);
                    }
                    continue;
                }
            }
            if (predicate.isOr()) {
                if (childRange instanceof ColumnRangePredicate) {
                    rangePredicates.add(childRange);
                    mergeColumnRange(rangePredicates,
                            (columnRange1, columnRange2) -> ColumnRangePredicate.orRange(columnRange1, columnRange2));
                } else if (childRange instanceof OrRangePredicate) {
                    rangePredicates.addAll(childRange.childPredicates);
                    mergeColumnRange(rangePredicates,
                            (columnRange1, columnRange2) -> ColumnRangePredicate.orRange(columnRange1, columnRange2));
                } else {
                    rangePredicates.add(childRange);
                }
            } else if (predicate.isAnd()) {
                if (childRange instanceof ColumnRangePredicate) {
                    rangePredicates.add(childRange);
                    mergeColumnRange(rangePredicates,
                            (columnRange1, columnRange2) -> ColumnRangePredicate.andRange(columnRange1, columnRange2));
                } else if (childRange instanceof AndRangePredicate) {
                    rangePredicates.addAll(childRange.childPredicates);
                    mergeColumnRange(rangePredicates,
                            (columnRange1, columnRange2) -> ColumnRangePredicate.andRange(columnRange1, columnRange2));
                } else {
                    rangePredicates.add(childRange);
                }
            } else if (predicate.isNot()) {
                // it is normalized, can not be here
                return null;
            }
        }
        if (rangePredicates.size() == 1 && (rangePredicates.get(0) instanceof ColumnRangePredicate)) {
            return rangePredicates.get(0);
        }
        if (predicate.isAnd()) {
            return new AndRangePredicate(rangePredicates);
        } else {
            return new OrRangePredicate(rangePredicates);
        }
    }

    private void mergeColumnRange(
            List<RangePredicate> rangePredicates,
            BiFunction<ColumnRangePredicate, ColumnRangePredicate, ColumnRangePredicate> mergeOp) {
        if (rangePredicates.size() < 2) {
            return;
        }
        Map<ScalarOperator, ColumnRangePredicate> columnRangePredicateMap = Maps.newHashMap();
        List<ColumnRangePredicate> columnRanges = Lists.newArrayList();
        for (RangePredicate rangePredicate : rangePredicates) {
            if (rangePredicate instanceof ColumnRangePredicate) {
                ColumnRangePredicate columnRangePredicate = rangePredicate.cast();
                columnRanges.add(columnRangePredicate);
                ColumnRangePredicate existing = columnRangePredicateMap.get(columnRangePredicate.getExpression());
                if (existing != null) {
                    ColumnRangePredicate newRangePredicate = mergeOp.apply(existing, columnRangePredicate);
                    if (newRangePredicate.equals(ColumnRangePredicate.FALSE)) {
                        rangePredicates.add(newRangePredicate);
                        return;
                    }
                    if (newRangePredicate.isUnbounded()) {
                        columnRangePredicateMap.remove(columnRangePredicate.getExpression());
                    } else {
                        columnRangePredicateMap.put(columnRangePredicate.getExpression(), newRangePredicate);
                    }
                } else {
                    columnRangePredicateMap.put(columnRangePredicate.getExpression(), columnRangePredicate);
                }
            }
        }
        // columnRanges contains every column predicate above; avoid pairwise range equality checks.
        rangePredicates.removeIf(rangePredicate -> rangePredicate instanceof ColumnRangePredicate);
        // Consume the local map in first-occurrence order; repeated expressions then find no entry.
        for (ColumnRangePredicate columnRangePredicate : columnRanges) {
            ColumnRangePredicate merged = columnRangePredicateMap.remove(columnRangePredicate.getExpression());
            if (merged != null) {
                rangePredicates.add(merged);
            }
        }
    }

    private static TreeRangeSet<ConstantOperator> range(BinaryType type, ConstantOperator value) {
        TreeRangeSet<ConstantOperator> rangeSet = TreeRangeSet.create();
        switch (type) {
            case EQ:
                rangeSet.add(Range.singleton(value));
                return rangeSet;
            case GE:
                rangeSet.add(Range.atLeast(value));
                return rangeSet;
            case GT:
                rangeSet.add(Range.greaterThan(value));
                return rangeSet;
            case LE:
                rangeSet.add(Range.atMost(value));
                return rangeSet;
            case LT:
                rangeSet.add(Range.lessThan(value));
                return rangeSet;
            case NE:
                Type valueType = value.getType();
                if (!valueType.isNumericType() && !valueType.isDateType()) {
                    return null;
                }
                rangeSet.add(Range.greaterThan(value));
                rangeSet.add(Range.lessThan(value));
                return rangeSet;
            default:
                throw new UnsupportedOperationException("unsupported type:" + type);
        }
    }
}
