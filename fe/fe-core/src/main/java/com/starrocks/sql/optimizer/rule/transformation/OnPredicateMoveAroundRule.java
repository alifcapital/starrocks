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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.property.DomainProperty;
import com.starrocks.sql.optimizer.property.DomainPropertyDeriver;
import com.starrocks.sql.optimizer.property.RangeExtractor;
import com.starrocks.sql.optimizer.property.ReplaceShuttle;
import com.starrocks.sql.optimizer.rewrite.MonotonicImage;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class OnPredicateMoveAroundRule extends TransformationRule {

    public static final OnPredicateMoveAroundRule INSTANCE = new OnPredicateMoveAroundRule(RuleType.TF_PREDICATE_PROPAGATE,
            Pattern.create(OperatorType.LOGICAL_JOIN).
                    addChildren(Pattern.create(OperatorType.PATTERN_LEAF), Pattern.create(OperatorType.PATTERN_LEAF)));

    private OnPredicateMoveAroundRule(RuleType type, Pattern pattern) {
        super(type, pattern);
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnablePredicateMoveAround()) {
            return false;
        }
        LogicalJoinOperator joinOperator = input.getOp().cast();
        if (joinOperator.getJoinType().isFullOuterJoin() || joinOperator.getJoinType().isCrossJoin()) {
            return false;
        }
        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalJoinOperator joinOperator = input.getOp().cast();
        ScalarOperator onPredicate = joinOperator.getOnPredicate();
        boolean enableMonotonicDerive = context.getSessionVariable().isEnableMonotonicPredicateMoveAround();

        OptExpression leftChild = input.inputAt(0);
        OptExpression rightChild = input.inputAt(1);

        List<BinaryPredicateOperator> binaryPredicates = extractBinaryPredicates(onPredicate,
                leftChild.getOutputColumns(), rightChild.getOutputColumns());
        if (binaryPredicates.isEmpty()) {
            return Lists.newArrayList();
        }

        DomainProperty leftDomainProperty = leftChild.getDomainProperty();
        DomainProperty rightDomainProperty = rightChild.getDomainProperty();

        // Definitions of projection output slots of both children. PushDownJoinOnExpressionToChildProject
        // hoists ON expressions into child projections before this rule runs, so an offspring
        // that looks like a bare column may in fact denote e.g. mod(v, 100) one level down —
        // the safe-target check must judge the defining expression, not the slot.
        Map<ColumnRefOperator, ScalarOperator> slotDefinitions = collectSlotDefinitions(leftChild, rightChild);

        OptExpression result = null;
        if (joinOperator.getJoinType().isAnyInnerJoin() || joinOperator.getJoinType().isSemiJoin()) {
            List<ScalarOperator> toLeftPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, rightDomainProperty, leftDomainProperty, true, enableMonotonicDerive,
                            slotDefinitions))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            ScalarOperator toLeftPredicate = Utils.compoundAnd(distinctPredicates(toLeftPredicates));

            List<ScalarOperator> toRightPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, leftDomainProperty, rightDomainProperty, false, enableMonotonicDerive,
                            slotDefinitions))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            ScalarOperator toRightPredicate = Utils.compoundAnd(distinctPredicates(toRightPredicates));

            if (toLeftPredicate == null && toRightPredicate == null) {
                return Lists.newArrayList();
            } else if (toLeftPredicate == null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toRightPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(input.inputAt(0), OptExpression.create(filter, input.inputAt(1)))
                );
            } else if (toRightPredicate == null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toLeftPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(OptExpression.create(filter, input.inputAt(0)), input.inputAt(1))
                );
            } else {
                LogicalFilterOperator toLeftFilter = new LogicalFilterOperator(toLeftPredicate);
                LogicalFilterOperator toRightFilter = new LogicalFilterOperator(toRightPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(OptExpression.create(toLeftFilter, input.inputAt(0)),
                                OptExpression.create(toRightFilter, input.inputAt(1)))
                );
            }
        } else if (joinOperator.getJoinType() == JoinOperator.LEFT_ANTI_JOIN) {
            List<ScalarOperator> toRightPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, leftDomainProperty, rightDomainProperty, false, enableMonotonicDerive,
                            slotDefinitions))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            ScalarOperator toRightPredicate = Utils.compoundAnd(distinctPredicates(toRightPredicates));
            if (toRightPredicate != null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toRightPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(input.inputAt(0), OptExpression.create(filter, input.inputAt(1)))
                );
            }
        } else if (joinOperator.getJoinType().isAnyLeftOuterJoin()) {
            List<ScalarOperator> toRightPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, leftDomainProperty, rightDomainProperty, false, enableMonotonicDerive,
                            slotDefinitions))
                    .collect(Collectors.toList());
            ScalarOperator toRightPredicate = Utils.compoundAnd(distinctPredicates(toRightPredicates));

            if (toRightPredicate != null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toRightPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(input.inputAt(0), OptExpression.create(filter, input.inputAt(1)))
                );
            }
        } else if (joinOperator.getJoinType().isRightOuterJoin()) {
            List<ScalarOperator> toLeftPredicates = binaryPredicates.stream()
                    .map(e -> derivePredicate(e, rightDomainProperty, leftDomainProperty, true, enableMonotonicDerive,
                            slotDefinitions))
                    .collect(Collectors.toList());
            ScalarOperator toLeftPredicate = Utils.compoundAnd(distinctPredicates(toLeftPredicates));

            if (toLeftPredicate != null) {
                LogicalFilterOperator filter = new LogicalFilterOperator(toLeftPredicate);
                result = OptExpression.create(joinOperator,
                        Lists.newArrayList(OptExpression.create(filter, input.inputAt(0)), input.inputAt(1))
                );
            }
        }

        return result == null ? Lists.newArrayList() : Lists.newArrayList(result);
    }

    private List<BinaryPredicateOperator> extractBinaryPredicates(ScalarOperator predicate,
                                                                  ColumnRefSet leftCols, ColumnRefSet rightCols) {
        List<BinaryPredicateOperator> result = Lists.newArrayList();
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        for (ScalarOperator conjunct : conjuncts) {
            if (conjunct instanceof BinaryPredicateOperator) {
                BinaryPredicateOperator binaryPredicate = (BinaryPredicateOperator) conjunct;
                if (binaryPredicate.getBinaryType().isEqualOrRange()) {
                    ColumnRefSet leftUsedCols = binaryPredicate.getChild(0).getUsedColumns();
                    ColumnRefSet rightUsedCols = binaryPredicate.getChild(1).getUsedColumns();
                    if (leftUsedCols.isEmpty() || rightUsedCols.isEmpty()) {
                        // skip constant predicate
                        continue;
                    }
                    if (leftCols.containsAll(leftUsedCols) && rightCols.containsAll(rightUsedCols)) {
                        result.add(binaryPredicate);
                    } else if (leftCols.containsAll(rightUsedCols) && rightCols.containsAll(leftUsedCols)) {
                        result.add(binaryPredicate.commutative());
                    }
                }
            }
        }

        return result;
    }

    /**
     * Builds a predicate for the {@code offspring} side of one ON comparison from the domain
     * of the {@code seed} side. Example:
     * <pre>
     *   ... ON cast(f.datamonth as varchar) = date_format(e.datadate, '%Y%m')
     *   WHERE e.datadate BETWEEN '2024-03-05' AND '2024-04-10'
     * </pre>
     * When deriving toward the fact side:
     * <pre>
     *   seed      = date_format(e.datadate, '%Y%m')   -- this side's child has the filter
     *   offspring = cast(f.datamonth as varchar)      -- this side gets the new predicate
     * </pre>
     * The contains(seed) branches fire when the seed has a domain entry. That covers bare
     * columns, filter expressions matched by structure, and projection slots whose domain
     * DomainProperty.projectDomainProperty computed from their defining monotonic expression
     * (equality keys arrive that way: PushDownJoinOnExpressionToChildProject moves them into
     * child projections before this rule runs). The enableMonotonicDerive fallbacks handle
     * seeds that are still expressions: range ON conjuncts are not moved into projections.
     * There the domain of the seed's column is mapped through the expression, see
     * {@link MonotonicImage}.
     */
    private ScalarOperator derivePredicate(BinaryPredicateOperator binaryPredicate, DomainProperty domainProperty,
                                           DomainProperty existDomainProperty, boolean toLeft,
                                           boolean enableMonotonicDerive,
                                           Map<ColumnRefOperator, ScalarOperator> slotDefinitions) {
        int idx = toLeft ? 1 : 0;
        ScalarOperator seed = binaryPredicate.getChild(idx);
        ScalarOperator offspring = binaryPredicate.getChild(1 - idx);
        BinaryType binaryType = binaryPredicate.getBinaryType();
        ScalarOperator rewriteResult = null;
        if (binaryType.isEqual()) {
            if (domainProperty.contains(seed)) {
                // a domain computed from an image may move only onto a safe target;
                // user-written domains keep the old unconditional transfer
                if (!domainProperty.getValueWrapper(seed).isMonotonicDerived()
                        || isSafeDeriveTarget(offspring, slotDefinitions)) {
                    ReplaceShuttle shuttle = new ReplaceShuttle(Map.of(seed, offspring));
                    rewriteResult = shuttle.rewrite(domainProperty.getPredicateDesc(seed));
                }
            } else if (enableMonotonicDerive && isSafeDeriveTarget(offspring, slotDefinitions)) {
                // seed = date_format(e.datadate, '%Y%m'), domain(datadate) = ['2024-03-05',
                // '2024-04-10'] => image = ['202403', '202404']. Every joined row has
                // offspring = seed, and seed is inside the image, so
                //   cast(f.datamonth as varchar) >= '202403' AND <= '202404'
                // holds for every row that can pass the equality (a NULL offspring cannot:
                // NULL = anything is not TRUE).
                Range<ConstantOperator> image = monotonicImageOfSeed(seed, domainProperty);
                rewriteResult = image == null ? null : buildEqualImagePredicate(offspring, image);
            }
        } else if (binaryType == BinaryType.LT || binaryType == BinaryType.LE) {
            if (domainProperty.contains(seed)) {
                // same safe-target gate as in the EQ branch
                if (!domainProperty.getValueWrapper(seed).isMonotonicDerived()
                        || isSafeDeriveTarget(offspring, slotDefinitions)) {
                    RangeExtractor.RangeDescriptor desc = domainProperty.getValueWrapper(seed).getRangeDesc();
                    rewriteResult = deriveLessPredicate(offspring, desc, toLeft);
                }
            } else if (enableMonotonicDerive && isSafeDeriveTarget(offspring, slotDefinitions)) {
                // range comparison in the ON clause, e.g. ON f.load_ts < months_add(e.datadate, 1)
                // (toLeft: seed is the right child of '<'): offspring < seed <= imageUpper, so
                // offspring <= imageUpper. The image is closed, so <= — same bound choice as
                // deriveLessPredicate.
                Range<ConstantOperator> image = monotonicImageOfSeed(seed, domainProperty);
                rewriteResult = image == null ? null : new BinaryPredicateOperator(
                        toLeft ? BinaryType.LE : BinaryType.GE, offspring,
                        toLeft ? image.upperEndpoint() : image.lowerEndpoint());
            }
        } else if (binaryType == BinaryType.GT || binaryType == BinaryType.GE) {
            if (domainProperty.contains(seed)) {
                if (!domainProperty.getValueWrapper(seed).isMonotonicDerived()
                        || isSafeDeriveTarget(offspring, slotDefinitions)) {
                    RangeExtractor.RangeDescriptor desc = domainProperty.getValueWrapper(seed).getRangeDesc();
                    rewriteResult = deriveGreaterPredicate(offspring, desc, toLeft);
                }
            } else if (enableMonotonicDerive && isSafeDeriveTarget(offspring, slotDefinitions)) {
                // mirror of the LT/LE fallback: offspring > seed >= imageLower, so
                // offspring >= imageLower — same bound choice as deriveGreaterPredicate
                Range<ConstantOperator> image = monotonicImageOfSeed(seed, domainProperty);
                rewriteResult = image == null ? null : new BinaryPredicateOperator(
                        toLeft ? BinaryType.GE : BinaryType.LE, offspring,
                        toLeft ? image.lowerEndpoint() : image.upperEndpoint());
            }
        }
        if (rewriteResult == null) {
            return null;
        }

        rewriteResult.setIsPushdown(true);
        return removeRedundantPredicate(offspring, rewriteResult, existDomainProperty);
    }

    /**
     * Checks that the target expression can safely receive a derived predicate. The predicate
     * itself is a correct row filter on any target: joined rows satisfy it anyway. The problem
     * is partition further-prune: it maps partition bounds through a CallOperator over the
     * partition column and has no monotonicity check of its own. A predicate like
     * mod(x, 100) = 50 handed to it would prune partitions that still contain matching rows.
     * So a CallOperator target must pass the same monotonicity check as the seed.
     * <p>
     * Bare columns are safe. Casts are safe too: further-prune drops CastOperator conjuncts,
     * so a predicate on cast(datamonth as varchar) stays a plain row filter.
     * <p>
     * A bare-column target is first resolved through the child projection: after the ON
     * expression moves into the projection, mod(f.v, 100) arrives here as a bare slot, and
     * the predicate put on that slot turns back into mod(f.v, 100) when pushed down through
     * the projection.
     */
    private boolean isSafeDeriveTarget(ScalarOperator offspring, Map<ColumnRefOperator, ScalarOperator> slotDefinitions) {
        ScalarOperator target = offspring.isColumnRef()
                ? slotDefinitions.getOrDefault((ColumnRefOperator) offspring, offspring) : offspring;
        if (target.isColumnRef() || target instanceof CastOperator) {
            return true;
        }
        return MonotonicImage.isMonotonicExpression(target);
    }

    private Map<ColumnRefOperator, ScalarOperator> collectSlotDefinitions(OptExpression... children) {
        Map<ColumnRefOperator, ScalarOperator> definitions = new HashMap<>();
        for (OptExpression child : children) {
            if (child.getOp() instanceof LogicalProjectOperator) {
                definitions.putAll(((LogicalProjectOperator) child.getOp()).getColumnRefMap());
            }
            if (child.getOp().getProjection() != null) {
                definitions.putAll(child.getOp().getProjection().getColumnRefMap());
            }
        }
        return definitions;
    }

    /**
     * Range of the seed expression, computed from the domain of its column. Steps for
     * seed = date_format(e.datadate, '%Y%m'):
     * <ol>
     * <li>seed uses exactly one column, e.datadate;</li>
     * <li>the child's domain has a range for that column (from the pushed-down scan predicate
     *     datadate BETWEEN '2024-03-05' AND '2024-04-10'; domains are keyed by exact
     *     structure, so only the bare column has an entry, not the date_format expression);</li>
     * <li>{@link MonotonicImage#imageRange} folds the seed at both endpoints and returns
     *     ['202403', '202404'], or empty when monotonicity cannot be proven.</li>
     * </ol>
     * An IN-list domain arrives as its covering range (RangeExtractor turns the value list
     * into [min, max] when it builds the descriptor). Boolean-call domains have no range and
     * stop at the getRange() check.
     */
    private Range<ConstantOperator> monotonicImageOfSeed(ScalarOperator seed, DomainProperty domainProperty) {
        Set<ColumnRefOperator> usedColumns = Sets.newHashSet(seed.getColumnRefs());
        if (usedColumns.size() != 1) {
            return null;
        }
        ColumnRefOperator column = usedColumns.iterator().next();
        // a bare-column seed either had its own domain entry (handled by the pre-existing
        // branches) or has no domain at all — nothing to map either way
        if (column.equals(seed) || !domainProperty.contains(column)) {
            return null;
        }
        RangeExtractor.RangeDescriptor desc = domainProperty.getValueWrapper(column).getRangeDesc();
        if (desc == null || desc.getRange() == null) {
            return null;
        }
        return MonotonicImage.imageRange(seed, column, desc.getRange()).orElse(null);
    }

    /**
     * offspring-side predicate for an equality whose seed image is known:
     * a point image [v, v] (from WHERE datadate = '2024-03-05') becomes offspring = v,
     * a proper interval becomes offspring >= lo AND offspring <= hi.
     */
    private ScalarOperator buildEqualImagePredicate(ScalarOperator offspring, Range<ConstantOperator> image) {
        if (image.lowerEndpoint().equals(image.upperEndpoint())) {
            return new BinaryPredicateOperator(BinaryType.EQ, offspring, image.lowerEndpoint());
        }
        return Utils.compoundAnd(
                new BinaryPredicateOperator(BinaryType.GE, offspring, image.lowerEndpoint()),
                new BinaryPredicateOperator(BinaryType.LE, offspring, image.upperEndpoint()));
    }

    private List<ScalarOperator> distinctPredicates(List<ScalarOperator> predicates) {
        return new ArrayList<>(new LinkedHashSet<>(predicates));
    }

    private ScalarOperator removeRedundantPredicate(ScalarOperator offspring, ScalarOperator rewriteResult,
                                                    DomainProperty existDomainProperty) {
        if (!existDomainProperty.contains(offspring)) {
            return rewriteResult;
        }
        Set<ScalarOperator> set = Sets.newLinkedHashSet();
        set.addAll(Utils.extractConjuncts(existDomainProperty.getPredicateDesc(offspring)));
        rewriteResult = Utils.compoundAnd(Utils.extractConjuncts(rewriteResult).stream()
                .filter(e  -> !set.contains(e)).collect(Collectors.toList()));
        if (rewriteResult == null) {
            return null;
        }

        DomainPropertyDeriver deriver = new DomainPropertyDeriver();
        DomainProperty newDomainProperty = deriver.derive(rewriteResult);
        Range<ConstantOperator> existRange = existDomainProperty.getValueWrapper(offspring).getRangeDesc().getRange();
        Range<ConstantOperator> newRange = newDomainProperty.getValueWrapper(offspring).getRangeDesc().getRange();
        if (existRange == null) {
            return newRange == null ? null : rewriteResult;
        } else if (newRange == null || newRange.encloses(existRange)) {
            return null;
        } else {
            return rewriteResult;
        }
    }

    // join on predicate left_tbl_col < right_tbl_col
    // if we want to derive predicate to left, we need obtain the upper bound value of right_tbl_col
    // if we want to derive predicate to right, we need obtain the lower bound value of left_tbl_col
    private ScalarOperator deriveLessPredicate(ScalarOperator offspring,
                                               RangeExtractor.RangeDescriptor desc, boolean toLeft) {
        if (desc == null) {
            return null;
        }
        Range<ConstantOperator> range = desc.getRange();

        if (range == null) {
            return null;
        }

        if (toLeft && range.hasUpperBound()) {
            return new BinaryPredicateOperator(BinaryType.LE, offspring, range.upperEndpoint());
        } else if (!toLeft && range.hasLowerBound()) {
            return new BinaryPredicateOperator(BinaryType.GE, offspring, range.lowerEndpoint());
        }
        return null;
    }

    // join on predicate left_tbl_col > right_tbl_col
    // if we want to derive predicate to left, we need obtain the lower bound value of right_tbl_col
    // if we want to derive predicate to right, we need obtain the upper bound value of left_tbl_col
    private ScalarOperator deriveGreaterPredicate(ScalarOperator offspring,
                                                  RangeExtractor.RangeDescriptor desc, boolean toLeft) {
        if (desc == null) {
            return null;
        }
        Range<ConstantOperator> range = desc.getRange();

        if (range == null) {
            return null;
        }
        if (toLeft && range.hasLowerBound()) {
            return new BinaryPredicateOperator(BinaryType.GE, offspring, range.lowerEndpoint());
        } else if (!toLeft && range.hasUpperBound()) {
            return new BinaryPredicateOperator(BinaryType.LE, offspring, range.upperEndpoint());
        }
        return null;
    }

}
