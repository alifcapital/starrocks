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

package com.starrocks.sql.optimizer.transformer;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalApplyOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.SubqueryOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.ReduceCastRule;
import com.starrocks.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Turns "x IN (S1) OR x IN (S2) [OR x IN (S3) ...]" into "x IN (S1 UNION ALL S2 [UNION ALL S3 ...])".
 * <p>
 * We want one LEFT SEMI JOIN for the whole OR. An "x IN (S)" under an OR cannot be a semi join, because the OR needs
 * its three-valued result (TRUE, FALSE, NULL), so each of them is planned as an outer join with a count aggregation.
 * But a top-level conjunct of WHERE or HAVING keeps a row only when it is TRUE, so there NULL and FALSE are the same,
 * and the OR is TRUE exactly when x is in one of the subqueries. The caller therefore asks for the merge only for the
 * top-level conjuncts.
 * <p>
 * We merge only when the same rows match after the merge, i.e. when
 * - every disjunct is a positive IN with the same left expression and an uncorrelated subquery
 * - every subquery returns the same type (the length of a VARCHAR does not matter)
 * We need the same type because x is compared with the values of a subquery in a type that depends on both types.
 * With one type, the union is compared with x as each subquery was. With DATE and DATETIME, for example, a VARCHAR x
 * is compared as DATE with one subquery and as DATETIME with the other, and a value can match in one and not in the
 * other.
 * If one of the conditions does not hold, the OR stays as it was translated.
 */
final class InSubqueryOrMerger {

    /**
     * An uncorrelated "left IN (subquery)" as the translator planned it. Left is the translated left expression
     * before the implicit casts of the IN predicate, right is the single output column of the subquery plan.
     */
    static final class Term {
        private final ScalarOperator left;
        private final ColumnRefOperator right;
        private final OptExprBuilder plan;
        private final QueryStatement statement;

        Term(ScalarOperator left, ColumnRefOperator right, OptExprBuilder plan, QueryStatement statement) {
            this.left = left;
            this.right = right;
            this.plan = plan;
            this.statement = statement;
        }
    }

    private InSubqueryOrMerger() {
    }

    /**
     * Returns the output column of one semi-join-able IN that replaces orPredicate, or null when orPredicate is
     * not an OR of mergeable IN subqueries. On success the merged terms are removed from terms and from
     * subqueryPlaceholders, and the new subquery is added to subqueryPlaceholders.
     */
    static ColumnRefOperator merge(ScalarOperator orPredicate, Map<ColumnRefOperator, Term> terms,
                                   Map<ScalarOperator, SubqueryOperator> subqueryPlaceholders,
                                   ColumnRefFactory columnRefFactory) {
        List<Term> disjuncts = new ArrayList<>();
        List<ColumnRefOperator> placeholders = new ArrayList<>();
        for (ScalarOperator disjunct : Utils.extractDisjunctive(orPredicate)) {
            Term term = disjunct instanceof ColumnRefOperator ? terms.get(disjunct) : null;
            if (term == null) {
                return null;
            }
            disjuncts.add(term);
            placeholders.add((ColumnRefOperator) disjunct);
        }

        // The translator gives every call of a nondeterministic function its own id, so two such calls are never
        // equal, and we never evaluate one nondeterministic expression once instead of once per disjunct.
        ScalarOperator left = disjuncts.get(0).left;
        for (Term term : disjuncts) {
            if (!left.equals(term.left)) {
                return null;
            }
        }

        Type unionType = unionType(left, disjuncts);
        if (unionType == null) {
            return null;
        }

        boolean nullable = false;
        List<OptExprBuilder> children = new ArrayList<>();
        List<List<ColumnRefOperator>> childOutputs = new ArrayList<>();
        for (Term term : disjuncts) {
            ColumnRefOperator output = term.right;
            ScalarOperator value = output;
            if (!output.getType().equals(unionType)) {
                output = columnRefFactory.create("cast", unionType, true);
                value = new ScalarOperatorRewriter().rewrite(new CastOperator(unionType, term.right, true),
                        Lists.newArrayList(new ReduceCastRule()));
            }
            nullable |= output.isNullable();
            Map<ColumnRefOperator, ScalarOperator> projection = Maps.newHashMap();
            projection.put(output, value);
            children.add(term.plan.withNewRoot(new LogicalProjectOperator(projection)));
            childOutputs.add(Lists.newArrayList(output));
        }

        ColumnRefOperator unionOutput = columnRefFactory.create(disjuncts.get(0).right, unionType, nullable);
        LogicalUnionOperator union = new LogicalUnionOperator.Builder()
                .setOutputColumnRefOp(Lists.newArrayList(unionOutput))
                .setChildOutputColumns(childOutputs)
                .isUnionAll(true)
                .build();
        OptExprBuilder unionPlan = new OptExprBuilder(union, children,
                new ExpressionMapping(children.get(0).getScope(), Lists.newArrayList(unionOutput)));

        ScalarOperator inPredicate = new ScalarOperatorRewriter().rewrite(
                new InPredicateOperator(false, true, left, unionOutput),
                ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
        ColumnRefOperator applyOutput = columnRefFactory.create(inPredicate, inPredicate.getType(),
                inPredicate.isNullable());
        LogicalApplyOperator apply = LogicalApplyOperator.builder().setOutput(applyOutput)
                .setSubqueryOperator(inPredicate)
                .setCorrelationColumnRefs(Lists.newArrayList())
                .setUseSemiAnti(true).build();

        for (ColumnRefOperator placeholder : placeholders) {
            terms.remove(placeholder);
            subqueryPlaceholders.remove(placeholder);
        }
        subqueryPlaceholders.put(applyOutput,
                new SubqueryOperator(unionType, disjuncts.get(0).statement, apply, unionPlan));
        return applyOutput;
    }

    // The type of the union column, or null when the subqueries return different types. We want x to be compared
    // with every value in the same type as before, and we know this only for subqueries of one type, so we merge
    // nothing else. We also merge only the types listed in isSupported, because we have not checked the others
    // (float, json, complex, char, ...).
    private static Type unionType(ScalarOperator left, List<Term> disjuncts) {
        if (!isSupported(left.getType())) {
            return null;
        }
        Type unionType = disjuncts.get(0).right.getType();
        for (Term term : disjuncts) {
            Type type = term.right.getType();
            if (!isSupported(type)) {
                return null;
            }
            if (type.equals(unionType)) {
                continue;
            }
            // Only the length of a VARCHAR may differ.
            if (!type.isVarchar() || !unionType.isVarchar()) {
                return null;
            }
            unionType = TypeManager.getCommonSuperType(unionType, type);
        }
        return unionType;
    }

    private static boolean isSupported(Type type) {
        return type.isIntegerType() || type.isLargeIntType() || type.isDecimalV3() || type.isVarchar()
                || type.isDate() || type.isDatetime();
    }
}
