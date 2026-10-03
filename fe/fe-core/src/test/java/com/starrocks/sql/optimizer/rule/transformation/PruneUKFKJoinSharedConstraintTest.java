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

import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.constraint.ForeignKeyConstraint;
import com.starrocks.catalog.constraint.UniqueConstraint;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.UKFKConstraintsCollector;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.UKFKConstraints;
import com.starrocks.sql.optimizer.operator.logical.LogicalFilterOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJoinOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

// UKFKConstraints.inheritFrom shares the unique-key wrapper between a child and its parents, and the memo
// can try several joins over the same UK child. We expect PruneUKFKJoinRule to leave the shared wrapper
// unchanged when it refuses a join, so the next attempt sees the same unique-key facts.
public class PruneUKFKJoinSharedConstraintTest {
    @Test
    public void refusedOuterJoinDoesNotChangeSharedUniqueKey() {
        for (boolean ukOnLeft : new boolean[] {false, true}) {
            try (Fixture f = new Fixture()) {
                OptExpression project = f.project(outerJoinPreservingFk(ukOnLeft), ukOnLeft);
                UKFKConstraintsCollector.collectColumnConstraints(project.inputAt(0));
                UKFKConstraints.JoinProperty property = project.inputAt(0).getConstraints().getJoinProperty();
                assertNotNull(property);
                assertSame(f.shared, property.ukConstraint);
                assertEquals(ukOnLeft, property.isLeftUK);
                ColumnRefSet before = f.shared.nonUKColumnRefs.clone();
                assertFalse(before.contains(f.uk));

                // The UK side has a predicate on the UK column, so the outer join is refused.
                assertTrue(f.transform(project).isEmpty());
                assertEquals(before, f.shared.nonUKColumnRefs);
                assertSame(f.shared, f.ukSide.getConstraints().getUniqueConstraint(f.uk.getId()));
            }
        }
    }

    @Test
    public void refusedOuterJoinDoesNotBlockInnerJoinOverTheSameUkChild() {
        for (boolean ukOnLeft : new boolean[] {false, true}) {
            try (Fixture f = new Fixture()) {
                assertTrue(f.transform(f.project(outerJoinPreservingFk(ukOnLeft), ukOnLeft)).isEmpty());

                // The inner join over the same UK child is allowed. The UK predicate moves to the FK side
                // and the FK column gets a NOT NULL filter.
                OptExpression inner = f.project(JoinOperator.INNER_JOIN, ukOnLeft);
                List<OptExpression> result = f.transform(inner);
                assertEquals(1, result.size());
                LogicalProjectOperator project = result.get(0).getOp().cast();
                assertEquals(Map.of(f.fk, f.fk), project.getColumnRefMap());
                LogicalFilterOperator filter = result.get(0).inputAt(0).getOp().cast();
                assertSame(f.fkSide, result.get(0).inputAt(0).inputAt(0));
                List<ScalarOperator> predicates = Utils.extractConjuncts(filter.getPredicate());
                assertEquals(2, predicates.size());
                BinaryPredicateOperator rewritten = (BinaryPredicateOperator) predicates.get(0);
                assertEquals(BinaryType.GE, rewritten.getBinaryType());
                assertEquals(f.fk, rewritten.getChild(0));
                assertEquals(1, ((ConstantOperator) rewritten.getChild(1)).getInt());
                IsNullPredicateOperator notNull = (IsNullPredicateOperator) predicates.get(1);
                assertTrue(notNull.isNotNull());
                assertEquals(f.fk, notNull.getChild(0));

                assertEquals(new ColumnRefSet(f.nonUk.getId()), f.shared.nonUKColumnRefs);
            }
        }
    }

    private static JoinOperator outerJoinPreservingFk(boolean ukOnLeft) {
        return ukOnLeft ? JoinOperator.RIGHT_OUTER_JOIN : JoinOperator.LEFT_OUTER_JOIN;
    }

    private static final class Fixture implements AutoCloseable {
        private final ConnectContext previous = ConnectContext.get();
        private final ColumnRefFactory factory = new ColumnRefFactory();
        private final OptimizerContext context;
        private final ColumnRefOperator uk;
        private final ColumnRefOperator nonUk;
        private final ColumnRefOperator fk;
        private final OptExpression ukSide;
        private final OptExpression fkSide;
        private final UKFKConstraints.UniqueConstraintWrapper shared;
        private final PruneUKFKJoinRule rule = new PruneUKFKJoinRule();

        private Fixture() {
            ConnectContext connection = new ConnectContext();
            connection.getSessionVariable().setEnableUKFKOpt(true);
            connection.setThreadLocalInfo();
            context = OptimizerFactory.mockContext(connection, factory);
            uk = factory.create("uk", IntegerType.INT, false);
            nonUk = factory.create("non_uk", IntegerType.INT, false);
            fk = factory.create("fk", IntegerType.INT, true);

            // UK side: values(uk, non_uk) where uk >= 1, projected to uk.
            OptExpression ukValues = OptExpression.create(new LogicalValuesOperator(List.of(uk, nonUk),
                    List.of(List.of(ConstantOperator.createInt(5), ConstantOperator.createInt(7)))));
            ukValues.getOp().setPredicate(new BinaryPredicateOperator(BinaryType.GE, uk, ConstantOperator.createInt(1)));
            ukValues.deriveLogicalPropertyItself();
            UniqueConstraint unique = new UniqueConstraint("catalog", "db", "uk_table",
                    List.of(ColumnId.create("uk")));
            shared = new UKFKConstraints.UniqueConstraintWrapper(unique, new ColumnRefSet(nonUk.getId()),
                    false, new ColumnRefSet(uk.getId()));
            UKFKConstraints ukFacts = new UKFKConstraints();
            ukFacts.addUniqueKey(uk.getId(), shared);
            ukFacts.addAggUniqueKey(shared);
            ukValues.setConstraints(ukFacts);
            ukSide = OptExpression.create(new LogicalProjectOperator(Map.of(uk, uk)), ukValues);
            ukSide.deriveLogicalPropertyItself();
            ukSide.setConstraints(UKFKConstraints.inheritFrom(ukFacts,
                    ukSide.getRowOutputInfo().getOutputColumnRefSet()));

            // FK side: values(fk) with fk referencing uk_table.uk.
            fkSide = OptExpression.create(new LogicalValuesOperator(List.of(fk),
                    List.of(List.of(ConstantOperator.createInt(5)))));
            fkSide.deriveLogicalPropertyItself();
            ForeignKeyConstraint foreign = new ForeignKeyConstraint(
                    new BaseTableInfo("catalog", "db", "uk_table", "identifier"), null,
                    List.of(Pair.create(ColumnId.create("fk"), ColumnId.create("uk"))));
            UKFKConstraints fkFacts = new UKFKConstraints();
            fkFacts.addForeignKey(fk.getId(), new UKFKConstraints.ForeignKeyConstraintWrapper(foreign, false));
            fkSide.setConstraints(fkFacts);
        }

        private OptExpression project(JoinOperator type, boolean ukOnLeft) {
            BinaryPredicateOperator on = new BinaryPredicateOperator(BinaryType.EQ, uk, fk);
            OptExpression join = OptExpression.create(new LogicalJoinOperator(type, on),
                    ukOnLeft ? ukSide : fkSide, ukOnLeft ? fkSide : ukSide);
            join.deriveLogicalPropertyItself();
            OptExpression project = OptExpression.create(new LogicalProjectOperator(Map.of(fk, fk)), join);
            project.deriveLogicalPropertyItself();
            return project;
        }

        private List<OptExpression> transform(OptExpression project) {
            assertTrue(rule.check(project, context));
            return rule.transform(project, context);
        }

        @Override
        public void close() {
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }
}
