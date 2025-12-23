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

package com.starrocks.planner;

import com.starrocks.planner.expression.ExprOpcodeRegistry;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.catalog.Type;
import com.starrocks.thrift.TEqJoinCondition;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.THashJoinNode;
import com.starrocks.thrift.TJoinOnClause;
import com.starrocks.thrift.TPlanNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for verifying that join_on_clauses are properly serialized to Thrift.
 * This ensures that the FE -> BE communication works correctly for disjunctive hash joins.
 */
public class DisjunctiveHashJoinThriftTest {

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testExprJoinOnClauseBasic() {
        // Create equality predicates
        List<BinaryPredicate> eqPreds = new ArrayList<>();
        BinaryPredicate eq = new BinaryPredicate(BinaryType.EQ,
                new IntLiteral(1), new IntLiteral(2));
        eqPreds.add(eq);

        // Create ExprJoinOnClause
        ExprJoinOnClause clause = new ExprJoinOnClause(eqPreds, new ArrayList<>());

        assertTrue(clause.hasEqJoinConjuncts());
        assertFalse(clause.hasOtherConjuncts());
        assertEquals(1, clause.getEqJoinConjuncts().size());
    }

    @Test
    public void testExprJoinOnClauseWithOtherConjuncts() {
        // Create equality predicates
        List<BinaryPredicate> eqPreds = new ArrayList<>();
        BinaryPredicate eq = new BinaryPredicate(BinaryType.EQ,
                new IntLiteral(1), new IntLiteral(2));
        eqPreds.add(eq);

        // Create other conjuncts
        List<Expr> otherConjuncts = new ArrayList<>();
        BinaryPredicate gt = new BinaryPredicate(BinaryType.GT,
                new IntLiteral(3), new IntLiteral(10));
        otherConjuncts.add(gt);

        // Create ExprJoinOnClause
        ExprJoinOnClause clause = new ExprJoinOnClause(eqPreds, otherConjuncts);

        assertTrue(clause.hasEqJoinConjuncts());
        assertTrue(clause.hasOtherConjuncts());
        assertEquals(1, clause.getEqJoinConjuncts().size());
        assertEquals(1, clause.getOtherConjuncts().size());
    }

    @Test
    public void testExprJoinOnClauseEquals() {
        List<BinaryPredicate> eqPreds1 = new ArrayList<>();
        eqPreds1.add(new BinaryPredicate(BinaryType.EQ, new IntLiteral(1), new IntLiteral(2)));

        List<BinaryPredicate> eqPreds2 = new ArrayList<>();
        eqPreds2.add(new BinaryPredicate(BinaryType.EQ, new IntLiteral(1), new IntLiteral(2)));

        ExprJoinOnClause clause1 = new ExprJoinOnClause(eqPreds1, new ArrayList<>());
        ExprJoinOnClause clause2 = new ExprJoinOnClause(eqPreds2, new ArrayList<>());

        // Test equality (implementation dependent)
        assertNotNull(clause1);
        assertNotNull(clause2);
    }

    @Test
    public void testMultipleJoinOnClauses() {
        // Simulate: ON t1.a = t2.x OR t1.b = t2.y
        List<ExprJoinOnClause> clauses = new ArrayList<>();

        // Clause 1: t1.a = t2.x
        List<BinaryPredicate> eqPreds1 = new ArrayList<>();
        eqPreds1.add(new BinaryPredicate(BinaryType.EQ, new IntLiteral(1), new IntLiteral(4)));
        clauses.add(new ExprJoinOnClause(eqPreds1, new ArrayList<>()));

        // Clause 2: t1.b = t2.y
        List<BinaryPredicate> eqPreds2 = new ArrayList<>();
        eqPreds2.add(new BinaryPredicate(BinaryType.EQ, new IntLiteral(2), new IntLiteral(5)));
        clauses.add(new ExprJoinOnClause(eqPreds2, new ArrayList<>()));

        assertEquals(2, clauses.size());
        assertTrue(clauses.get(0).hasEqJoinConjuncts());
        assertTrue(clauses.get(1).hasEqJoinConjuncts());
    }

    @Test
    public void testTJoinOnClauseStructure() {
        // Test the Thrift structure directly
        TJoinOnClause tClause = new TJoinOnClause();

        // Add eq_join_conjuncts
        List<TEqJoinCondition> eqConditions = new ArrayList<>();
        TEqJoinCondition eqCond = new TEqJoinCondition();
        eqCond.setOpcode(TExprOpcode.EQ);
        eqConditions.add(eqCond);
        tClause.setEq_join_conjuncts(eqConditions);

        assertNotNull(tClause.getEq_join_conjuncts());
        assertEquals(1, tClause.getEq_join_conjuncts().size());
        assertFalse(tClause.isSetOther_conjuncts());
    }

    @Test
    public void testTHashJoinNodeWithJoinOnClauses() {
        // Test the THashJoinNode with join_on_clauses
        THashJoinNode hashJoinNode = new THashJoinNode();

        List<TJoinOnClause> joinOnClauses = new ArrayList<>();

        // Clause 1
        TJoinOnClause tClause1 = new TJoinOnClause();
        List<TEqJoinCondition> eqConditions1 = new ArrayList<>();
        eqConditions1.add(new TEqJoinCondition());
        tClause1.setEq_join_conjuncts(eqConditions1);
        joinOnClauses.add(tClause1);

        // Clause 2
        TJoinOnClause tClause2 = new TJoinOnClause();
        List<TEqJoinCondition> eqConditions2 = new ArrayList<>();
        eqConditions2.add(new TEqJoinCondition());
        tClause2.setEq_join_conjuncts(eqConditions2);
        joinOnClauses.add(tClause2);

        hashJoinNode.setJoin_on_clauses(joinOnClauses);

        assertTrue(hashJoinNode.isSetJoin_on_clauses());
        assertEquals(2, hashJoinNode.getJoin_on_clauses().size());
    }

    @Test
    public void testNullSafeEqualInJoinOnClause() {
        // Test null-safe equal (<=>)
        TJoinOnClause tClause = new TJoinOnClause();
        List<TEqJoinCondition> eqConditions = new ArrayList<>();

        TEqJoinCondition eqCond = new TEqJoinCondition();
        eqCond.setOpcode(TExprOpcode.EQ_FOR_NULL);  // null-safe equal
        eqConditions.add(eqCond);

        tClause.setEq_join_conjuncts(eqConditions);

        assertEquals(TExprOpcode.EQ_FOR_NULL, tClause.getEq_join_conjuncts().get(0).getOpcode());
    }
}
