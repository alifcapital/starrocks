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

package com.starrocks.sql.optimizer.rule.join;

import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

// An edge covers the atoms whose output columns its predicate uses. A column computed by a projection inside
// the multi-join is replaced by the columns of its expression, one level deep. We expect the cover not to depend
// on the order of the expression map, and the predicate's own used-column set not to change.
public class JoinEdgeCoverTest {
    static class Probe extends JoinOrder {
        Probe() {
            super(null);
        }

        @Override
        protected void enumerate() {
        }

        @Override
        public List<OptExpression> getResult() {
            return List.of();
        }

        BitSet cover(ScalarOperator predicate, Map<ColumnRefOperator, ScalarOperator> expressions) {
            edges.clear();
            edges.add(new Edge(predicate));
            edgeSize = 1;
            atomSize = 3;
            List<OptExpression> atoms = List.of(atom(1), atom(2), atom(3));
            computeEdgeCover(atoms, expressions);
            return edges.get(0).vertexes;
        }
    }

    private static OptExpression atom(int id) {
        OptExpression atom = OptExpression.create(new LogicalValuesOperator(List.of()));
        atom.setLogicalProperty(new LogicalProperty(new ColumnRefSet(id)));
        return atom;
    }

    @Test
    public void expansionIsOneLevelAndIndependentOfMapOrder() {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        ColumnRefOperator c = new ColumnRefOperator(3, IntegerType.INT, "c", true);
        Map<ColumnRefOperator, ScalarOperator> expressions = new LinkedHashMap<>();
        expressions.put(a, b);
        expressions.put(b, c);
        BitSet expected = new BitSet();
        expected.set(0);
        expected.set(1);
        assertEquals(expected, new Probe().cover(a, expressions));
        expressions.clear();
        expressions.put(b, c);
        expressions.put(a, b);
        assertEquals(expected, new Probe().cover(a, expressions));
        assertEquals(new ColumnRefSet(1), a.getUsedColumns());
    }

    @Test
    public void unmappedPredicateCoversOnlyItsOwnAtom() {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        BitSet expected = new BitSet();
        expected.set(0);
        assertEquals(expected, new Probe().cover(a, Map.of()));
        assertEquals(expected, new Probe().cover(a, Map.of(b, a)));
    }
}
