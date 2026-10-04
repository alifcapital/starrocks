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

package com.starrocks.sql.optimizer.rule.transformation.pruner;

import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class CPJoinGardenerPruneContextTest {
    @Test
    public void testMergedMappingCanBeRewritten() throws Exception {
        // Two pruned children map the same column. After the merge the parent rewrites that column, which adds to
        // its merged set, so the merged set must be modifiable.
        Class<?> type = Class.forName(CPJoinGardener.class.getName() + "$PruneContext");
        Constructor<?> constructor = type.getDeclaredConstructor();
        constructor.setAccessible(true);
        Method pruned = type.getDeclaredMethod("pruned", Map.class);
        Method merge = type.getDeclaredMethod("merge", type);
        Method rewrite = type.getDeclaredMethod("rewrite", Set.class, Map.class);
        Method mapping = type.getDeclaredMethod("getRewriteMapping");
        for (Method method : new Method[] {pruned, merge, rewrite, mapping}) {
            method.setAccessible(true);
        }
        ColumnRefOperator id = new ColumnRefOperator(1, IntegerType.INT, "id", true);
        ColumnRefOperator first = new ColumnRefOperator(2, IntegerType.INT, "first", true);
        ColumnRefOperator second = new ColumnRefOperator(3, IntegerType.INT, "second", true);
        ColumnRefOperator foreignKey = new ColumnRefOperator(4, IntegerType.INT, "fk", true);
        Object empty = constructor.newInstance();
        Object left = pruned.invoke(empty, new HashMap<>(Map.of(id, new HashSet<>(Set.of(first)))));
        Object right = pruned.invoke(empty, new HashMap<>(Map.of(id, new HashSet<>(Set.of(second)))));
        Object parent = constructor.newInstance();
        merge.invoke(parent, left);
        merge.invoke(parent, right);
        rewrite.invoke(parent, Set.of(id), Map.of(id, foreignKey));
        Assertions.assertEquals(Map.of(foreignKey, Set.of(first, second, id)), mapping.invoke(parent));
    }
}
