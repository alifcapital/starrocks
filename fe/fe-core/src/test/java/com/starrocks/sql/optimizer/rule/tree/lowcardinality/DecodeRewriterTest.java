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

import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.base.OrderSpec;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDecodeOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DecodeRewriterTest {

    private static PhysicalTopNOperator newPartitionTopN(ColumnRefOperator partitionRef) {
        return new PhysicalTopNOperator(
                new OrderSpec(List.of(new Ordering(partitionRef, true, true))),
                Operator.DEFAULT_LIMIT, 0,
                List.of(partitionRef),
                1,
                SortPhase.FINAL,
                TopNType.ROW_NUMBER,
                false, false, false,
                null, null, null);
    }

    private static OptExpression newOptExpression(Operator op) {
        return OptExpression.builder()
                .setOp(op)
                .setInputs(List.of())
                .setLogicalProperty(new LogicalProperty(new ColumnRefSet()))
                .build();
    }

    @Test
    public void testTopNPartitionColumnDecodedBelowKeepsStringRef() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator stringRef = factory.create("s", TypeFactory.createVarcharType(128), true);
        ColumnRefOperator dictRef = factory.create("s", IntegerType.INT, true);

        DecodeContext context = new DecodeContext(factory);
        context.stringRefToDictRefMap.put(stringRef, dictRef);

        PhysicalTopNOperator topN = newPartitionTopN(stringRef);
        // the column was decoded below this TopN (e.g. under a join): it does NOT
        // arrive in dict form, so inputStringColumns stays empty
        context.operatorDecodeInfo.put(topN, DecodeInfo.create());

        DecodeRewriter rewriter = new DecodeRewriter(factory, context, new SessionVariable());
        OptExpression result = rewriter.visitPhysicalTopN(newOptExpression(topN), new ColumnRefSet());

        PhysicalTopNOperator newTopN = result.getOp().cast();
        // partition-by must keep the string ref; the dict slot does not exist here and
        // referencing it fails on BE with "slot_id not found"
        Assertions.assertEquals(List.of(stringRef), newTopN.getPartitionByColumns());
        Assertions.assertEquals(stringRef, newTopN.getOrderSpec().getOrderDescs().get(0).getColumnRef());
    }

    @Test
    public void testTopNPartitionColumnInDictFormRewrittenToDictRef() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator stringRef = factory.create("s", TypeFactory.createVarcharType(128), true);
        ColumnRefOperator dictRef = factory.create("s", IntegerType.INT, true);

        DecodeContext context = new DecodeContext(factory);
        context.stringRefToDictRefMap.put(stringRef, dictRef);

        PhysicalTopNOperator topN = newPartitionTopN(stringRef);
        // the column arrives at this TopN still in dict form
        DecodeInfo info = DecodeInfo.create();
        info.inputStringColumns.union(new ColumnRefSet(stringRef.getId()));
        context.operatorDecodeInfo.put(topN, info);

        DecodeRewriter rewriter = new DecodeRewriter(factory, context, new SessionVariable());
        OptExpression result = rewriter.visitPhysicalTopN(newOptExpression(topN), new ColumnRefSet());

        PhysicalTopNOperator newTopN = result.getOp().cast();
        Assertions.assertEquals(List.of(dictRef), newTopN.getPartitionByColumns());
        Assertions.assertEquals(dictRef, newTopN.getOrderSpec().getOrderDescs().get(0).getColumnRef());
    }

    private static ColumnRefSet idSet(ColumnRefOperator... refs) {
        ColumnRefSet set = new ColumnRefSet();
        for (ColumnRefOperator ref : refs) {
            set.union(ref);
        }
        return set;
    }

    private static PhysicalTopNOperator newTopN(ColumnRefOperator orderBy) {
        return new PhysicalTopNOperator(
                new OrderSpec(List.of(new Ordering(orderBy, true, true))),
                Operator.DEFAULT_LIMIT, 0,
                null,
                1,
                SortPhase.FINAL,
                TopNType.ROW_NUMBER,
                false, false, false,
                null, null, null);
    }

    private static OptExpression node(Operator op, ColumnRefSet outputs, OptExpression... children) {
        return OptExpression.builder()
                .setOp(op)
                .setInputs(new ArrayList<>(List.of(children)))
                .setLogicalProperty(new LogicalProperty(outputs))
                .build();
    }

    @Test
    public void testOutputColumnsOfRewrittenOperatorMapStringToDict() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator s = factory.create("s", TypeFactory.createVarcharType(32), true);
        ColumnRefOperator d = factory.create("s", IntegerType.INT, true);
        ColumnRefOperator o = factory.create("o", IntegerType.INT, true);
        ColumnRefOperator c = factory.create("c", TypeFactory.createVarcharType(32), true);
        DecodeContext context = new DecodeContext(factory);
        context.stringRefToDictRefMap.put(s, d);
        OptExpression input = node(newTopN(o), idSet(s, o, c));
        PhysicalTopNOperator topN = input.getOp().cast();
        DecodeInfo info = DecodeInfo.create();
        info.outputStringColumns.union(idSet(s, c));
        context.operatorDecodeInfo.put(topN, info);

        DecodeRewriter rewriter = new DecodeRewriter(factory, context, new SessionVariable());
        OptExpression result = rewriter.visitPhysicalTopN(input, new ColumnRefSet());

        // c is an output string column without a dict ref, so it stays a string column.
        Assertions.assertEquals(idSet(d, o, c), result.getLogicalProperty().getOutputColumns());
        // The input plan node can be shared with other plans, so it must keep its own output columns.
        Assertions.assertEquals(idSet(s, o, c), input.getLogicalProperty().getOutputColumns());
    }

    @Test
    public void testRewriteKeepsPlanWithoutDecodeInfoUntouched() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator s = factory.create("s", TypeFactory.createVarcharType(32), true);
        ColumnRefOperator o = factory.create("o", IntegerType.INT, true);
        DecodeContext context = new DecodeContext(factory);
        context.allStringColumns.add(s.getId());
        OptExpression leafA = node(newTopN(o), idSet(s, o));
        OptExpression leafB = node(newTopN(o), idSet(o));
        OptExpression middle = node(newTopN(o), idSet(s, o), leafA);
        OptExpression root = node(newTopN(o), idSet(s, o), middle, leafB);

        OptExpression result = new DecodeRewriter(factory, context, new SessionVariable()).rewrite(root);

        // No operator has a DecodeInfo, so we expect the same plan with the same output columns.
        Assertions.assertSame(root, result);
        Assertions.assertEquals(idSet(s, o), result.getLogicalProperty().getOutputColumns());
        Assertions.assertSame(middle, result.inputAt(0));
        Assertions.assertSame(leafB, result.inputAt(1));
        Assertions.assertSame(leafA, middle.inputAt(0));
        Assertions.assertTrue(context.operatorDecodeInfo.isEmpty());
    }

    @Test
    public void testRewriteInsertsDecodeBetweenDictOutputAndDecodingParent() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator s = factory.create("s", TypeFactory.createVarcharType(32), true);
        ColumnRefOperator d = factory.create("s", IntegerType.INT, true);
        ColumnRefOperator o = factory.create("o", IntegerType.INT, true);
        DecodeContext context = new DecodeContext(factory);
        context.allStringColumns.add(s.getId());
        context.stringRefToDictRefMap.put(s, d);
        PhysicalTopNOperator childOp = newTopN(o);
        OptExpression plain = node(newTopN(o), idSet(o));
        OptExpression child = node(childOp, idSet(s, o));
        PhysicalTopNOperator rootOp = newTopN(o);
        OptExpression root = node(rootOp, idSet(s, o), plain, child);

        DecodeInfo childInfo = DecodeInfo.create();
        childInfo.outputStringColumns.union(s);
        context.operatorDecodeInfo.put(childOp, childInfo);
        DecodeInfo rootInfo = DecodeInfo.create();
        rootInfo.decodeStringColumns.union(s);
        context.operatorDecodeInfo.put(rootOp, rootInfo);

        OptExpression result = new DecodeRewriter(factory, context, new SessionVariable()).rewrite(root);

        // The child without a DecodeInfo is neither rewritten nor decoded.
        Assertions.assertSame(plain, result.inputAt(0));
        // The second child outputs dict ids and the root needs strings, so a decode node goes between them.
        OptExpression decode = result.inputAt(1);
        PhysicalDecodeOperator decodeOp = decode.getOp().cast();
        Assertions.assertEquals(Map.of(d, s), decodeOp.getDictToStrings());
        Assertions.assertEquals(idSet(s, o), decode.getLogicalProperty().getOutputColumns());
        Assertions.assertEquals(idSet(d, o), decode.inputAt(0).getLogicalProperty().getOutputColumns());
    }

    @Test
    public void testRewriteDecodesDictOutputOfRoot() {
        ColumnRefFactory factory = new ColumnRefFactory();
        ColumnRefOperator s = factory.create("s", TypeFactory.createVarcharType(32), true);
        ColumnRefOperator d = factory.create("s", IntegerType.INT, true);
        ColumnRefOperator o = factory.create("o", IntegerType.INT, true);
        DecodeContext context = new DecodeContext(factory);
        context.allStringColumns.add(s.getId());
        context.stringRefToDictRefMap.put(s, d);
        PhysicalTopNOperator rootOp = newTopN(o);
        OptExpression root = node(rootOp, idSet(s, o), node(newTopN(o), idSet(s, o)));
        DecodeInfo rootInfo = DecodeInfo.create();
        rootInfo.outputStringColumns.union(s);
        context.operatorDecodeInfo.put(rootOp, rootInfo);

        OptExpression result = new DecodeRewriter(factory, context, new SessionVariable()).rewrite(root);

        // The client needs strings, so a decode node is added on top of a root that outputs dict ids.
        PhysicalDecodeOperator decodeOp = result.getOp().cast();
        Assertions.assertEquals(Map.of(d, s), decodeOp.getDictToStrings());
        Assertions.assertEquals(idSet(s, o), result.getLogicalProperty().getOutputColumns());
        Assertions.assertEquals(idSet(d, o), result.inputAt(0).getLogicalProperty().getOutputColumns());
        Assertions.assertEquals(idSet(s, o), root.getLogicalProperty().getOutputColumns());
    }
}
