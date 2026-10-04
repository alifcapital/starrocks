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


package com.starrocks.sql.ast.expression;

import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;

public class ExprUtilsUsedSlotIdsTest {

    private static SlotRef slot(int id) {
        return new SlotRef(Integer.toString(id), new SlotDescriptor(new SlotId(id), "c" + id, IntegerType.INT, true));
    }

    private static RoaringBitmap bitmap(int... ids) {
        RoaringBitmap bitmap = new RoaringBitmap();
        for (int id : ids) {
            bitmap.add(id);
        }
        return bitmap;
    }

    private static List<Expr> exprs() {
        List<Expr> exprs = new ArrayList<>();
        exprs.add(slot(1));
        exprs.add(slot(7));
        exprs.add(new IntLiteral(3));
        exprs.add(new CastExpr(IntegerType.BIGINT, slot(2)));
        exprs.add(new CastExpr(IntegerType.BIGINT, new IntLiteral(3)));
        exprs.add(new ArithmeticExpr(ArithmeticExpr.Operator.ADD, slot(1), slot(2)));
        exprs.add(new ArithmeticExpr(ArithmeticExpr.Operator.ADD, slot(1), slot(1)));
        exprs.add(new ArithmeticExpr(ArithmeticExpr.Operator.ADD, new IntLiteral(1),
                new CastExpr(IntegerType.BIGINT, slot(4))));
        exprs.add(new BinaryPredicate(BinaryType.EQ, slot(1), slot(2)));
        exprs.add(new BinaryPredicate(BinaryType.EQ, new CastExpr(IntegerType.BIGINT, slot(3)), slot(5)));
        exprs.add(new BinaryPredicate(BinaryType.EQ, slot(2), new IntLiteral(9)));
        return exprs;
    }

    @Test
    public void testContainsUsedSlotIdsMatchesBitmapContains() {
        List<RoaringBitmap> sets = new ArrayList<>();
        sets.add(bitmap());
        sets.add(bitmap(1));
        sets.add(bitmap(2));
        sets.add(bitmap(1, 2));
        sets.add(bitmap(1, 2, 3, 4, 5));
        sets.add(bitmap(7, 100000));
        for (RoaringBitmap set : sets) {
            for (Expr expr : exprs()) {
                Assertions.assertEquals(set.contains(ExprUtils.getUsedSlotIds(expr)),
                        ExprUtils.containsUsedSlotIds(set, expr), set + " " + expr);
            }
        }
    }

    @Test
    public void testUsesSlotIdMatchesBitmapContains() {
        for (int id = 0; id < 10; id++) {
            for (Expr expr : exprs()) {
                Assertions.assertEquals(ExprUtils.getUsedSlotIds(expr).contains(id),
                        ExprUtils.usesSlotId(expr, id), id + " " + expr);
            }
        }
    }

    @Test
    public void testUnanalyzedSlotRefFailsLikeBefore() {
        Expr unanalyzed = new SlotRef(QualifiedName.of("a"));
        Expr nested = new BinaryPredicate(BinaryType.EQ, slot(1), unanalyzed);
        Assertions.assertThrows(IllegalStateException.class, () -> ExprUtils.getUsedSlotIds(unanalyzed));
        Assertions.assertThrows(IllegalStateException.class,
                () -> ExprUtils.containsUsedSlotIds(bitmap(1), unanalyzed));
        Assertions.assertThrows(IllegalStateException.class, () -> ExprUtils.getUsedSlotIds(nested));
        // The slot of the first child is not the slot asked for, the unanalyzed child must still fail.
        Assertions.assertThrows(IllegalStateException.class, () -> ExprUtils.usesSlotId(nested, 5));
    }
}
