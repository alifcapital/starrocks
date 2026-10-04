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

package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.sql.optimizer.rule.mv.ModifyInference.ModifyKind;
import com.starrocks.sql.optimizer.rule.mv.ModifyInference.ModifyOp;
import com.starrocks.sql.optimizer.rule.mv.ModifyInference.UpdateKind;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Operands of ModifyOp.union are often the shared constants (NONE, INSERT_ONLY, UPSERT, ALL), so the union
 * must return a new value with the kinds of both operands and must not change either operand.
 */
public class ModifyOpUnionTest {
    @Test
    public void unionIncludesBothOperandsWithoutChangingEither() {
        // Bits 0..2 select ModifyKind values and bits 3..4 select UpdateKind values, so 32 x 32 covers all pairs.
        for (int left = 0; left < 32; left++) {
            for (int right = 0; right < 32; right++) {
                EnumSet<ModifyKind> leftModify = modifications(left);
                EnumSet<UpdateKind> leftUpdate = updates(left);
                EnumSet<ModifyKind> rightModify = modifications(right);
                EnumSet<UpdateKind> rightUpdate = updates(right);
                ModifyOp result = ModifyOp.union(new ModifyOp(leftModify, leftUpdate),
                        new ModifyOp(rightModify, rightUpdate));
                String scenario = left + " union " + right;
                assertEquals(new ModifyOp(modifications(left | right), updates(left | right)), result, scenario);
                assertEquals(modifications(left), leftModify, scenario);
                assertEquals(updates(left), leftUpdate, scenario);
                assertEquals(modifications(right), rightModify, scenario);
                assertEquals(updates(right), rightUpdate, scenario);
            }
        }
    }

    @Test
    public void resultDoesNotShareSetsWithTheOperands() {
        EnumSet<ModifyKind> modify = EnumSet.of(ModifyKind.INSERT, ModifyKind.UPDATE);
        EnumSet<UpdateKind> update = EnumSet.of(UpdateKind.UPDATE_AFTER);
        ModifyOp operand = new ModifyOp(modify, update);
        ModifyOp result = ModifyOp.union(operand, operand);
        modify.clear();
        update.clear();
        assertEquals(new ModifyOp(EnumSet.of(ModifyKind.INSERT, ModifyKind.UPDATE),
                EnumSet.of(UpdateKind.UPDATE_AFTER)), result);
    }

    private static EnumSet<ModifyKind> modifications(int bits) {
        EnumSet<ModifyKind> result = EnumSet.noneOf(ModifyKind.class);
        for (ModifyKind kind : ModifyKind.values()) {
            if ((bits & (1 << kind.ordinal())) != 0) {
                result.add(kind);
            }
        }
        return result;
    }

    private static EnumSet<UpdateKind> updates(int bits) {
        EnumSet<UpdateKind> result = EnumSet.noneOf(UpdateKind.class);
        for (UpdateKind kind : UpdateKind.values()) {
            if ((bits & (1 << (3 + kind.ordinal()))) != 0) {
                result.add(kind);
            }
        }
        return result;
    }
}
