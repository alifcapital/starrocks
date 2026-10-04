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

package com.starrocks.sql.spm;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SPMFunctionsOperatorCheckTest {
    private final ColumnRefOperator col = new ColumnRefOperator(1, IntegerType.INT, "c", true);
    private final ConstantOperator id = ConstantOperator.createBigint(1);

    private CallOperator call(String name) {
        return new CallOperator(name, NullType.NULL, Lists.newArrayList(id));
    }

    @Test
    public void testSpmFunctionNamesAreMatchedIgnoringCase() {
        for (String name : new String[] {"_spm_const_list", "_spm_const_var", "_spm_const_range", "_spm_const_enum",
                "_SPM_CONST_LIST", "_Spm_Const_Var", "_sPm_cOnSt_RaNgE", "_spm_CONST_ENUM"}) {
            assertTrue(SPMFunctions.isSPMFunctions(call(name)), name);
        }
    }

    @Test
    public void testOtherNamesAreNotSpmFunctions() {
        for (String name : new String[] {"_spm_const_unknown", "_spm_", "_spm", "spm_const_var", "_xspm_const_var",
                "_spm_const_var_x", "abs", "a", "", "_spm_const_lis", "cast"}) {
            assertFalse(SPMFunctions.isSPMFunctions(call(name)), name);
        }
    }

    @Test
    public void testNonCallOperatorsAreNotSpmFunctions() {
        assertFalse(SPMFunctions.isSPMFunctions(col));
        assertFalse(SPMFunctions.isSPMFunctions(id));
    }

    @Test
    public void testInPredicateIsUnwrappedOnlyWithTwoChildren() {
        ScalarOperator spm = call("_spm_const_list");
        assertTrue(SPMFunctions.isSPMFunctions(new InPredicateOperator(col, spm)));
        assertFalse(SPMFunctions.isSPMFunctions(new InPredicateOperator(col, id)));
        assertFalse(SPMFunctions.isSPMFunctions(new InPredicateOperator(col, spm, id)));
        // the unwrapped child is the second one, not the first
        assertFalse(SPMFunctions.isSPMFunctions(new InPredicateOperator(spm, col)));
        assertFalse(SPMFunctions.isSPMFunctions(new InPredicateOperator(spm, id, col)));
    }
}
