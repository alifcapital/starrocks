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

package com.starrocks.sql.optimizer.operator.scalar;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// Clones share one hint list. We expect that list to be read-only and detached from the caller,
// so a change through one operator can never show up in another.
public class ScalarHintOwnershipTest {
    @Test
    public void assignedHintsAreDetachedAndReadOnly() {
        ConstantOperator value = ConstantOperator.createInt(1);
        List<String> input = new ArrayList<>(Arrays.asList("skew", null, "skew"));
        value.setHints(input);
        input.clear();
        Assertions.assertEquals(Arrays.asList("skew", null, "skew"), value.getHints());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> value.getHints().add("other"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> value.getHints().set(0, "other"));
    }

    @Test
    public void replacingHintsOnACloneDoesNotAffectTheSource() {
        ConstantOperator value = ConstantOperator.createInt(1);
        value.setHints(List.of("first"));
        ScalarOperator copy = value.clone();
        Assertions.assertEquals(List.of("first"), copy.getHints());
        value.setHints(List.of("second"));
        Assertions.assertEquals(List.of("first"), copy.getHints());
        copy.setHints(List.of("third"));
        Assertions.assertEquals(List.of("second"), value.getHints());
        Assertions.assertEquals(List.of("third"), copy.getHints());
    }
}
