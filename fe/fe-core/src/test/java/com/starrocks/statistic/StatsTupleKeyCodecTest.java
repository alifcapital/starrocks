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

package com.starrocks.statistic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

public class StatsTupleKeyCodecTest {
    @Test
    public void testDecodePlainComponents() {
        Assertions.assertEquals(List.of("approved", "0", "0"), StatsTupleKeyCodec.decode("approved#0#0"));
        Assertions.assertEquals(List.of("only"), StatsTupleKeyCodec.decode("only"));
        Assertions.assertEquals(List.of("", ""), StatsTupleKeyCodec.decode("#"));
        Assertions.assertEquals(List.of(""), StatsTupleKeyCodec.decode(""));
    }

    @Test
    public void testDecodeEscapesAndNulls() {
        Assertions.assertEquals(List.of("a#b", "1", "x#"), StatsTupleKeyCodec.decode("a\\#b#1#x\\#"));
        Assertions.assertEquals(Arrays.asList(null, "2", "y"), StatsTupleKeyCodec.decode("\\N#2#y"));
        Assertions.assertEquals(Arrays.asList("", null, "z"), StatsTupleKeyCodec.decode("#\\N#z"));
        Assertions.assertEquals(Arrays.asList(null, null), StatsTupleKeyCodec.decode("\\N#\\N"));
        // A literal "\N" value arrives escaped as "\\N" and stays a string.
        Assertions.assertEquals(List.of("back\\slash", "\\N", "#"), StatsTupleKeyCodec.decode("back\\\\slash#\\\\N#\\#"));
    }

    @Test
    public void testDecodeRejectsMalformedKeys() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> StatsTupleKeyCodec.decode("a\\"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> StatsTupleKeyCodec.decode("a\\x"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> StatsTupleKeyCodec.decode("\\Nx"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> StatsTupleKeyCodec.decode("x\\N"));
    }

    @Test
    public void testBuildKeyExpr() {
        Assertions.assertEquals(
                "stats_tuple_key(cast(`status` as varchar), cast(`dest_acc_gate` as varchar), cast(`dest_acc_type` as varchar))",
                StatsTupleKeyCodec.buildKeyExpr(null, List.of("status", "dest_acc_gate", "dest_acc_type")));
    }
}
