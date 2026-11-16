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

package com.starrocks.common.io;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

// Text.decode reads every journal entry and must give the string a REPLACE decoder gives, malformed bytes included.
public class TextDecodeTest {

    private static String replaceDecoder(byte[] bytes, int start, int length) throws CharacterCodingException {
        return StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE)
                .decode(ByteBuffer.wrap(bytes, start, length)).toString();
    }

    private static void check(byte[] bytes) throws CharacterCodingException {
        Assertions.assertEquals(replaceDecoder(bytes, 0, bytes.length), Text.decode(bytes), Arrays.toString(bytes));
        if (bytes.length > 2) {
            Assertions.assertEquals(replaceDecoder(bytes, 1, bytes.length - 2), Text.decode(bytes, 1, bytes.length - 2),
                    Arrays.toString(bytes));
        }
    }

    @Test
    public void decodesAsAReplaceDecoder() throws CharacterCodingException {
        check(new byte[0]);
        check("{\"clazz\":\"LoadJobFinalOperation\",\"id\":1}".getBytes(StandardCharsets.UTF_8));
        check("загрузка 数据 😀".getBytes(StandardCharsets.UTF_8));
        // truncated, overlong, surrogate, out of range and stray continuation sequences
        int[][] malformed = {
                {0xC3}, {0xE2, 0x82}, {0xF0, 0x9F, 0x98}, {0xC0, 0xAF}, {0xE0, 0x80, 0xAF},
                {0xED, 0xA0, 0x80}, {0xF4, 0x90, 0x80, 0x80}, {0xF8, 0x88, 0x80, 0x80, 0x80}, {0x80}, {0xBF, 0x41},
                {0x41, 0xE2, 0x41, 0x82, 0xAC}, {0xFE}, {0xFF, 0xFE}
        };
        for (int[] sequence : malformed) {
            byte[] bytes = new byte[sequence.length + 2];
            bytes[0] = 'a';
            for (int i = 0; i < sequence.length; i++) {
                bytes[i + 1] = (byte) sequence[i];
            }
            bytes[bytes.length - 1] = 'z';
            check(bytes);
        }
        Random random = new Random(42);
        for (int i = 0; i < 20_000; i++) {
            byte[] bytes = new byte[random.nextInt(24)];
            random.nextBytes(bytes);
            check(bytes);
        }
    }
}
