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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.statistic.StatsTupleKeyCodec;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.List;

/** Original equality values, once per shared head, never repeated per predicate slice.
 * Text stays in a packed UTF-8 buffer; only selected skew keys become Java Strings during planning.
 */
public final class JoinStatisticsHeadKeys {
    private final long[] integers;
    private final byte[] text;
    private final int[] offsets;
    private final BitSet omitted;

    public JoinStatisticsHeadKeys(long[] integers) {
        this.integers = integers.clone();
        this.text = null;
        this.offsets = null;
        this.omitted = null;
        checkSize();
    }

    public JoinStatisticsHeadKeys(String[] values) {
        this.integers = null;
        if (values.length > JoinStatisticsCorrelation.HEAD_BUDGET) {
            throw new IllegalArgumentException("Oversized JOIN head dictionary");
        }
        this.offsets = new int[values.length + 1];
        this.omitted = new BitSet(values.length);
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        for (int i = 0; i < values.length; i++) {
            offsets[i] = encoded.size();
            if (values[i] == null) {
                omitted.set(i);
            } else {
                encoded.writeBytes(values[i].getBytes(StandardCharsets.UTF_8));
            }
        }
        offsets[values.length] = encoded.size();
        this.text = encoded.toByteArray();
    }

    private void checkSize() {
        if (size() > JoinStatisticsCorrelation.HEAD_BUDGET) {
            throw new IllegalArgumentException("Oversized JOIN head dictionary");
        }
    }

    public int size() {
        return integers == null ? offsets.length - 1 : integers.length;
    }

    boolean hasValue(int index) {
        return integers != null || !omitted.get(index);
    }

    boolean isInteger() {
        return integers != null;
    }

    long integer(int index) {
        return integers[index];
    }

    String text(int index) {
        return omitted.get(index) ? null
                : new String(text, offsets[index], offsets[index + 1] - offsets[index], StandardCharsets.UTF_8);
    }

    public List<String> tuple(int index, int arity) {
        if (integers != null) {
            return List.of(Long.toString(integers[index]));
        }
        // Oversized textual keys may be omitted without losing their numeric frequency summaries.
        String value = text(index);
        return value == null ? List.of() : arity == 1 ? List.of(value) : StatsTupleKeyCodec.decode(value);
    }

    public long estimatedSize() {
        return integers != null ? 56 + 8L * integers.length
                : 112L + text.length + 4L * offsets.length + 8L * ((size() + 63) / 64);
    }
}
