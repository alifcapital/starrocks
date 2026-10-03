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

import java.util.Arrays;

/**
 * Immutable BE-format HLL retained by the statistics cache. Small explicit/sparse sketches stay
 * compact; only a union accumulator expands registers. Encoding and estimation follow types/hll.cpp.
 * This is a reader/union implementation, not a second value hashing or collection implementation.
 */
public final class StatisticsHll {
    private static final int PRECISION = 14;
    private static final int REGISTERS = 1 << PRECISION;
    private static final int EXPLICIT_LIMIT = 160;
    private static final int MAX_RANK = 64 - PRECISION + 1;
    private static final float[] INVERSE_POWERS = new float[MAX_RANK + 1];

    static {
        for (int i = 0; i < INVERSE_POWERS.length; i++) {
            INVERSE_POWERS[i] = Math.scalb(1.0f, -i);
        }
    }

    private final byte[] encoded;

    private StatisticsHll(byte[] encoded) {
        this.encoded = encoded;
    }

    public static StatisticsHll fromSerialized(byte[] bytes) {
        if (bytes == null || bytes.length == 0 || bytes.length > REGISTERS + 1) {
            throw new IllegalArgumentException("Invalid statistics HLL length");
        }
        byte[] data = bytes.clone();
        switch (data[0]) {
            case 0:
                require(data.length == 1);
                break;
            case 1:
                require(data.length >= 2);
                int count = data[1] & 0xff;
                require(count <= EXPLICIT_LIMIT && data.length == 2 + count * Long.BYTES);
                break;
            case 2:
                require(data.length >= 5);
                int entries = readInt(data, 1);
                require(entries >= 0 && entries <= 4096 && data.length == 5 + entries * 3);
                for (int offset = 5; offset < data.length; offset += 3) {
                    require(readIndex(data, offset) < REGISTERS);
                    require((data[offset + 2] & 0xff) <= MAX_RANK);
                }
                break;
            case 3:
                require(data.length == REGISTERS + 1);
                for (int offset = 1; offset < data.length; offset++) {
                    require((data[offset] & 0xff) <= MAX_RANK);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown statistics HLL encoding: " + data[0]);
        }
        return new StatisticsHll(data);
    }

    public int retainedBytes() {
        // Object + byte-array headers, payload and alignment (a conservative cache weight).
        return 40 + ((encoded.length + 7) & ~7);
    }

    private static void require(boolean condition) {
        if (!condition) {
            throw new IllegalArgumentException("Malformed statistics HLL");
        }
    }

    private static int readIndex(byte[] data, int offset) {
        return (data[offset] & 0xff) | ((data[offset + 1] & 0xff) << 8);
    }

    private static int readInt(byte[] data, int offset) {
        return readIndex(data, offset) | (readIndex(data, offset + 2) << 16);
    }

    private static long readLong(byte[] data, int offset) {
        return (readInt(data, offset) & 0xffffffffL) | ((long) readInt(data, offset + 4) << 32);
    }

    /** A request-local union; neither merge nor estimate mutates a cached sketch. */
    public static final class Union {
        private long[] explicit;
        private int size;
        private byte[] registers;

        public void merge(StatisticsHll sketch) {
            byte[] data = sketch.encoded;
            switch (data[0]) {
                case 0:
                    return;
                case 1:
                    for (int offset = 2; offset < data.length; offset += Long.BYTES) {
                        addHash(readLong(data, offset));
                    }
                    return;
                case 2:
                    expand();
                    for (int offset = 5; offset < data.length; offset += 3) {
                        int index = readIndex(data, offset);
                        registers[index] = (byte) Math.max(registers[index], data[offset + 2]);
                    }
                    return;
                case 3:
                    if (registers == null && size == 0) {
                        registers = Arrays.copyOfRange(data, 1, data.length);
                    } else {
                        expand();
                        for (int i = 0; i < REGISTERS; i++) {
                            registers[i] = (byte) Math.max(registers[i], data[i + 1]);
                        }
                    }
                    return;
                default:
                    throw new IllegalStateException("Unvalidated statistics HLL");
            }
        }

        private void addHash(long hash) {
            if (registers != null) {
                updateRegister(hash);
                return;
            }
            for (int i = 0; i < size; i++) {
                if (explicit[i] == hash) {
                    return;
                }
            }
            if (size == EXPLICIT_LIMIT) {
                expand();
                updateRegister(hash);
            } else {
                if (explicit == null) {
                    explicit = new long[EXPLICIT_LIMIT];
                }
                explicit[size++] = hash;
            }
        }

        private void expand() {
            if (registers == null) {
                registers = new byte[REGISTERS];
                for (int i = 0; i < size; i++) {
                    updateRegister(explicit[i]);
                }
                explicit = null;
                size = 0;
            }
        }

        private void updateRegister(long hash) {
            int index = (int) (hash & (REGISTERS - 1));
            long remaining = (hash >>> PRECISION) | (1L << (64 - PRECISION));
            int rank = Long.numberOfTrailingZeros(remaining) + 1;
            registers[index] = (byte) Math.max(registers[index], rank);
        }

        public StatisticsHll snapshot() {
            if (registers != null) {
                byte[] bytes = new byte[REGISTERS + 1];
                bytes[0] = 3;
                System.arraycopy(registers, 0, bytes, 1, REGISTERS);
                return new StatisticsHll(bytes);
            }
            if (size == 0) {
                return new StatisticsHll(new byte[] {0});
            }
            byte[] bytes = new byte[2 + size * Long.BYTES];
            bytes[0] = 1;
            bytes[1] = (byte) size;
            for (int i = 0; i < size; i++) {
                for (int j = 0; j < Long.BYTES; j++) {
                    bytes[2 + i * Long.BYTES + j] = (byte) (explicit[i] >>> (j * 8));
                }
            }
            return new StatisticsHll(bytes);
        }

        public long estimate() {
            if (registers == null) {
                return size;
            }
            // Preserve BE's float arithmetic before bias correction, including accumulation order.
            float alpha = 0.7213f / (1 + 1.079f / REGISTERS);
            float harmonic = 0;
            int zeros = 0;
            for (byte rank : registers) {
                harmonic += INVERSE_POWERS[rank];
                if (rank == 0) {
                    zeros++;
                }
            }
            harmonic = 1.0f / harmonic;
            double estimate = alpha * REGISTERS * REGISTERS * harmonic;
            if (estimate <= REGISTERS * 2.5 && zeros != 0) {
                estimate = REGISTERS * Math.log((float) REGISTERS / zeros);
            } else if (estimate < 72000) {
                double bias = 5.9119e-18 * (estimate * estimate * estimate * estimate)
                        - 1.4253e-12 * (estimate * estimate * estimate)
                        + 1.2940e-7 * (estimate * estimate) - 5.2921e-3 * estimate + 83.3216;
                estimate -= estimate * (bias / 100);
            }
            return Math.round(estimate);
        }
    }
}
