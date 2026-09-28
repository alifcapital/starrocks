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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

class CompactDegreeVectorTest {
    @Test
    void multiwayProductsMatchUncompressedFrequenciesAcrossEncodingsAndWordBoundaries() {
        Random random = new Random(62429);
        for (int size : new int[] {0, 1, 63, 64, 65, 1003}) {
            for (int repeat = 0; repeat < 12; repeat++) {
                long[][] frequencies = new long[4][size];
                for (int side = 0; side < 4; side++) {
                    int encoding = (repeat + side) % 6;
                    for (int key = 0; key < size; key++) {
                        frequencies[side][key] = switch (encoding) {
                            case 0 -> random.nextInt(2);
                            case 1 -> random.nextInt(200);
                            case 2 -> random.nextInt(50000);
                            case 3 -> random.nextInt(1000000);
                            case 4 -> (long) random.nextInt(10) * Integer.MAX_VALUE;
                            default -> key % 61 == 0 ? 3 : 0;
                        };
                    }
                }
                CompactDegreeVector[] vectors = Arrays.stream(frequencies).map(CompactDegreeVector::copyOf)
                        .toArray(CompactDegreeVector[]::new);
                for (int presence = 0; presence < 16; presence++) {
                    for (int power = 1; power <= 3; power++) {
                        double expected = 0;
                        for (int key = 0; key < size; key++) {
                            double product = 1;
                            for (int side = 0; side < 4; side++) {
                                product *= (presence & (1 << side)) == 0
                                        ? frequencies[side][key] : Math.min(1, frequencies[side][key]);
                            }
                            expected += Math.pow(product, power);
                        }
                        Assertions.assertEquals(expected, CompactDegreeVector.product(vectors, presence, power),
                                Math.max(1, expected) * 1e-12);
                    }
                }
            }
        }
    }

    private byte[] encode(CompactDegreeVector vector) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        vector.write(new DataOutputStream(bytes));
        return bytes.toByteArray();
    }

    private CompactDegreeVector decode(byte[] bytes, int maxSize) throws IOException {
        return CompactDegreeVector.read(new DataInputStream(new ByteArrayInputStream(bytes)), maxSize);
    }

    @Test
    void exactCountsAndProductsForEveryRepresentation() throws Exception {
        Random random = new Random(3703);
        for (int size : new int[] {0, 1, 63, 64, 65, 127, 129, 1025}) {
            List<long[]> vectors = new ArrayList<>();
            vectors.add(new long[size]);
            for (long maximum : new long[] {1, 255, 256, 65535, 65536, Integer.MAX_VALUE, Long.MAX_VALUE}) {
                for (int density : new int[] {2, 50, 100}) {
                    long[] values = new long[size];
                    for (int i = 0; i < size; i++) {
                        if (random.nextInt(100) < density) {
                            values[i] = Math.max(1, (long) (random.nextDouble() * maximum));
                        }
                    }
                    if (size > 0) {
                        values[size - 1] = maximum;
                    }
                    vectors.add(values);
                }
            }
            List<CompactDegreeVector> packed = new ArrayList<>();
            for (long[] values : vectors) {
                CompactDegreeVector vector = decode(encode(CompactDegreeVector.copyOf(values)), size);
                long[] actual = new long[size];
                vector.addTo(actual);
                Assertions.assertArrayEquals(values, actual);
                packed.add(vector);
            }
            for (int i = 0; i < vectors.size(); i++) {
                for (int j = 0; j < vectors.size(); j++) {
                    for (int roles = 0; roles < 4; roles++) {
                        double[] moments = CompactDegreeVector.products(
                                new CompactDegreeVector[] {packed.get(i), packed.get(j)}, roles);
                        for (int power = 1; power <= 3; power++) {
                            double expected = 0;
                            for (int k = 0; k < size; k++) {
                                long a = vectors.get(i)[k];
                                long b = vectors.get(j)[k];
                                if (a != 0 && b != 0) {
                                    double product = ((roles & 1) == 0 ? (double) a : 1)
                                            * ((roles & 2) == 0 ? (double) b : 1);
                                    expected += Math.pow(product, power);
                                }
                            }
                            double actual = packed.get(i).product(packed.get(j), (roles & 1) != 0,
                                    (roles & 2) != 0, power);
                            Assertions.assertEquals(expected, actual, Math.max(1, expected) * 1e-12);
                            Assertions.assertEquals(expected, moments[power - 1], Math.max(1, expected) * 1e-12);
                        }
                    }
                }
            }
        }
    }

    @Test
    void ownsItsInputAndPreservesLargeCounts() {
        long[] source = {Long.MAX_VALUE, 1L << 54};
        CompactDegreeVector vector = CompactDegreeVector.copyOf(source);
        Arrays.fill(source, 0);
        vector.addTo(source);
        Assertions.assertArrayEquals(new long[] {Long.MAX_VALUE, 1L << 54}, source);
        Assertions.assertThrows(ArithmeticException.class, () -> vector.addTo(source));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CompactDegreeVector.copyOf(new long[] {-1}));
    }

    @Test
    void rejectsTruncationInvalidBitmapAndOversizedVectors() throws Exception {
        byte[] encoded = encode(CompactDegreeVector.copyOf(new long[] {0, 1, 0, 1, 1}));
        for (int length = 0; length < encoded.length; length++) {
            byte[] truncated = Arrays.copyOf(encoded, length);
            Assertions.assertThrows(IOException.class, () -> decode(truncated, 5));
        }
        Assertions.assertThrows(IOException.class, () -> decode(encoded, 4));
        byte[] paddingBit = encoded.clone();
        paddingBit[9] = (byte) 128;
        Assertions.assertThrows(IOException.class, () -> decode(paddingBit, 5));
        byte[] invalidKind = encoded.clone();
        invalidKind[0] = 99;
        Assertions.assertThrows(IOException.class, () -> decode(invalidKind, 5));
        byte[] count = encoded.clone();
        count[8] = 4;
        Assertions.assertThrows(IOException.class, () -> decode(count, 5));
    }

    @Test
    void sparseHeadDoesNotRetainDenseZeroMatrix() {
        long[] values = new long[25048];
        values[64] = 1;
        values[25047] = 1;
        CompactDegreeVector presence = CompactDegreeVector.copyOf(values);
        Assertions.assertTrue(presence.estimatedSize() < 3400);
        values[64] = 100_000;
        CompactDegreeVector sparse = CompactDegreeVector.copyOf(values);
        Assertions.assertTrue(sparse.estimatedSize() < 3500);
        Assertions.assertEquals(100001, sparse.product(presence, false, true, 1));
    }
}
