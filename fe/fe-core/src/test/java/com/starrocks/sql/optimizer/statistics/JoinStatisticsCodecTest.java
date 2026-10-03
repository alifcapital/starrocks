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

import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

class JoinStatisticsCodecTest {
    private static final int LIMIT = 1 << 20;

    static JoinStatisticsData fixture() {
        DegreeStatistics degree = new DegreeStatistics(5, 0, 2, 3,
                new double[] {5, 13, 35, 97, 275, 793, 2315, 6817, 20195, 60073});
        JoinStatisticsData.Source left = new JoinStatisticsData.Source("transactions-uuid", 123, 10,
                List.of("status", "gate"), List.of(VarcharType.VARCHAR, IntegerType.BIGINT),
                List.of(List.of("approved", "0"), Arrays.asList(null, "1")), new long[] {5, 5},
                Map.of(0, List.of(degree, degree)));
        JoinStatisticsData.Source right = new JoinStatisticsData.Source("users-uuid", 456, 5,
                List.of("country"), List.of(VarcharType.VARCHAR), List.of(List.of("Германия")), new long[] {5},
                Map.of(0, List.of(degree)));
        var slice = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {2, 3}),
                new double[0][], false);
        return new JoinStatisticsData(1, 2, List.of(left, right),
                List.of(new JoinStatisticsBasis(0, List.of(0, 1), List.of(List.of(slice, slice), List.of(slice)))));
    }

    @Test
    void pairMatrixSurvivesRoundTrip() throws Exception {
        var original = fixture();
        var basis = original.getBases().get(0);
        var pair = new JoinStatisticsBasis.Pair(0, 1, 2, 1, new double[] {13, 5, 5, 2, 13, 5, 5, 2});
        var enriched = new JoinStatisticsBasis(basis.getDomain(), basis.getSources(),
                List.of(basis.getSlices(0), basis.getSlices(1)), List.of(pair));
        var data = new JoinStatisticsData(original.getObjectId(), original.getGeneration(),
                original.getSources(), List.of(enriched));
        var decoded = JoinStatisticsCodec.decode(JoinStatisticsCodec.encode(data, 1 << 20), 1 << 20,
                data.getObjectId(), data.getGeneration());
        Assertions.assertEquals(13, decoded.getBases().get(0)
                .pairEstimate(0, 1, new int[] {0}, new int[] {0}, 0).orElseThrow());
        Assertions.assertEquals(5, decoded.getBases().get(0)
                .pairEstimate(0, 1, new int[] {0}, new int[] {0}, 2).orElseThrow());
    }

    @Test
    void roundTripPreparesTypedValuesAndKeepsExactFrequencies() throws Exception {
        JoinStatisticsData original = fixture();
        JoinStatisticsData restored = JoinStatisticsCodec.decode(JoinStatisticsCodec.encode(original, LIMIT), LIMIT, 1, 2);
        Assertions.assertEquals(13, restored.getBases().get(0).getSlices(0).get(0).getHead()
                .product(restored.getBases().get(0).getSlices(1).get(0).getHead(), false, false, 1), 1e-8);
        Assertions.assertEquals("Германия", restored.getSources().get(1).predicateValue(0, 0).getVarchar());
        Assertions.assertEquals(0, restored.getSources().get(0).predicateValue(0, 1).getBigint());
        Assertions.assertTrue(restored.getSources().get(0).predicateValue(1, 0).isNull());
        Assertions.assertEquals(2, restored.getSources().get(0).getDegrees().get(0).get(0).getDistinctCount());
        long[] values = new long[2];
        restored.getBases().get(0).getSlices(0).get(0).getHead().addTo(values);
        Assertions.assertArrayEquals(new long[] {2, 3}, values);
    }

    @Test
    void sparseSlicesAcceptedByWriterRemainReadableAtTheSameByteLimit() throws Exception {
        int count = 300;
        var tuples = new java.util.ArrayList<List<String>>();
        var slices = new java.util.ArrayList<JoinStatisticsBasis.Slice>();
        var degrees = new java.util.ArrayList<DegreeStatistics>();
        long[] rows = new long[count];
        Arrays.fill(rows, 5);
        double[][] tail = new double[JoinStatisticsBasis.momentOrders().length][JoinStatisticsBasis.WIDTH];
        for (int p = 0; p < tail.length; p++) {
            for (int layout = 0; layout < 3; layout++) {
                tail[p][layout * 256] = Math.pow(2, JoinStatisticsBasis.momentOrders()[p]);
            }
        }
        for (int i = 0; i < count; i++) {
            tuples.add(List.of("v" + i));
            degrees.add(fixture().getSources().get(0).getDegrees().get(0).get(0));
            slices.add(new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {3}), tail, false));
        }
        var sources = new java.util.ArrayList<JoinStatisticsData.Source>();
        for (int side = 0; side < 2; side++) {
            sources.add(new JoinStatisticsData.Source("s" + side, 1, count * 5L, List.of("p"),
                    List.of(VarcharType.VARCHAR), tuples, rows, Map.of(0, degrees)));
        }
        var data = new JoinStatisticsData(1, 2, sources,
                List.of(new JoinStatisticsBasis(0, List.of(0, 1), List.of(slices, slices))));
        int limit = 29_620_000;
        var restored = JoinStatisticsCodec.decode(JoinStatisticsCodec.encode(data, limit), limit, 1, 2);
        Assertions.assertEquals(data.estimatedSize(), restored.estimatedSize());
        Assertions.assertEquals(count, restored.getBases().get(0).getSlices(0).size());
    }

    @Test
    void rejectsWrongGenerationCorruptionTruncationAndOversizedDecode() throws Exception {
        byte[] bytes = JoinStatisticsCodec.encode(fixture(), LIMIT);
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsCodec.decode(bytes, LIMIT, 1, 3));
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsCodec.decode(bytes, LIMIT, 3, 2));
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsCodec.decode(bytes, 100, 1, 2));
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsCodec.encode(fixture(), 100));
        for (int length : new int[] {0, 3, 7, 8, bytes.length / 2, bytes.length - 1}) {
            byte[] truncated = Arrays.copyOf(bytes, length);
            Assertions.assertThrows(IOException.class, () -> JoinStatisticsCodec.decode(truncated, LIMIT, 1, 2));
        }
        byte[] version = bytes.clone();
        version[7] = 99;
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsCodec.decode(version, LIMIT, 1, 2));
        byte[] corrupt = bytes.clone();
        corrupt[corrupt.length - 1] ^= 1;
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsCodec.decode(corrupt, LIMIT, 1, 2));
        byte[] extra = Arrays.copyOf(bytes, bytes.length + 1);
        extra[extra.length - 1] = 1;
        Assertions.assertThrows(IOException.class, () -> JoinStatisticsCodec.decode(extra, LIMIT, 1, 2));
    }

    @Test
    void sourcesOwnTheirArraysAndCannotMixSliceDictionaries() {
        JoinStatisticsData data = fixture();
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> data.getSources().get(0).getTuples().get(0).set(0, "changed"));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> data.getSources().get(0).getDegrees().clear());
        JoinStatisticsBasis original = data.getBases().get(0);
        Assertions.assertThrows(IllegalArgumentException.class, () -> new JoinStatisticsData(1, 2, data.getSources(),
                List.of(new JoinStatisticsBasis(0, List.of(1, 0),
                        List.of(original.getSlices(0), original.getSlices(1))))));
    }

    @Test
    void sharedTailBasisRoundTripsAllOrdersWithoutRoleSpecificCopies() throws Exception {
        var old = fixture();
        double[][] moments = new double[JoinStatisticsBasis.momentOrders().length][JoinStatisticsBasis.WIDTH];
        int[] orders = JoinStatisticsBasis.momentOrders();
        for (int p = 0; p < orders.length; p++) {
            for (int layout = 0; layout < JoinStatisticsBasis.TAIL_LAYOUTS; layout++) {
                moments[p][layout * JoinStatisticsBasis.TAIL_BUCKETS] = orders[p] == 0 ? 2 : Math.pow(2, orders[p]) + 1;
            }
        }
        var slice = new JoinStatisticsBasis.Slice(CompactDegreeVector.copyOf(new long[] {2, 3}), moments, false);
        var data = new JoinStatisticsData(1, 2, old.getSources(), List.of(new JoinStatisticsBasis(0, List.of(0, 1),
                List.of(List.of(slice, slice), List.of(slice)))));
        var restored = JoinStatisticsCodec.decode(JoinStatisticsCodec.encode(data, LIMIT), LIMIT, 1, 2);
        Assertions.assertEquals(1, restored.getBases().size());
        var actual = restored.getBases().get(0).getSlices(0).get(0);
        for (int arity = 2; arity <= 4; arity++) {
            for (int power = 1; power <= 3; power++) {
                for (boolean presence : new boolean[] {false, true}) {
                    Assertions.assertEquals(slice.project(arity, power, presence).getTailNorm(),
                            actual.project(arity, power, presence).getTailNorm());
                    Assertions.assertEquals(slice.project(arity, power, presence).getBucket(1, 0),
                            actual.project(arity, power, presence).getBucket(1, 0));
                }
            }
        }
        moments[0][0] = 999;
        Assertions.assertEquals(Math.sqrt(2), slice.project(2, 1, true).getTailNorm(), 1e-10);
    }
}
