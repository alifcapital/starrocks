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

import com.starrocks.catalog.Function;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ExpressionStatisticsCacheTest {
    private static final ColumnRefOperator C = new ColumnRefOperator(1, VarcharType.VARCHAR, "c", true);
    private static final ColumnRefOperator X = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);

    @BeforeAll
    public static void beforeAll() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.createDefaultCtx();
    }

    private static ScalarOperator left(int length) {
        Function function = ExprUtils.getBuiltinFunction("left", new Type[] {VarcharType.VARCHAR, IntegerType.INT},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator("left", VarcharType.VARCHAR, List.of(C, ConstantOperator.createInt(length)), function);
    }

    private static ScanExpressionStatistics expressions() {
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(left(8));
        List<MultiColumnCombinedStats.McvEntry> head =
                List.of(new MultiColumnCombinedStats.McvEntry(List.of("10312345"), 600, List.of(600L)));
        return new ScanExpressionStatistics("t", List.of(new ScanExpressionStatistics.Entry(canonical,
                ExpressionStatisticsCanonicalizer.keyOf(canonical), X,
                new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8, 10))),
                Map.of(Set.of(X), new MultiColumnCombinedStats(10, 1000, List.of(X), head, List.of(0L))));
    }

    @Test
    public void testCanonicalFormAndKeyAreComputedOncePerObject() {
        ExpressionStatisticsCache cache = new ExpressionStatisticsCache();
        ScalarOperator expression = left(3);
        ScalarOperator canonical = cache.canonical(expression);
        Assertions.assertSame(canonical, cache.canonical(expression));
        Assertions.assertSame(cache.key(canonical), cache.key(canonical));
        Assertions.assertEquals(ExpressionStatisticsCanonicalizer.keyOf(canonical), cache.key(canonical));
        // Another object of the same expression has its own entry with an equal key.
        ScalarOperator other = cache.canonical(left(3));
        Assertions.assertNotSame(canonical, other);
        Assertions.assertEquals(cache.key(canonical), cache.key(other));
    }

    @Test
    public void testMatchAndProjectionAreComputedOncePerScan() {
        ScanExpressionStatistics expressions = expressions();
        Statistics statistics = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8,
                        1000)).build();
        ScalarOperator predicate = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, C,
                ConstantOperator.createVarchar("103%"));
        ExpressionStatisticsMatcher.Result first = ExpressionStatisticsMatcher.match(predicate, expressions, statistics);
        Assertions.assertNotNull(first);
        Assertions.assertSame(first, ExpressionStatisticsMatcher.match(predicate, expressions, statistics));
        // Another scan of the same table matches on its own.
        Assertions.assertNotSame(first, ExpressionStatisticsMatcher.match(predicate, expressions(), statistics));

        AtomicInteger computed = new AtomicInteger();
        ScalarOperator projection = left(3);
        for (int i = 0; i < 3; i++) {
            expressions.getCache().projection(projection, expressions, () -> {
                computed.incrementAndGet();
                return null;
            });
        }
        Assertions.assertEquals(1, computed.get());
    }

    @Test
    public void testEqualPredicateWithOtherFlagsMatchesOnItsOwn() {
        ScanExpressionStatistics expressions = expressions();
        Statistics statistics = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8,
                        1000)).build();
        ScalarOperator estimated = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, C,
                ConstantOperator.createVarchar("103%"));
        Assertions.assertNotNull(ExpressionStatisticsMatcher.match(estimated, expressions, statistics));
        // An equal predicate kept only to skip files is not estimated, so it writes no condition.
        ScalarOperator skipOnly = estimated.clone();
        skipOnly.setNotEvalEstimate(true);
        Assertions.assertEquals(estimated, skipOnly);
        Assertions.assertNull(ExpressionStatisticsMatcher.match(skipOnly, expressions, statistics));
        // A flag set on the same object later is read again.
        estimated.setNotEvalEstimate(true);
        Assertions.assertNull(ExpressionStatisticsMatcher.match(estimated, expressions, statistics));
    }

    @Test
    public void testEqualPredicateInOtherOrderMatchesOnItsOwn() {
        ScanExpressionStatistics expressions = expressions();
        Statistics statistics = Statistics.builder().setOutputRowCount(1000)
                .addColumnStatistic(C, new ColumnStatistic(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0, 8,
                        1000)).build();
        ScalarOperator like = new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, C,
                ConstantOperator.createVarchar("103%"));
        ScalarOperator equal = new BinaryPredicateOperator(BinaryType.EQ, left(3), ConstantOperator.createVarchar("103"));
        ScalarOperator likeSkipped = like.clone();
        likeSkipped.setNotEvalEstimate(true);
        ScalarOperator equalSkipped = equal.clone();
        equalSkipped.setNotEvalEstimate(true);
        // Equal predicates whose first conjuncts are not estimated, but these are other conditions.
        ScalarOperator first = Utils.compoundAnd(likeSkipped, equal);
        ScalarOperator second = Utils.compoundAnd(equalSkipped, like.clone());
        Assertions.assertEquals(first, second);
        ExpressionStatisticsMatcher.Result one = ExpressionStatisticsMatcher.match(first, expressions, statistics);
        ExpressionStatisticsMatcher.Result two = ExpressionStatisticsMatcher.match(second, expressions, statistics);
        ExpressionStatisticsMatcher.Result alone = ExpressionStatisticsMatcher.match(second, expressions(), statistics);
        Assertions.assertEquals(alone.getPredicate(), two.getPredicate());
        Assertions.assertNotEquals(one.getPredicate(), two.getPredicate());

        // A predicate of equal conjuncts in the same order takes the match of the first one, the same as a cold
        // match, with its own kept conjunct and the condition written for the first one.
        ScalarOperator kept = likeSkipped.clone();
        ScalarOperator third = Utils.compoundAnd(kept, equal.clone());
        ExpressionStatisticsMatcher.Result three = ExpressionStatisticsMatcher.match(third, expressions, statistics);
        Assertions.assertEquals(ExpressionStatisticsMatcher.match(third, expressions(), statistics).getPredicate(),
                three.getPredicate());
        List<ScalarOperator> written = Utils.extractConjuncts(three.getPredicate());
        Assertions.assertTrue(written.stream().anyMatch(conjunct -> conjunct == kept));
        Assertions.assertTrue(written.stream().noneMatch(conjunct -> conjunct == likeSkipped));
        Assertions.assertTrue(written.stream().anyMatch(conjunct -> Utils.extractConjuncts(one.getPredicate()).stream()
                .anyMatch(other -> other == conjunct && other != likeSkipped)));
    }

    @Test
    public void testPartsAreBoundedAndFailuresAreNotKept() {
        ExpressionStatisticsCache cache = new ExpressionStatisticsCache();
        AtomicInteger computed = new AtomicInteger();
        for (int i = 0; i < 2; i++) {
            cache.part(List.of("small"), () -> computed.incrementAndGet(), value -> 10);
        }
        Assertions.assertEquals(1, computed.get());
        computed.set(0);
        for (int i = 0; i < 2; i++) {
            cache.part(List.of("large"), () -> computed.incrementAndGet(), value -> 1_000_000);
        }
        Assertions.assertEquals(2, computed.get());
        computed.set(0);
        for (int i = 0; i < 2; i++) {
            Assertions.assertNull(cache.part(List.of("failed"), () -> {
                computed.incrementAndGet();
                return null;
            }, value -> 1));
        }
        Assertions.assertEquals(2, computed.get());
        // Parts that together pass the bound clear the cache, and the first part is computed again.
        computed.set(0);
        cache.part(List.of("first"), () -> computed.incrementAndGet(), value -> 150_000);
        cache.part(List.of("second"), () -> computed.incrementAndGet(), value -> 150_000);
        cache.part(List.of("first"), () -> computed.incrementAndGet(), value -> 150_000);
        Assertions.assertEquals(3, computed.get());
    }

    @Test
    public void testThreadHoldsTheCacheOnlyWhileTheEstimateRuns() {
        ExpressionStatisticsCache cache = new ExpressionStatisticsCache();
        Assertions.assertNull(ExpressionStatisticsCache.current());
        Assertions.assertSame(cache, cache.within(ExpressionStatisticsCache::current));
        Assertions.assertNull(ExpressionStatisticsCache.current());
        Assertions.assertThrows(IllegalStateException.class, () -> cache.within(() -> {
            throw new IllegalStateException();
        }));
        Assertions.assertNull(ExpressionStatisticsCache.current());
    }
}
