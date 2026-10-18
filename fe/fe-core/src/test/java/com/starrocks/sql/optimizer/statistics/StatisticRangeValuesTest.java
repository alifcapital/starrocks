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

/**
 * The share of a range that another range covers. The range of a bucket has no distinct value count (NaN), and a share
 * that is NaN becomes zero rows in the caller by the cast to a long, so no pair of ranges may give one.
 */
public class StatisticRangeValuesTest {
    private static final double NAN = Double.NaN;

    @Test
    public void testPointOfALongerRangeWithoutDistinctValuesHoldsNoneOfIt() {
        StatisticRangeValues bucket = new StatisticRangeValues(10, 20, NAN);
        for (double point : new double[] {10, 15, 20}) {
            Assertions.assertEquals(0.0, bucket.overlapPercentWith(new StatisticRangeValues(point, point, NAN)), "" + point);
            Assertions.assertEquals(0.0, bucket.overlapPercentWith(new StatisticRangeValues(point, point, 1)), "" + point);
        }
    }

    @Test
    public void testPointWithoutDistinctValuesIsCoveredByARangeThatHoldsIt() {
        StatisticRangeValues point = new StatisticRangeValues(15, 15, NAN);
        Assertions.assertEquals(1.0, point.overlapPercentWith(new StatisticRangeValues(10, 20, NAN)));
        Assertions.assertEquals(1.0, point.overlapPercentWith(new StatisticRangeValues(10, 20, 5)));
        Assertions.assertEquals(1.0, point.overlapPercentWith(new StatisticRangeValues(15, 20, NAN)));
        Assertions.assertEquals(1.0, point.overlapPercentWith(new StatisticRangeValues(10, 15, NAN)));
        Assertions.assertEquals(0.0, point.overlapPercentWith(new StatisticRangeValues(16, 20, NAN)));
        Assertions.assertEquals(0.0, point.overlapPercentWith(new StatisticRangeValues(10, 14, NAN)));
    }

    @Test
    public void testKnownDistinctValuesKeepTheirShare() {
        // One value of five in a range, and the unknown statistics that have a distinct value count of one
        Assertions.assertEquals(0.2, new StatisticRangeValues(10, 20, 5).overlapPercentWith(
                new StatisticRangeValues(15, 15, 1)));
        Assertions.assertEquals(1.0, new StatisticRangeValues(10, 20, 0).overlapPercentWith(
                new StatisticRangeValues(15, 15, 1)));
        Assertions.assertEquals(0.5, new StatisticRangeValues(10, 20, 1).overlapPercentWith(
                new StatisticRangeValues(15, 15, 1)));
    }

    @Test
    public void testNoPairOfRangesGivesANaNShare() {
        double[] ends = {0, 1, 2, 3};
        double[] distinct = {NAN, 0, 1, 7};
        for (double firstLow : ends) {
            for (double firstHigh : ends) {
                for (double firstDistinct : distinct) {
                    for (double secondLow : ends) {
                        for (double secondHigh : ends) {
                            for (double secondDistinct : distinct) {
                                if (firstLow > firstHigh || secondLow > secondHigh) {
                                    continue;
                                }
                                double share = new StatisticRangeValues(firstLow, firstHigh, firstDistinct)
                                        .overlapPercentWith(new StatisticRangeValues(secondLow, secondHigh,
                                                secondDistinct));
                                String message = firstLow + ".." + firstHigh + " (" + firstDistinct + ") with "
                                        + secondLow + ".." + secondHigh + " (" + secondDistinct + ")";
                                Assertions.assertFalse(Double.isNaN(share), message);
                                Assertions.assertTrue(share >= 0 && share <= 1, message + " " + share);
                            }
                        }
                    }
                }
            }
        }
    }
}
