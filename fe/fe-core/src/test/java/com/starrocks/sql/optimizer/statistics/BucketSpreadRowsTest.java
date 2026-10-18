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
 * The rows of a bucket on a segment of its range: the rows other than those of its upper value, in the part of the range
 * that the segment is.
 */
public class BucketSpreadRowsTest {
    // 100 rows other than those of the upper value, over the range from 10 to 20
    private static final Bucket BUCKET = new Bucket(10, 20, 105L, 5L);

    @Test
    public void testSegmentInsideTheRangeHoldsItsPart() {
        Assertions.assertEquals(10.0, BUCKET.spreadRowsOn(100, 12, 13), 1e-9);
        Assertions.assertEquals(50.0, BUCKET.spreadRowsOn(100, 10, 15), 1e-9);
        Assertions.assertEquals(100.0, BUCKET.spreadRowsOn(100, 10, 20), 1e-9);
    }

    @Test
    public void testSegmentIsCutToTheRange() {
        Assertions.assertEquals(30.0, BUCKET.spreadRowsOn(100, 7, 13), 1e-9);
        Assertions.assertEquals(20.0, BUCKET.spreadRowsOn(100, 18, 40), 1e-9);
        Assertions.assertEquals(100.0, BUCKET.spreadRowsOn(100, 0, 40), 1e-9);
    }

    @Test
    public void testSegmentOutsideTheRangeOrWithNoWidthHoldsNone() {
        Assertions.assertEquals(0.0, BUCKET.spreadRowsOn(100, 21, 22), 1e-9);
        Assertions.assertEquals(0.0, BUCKET.spreadRowsOn(100, 1, 9), 1e-9);
        Assertions.assertEquals(0.0, BUCKET.spreadRowsOn(100, 15, 15), 1e-9);
        Assertions.assertEquals(0.0, BUCKET.spreadRowsOn(0, 10, 20), 1e-9);
    }
}
