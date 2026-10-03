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

#include "exec/pipeline/hashjoin/local_runtime_filter_feedback.h"

#include <gtest/gtest.h>

namespace starrocks::pipeline {
namespace {
void chunks(LocalRuntimeFilterFeedback& feedback, int count, int64_t passed, int64_t filter_ns, int64_t lookup_ns) {
    for (int i = 0; i < count; ++i) {
        feedback.observe_filter(4096, passed, filter_ns);
        feedback.observe_lookup(passed, lookup_ns);
    }
}
} // namespace

TEST(LocalRuntimeFilterFeedbackTest, WaitsForEveryBufferedRow) {
    LocalRuntimeFilterFeedback feedback;
    for (int i = 0; i < 64; ++i) feedback.observe_filter(4096, 1024, 100000);
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_TRUE(feedback.needs_drain());
    EXPECT_EQ(65536, feedback.outstanding_rows());
    feedback.observe_lookup(65535, 640000);
    EXPECT_TRUE(feedback.use_filter());
    feedback.observe_lookup(1, 10);
    EXPECT_FALSE(feedback.use_filter());
    EXPECT_EQ(0, feedback.outstanding_rows());
}

TEST(LocalRuntimeFilterFeedbackTest, DrainBoundaryStopsContinuousPrefetchBeforeSwitching) {
    LocalRuntimeFilterFeedback feedback;
    feedback.observe_filter(4096, 1024, 100000);
    int accepted_chunks = 1;
    // Model one prefetched chunk while JOIN finishes the previous input.
    while (!feedback.needs_drain()) {
        feedback.observe_filter(4096, 1024, 100000);
        ++accepted_chunks;
        feedback.observe_lookup(1024, 10000);
        ASSERT_LT(accepted_chunks, 100);
    }
    ASSERT_EQ(64, accepted_chunks);
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_EQ(1024, feedback.outstanding_rows());
    // The driver stops pulling the source at the boundary and drains its
    // existing operators. New input resumes only under the next RF mode.
    feedback.observe_lookup(1024, 10000);
    EXPECT_FALSE(feedback.needs_drain());
    EXPECT_FALSE(feedback.use_filter());
    EXPECT_EQ(0, feedback.outstanding_rows());
    chunks(feedback, 64, 4096, 0, 20000);
    EXPECT_EQ(1, feedback.totals().decisions);
}

TEST(LocalRuntimeFilterFeedbackTest, ExpensiveFilterStaysOff) {
    LocalRuntimeFilterFeedback feedback;
    chunks(feedback, 64, 1024, 100000, 10000);
    ASSERT_FALSE(feedback.use_filter());
    chunks(feedback, 64, 4096, 0, 20000);
    EXPECT_FALSE(feedback.use_filter());
    EXPECT_EQ(1, feedback.totals().decisions);
    EXPECT_EQ(262144, feedback.totals().input_rows[0]);
    EXPECT_EQ(262144, feedback.totals().input_rows[1]);
}

TEST(LocalRuntimeFilterFeedbackTest, ExpensiveMissesRestoreFilter) {
    LocalRuntimeFilterFeedback feedback;
    chunks(feedback, 64, 1024, 100000, 10000);
    ASSERT_FALSE(feedback.use_filter());
    chunks(feedback, 64, 4096, 0, 500000);
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_EQ(2, feedback.totals().switches);
}

TEST(LocalRuntimeFilterFeedbackTest, HysteresisKeepsInitialFilter) {
    LocalRuntimeFilterFeedback feedback;
    chunks(feedback, 64, 4096, 60000, 40000);
    ASSERT_FALSE(feedback.use_filter());
    chunks(feedback, 64, 4096, 0, 95000);
    EXPECT_TRUE(feedback.use_filter());
}

TEST(LocalRuntimeFilterFeedbackTest, UsefulFilterDoesNotRequireInitialOffWindow) {
    LocalRuntimeFilterFeedback feedback;
    chunks(feedback, 64, 40, 1000, 1000);
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_EQ(1, feedback.totals().decisions);
    EXPECT_EQ(0, feedback.totals().switches);
}

TEST(LocalRuntimeFilterFeedbackTest, EmptyOutputKeepsFilter) {
    LocalRuntimeFilterFeedback feedback;
    for (int i = 0; i < 64; ++i) feedback.observe_filter(4096, 0, 1000);
    EXPECT_EQ(0, feedback.outstanding_rows());
    EXPECT_EQ(1, feedback.totals().decisions);
    EXPECT_TRUE(feedback.use_filter());
}

TEST(LocalRuntimeFilterFeedbackTest, IncludesWorkBetweenFilterAndJoin) {
    LocalRuntimeFilterFeedback feedback;
    for (int i = 0; i < 64; ++i) {
        feedback.observe_filter(4096, 1024, 100000);
        feedback.observe_lookup(1024, 10000, 1000000);
    }
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_EQ(1, feedback.totals().decisions);
    EXPECT_EQ(64000000, feedback.totals().intermediate_ns[1]);
    EXPECT_EQ(640000, feedback.totals().lookup_ns[1]);
}

TEST(LocalRuntimeFilterFeedbackTest, RechecksBackOffAndStayBounded) {
    LocalRuntimeFilterFeedback feedback;
    chunks(feedback, 64, 40, 1000, 1000);
    for (int expected : {1024, 4096, 16384, 16384}) {
        chunks(feedback, feedback.recheck_interval(), 40, 1000, 1000);
        chunks(feedback, 64, 40, 1000, 1000);
        ASSERT_FALSE(feedback.use_filter());
        chunks(feedback, 64, 4096, 0, 100000);
        EXPECT_TRUE(feedback.use_filter());
        EXPECT_EQ(expected, feedback.recheck_interval());
    }
}

TEST(LocalRuntimeFilterFeedbackTest, ChangedDistributionResetsBackoff) {
    LocalRuntimeFilterFeedback feedback;
    chunks(feedback, 64, 40, 1000, 1000);
    chunks(feedback, 256, 40, 1000, 1000);
    chunks(feedback, 64, 4096, 100000, 1000);
    chunks(feedback, 64, 4096, 0, 1000);
    EXPECT_FALSE(feedback.use_filter());
    EXPECT_EQ(256, feedback.recheck_interval());
}

TEST(LocalRuntimeFilterFeedbackTest, BrokenAccountingRestoresNativeFilter) {
    LocalRuntimeFilterFeedback feedback;
    chunks(feedback, 64, 1024, 100000, 10000);
    ASSERT_FALSE(feedback.use_filter());
    feedback.observe_lookup(1, 0);
    EXPECT_FALSE(feedback.enabled());
    EXPECT_TRUE(feedback.use_filter());
}

TEST(LocalRuntimeFilterFeedbackTest, SmallChunksNeedEnoughRows) {
    LocalRuntimeFilterFeedback feedback;
    for (int i = 0; i < 100; ++i) {
        feedback.observe_filter(10, 10, 100);
        feedback.observe_lookup(10, 1);
    }
    EXPECT_FALSE(feedback.needs_drain());
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_EQ(0, feedback.totals().decisions);
}

} // namespace starrocks::pipeline
