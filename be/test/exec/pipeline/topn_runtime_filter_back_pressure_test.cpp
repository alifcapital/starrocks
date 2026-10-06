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

#include "exec/pipeline/topn_runtime_filter_back_pressure.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

namespace starrocks::pipeline {

// Constructor arguments: selectivity_lower_bound, throttle_time_upper_bound (ms), max_rounds,
// throttle_time (ms), num_rows.

// Few rows pass the scan, so the row-driven rounds never start. The time bound alone must end the
// wait, also when only is_pass_through() is called.
TEST(TopnRfBackPressureTest, few_rows_released_by_time_bound) {
    TopnRfBackPressure bp(0.1, 100, 8, 8, 1024);
    bp.start_wait();
    bp.inc_num_rows(100);
    EXPECT_FALSE(bp.should_throttle());
    EXPECT_FALSE(bp.is_pass_through());

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    EXPECT_TRUE(bp.is_pass_through());
    bp.inc_num_rows(100);
    EXPECT_FALSE(bp.should_throttle());
    EXPECT_TRUE(bp.is_pass_through());
}

// The time bound counts from start_wait(), not from construction.
TEST(TopnRfBackPressureTest, time_bound_starts_with_start_wait) {
    TopnRfBackPressure bp(0.1, 0, 8, 8, 1024);
    EXPECT_FALSE(bp.is_pass_through());
    bp.start_wait();
    EXPECT_TRUE(bp.is_pass_through());
}

// A later start_wait() keeps the first start time.
TEST(TopnRfBackPressureTest, start_wait_keeps_first_start) {
    TopnRfBackPressure bp(0.1, 200, 8, 8, 1024);
    bp.start_wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    bp.start_wait();
    EXPECT_FALSE(bp.is_pass_through());
    std::this_thread::sleep_for(std::chrono::milliseconds(160));
    EXPECT_TRUE(bp.is_pass_through());
}

// A throttle window longer than the time bound ends when the bound passes.
TEST(TopnRfBackPressureTest, time_bound_ends_long_throttle_window) {
    TopnRfBackPressure bp(0.1, 100, 8, 10000, 16);
    bp.start_wait();
    bp.inc_num_rows(100);
    EXPECT_TRUE(bp.should_throttle());
    EXPECT_GT(bp.current_throttle_deadline(), 0);

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    EXPECT_FALSE(bp.should_throttle());
    EXPECT_TRUE(bp.is_pass_through());
    EXPECT_EQ(bp.current_throttle_deadline(), -1);
}

// The filter arrives long before the time bound: back pressure releases at once.
TEST(TopnRfBackPressureTest, rf_arrival_releases_before_time_bound) {
    TopnRfBackPressure bp(0.1, 100000, 8, 10000, 16);
    bp.start_wait();
    bp.inc_num_rows(100);
    EXPECT_TRUE(bp.should_throttle());
    EXPECT_FALSE(bp.is_pass_through());

    bp.notify_rf_arrived();
    EXPECT_FALSE(bp.should_throttle());
    EXPECT_TRUE(bp.is_pass_through());
}

// Many rows pass the scan and the time bound is far: rounds run as before and end when the round
// budget is exhausted.
TEST(TopnRfBackPressureTest, many_rows_rounds_unchanged) {
    TopnRfBackPressure bp(0.1, 100000, 2, 1, 16);
    bp.start_wait();

    // Round 1: more than 16 rows start a 1 ms window.
    bp.inc_num_rows(100);
    EXPECT_TRUE(bp.should_throttle());
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    EXPECT_FALSE(bp.should_throttle());
    EXPECT_FALSE(bp.is_pass_through());

    // Round 2: the row limit doubles to 32 and the window to 2 ms.
    bp.inc_num_rows(20);
    EXPECT_FALSE(bp.should_throttle());
    bp.inc_num_rows(20);
    EXPECT_TRUE(bp.should_throttle());
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    EXPECT_FALSE(bp.should_throttle());
    EXPECT_FALSE(bp.is_pass_through());

    // The round budget is exhausted.
    bp.inc_num_rows(100);
    EXPECT_FALSE(bp.should_throttle());
    EXPECT_TRUE(bp.is_pass_through());
}

} // namespace starrocks::pipeline
