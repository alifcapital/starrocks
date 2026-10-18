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

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <vector>

#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/priority_morsel_queue.h"
#include "gen_cpp/PlanNodes_types.h"
#include "storage/lake/tablet.h"

namespace starrocks::pipeline {
namespace {

Morsels make_morsels(int count) {
    Morsels morsels;
    for (int i = 0; i < count; ++i) {
        morsels.emplace_back(std::make_unique<ScanMorsel>(1, TScanRange()));
    }
    return morsels;
}

MorselPtr take(MorselQueue& queue) {
    auto morsel = queue.try_get();
    EXPECT_TRUE(morsel.ok());
    return std::move(morsel.value());
}

void expect_progress(const MorselProgress& progress, int64_t enqueued, int64_t remaining) {
    EXPECT_EQ(enqueued, progress.enqueued);
    EXPECT_EQ(remaining, progress.remaining);
    EXPECT_EQ(enqueued - remaining, progress.taken());
}

} // namespace

TEST(MorselProgressTest, FixedQueueCountsTheMorselsThatAreTaken) {
    FixedMorselQueue queue(make_morsels(5));
    expect_progress(queue.progress(), 5, 5);
    auto first = take(queue);
    auto second = take(queue);
    expect_progress(queue.progress(), 5, 3);
    for (int i = 0; i < 5; ++i) take(queue);
    expect_progress(queue.progress(), 5, 0);
}

// Covers: the morsels that the queue holds now are the lower bound of what is left, and a morsel that arrives later
//         adds to both what was put in the queue and what is left.
TEST(MorselProgressTest, DynamicQueueGrowsWithTheRangesThatArrive) {
    DynamicMorselQueue queue(make_morsels(3), /*has_more=*/true);
    const auto start = queue.progress();
    expect_progress(start, 3, 3);

    auto first = take(queue);
    auto second = take(queue);
    expect_progress(queue.progress(), 3, 1);
    EXPECT_EQ(start.version, queue.progress().version);

    ASSERT_TRUE(queue.append_morsels(make_morsels(4)).ok());
    const auto after_append = queue.progress();
    expect_progress(after_append, 7, 5);
    EXPECT_GT(after_append.version, start.version);

    // A morsel that goes back is not taken any more, and it is not a new morsel.
    queue.unget(std::move(second));
    expect_progress(queue.progress(), 7, 6);
    EXPECT_EQ(after_append.version, queue.progress().version);
}

// Covers: only morsels that arrive can raise the remaining morsels, so only they change the version. A queue that
//         learns that more ranges are or are not to come, or that gives a morsel away, leaves it as it is.
TEST(MorselProgressTest, OnlyArrivingMorselsChangeTheVersion) {
    DynamicMorselQueue queue(make_morsels(2), /*has_more=*/true);
    const auto start = queue.progress().version;
    queue.set_has_more_scan_ranges(true);
    queue.set_has_more_scan_ranges(false);
    queue.set_has_more_from_split(true);
    auto morsel = take(queue);
    queue.unget(std::move(morsel));
    EXPECT_EQ(start, queue.progress().version);
    ASSERT_TRUE(queue.append_morsels(make_morsels(1)).ok());
    EXPECT_GT(queue.progress().version, start);
}

TEST(MorselProgressTest, PriorityQueueAnswersLikeTheDynamicQueueWhateverTheOrder) {
    PriorityMorselQueue queue(make_morsels(3), /*has_more=*/true, /*reorder_slot_id=*/5, /*desc=*/false,
                              /*nulls_first=*/false);
    const auto start = queue.progress();
    expect_progress(start, 3, 3);
    auto first = take(queue);
    expect_progress(queue.progress(), 3, 2);
    ASSERT_TRUE(queue.append_morsels(make_morsels(2)).ok());
    expect_progress(queue.progress(), 5, 4);
    EXPECT_GT(queue.progress().version, start.version);
    queue.unget(std::move(first));
    expect_progress(queue.progress(), 5, 5);
}

// Covers: the unit of a split queue is a tablet. The tablet that is being split counts as taken, because the scan has
//         emitted rows of it, and the tablets behind it are what is left.
TEST(MorselProgressTest, SplitQueueCountsTabletsAndTheCurrentTabletIsTaken) {
    PhysicalSplitMorselQueue queue(make_morsels(3), /*degree_of_parallelism=*/1, /*splitted_scan_rows=*/1024);
    std::vector<BaseTabletSharedPtr> tablets;
    for (int64_t id = 1; id <= 3; ++id) tablets.push_back(std::make_shared<lake::Tablet>(nullptr, id));
    queue.set_tablets(tablets);

    expect_progress(queue.progress(), 3, 2);
    queue._tablet_idx = 1;
    expect_progress(queue.progress(), 3, 1);
    queue._tablet_idx = 2;
    expect_progress(queue.progress(), 3, 0);
    queue._tablet_idx = 3;
    expect_progress(queue.progress(), 3, 0);
}

TEST(MorselProgressTest, SplitQueueWithoutTabletsReportsNoMorsels) {
    PhysicalSplitMorselQueue queue(make_morsels(1), /*degree_of_parallelism=*/1, /*splitted_scan_rows=*/1024);
    expect_progress(queue.progress(), 0, 0);
}

TEST(MorselProgressTest, BucketSequenceQueueAsksTheQueueBehindIt) {
    auto nested = std::make_unique<DynamicMorselQueue>(make_morsels(4), /*has_more=*/true);
    auto* nested_raw = nested.get();
    BucketSequenceMorselQueue queue(std::move(nested));
    expect_progress(queue.progress(), 4, 4);
    const auto start = queue.progress().version;

    ASSERT_TRUE(nested_raw->append_morsels(make_morsels(2)).ok());
    expect_progress(queue.progress(), 6, 6);
    EXPECT_GT(queue.progress().version, start);
}

TEST(MorselProgressTest, SharedFactoryAsksItsQueue) {
    SharedMorselQueueFactory factory(std::make_unique<DynamicMorselQueue>(make_morsels(3), /*has_more=*/true), 2);
    expect_progress(factory.progress(), 3, 3);
    auto morsel = take(*factory.create(0));
    expect_progress(factory.progress(), 3, 2);
    ASSERT_TRUE(factory.append_morsels(0, make_morsels(5)).ok());
    expect_progress(factory.progress(), 8, 7);
}

// Covers: a scan with a queue per driver asks for the sum of the queues, and a range that arrives in any of them is a
//         change of the version.
TEST(MorselProgressTest, IndividualFactorySumsTheQueuesOfTheDrivers) {
    std::map<int, MorselQueuePtr> queues;
    queues.emplace(0, std::make_unique<DynamicMorselQueue>(make_morsels(3), /*has_more=*/true));
    queues.emplace(1, std::make_unique<DynamicMorselQueue>(make_morsels(2), /*has_more=*/true));
    IndividualMorselQueueFactory factory(std::move(queues), /*could_local_shuffle=*/false,
                                         /*enable_random_append_split_morsel=*/false);
    const auto start = factory.progress();
    expect_progress(start, 5, 5);

    auto morsel = take(*factory.create(1));
    expect_progress(factory.progress(), 5, 4);
    EXPECT_EQ(start.version, factory.progress().version);

    ASSERT_TRUE(factory.append_morsels(1, make_morsels(6)).ok());
    expect_progress(factory.progress(), 11, 10);
    EXPECT_GT(factory.progress().version, start.version);
}

} // namespace starrocks::pipeline
