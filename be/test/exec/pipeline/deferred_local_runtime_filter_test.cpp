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

#include "exec/pipeline/hashjoin/deferred_local_runtime_filter.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <functional>
#include <thread>

#include "column/chunk.h"
#include "common/object_pool.h"
#include "exec/pipeline/hashjoin/hash_joiner_fwd.h"
#include "exec/pipeline/operator.h"
#include "exec/pipeline/scan/morsel.h"
#include "exec/pipeline/scan/scan_range_ledger.h"
#include "exprs/column_ref.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"
#include "util/runtime_profile.h"

namespace starrocks::pipeline {
namespace {

constexpr int32_t kFilterId = 7;

// The key column of a hash table: its first row is the row that the hash table reserves in front of the keys.
ColumnPtr key_column(const std::vector<int64_t>& keys) {
    std::vector<int64_t> values{0};
    values.insert(values.end(), keys.begin(), keys.end());
    return ColumnTestHelper::build_column<int64_t>(values);
}

std::vector<int64_t> range(int64_t begin, int64_t end) {
    std::vector<int64_t> values;
    for (int64_t value = begin; value < end; ++value) values.push_back(value);
    return values;
}

// The rows of values that the filter keeps.
std::vector<uint8_t> probe(const RuntimeFilter* filter, const std::vector<int64_t>& values) {
    ColumnPtr column = ColumnTestHelper::build_column<int64_t>(values);
    RuntimeFilter::RunningContext context;
    context.use_merged_selection = false;
    context.compatibility = false;
    context.selection.assign(values.size(), 1);
    filter->evaluate(column.get(), &context);
    return std::vector<uint8_t>(context.selection.begin(), context.selection.end());
}

// The section where a profile holds the numbers of the filter.
RuntimeProfile* section_of(RuntimeProfile& profile) {
    return profile.get_child(deferred_rf_section_name(kFilterId));
}

// A scan whose rows cost a lot to read, and whose JOIN finds a pair for one row of sixteen.
DeferredRfWindow expensive_window() {
    DeferredRfWindow window;
    window.chunks = 64;
    window.rows = 262144;
    window.lookup_ns = window.rows * 50;
    window.scan_ns = window.rows * 20;
    window.probe_output_rows = window.rows / 16;
    window.morsels_taken = 10;
    window.morsels_remaining = 90;
    return window;
}

// A scan whose rows cost nothing but the lookup, and whose JOIN finds a pair for every other row.
DeferredRfWindow cheap_window() {
    DeferredRfWindow window;
    window.chunks = 64;
    window.rows = 262144;
    window.lookup_ns = window.rows * 5;
    window.probe_output_rows = window.rows / 2;
    window.morsels_taken = 10;
    window.morsels_remaining = 90;
    return window;
}

// A scan whose JOIN finds a pair for one row of sixteen and spends 50 ns on every row, with the scan ranges as given.
DeferredRfWindow lookup_window(int64_t taken, int64_t remaining) {
    DeferredRfWindow window;
    window.chunks = 64;
    window.rows = 262144;
    window.lookup_ns = window.rows * 50;
    window.probe_output_rows = window.rows / 16;
    window.morsels_taken = taken;
    window.morsels_remaining = remaining;
    return window;
}

// A scan that reads a few large files: it has taken all of them at the start, and the queue has none left. Every file
// has 50 million rows, and the readers have read 100000 rows of each.
DeferredRfWindow large_files_window(int files) {
    DeferredRfWindow window = lookup_window(files, 0);
    auto ledger = std::make_shared<ScanRangeLedger>();
    std::vector<std::unique_ptr<ScanRangeLedger::Range>> ranges;
    for (int i = 0; i < files; ++i) {
        ranges.push_back(std::make_unique<ScanRangeLedger::Range>(ledger, 1L << 30));
        ranges.back()->update(100000, 50000000);
    }
    window.remaining_raw = estimate_remaining_raw_rows(ledger->snapshot(), 0);
    return window;
}

// The build side of the lookup windows: so many keys that putting them in the filter costs as much as the rows of a
// few scan ranges.
constexpr size_t kManyKeys = 100000;

DeferredRfInputs expensive_inputs() {
    DeferredRfInputs inputs;
    inputs.ndv = 10000;
    inputs.build_rows = 10000;
    inputs.key_ns = 10;
    inputs.reader_lazy_ns_per_row = 2000;
    inputs.scan_rows_seen = 262144;
    return inputs;
}

} // namespace

DeferredRfEligibilityInput eligible_input() {
    DeferredRfEligibilityInput input;
    input.join_type = TJoinOp::INNER_JOIN;
    input.broadcast_join = true;
    input.broadcast_filter = true;
    input.key_type_has_bloom_filter = true;
    input.ndv = 1 << 20;
    return input;
}

// Covers: the joins that drop the probe rows without a pair and have a broadcast filter keep the filter back, with
//         conditions on the pairs or without them.
TEST(DeferredRfEligibilityTest, InnerAndLeftSemiJoinsKeepTheFilterBack) {
    for (const auto join_type : {TJoinOp::INNER_JOIN, TJoinOp::LEFT_SEMI_JOIN}) {
        for (const bool conjuncts : {false, true}) {
            auto input = eligible_input();
            input.join_type = join_type;
            input.has_other_join_conjuncts = conjuncts;
            EXPECT_TRUE(decide_deferred_rf_eligibility(input).defer) << join_type << " " << conjuncts;
        }
    }
}

// Covers: these joins keep the probe rows without a pair, so the planner makes no filter for them.
TEST(DeferredRfEligibilityTest, JoinsThatKeepProbeRowsWithoutAPairDoNotKeepAFilterBack) {
    for (const auto join_type : {TJoinOp::LEFT_OUTER_JOIN, TJoinOp::FULL_OUTER_JOIN, TJoinOp::LEFT_ANTI_JOIN,
                                 TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN, TJoinOp::CROSS_JOIN, TJoinOp::ASOF_INNER_JOIN}) {
        auto input = eligible_input();
        input.join_type = join_type;
        const auto eligibility = decide_deferred_rf_eligibility(input);
        EXPECT_FALSE(eligibility.defer) << join_type;
        EXPECT_STREQ("join type is not supported", eligibility.reason) << join_type;
    }
}

// Covers: RIGHT joins drop the probe rows without a pair, but the planner only shuffles them, so their filter is
//         partitioned and is not kept back.
TEST(DeferredRfEligibilityTest, RightJoinsHavePartitionedFiltersAndDoNotKeepAFilterBack) {
    for (const auto join_type : {TJoinOp::RIGHT_OUTER_JOIN, TJoinOp::RIGHT_SEMI_JOIN, TJoinOp::RIGHT_ANTI_JOIN}) {
        auto input = eligible_input();
        input.join_type = join_type;
        const auto eligibility = decide_deferred_rf_eligibility(input);
        EXPECT_FALSE(eligibility.defer) << join_type;
        EXPECT_STREQ("right join has a partitioned filter", eligibility.reason) << join_type;
    }
}

TEST(DeferredRfEligibilityTest, SkewJoinPartitionedBuildAndColocateDoNotKeepAFilterBack) {
    auto skew = eligible_input();
    skew.skew_join = true;
    EXPECT_STREQ("skew join", decide_deferred_rf_eligibility(skew).reason);

    auto shuffled = eligible_input();
    shuffled.broadcast_join = false;
    EXPECT_STREQ("join is not a broadcast join", decide_deferred_rf_eligibility(shuffled).reason);

    auto partitioned = eligible_input();
    partitioned.partitioned_filter = true;
    EXPECT_STREQ("filter is partitioned", decide_deferred_rf_eligibility(partitioned).reason);

    auto not_broadcast_filter = eligible_input();
    not_broadcast_filter.broadcast_filter = false;
    EXPECT_STREQ("filter is partitioned", decide_deferred_rf_eligibility(not_broadcast_filter).reason);
    EXPECT_FALSE(decide_deferred_rf_eligibility(not_broadcast_filter).defer);
}

TEST(DeferredRfEligibilityTest, KeyTypeWithoutABloomFilterDoesNotKeepAFilterBack) {
    auto input = eligible_input();
    input.key_type_has_bloom_filter = false;
    EXPECT_STREQ("key type has no Bloom filter", decide_deferred_rf_eligibility(input).reason);
}

// Covers: a filter for more distinct keys than a global runtime filter builds a Bloom filter for is kept back without
//         a Bloom filter, as a global runtime filter keeps its min/max range only above the limit.
TEST(DeferredRfEligibilityTest, FilterWithMoreKeysThanTheLimitOfTheGlobalFilterIsKeptBackWithoutABloomFilter) {
    auto input = eligible_input();
    input.ndv = 1000;
    input.max_ndv = 1000;
    auto eligibility = decide_deferred_rf_eligibility(input);
    EXPECT_TRUE(eligibility.defer);
    EXPECT_TRUE(eligibility.bloom);
    input.ndv = 1001;
    eligibility = decide_deferred_rf_eligibility(input);
    EXPECT_TRUE(eligibility.defer);
    EXPECT_FALSE(eligibility.bloom);

    // No limit in the query options means no limit.
    input.max_ndv = UINT64_MAX;
    input.ndv = size_t(1) << 40;
    eligibility = decide_deferred_rf_eligibility(input);
    EXPECT_TRUE(eligibility.defer);
    EXPECT_TRUE(eligibility.bloom);
}

// Covers: the limit changes nothing for a filter that is not kept back for another reason.
TEST(DeferredRfEligibilityTest, FilterThatIsNotKeptBackStaysSoWhateverTheKeys) {
    auto input = eligible_input();
    input.join_type = TJoinOp::LEFT_OUTER_JOIN;
    input.ndv = 1001;
    input.max_ndv = 1000;
    EXPECT_FALSE(decide_deferred_rf_eligibility(input).defer);
}

TEST(DeferredRfDecisionTest, BuildsWhenDroppedRowsAreExpensive) {
    const auto decision = decide_deferred_runtime_filter(expensive_window(), expensive_inputs(), 2.0);
    EXPECT_TRUE(decision.build);
    EXPECT_DOUBLE_EQ(0.0625, decision.match_ratio);
    // The scan 20, the lookup 50 and the lazy columns 2000.
    EXPECT_DOUBLE_EQ(2070, decision.saved_ns_per_row);
    // 262144 rows were seen in 10 ranges, and 90 ranges are left.
    EXPECT_EQ(2359296, decision.remaining_rows);
    EXPECT_GT(decision.benefit_ns, 2.0 * decision.cost_ns);
}

TEST(DeferredRfDecisionTest, DoesNotBuildWhenMostRowsHaveAPair) {
    auto window = expensive_window();
    window.probe_output_rows = window.rows;
    const auto decision = decide_deferred_runtime_filter(window, expensive_inputs(), 2.0);
    EXPECT_FALSE(decision.build);
    EXPECT_DOUBLE_EQ(1.0, decision.match_ratio);
    EXPECT_DOUBLE_EQ(0, decision.benefit_ns);
}

TEST(DeferredRfDecisionTest, DoesNotBuildWhenTheSavingDoesNotCoverTheCostWithMargin) {
    auto inputs = expensive_inputs();
    inputs.reader_lazy_ns_per_row = 0;
    const auto decision = decide_deferred_runtime_filter(cheap_window(), inputs, 2.0);
    EXPECT_FALSE(decision.build);
    EXPECT_LT(decision.benefit_ns, 2.0 * decision.cost_ns);
    EXPECT_STREQ("benefit below cost", decision.reason);

    // The same numbers build when the margin allows a smaller gain.
    auto expensive_lookup = cheap_window();
    expensive_lookup.lookup_ns = expensive_lookup.rows * 100;
    EXPECT_TRUE(decide_deferred_runtime_filter(expensive_lookup, inputs, 2.0).build);
    EXPECT_FALSE(decide_deferred_runtime_filter(expensive_lookup, inputs, 1000.0).build);
}

TEST(DeferredRfDecisionTest, InnerJoinOutputRowsAreDividedByTheDuplicatesOfTheKeys) {
    auto window = expensive_window();
    // Every build key is in the hash table four times, and the lookups of the window returned one row for every
    // second probe row, so one probe row of eight found a pair.
    window.probe_output_rows = window.rows / 2;
    auto inputs = expensive_inputs();
    inputs.build_rows = inputs.ndv * 4;
    EXPECT_DOUBLE_EQ(0.125, decide_deferred_runtime_filter(window, inputs, 2.0).match_ratio);

    // A LEFT SEMI JOIN returns a probe row once, whatever the duplicates.
    inputs.semi_join = true;
    EXPECT_DOUBLE_EQ(0.5, decide_deferred_runtime_filter(window, inputs, 2.0).match_ratio);
}

TEST(DeferredRfDecisionTest, WaitsWhenNoScanRangeIsStarted) {
    for (const int64_t taken : {0, -1}) {
        auto window = expensive_window();
        window.morsels_taken = taken;
        const auto decision = decide_deferred_runtime_filter(window, expensive_inputs(), 2.0);
        EXPECT_FALSE(decision.build);
        EXPECT_STREQ("no scan range started", decision.reason);
    }
}

TEST(DeferredRfDecisionTest, NothingLeftToScanDoesNotBuild) {
    auto window = expensive_window();
    window.morsels_remaining = 0;
    const auto decision = decide_deferred_runtime_filter(window, expensive_inputs(), 2.0);
    EXPECT_FALSE(decision.build);
    EXPECT_STREQ("nothing left to scan", decision.reason);
}

// Covers: the remaining rows come from the ranges that the scan queues hold now, so more ranges give a bigger
//         benefit and the same measurement builds the filter.
TEST(DeferredRfDecisionTest, MoreScanRangesMakeTheSameMeasurementPayOff) {
    auto inputs = expensive_inputs();
    inputs.ndv = kManyKeys;
    inputs.build_rows = kManyKeys;
    inputs.reader_lazy_ns_per_row = 0;
    const auto few = decide_deferred_runtime_filter(lookup_window(8, 2), inputs, 2.0);
    EXPECT_FALSE(few.build);
    EXPECT_STREQ("benefit below cost", few.reason);
    EXPECT_EQ(65536, few.remaining_rows);
    // The numbers that explain the decision are in the result.
    EXPECT_EQ(262144, few.scan_rows_seen);
    EXPECT_EQ(8, few.morsels_taken);
    EXPECT_EQ(2, few.morsels_remaining);
    EXPECT_LT(few.benefit_ns, 2.0 * few.cost_ns);

    const auto many = decide_deferred_runtime_filter(lookup_window(8, 92), inputs, 2.0);
    EXPECT_TRUE(many.build);
    EXPECT_GT(many.remaining_rows, few.remaining_rows);
    EXPECT_GT(many.benefit_ns, 2.0 * many.cost_ns);
}

// Covers: a scan that reads a few large files takes all of them at the start, so the queue has none left and the
//         ranges alone say that nothing is left to scan. The rows of the files that the readers have not read are left,
//         and the estimate counts them.
TEST(DeferredRfDecisionTest, RowsOfTakenRangesThatAreNotReadYetAreLeftToScan) {
    for (int files = 1; files <= 4; ++files) {
        auto inputs = expensive_inputs();
        inputs.scan_rows_seen = 262144;
        inputs.reader_lazy_ns_per_row = 0;

        auto ranges_only = large_files_window(files);
        ranges_only.remaining_raw = RemainingRawRows();
        const auto without = decide_deferred_runtime_filter(ranges_only, inputs, 2.0);
        EXPECT_FALSE(without.build) << files;
        EXPECT_STREQ("nothing left to scan", without.reason) << files;
        EXPECT_EQ(DeferredRfDecision::RemainingSource::RANGES, without.remaining_source);

        const auto window = large_files_window(files);
        const auto decision = decide_deferred_runtime_filter(window, inputs, 2.0);
        EXPECT_TRUE(decision.build) << files;
        EXPECT_EQ(DeferredRfDecision::RemainingSource::ROW_COUNTS, decision.remaining_source);
        // Every file has 49.9 million rows that nobody read. The scan has read 100000 rows of each file and the
        // drivers have seen 262144 rows, so the share of the rows that the scan emits is 262144 / (files * 100000),
        // capped at 1.
        const double raw_left = files * 49900000.0;
        const double share = std::min(1.0, 262144.0 / (files * 100000.0));
        EXPECT_EQ(static_cast<int64_t>(raw_left * share), decision.remaining_rows) << files;
        EXPECT_EQ(static_cast<int64_t>(raw_left), decision.remaining_raw.open_ranges);
        EXPECT_EQ(0, decision.remaining_raw.pending_ranges);
        EXPECT_EQ(0, decision.remaining_raw.not_started_ranges);
    }
}

// Covers: the rows that the scan read and the drivers have not seen yet are in the buffers of the scan. They lower the
//         share of the rows that the scan emits, and the estimate stays a lower bound.
TEST(DeferredRfDecisionTest, RowsInTheBuffersOfTheScanLowerTheShareOfEmittedRows) {
    auto inputs = expensive_inputs();
    auto window = large_files_window(2);
    inputs.scan_rows_seen = 100000;
    const auto fewer = decide_deferred_runtime_filter(window, inputs, 2.0);
    inputs.scan_rows_seen = 200000;
    const auto all = decide_deferred_runtime_filter(window, inputs, 2.0);
    EXPECT_DOUBLE_EQ(0.5, fewer.emitted_per_raw_row);
    EXPECT_DOUBLE_EQ(1.0, all.emitted_per_raw_row);
    EXPECT_LT(fewer.remaining_rows, all.remaining_rows);
    // More rows seen than read cannot raise the estimate above the raw rows that are left.
    inputs.scan_rows_seen = 500000;
    EXPECT_EQ(all.remaining_rows, decide_deferred_runtime_filter(window, inputs, 2.0).remaining_rows);
}

// Covers: a scan that has no reader with a row count falls back to the ranges, as a scan that has none does.
TEST(DeferredRfDecisionTest, WithoutRowCountsTheRangesGiveTheRemainingRows) {
    auto window = expensive_window();
    EXPECT_FALSE(window.remaining_raw.known);
    const auto decision = decide_deferred_runtime_filter(window, expensive_inputs(), 2.0);
    EXPECT_EQ(DeferredRfDecision::RemainingSource::RANGES, decision.remaining_source);
    EXPECT_EQ(2359296, decision.remaining_rows);
}

TEST(DeferredRfDecisionTest, DoesNotBuildForAnEmptyBuildSideOrAnEmptyWindow) {
    auto inputs = expensive_inputs();
    inputs.ndv = 0;
    EXPECT_STREQ("empty build side", decide_deferred_runtime_filter(expensive_window(), inputs, 2.0).reason);
    auto window = expensive_window();
    window.rows = 0;
    EXPECT_STREQ("no rows measured", decide_deferred_runtime_filter(window, expensive_inputs(), 2.0).reason);
}

TEST(DeferredRfDecisionTest, KeyCostGrowsWithTheLengthOfAString) {
    EXPECT_DOUBLE_EQ(estimate_deferred_rf_key_ns(TYPE_BIGINT, 8), estimate_deferred_rf_key_ns(TYPE_BIGINT, 100));
    EXPECT_GT(estimate_deferred_rf_key_ns(TYPE_VARCHAR, 36), estimate_deferred_rf_key_ns(TYPE_BIGINT, 8));
    EXPECT_GT(estimate_deferred_rf_key_ns(TYPE_VARCHAR, 72), estimate_deferred_rf_key_ns(TYPE_VARCHAR, 36));
}

class DeferredLocalRuntimeFilterTest : public ::testing::Test {
protected:
    // The filter for the keys, with the scan descriptor that it publishes to.
    std::shared_ptr<DeferredLocalRuntimeFilter> make_filter(Columns columns, size_t ndv, size_t build_rows) {
        DeferredLocalRuntimeFilter::Params params;
        params.filter_id = kFilterId;
        params.build_type = TYPE_BIGINT;
        params.join_mode = TRuntimeFilterBuildJoinMode::BROADCAST;
        params.ndv = ndv;
        params.build_rows = build_rows;
        params.key_ns = 10;
        params.benefit_margin = 2.0;
        params.skip_bloom_reason = _skip_bloom_reason;
        params.columns = std::move(columns);
        params.pool = &_pool;
        params.scan_sinks = {_descriptor.enable_scan_sink()};
        params.publish = [this](const RuntimeFilter* filter) {
            ++_publishes;
            _descriptor.set_runtime_filter(filter);
        };
        params.cancelled = [this]() { return _cancelled.load(); };
        return std::make_shared<DeferredLocalRuntimeFilter>(std::move(params));
    }

    // What the readers of the scan report once they have read some lazy columns.
    void report_expensive_lazy_columns() { _descriptor.scan_sink()->add_lazy_read(2000L * 5000, 5000); }

    // The filter that is built at once, from the same keys.
    const RuntimeFilter* build_at_once(const Columns& columns, size_t ndv) {
        auto* filter = RuntimeFilterHelper::create_runtime_bloom_filter(&_pool, TYPE_BIGINT,
                                                                        TRuntimeFilterBuildJoinMode::BROADCAST);
        filter->get_membership_filter()->init(ndv);
        EXPECT_OK(RuntimeFilterHelper::fill_runtime_filter(columns, TYPE_BIGINT, filter, kHashJoinKeyColumnOffset,
                                                           false));
        return filter;
    }

    // Set before the filter is made to make a filter that has no Bloom filter.
    const char* _skip_bloom_reason = nullptr;
    ObjectPool _pool;
    RuntimeFilterProbeDescriptor _descriptor;
    std::atomic<int> _publishes{0};
    std::atomic<bool> _cancelled{false};
};

// Covers: the first window that asks for the filter builds it from the keys, and the scan finds it in its probe
//         descriptor, which is where the operator and the reader of the scan look for it.
TEST_F(DeferredLocalRuntimeFilterTest, FirstWindowBuildsAndPublishesTheFilter) {
    auto filter = make_filter({key_column(range(0, 100))}, 100, 100);
    report_expensive_lazy_columns();
    ASSERT_EQ(nullptr, _descriptor.runtime_filter(0));
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    ASSERT_TRUE(filter->holds_keys());

    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));

    EXPECT_EQ(1, _publishes.load());
    ASSERT_NE(nullptr, _descriptor.runtime_filter(0));
    EXPECT_FALSE(filter->holds_keys());
    EXPECT_TRUE(filter->decision().build);
    // A Bloom filter has no false negatives. The range of the keys is 0..99, so the min/max filter drops 100..199.
    const auto kept = probe(_descriptor.runtime_filter(0), range(0, 200));
    for (int i = 0; i < 100; ++i) EXPECT_EQ(1, kept[i]) << i;
    for (int i = 100; i < 200; ++i) EXPECT_EQ(0, kept[i]) << i;
    ASSERT_NE(nullptr, _descriptor.runtime_filter(0)->get_min_max_filter());
}

// The values that the filter keeps among 499..601: the keys of the tests below are the even numbers from 500 to 598.
std::vector<uint8_t> kept_around_the_range(const RuntimeFilter* filter) {
    return probe(filter, {499, 500, 501, 550, 551, 598, 599, 600, 601});
}

std::vector<int64_t> even_keys() {
    std::vector<int64_t> keys;
    for (int64_t key = 500; key < 600; key += 2) keys.push_back(key);
    return keys;
}

// Covers: a filter for more keys than a global runtime filter builds a Bloom filter for has its min/max range only. The
//         range drops what is outside it and keeps what is inside, whether the key is in the build side or not, and the
//         filter is not taken for one that keeps everything.
TEST_F(DeferredLocalRuntimeFilterTest, FilterForMoreKeysThanTheLimitHasItsMinMaxRangeOnly) {
    _skip_bloom_reason = "NDV > cap";
    auto filter = make_filter({key_column(even_keys())}, 50, 50);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);

    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));

    EXPECT_STREQ("NDV > cap", filter->bloom_skipped_reason());
    EXPECT_TRUE(filter->error().ok());
    ASSERT_EQ(1, _publishes.load());
    const RuntimeFilter* published = _descriptor.runtime_filter(0);
    ASSERT_NE(nullptr, published);
    EXPECT_FALSE(published->always_true());
    EXPECT_FALSE(published->get_membership_filter()->can_use_bf());
    ASSERT_NE(nullptr, published->get_min_max_filter());
    // 499, 599, 600 and 601 are outside the range 500..598. 501 and 551 are inside it and not in the build side, and
    // they stay.
    EXPECT_EQ(std::vector<uint8_t>({0, 1, 1, 1, 1, 1, 0, 0, 0}), kept_around_the_range(published));
    EXPECT_FALSE(filter->holds_keys());
}

// Covers: the memory of the query does not fit the directory of the Bloom filter. The build does not fail the query, and
//         the filter has its min/max range only.
TEST_F(DeferredLocalRuntimeFilterTest, FilterWhoseBloomFilterDoesNotFitInMemoryHasItsMinMaxRangeOnly) {
    auto* fail_point = failpoint::FailPointRegistry::GetInstance()->get("deferred_rf_bloom_alloc_fails");
    ASSERT_NE(nullptr, fail_point);
    PFailPointTriggerMode mode;
    mode.set_mode(FailPointTriggerModeType::ENABLE);
    fail_point->setMode(mode);
    DeferOp reset([&] {
        mode.set_mode(FailPointTriggerModeType::DISABLE);
        fail_point->setMode(mode);
    });
    auto filter = make_filter({key_column(even_keys())}, 50, 50);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);

    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));

    EXPECT_STREQ("memory limit", filter->bloom_skipped_reason());
    EXPECT_TRUE(filter->error().ok());
    ASSERT_EQ(1, _publishes.load());
    const RuntimeFilter* published = _descriptor.runtime_filter(0);
    ASSERT_NE(nullptr, published);
    EXPECT_FALSE(published->always_true());
    EXPECT_FALSE(published->get_membership_filter()->can_use_bf());
    EXPECT_EQ(std::vector<uint8_t>({0, 1, 1, 1, 1, 1, 0, 0, 0}), kept_around_the_range(published));
}

// Covers: a filter that has its Bloom filter drops the keys inside the range that the build side does not have.
TEST_F(DeferredLocalRuntimeFilterTest, FilterWithABloomFilterDropsKeysInsideTheRangeThatTheBuildSideDoesNotHave) {
    auto filter = make_filter({key_column(even_keys())}, 50, 50);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));
    EXPECT_EQ(nullptr, filter->bloom_skipped_reason());
    const RuntimeFilter* published = _descriptor.runtime_filter(0);
    ASSERT_NE(nullptr, published);
    EXPECT_TRUE(published->get_membership_filter()->can_use_bf());
    const auto kept = kept_around_the_range(published);
    // The keys of the build side, and the range, stay: a Bloom filter has no false negatives.
    EXPECT_EQ(1, kept[1]);
    EXPECT_EQ(1, kept[3]);
    EXPECT_EQ(1, kept[5]);
    EXPECT_EQ(0, kept[0]);
    EXPECT_EQ(0, kept[7]);
}

// Covers: an error of the build that is not a lack of memory still goes to the query.
TEST_F(DeferredLocalRuntimeFilterTest, ErrorOfTheBuildThatIsNotALackOfMemoryStillFailsTheFilter) {
    _skip_bloom_reason = "NDV > cap";
    DeferredLocalRuntimeFilter::Params params;
    params.filter_id = kFilterId;
    params.build_type = TYPE_UNKNOWN;
    params.ndv = 100;
    params.build_rows = 100;
    params.key_ns = 10;
    params.skip_bloom_reason = _skip_bloom_reason;
    params.columns = {key_column(range(0, 100))};
    params.pool = &_pool;
    DeferredLocalRuntimeFilter filter(std::move(params));
    filter.observe_scan_rows(262144);
    EXPECT_EQ(DeferredRfDecider::State::FAILED, filter.submit(expensive_window()));
    EXPECT_FALSE(filter.error().ok());
}

// Covers: the profile says that the filter has its min/max range only, and why.
TEST_F(DeferredLocalRuntimeFilterTest, ProfileSaysWhyTheBloomFilterWasSkipped) {
    _skip_bloom_reason = "NDV > cap";
    auto filter = make_filter({key_column(even_keys())}, 50, 50);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));

    RuntimeProfile profile("join");
    filter->report(&profile);
    ASSERT_NE(nullptr, section_of(profile));
    EXPECT_EQ("built min/max only by a window of the probe: bloom skipped: NDV > cap",
              *section_of(profile)->get_info_string("State"));
}

// Covers: the filter that is built late has the same content as the one that is built at once.
TEST_F(DeferredLocalRuntimeFilterTest, FilterEqualsTheOneBuiltAtOnce) {
    Columns columns{key_column(range(0, 3000)), key_column(range(2000, 5000))};
    const auto* at_once = build_at_once(columns, 6000);
    auto filter = make_filter(columns, 6000, 6000);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));

    const auto values = range(-1000, 9000);
    EXPECT_EQ(probe(at_once, values), probe(_descriptor.runtime_filter(0), values));
    EXPECT_EQ(at_once->debug_string(), _descriptor.runtime_filter(0)->debug_string());
}

TEST_F(DeferredLocalRuntimeFilterTest, BuildIsFinalAndLaterWindowsDoNotBuildAgain) {
    auto filter = make_filter({key_column(range(0, 100))}, 100, 100);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));
    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->submit(cheap_window()));
    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));
    EXPECT_EQ(1, _publishes.load());
}

// Covers: a window that shows no gain leaves the filter waiting with its keys, and a better measurement of the same
//         scan builds it. Here the readers report that the lazy columns are expensive after the first window.
TEST_F(DeferredLocalRuntimeFilterTest, WindowWithoutGainKeepsTheFilterWaitingAndABetterMeasurementBuildsIt) {
    auto filter = make_filter({key_column(range(0, 100))}, 100, 100);
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(cheap_window()));
    EXPECT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    EXPECT_STREQ("benefit below cost", filter->decision().reason);
    EXPECT_EQ("benefit below cost", filter->last_wait_reason());
    EXPECT_EQ(1, filter->evaluations());
    EXPECT_TRUE(filter->holds_keys());
    EXPECT_EQ(0, _publishes.load());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));

    report_expensive_lazy_columns();
    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->submit(cheap_window()));
    EXPECT_EQ(2, filter->evaluations());
    EXPECT_EQ(1, _publishes.load());
    EXPECT_FALSE(filter->holds_keys());
}

// Covers: the first estimate is below the cost because the queue holds few scan ranges, and the filter is built when
//         the ranges arrive. The measurement of the window is the same.
TEST_F(DeferredLocalRuntimeFilterTest, ScanRangesThatArriveLaterBuildTheFilter) {
    auto filter = make_filter({key_column(range(0, kManyKeys))}, kManyKeys, kManyKeys);
    filter->observe_scan_rows(262144);

    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    EXPECT_TRUE(filter->holds_keys());
    EXPECT_EQ(0, _publishes.load());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
    EXPECT_STREQ("benefit below cost", filter->decision().reason);

    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(lookup_window(8, 92)));
    EXPECT_EQ(2, filter->evaluations());
    EXPECT_EQ(1, _publishes.load());
    ASSERT_NE(nullptr, _descriptor.runtime_filter(0));
    EXPECT_FALSE(filter->holds_keys());
    // What kept the filter back is still known.
    EXPECT_EQ("benefit below cost", filter->last_wait_reason());
    const auto kept = probe(_descriptor.runtime_filter(0), {0, 99999, kManyKeys, kManyKeys + 5});
    EXPECT_EQ(1, kept[0]);
    EXPECT_EQ(1, kept[1]);
    EXPECT_EQ(0, kept[2]);
    EXPECT_EQ(0, kept[3]);
}

// Covers: the share of probe rows with a pair is high in the first window, so the filter would drop little. A later
//         window of the probe shows a lower share, and the same scan ranges now pay for the filter.
TEST_F(DeferredLocalRuntimeFilterTest, ChangedShareOfRowsWithAPairInALaterWindowBuildsTheFilter) {
    auto filter = make_filter({key_column(range(0, kManyKeys))}, kManyKeys, kManyKeys);
    filter->observe_scan_rows(262144);

    auto most_rows_have_a_pair = lookup_window(8, 92);
    most_rows_have_a_pair.probe_output_rows = most_rows_have_a_pair.rows * 9 / 10;
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(most_rows_have_a_pair));
    EXPECT_EQ("benefit below cost", filter->last_wait_reason());
    EXPECT_FALSE(filter->input_changed());
    EXPECT_EQ(0, _publishes.load());

    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->submit(lookup_window(8, 92)));
    EXPECT_EQ(1, _publishes.load());
}

// Covers: the filter tells the probe when the gain per remaining row of a window differs from the window before by
//         more than the hysteresis of the adaptive filters, and does not when it stays the same or moves a little.
TEST_F(DeferredLocalRuntimeFilterTest, TellsTheProbeWhenTheGainPerRowChanges) {
    auto filter = make_filter({key_column(range(0, kManyKeys))}, kManyKeys, kManyKeys);
    filter->observe_scan_rows(262144);

    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    EXPECT_FALSE(filter->input_changed());
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    EXPECT_FALSE(filter->input_changed());
    auto little = lookup_window(8, 2);
    little.lookup_ns = little.rows * 52;
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(little));
    EXPECT_FALSE(filter->input_changed());
    auto much = lookup_window(8, 2);
    much.lookup_ns = much.rows * 40;
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(much));
    EXPECT_TRUE(filter->input_changed());
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(much));
    EXPECT_FALSE(filter->input_changed());
}

// Covers: new scan ranges ask again with the measurement of the last window, so they do not change what the probe
//         sees: the gain per row is the same. The build is credited to what asked for it.
TEST_F(DeferredLocalRuntimeFilterTest, NewScanRangesAskAgainWithTheMeasurementOfTheLastWindow) {
    auto filter = make_filter({key_column(range(0, kManyKeys))}, kManyKeys, kManyKeys);
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    auto more_ranges = lookup_window(8, 2);
    more_ranges.trigger = DeferredRfTrigger::NEW_RANGES;
    more_ranges.ranges_version = 5;
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(more_ranges));
    EXPECT_FALSE(filter->input_changed());
    more_ranges.morsels_remaining = 92;
    more_ranges.ranges_version = 6;
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(more_ranges));

    EXPECT_EQ(4, filter->evaluations());
    EXPECT_EQ(6, filter->decision().ranges_batch);
    EXPECT_EQ(DeferredRfTrigger::NEW_RANGES, filter->decision().trigger);
    RuntimeProfile profile("built");
    filter->report(&profile);
    ASSERT_NE(nullptr, section_of(profile));
    EXPECT_EQ("built late by new scan ranges: benefit exceeds cost", *section_of(profile)->get_info_string("State"));
}

// Covers: one to four large files, all taken at the start, and a filter that pays off: the filter is built from the
//         first window, which the ranges alone would refuse because nothing is left in the queue.
TEST_F(DeferredLocalRuntimeFilterTest, LargeFilesThatAreAllTakenAtTheStartBuildTheFilter) {
    for (int files = 1; files <= 4; ++files) {
        _publishes = 0;
        RuntimeFilterProbeDescriptor descriptor;
        DeferredLocalRuntimeFilter::Params params;
        params.filter_id = kFilterId;
        params.build_type = TYPE_BIGINT;
        params.join_mode = TRuntimeFilterBuildJoinMode::BROADCAST;
        params.ndv = 100;
        params.build_rows = 100;
        params.key_ns = 10;
        params.benefit_margin = 2.0;
        params.columns = {key_column(range(0, 100))};
        params.pool = &_pool;
        params.scan_sinks = {descriptor.enable_scan_sink()};
        params.publish = [this, &descriptor](const RuntimeFilter* filter) {
            ++_publishes;
            descriptor.set_runtime_filter(filter);
        };
        DeferredLocalRuntimeFilter filter(std::move(params));
        filter.observe_scan_rows(262144);
        auto ranges_only = large_files_window(files);
        ranges_only.remaining_raw = RemainingRawRows();

        EXPECT_EQ(DeferredRfDecider::State::WAITING, filter.submit(ranges_only)) << files;
        EXPECT_STREQ("nothing left to scan", filter.decision().reason);
        EXPECT_EQ(0, _publishes.load());

        EXPECT_EQ(DeferredRfDecider::State::BUILT, filter.submit(large_files_window(files))) << files;
        EXPECT_EQ(1, _publishes.load()) << files;
        EXPECT_NE(nullptr, descriptor.runtime_filter(0));
    }
}

// Covers: the profile tells where the remaining rows come from.
TEST_F(DeferredLocalRuntimeFilterTest, ProfileTellsWhereTheRemainingRowsComeFrom) {
    auto filter = make_filter({key_column(range(0, kManyKeys))}, kManyKeys, kManyKeys);
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    {
        RuntimeProfile profile("ranges");
        filter->report(&profile);
        ASSERT_NE(nullptr, section_of(profile));
        const auto* from = section_of(profile)->get_info_string("RemainingRowsFrom");
        ASSERT_NE(nullptr, from);
        EXPECT_NE(std::string::npos, from->find("ranges:")) << *from;
    }

    auto window = large_files_window(2);
    // The lookups are cheap, so the filter does not pay off yet.
    window.lookup_ns = window.rows;
    window.remaining_raw.pending_ranges = 7;
    window.remaining_raw.not_started_ranges = 9;
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(window));
    RuntimeProfile profile("row counts");
    filter->report(&profile);
    ASSERT_NE(nullptr, section_of(profile));
    const auto* from = section_of(profile)->get_info_string("RemainingRowsFrom");
    ASSERT_NE(nullptr, from);
    EXPECT_NE(std::string::npos, from->find("row counts of the files")) << *from;
    EXPECT_NE(std::string::npos, from->find("99800000 rows in opened ranges")) << *from;
    EXPECT_NE(std::string::npos, from->find("7 rows in taken ranges without a row count")) << *from;
    EXPECT_NE(std::string::npos, from->find("9 rows in ranges not started")) << *from;
}

// Covers: windows that do not pay off come until the probe ends. Nothing is built, the filter stays in the question
//         and keeps the keys, and the end of the probe releases them.
TEST_F(DeferredLocalRuntimeFilterTest, ManyWindowsWithoutGainBuildNothingAndTheEndOfTheProbeReleasesTheKeys) {
    ColumnPtr keys = key_column(range(0, kManyKeys));
    auto filter = make_filter({keys}, kManyKeys, kManyKeys);
    filter->observe_scan_rows(262144);
    ASSERT_EQ(2, keys->use_count());

    for (int i = 0; i < 500; ++i) {
        ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2))) << i;
    }
    EXPECT_EQ(500, filter->evaluations());
    EXPECT_EQ(0, _publishes.load());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
    EXPECT_TRUE(filter->holds_keys());
    EXPECT_EQ(2, keys->use_count());

    filter->cancel();
    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->state());
    EXPECT_FALSE(filter->holds_keys());
    EXPECT_EQ(1, keys->use_count());
    EXPECT_EQ(0, _publishes.load());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
}

// Covers: an empty build side has no filter to build, and nothing is built while the probe runs.
TEST_F(DeferredLocalRuntimeFilterTest, EmptyBuildSideNeverBuilds) {
    auto filter = make_filter({key_column({})}, 0, 0);
    filter->observe_scan_rows(262144);
    EXPECT_EQ(DeferredRfDecider::State::WAITING, filter->submit(expensive_window()));
    EXPECT_STREQ("empty build side", filter->decision().reason);
    EXPECT_EQ(0, _publishes.load());
}

// Covers: the query ends or the hash table is closed before any window has decided.
TEST_F(DeferredLocalRuntimeFilterTest, CancelBeforeTheDecisionRefusesAndReleasesKeys) {
    auto filter = make_filter({key_column(range(0, 100))}, 100, 100);
    filter->observe_scan_rows(262144);
    ASSERT_TRUE(filter->holds_keys());
    filter->cancel();
    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->state());
    EXPECT_FALSE(filter->holds_keys());
    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->submit(expensive_window()));
    EXPECT_EQ(0, _publishes.load());
    // Closing the hash table after the decision changes nothing.
    filter->cancel();
    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->state());
}

// Covers: the filter waits with its keys when the hash table is closed or the build side spills, and nobody
//         asks for it any more.
TEST_F(DeferredLocalRuntimeFilterTest, CancelWhileWaitingRefusesAndReleasesKeys) {
    ColumnPtr keys = key_column(range(0, kManyKeys));
    auto filter = make_filter({keys}, kManyKeys, kManyKeys);
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    ASSERT_TRUE(filter->holds_keys());
    ASSERT_EQ(2, keys->use_count());

    filter->cancel();

    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->state());
    EXPECT_STREQ("cancelled", filter->decision().reason);
    EXPECT_FALSE(filter->holds_keys());
    EXPECT_EQ(1, keys->use_count());
    // Ranges that arrive afterwards do not bring the filter back.
    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->submit(lookup_window(8, 92)));
    EXPECT_EQ(0, _publishes.load());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
    // The numbers of the last evaluation stay for the profile.
    EXPECT_EQ(1, filter->evaluations());
    EXPECT_EQ("benefit below cost", filter->last_wait_reason());
}

TEST_F(DeferredLocalRuntimeFilterTest, CancelAfterTheBuildKeepsTheFilter) {
    auto filter = make_filter({key_column(range(0, 100))}, 100, 100);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));
    filter->cancel();
    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->state());
    EXPECT_NE(nullptr, _descriptor.runtime_filter(0));
}

TEST_F(DeferredLocalRuntimeFilterTest, CancelledQueryRefusesTheWindow) {
    auto filter = make_filter({key_column(range(0, 100))}, 100, 100);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);
    _cancelled = true;
    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->submit(expensive_window()));
    EXPECT_STREQ("cancelled", filter->decision().reason);
    EXPECT_FALSE(filter->holds_keys());
    EXPECT_EQ(0, _publishes.load());
}

// Covers: the filter holds the only reference to the key columns, as the hash table may be closed by the time a
//         window builds the filter.
TEST_F(DeferredLocalRuntimeFilterTest, KeysLiveInTheFilterUntilTheBuild) {
    auto filter = make_filter({key_column(range(500, 600))}, 100, 100);
    report_expensive_lazy_columns();
    filter->observe_scan_rows(262144);
    ASSERT_TRUE(filter->holds_keys());
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));
    const auto kept = probe(_descriptor.runtime_filter(0), {499, 500, 550, 599, 600});
    EXPECT_EQ(std::vector<uint8_t>({0, 1, 1, 1, 0}), kept);
    EXPECT_FALSE(filter->holds_keys());
}

// Covers: the build returns an error. The filter FAILED with that error, which the operators return to the query,
//         and it publishes nothing and releases the keys.
TEST_F(DeferredLocalRuntimeFilterTest, BuildFailureFailsTheFilterWithTheError) {
    DeferredLocalRuntimeFilter::Params params;
    params.filter_id = kFilterId;
    params.build_type = TYPE_UNKNOWN;
    params.ndv = 100;
    params.build_rows = 100;
    params.key_ns = 10;
    params.columns = {key_column(range(0, 100))};
    params.pool = &_pool;
    params.publish = [this](const RuntimeFilter*) { ++_publishes; };
    DeferredLocalRuntimeFilter filter(std::move(params));
    filter.observe_scan_rows(262144);
    ASSERT_TRUE(filter.error().ok());

    EXPECT_EQ(DeferredRfDecider::State::FAILED, filter.submit(expensive_window()));

    EXPECT_STREQ("build failed", filter.decision().reason);
    EXPECT_FALSE(filter.error().ok());
    EXPECT_EQ(0, _publishes.load());
    EXPECT_FALSE(filter.holds_keys());
    // Later windows find the filter final.
    EXPECT_EQ(DeferredRfDecider::State::FAILED, filter.submit(expensive_window()));
    RuntimeProfile profile("failed");
    filter.report(&profile);
    ASSERT_NE(nullptr, section_of(profile));
    EXPECT_EQ("failed: build failed", *section_of(profile)->get_info_string("State"));
    EXPECT_NE(nullptr, section_of(profile)->get_info_string("Error"));
}

// Covers: two drivers close their windows together. One of them builds the filter, the other either sees the
//         result or learns that a build is under way, and the filter is published once.
TEST_F(DeferredLocalRuntimeFilterTest, DriversThatCloseTogetherBuildOnce) {
    for (int round = 0; round < 20; ++round) {
        _publishes = 0;
        RuntimeFilterProbeDescriptor descriptor;
        DeferredLocalRuntimeFilter::Params params;
        params.filter_id = kFilterId;
        params.build_type = TYPE_BIGINT;
        params.join_mode = TRuntimeFilterBuildJoinMode::BROADCAST;
        params.ndv = 50000;
        params.build_rows = 50000;
        params.key_ns = 10;
        params.benefit_margin = 2.0;
        params.columns = {key_column(range(0, 50000))};
        params.pool = &_pool;
        params.scan_sinks = {descriptor.enable_scan_sink()};
        params.publish = [this, &descriptor](const RuntimeFilter* filter) {
            ++_publishes;
            descriptor.set_runtime_filter(filter);
        };
        descriptor.scan_sink()->add_lazy_read(2000L * 5000, 5000);
        DeferredLocalRuntimeFilter filter(std::move(params));
        filter.observe_scan_rows(262144);

        std::atomic<bool> go{false};
        std::vector<DeferredRfDecider::State> results(4);
        std::vector<std::thread> threads;
        for (size_t i = 0; i < results.size(); ++i) {
            threads.emplace_back([&, i]() {
                while (!go.load()) {
                }
                results[i] = filter.submit(expensive_window());
            });
        }
        go = true;
        for (auto& thread : threads) thread.join();

        EXPECT_EQ(1, _publishes.load());
        EXPECT_EQ(DeferredRfDecider::State::BUILT, filter.state());
        EXPECT_EQ(DeferredRfDecider::State::BUILT, filter.submit(cheap_window()));
        // Nobody sees a refusal, and someone saw the build finish or did it.
        for (auto result : results) EXPECT_NE(DeferredRfDecider::State::REFUSED, result);
        EXPECT_NE(results.end(), std::find(results.begin(), results.end(), DeferredRfDecider::State::BUILT));
    }
}

// Covers: each deferred filter has its own section in the profile of the JOIN, with the metrics that stay and without the
//         ones that were taken out.
TEST_F(DeferredLocalRuntimeFilterTest, ReportsTheStateAndTheLastEvaluationInASectionOfTheProfile) {
    auto filter = make_filter({key_column(range(0, kManyKeys))}, kManyKeys, kManyKeys);
    {
        RuntimeProfile profile("not evaluated");
        filter->report(&profile);
        ASSERT_NE(nullptr, section_of(profile));
        EXPECT_EQ("DeferredRuntimeFilter (filter_id=7)", section_of(profile)->name());
        EXPECT_EQ("waiting: not evaluated yet", *section_of(profile)->get_info_string("State"));
        EXPECT_EQ(0, section_of(profile)->get_counter("Evaluations")->value());
    }
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));

    {
        RuntimeProfile profile("waiting");
        filter->report(&profile);
        auto* section = section_of(profile);
        ASSERT_NE(nullptr, section);
        // Nothing is put in the profile of the JOIN itself.
        EXPECT_EQ(nullptr, profile.get_info_string("DeferredRf7State"));
        EXPECT_EQ(nullptr, profile.get_counter("DeferredRf7Evaluations"));
        EXPECT_EQ("waiting: benefit below cost", *section->get_info_string("State"));
        EXPECT_EQ("benefit below cost", *section->get_info_string("LastWaitReason"));
        EXPECT_EQ(2, section->get_counter("Evaluations")->value());
        EXPECT_EQ(static_cast<int64_t>(kManyKeys), section->get_counter("Ndv")->value());
        // 262144 rows came from 8 started ranges, and 2 ranges are not started, so 65536 rows are left.
        EXPECT_EQ(65536, section->get_counter("RemainingRows")->value());
        EXPECT_NE(nullptr, section->get_info_string("RemainingRowsFrom"));
        EXPECT_EQ(3072000, section->get_counter("BenefitTime")->value());
        EXPECT_EQ(1655360, section->get_counter("CostTime")->value());
        EXPECT_EQ(0, section->get_counter("BuildTime")->value());
        // These stay until the estimate of the remaining rows is checked on the stand.
        EXPECT_EQ(262144, section->get_counter("ScanRowsSeen")->value());
        EXPECT_EQ(8, section->get_counter("RangesStarted")->value());
        EXPECT_EQ(2, section->get_counter("RangesNotStarted")->value());
        EXPECT_NE(nullptr, section->get_counter("RangesBatch"));
        // These were taken out.
        for (const char* removed :
             {"EvaluationsByProbeWindow", "EvaluationsByNewRanges", "ReaderLazyNsPerRow", "ReaderLazyRows",
              "DecisionChunks", "DecisionRows", "BloomBytes", "MaxBloomBytes"}) {
            EXPECT_EQ(nullptr, section->get_counter(removed)) << removed;
        }
        EXPECT_EQ(nullptr, section->get_info_string("Error"));
    }

    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(lookup_window(8, 92)));
    RuntimeProfile join_profile("join");
    filter->report(&join_profile);
    auto* section = section_of(join_profile);
    ASSERT_NE(nullptr, section);
    EXPECT_EQ("built late by a window of the probe: benefit exceeds cost", *section->get_info_string("State"));
    EXPECT_EQ("benefit below cost", *section->get_info_string("LastWaitReason"));
    EXPECT_EQ(3, section->get_counter("Evaluations")->value());
    EXPECT_EQ(62, section->get_counter("MatchPermille")->value());
    EXPECT_EQ(50, section->get_counter("SavedNsPerRow")->value());
    EXPECT_EQ(92, section->get_counter("RangesNotStarted")->value());
}

TEST_F(DeferredLocalRuntimeFilterTest, ReportsACancelledFilterWithTheReasonItWaited) {
    auto filter = make_filter({key_column(range(0, kManyKeys))}, kManyKeys, kManyKeys);
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->submit(lookup_window(8, 2)));
    filter->cancel();

    RuntimeProfile profile("cancelled");
    filter->report(&profile);
    ASSERT_NE(nullptr, section_of(profile));
    EXPECT_EQ("refused: cancelled", *section_of(profile)->get_info_string("State"));
    EXPECT_EQ("benefit below cost", *section_of(profile)->get_info_string("LastWaitReason"));
    EXPECT_EQ(1, section_of(profile)->get_counter("Evaluations")->value());
}

// Covers: two filters of one JOIN have two sections, each with its own numbers.
TEST_F(DeferredLocalRuntimeFilterTest, EachFilterHasItsOwnSection) {
    auto first = make_filter({key_column(range(0, kManyKeys))}, kManyKeys, kManyKeys);
    DeferredLocalRuntimeFilter::Params params;
    params.filter_id = 9;
    params.build_type = TYPE_BIGINT;
    params.join_mode = TRuntimeFilterBuildJoinMode::BROADCAST;
    params.ndv = 5;
    params.build_rows = 5;
    params.key_ns = 10;
    params.columns = {key_column(range(0, 5))};
    params.pool = &_pool;
    DeferredLocalRuntimeFilter second(std::move(params));
    first->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, first->submit(lookup_window(8, 2)));

    RuntimeProfile profile("join");
    first->report(&profile);
    second.report(&profile);

    ASSERT_NE(nullptr, profile.get_child("DeferredRuntimeFilter (filter_id=7)"));
    ASSERT_NE(nullptr, profile.get_child("DeferredRuntimeFilter (filter_id=9)"));
    EXPECT_EQ(static_cast<int64_t>(kManyKeys),
              profile.get_child("DeferredRuntimeFilter (filter_id=7)")->get_counter("Ndv")->value());
    EXPECT_EQ("waiting: not evaluated yet",
              *profile.get_child("DeferredRuntimeFilter (filter_id=9)")->get_info_string("State"));
}

// A scan operator that holds a collector with the descriptor, to run Operator::eval_runtime_bloom_filters().
namespace {

class TestOperatorFactory final : public OperatorFactory {
public:
    TestOperatorFactory() : OperatorFactory(1, "test", 1) {}
    OperatorPtr create(int32_t, int32_t) override { return nullptr; }
};

class TestScanOperator final : public Operator {
public:
    TestScanOperator(OperatorFactory* factory, RuntimeFilterProbeCollector* collector)
            : Operator(factory, 1, "test_scan", 1, false, 0), _collector(collector) {}
    RuntimeFilterProbeCollector* runtime_bloom_filters() override { return _collector; }
    const RuntimeFilterProbeCollector* runtime_bloom_filters() const override { return _collector; }
    bool has_output() const override { return false; }
    bool need_input() const override { return false; }
    bool is_finished() const override { return false; }
    StatusOr<ChunkPtr> pull_chunk(RuntimeState*) override { return Status::NotSupported("test"); }
    Status push_chunk(RuntimeState*, const ChunkPtr&) override { return Status::NotSupported("test"); }

private:
    RuntimeFilterProbeCollector* _collector;
};

} // namespace

class DeferredLocalRuntimeFilterOperatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        _expr = std::make_unique<ExprContext>(&_slot);
        ASSERT_OK(_expr->prepare(&_state));
        ASSERT_OK(_expr->open(&_state));
        _descriptor._filter_id = kFilterId;
        _descriptor._probe_expr_ctx = _expr.get();
        _collector._runtime_state = &_state;
        _collector._descriptors.emplace(kFilterId, &_descriptor);
        _scan = std::make_unique<TestScanOperator>(&_factory, &_collector);
        _feedback = std::make_shared<LocalRuntimeFilterFeedback>();
        _scan->set_local_runtime_filter_feedback(_feedback);
    }

    void TearDown() override { _expr->close(&_state); }

    static std::function<void(DeferredRfWindow*)> default_scan_progress() {
        return [](DeferredRfWindow* window) {
            window->morsels_taken = 10;
            window->morsels_remaining = 90;
        };
    }

    // The scan has started 10 ranges and 90 are left, unless the test says what the scan reports.
    std::shared_ptr<DeferredLocalRuntimeFilter> make_filter(
            const std::vector<int64_t>& keys,
            std::function<void(DeferredRfWindow*)> scan_progress = default_scan_progress(),
            LogicalType build_type = TYPE_BIGINT) {
        DeferredLocalRuntimeFilter::Params params;
        params.filter_id = kFilterId;
        params.build_type = build_type;
        params.join_mode = TRuntimeFilterBuildJoinMode::BROADCAST;
        params.ndv = keys.size();
        params.build_rows = keys.size();
        params.key_ns = 10;
        params.benefit_margin = 2.0;
        params.skip_bloom_reason = _skip_bloom_reason;
        _keys = key_column(keys);
        params.columns = {_keys};
        params.pool = &_pool;
        params.scan_sinks = {_descriptor.enable_scan_sink()};
        params.publish = [this](const RuntimeFilter* filter) { _descriptor.set_runtime_filter(filter); };
        auto filter = std::make_shared<DeferredLocalRuntimeFilter>(std::move(params));
        _feedback->set_deferred_resolver([filter]() { return std::vector<std::shared_ptr<DeferredRfDecider>>{filter}; },
                                         std::move(scan_progress));
        return filter;
    }

    // The chunk that the scan emits: values 0..999 over and over.
    static ChunkPtr scan_chunk(size_t rows) {
        std::vector<int64_t> values;
        for (size_t i = 0; i < rows; ++i) values.push_back(i % 1000);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(ColumnTestHelper::build_column<int64_t>(values), 1);
        return chunk;
    }

    // A filter for a scan that has taken its four files at the start, so the queue has no range left. Every file has 50
    // million rows and the reader has read 100000 of them. The readers say so only if readers_know_their_rows.
    std::shared_ptr<DeferredLocalRuntimeFilter> make_filter_for_four_large_files(bool readers_know_their_rows) {
        Morsels morsels;
        for (int i = 0; i < 4; ++i) morsels.emplace_back(std::make_unique<ScanMorsel>(1, TScanRange()));
        _queue = std::make_unique<DynamicMorselQueue>(std::move(morsels), /*has_more=*/false);
        for (int i = 0; i < 4; ++i) _taken.push_back(std::move(_queue->try_get().value()));
        EXPECT_EQ(0, _queue->progress().remaining);

        _ledger = std::make_shared<ScanRangeLedger>();
        for (int i = 0; i < 4; ++i) {
            _ranges.push_back(std::make_unique<ScanRangeLedger::Range>(_ledger, 1L << 30));
            _ranges.back()->update(100000, readers_know_their_rows ? 50000000 : -1);
        }
        return make_filter(range(0, 100), [this](DeferredRfWindow* window) {
            const MorselProgress progress = _queue->progress();
            window->morsels_taken = progress.taken();
            window->morsels_remaining = progress.remaining;
            window->ranges_version = progress.version;
            window->remaining_raw = estimate_remaining_raw_rows(_ledger->snapshot(), progress.remaining);
        });
    }

    // The scan emits the chunks, and its JOIN looks all of them up at the cost of lookup_ns per row and finds the given
    // number of pairs, which is one in twenty rows unless the test says otherwise.
    void run_chunks(int count, int64_t lookup_ns_per_row, int64_t pairs_per_chunk = 4096 / 20) {
        for (int i = 0; i < count; ++i) {
            auto chunk = scan_chunk(4096);
            _scan->eval_runtime_bloom_filters(chunk.get());
            ASSERT_EQ(4096, chunk->num_rows());
            _feedback->observe_lookup(4096, 4096 * lookup_ns_per_row, 0, pairs_per_chunk);
        }
    }

    void run_window(int64_t lookup_ns_per_row) { run_chunks(64, lookup_ns_per_row); }

    const char* _skip_bloom_reason = nullptr;
    std::unique_ptr<DynamicMorselQueue> _queue;
    std::vector<MorselPtr> _taken;
    std::shared_ptr<ScanRangeLedger> _ledger;
    std::vector<std::unique_ptr<ScanRangeLedger::Range>> _ranges;
    ObjectPool _pool;
    ColumnPtr _keys;
    RuntimeState _state;
    ColumnRef _slot{TypeDescriptor(TYPE_BIGINT), 1};
    std::unique_ptr<ExprContext> _expr;
    RuntimeFilterProbeDescriptor _descriptor;
    RuntimeFilterProbeCollector _collector;
    TestOperatorFactory _factory;
    std::unique_ptr<TestScanOperator> _scan;
    std::shared_ptr<LocalRuntimeFilterFeedback> _feedback;
};

// Covers: until a window decides, the scan operator neither filters nor gives up on the filter, and the filter
//         that is built by the window reaches the next chunk through the probe descriptor of the scan.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, FilterBuiltByTheWindowFiltersTheNextChunk) {
    auto filter = make_filter(range(0, 100));
    _descriptor.scan_sink()->add_lazy_read(2000L * 5000, 5000);

    run_window(50);

    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->state());
    ASSERT_NE(nullptr, _descriptor.runtime_filter(0));
    ASSERT_TRUE(_feedback->enabled());
    EXPECT_FALSE(_feedback->deferred());
    EXPECT_TRUE(_feedback->use_filter());

    auto chunk = scan_chunk(4096);
    _scan->eval_runtime_bloom_filters(chunk.get());
    // The values below 100 are one tenth of the chunk and have to stay, the others may stay as false positives.
    EXPECT_GE(chunk->num_rows(), 410);
    EXPECT_LT(chunk->num_rows(), 1000);
    const auto* column = chunk->get_column_by_slot_id(1).get();
    for (size_t i = 0; i < chunk->num_rows(); ++i) {
        EXPECT_LT(column->get(i).get_int64(), 1000);
    }
}

// Covers: a filter with its min/max range only is evaluated by the scan operator on the next chunk: it drops the values
//         outside the range and keeps all inside it.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, MinMaxOnlyFilterFiltersTheNextChunkByItsRange) {
    _skip_bloom_reason = "NDV > cap";
    auto filter = make_filter(range(0, 100));
    _descriptor.scan_sink()->add_lazy_read(2000L * 5000, 5000);

    run_window(50);

    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->state());
    ASSERT_NE(nullptr, _descriptor.runtime_filter(0));
    EXPECT_FALSE(_descriptor.runtime_filter(0)->get_membership_filter()->can_use_bf());
    auto chunk = scan_chunk(4096);
    _scan->eval_runtime_bloom_filters(chunk.get());
    // The values are 0..999 over and over, and the range is 0..99: 4 full rounds and the first 96 values.
    EXPECT_EQ(4 * 100 + 96, chunk->num_rows());
    const auto* column = chunk->get_column_by_slot_id(1).get();
    for (size_t i = 0; i < chunk->num_rows(); ++i) {
        EXPECT_LT(column->get(i).get_int64(), 100);
    }
}

// Covers: a filter that does not pay off yet leaves the scan chunks as they are, and the feedback goes on measuring
//         without a failure reason.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, WaitingFilterLeavesChunksUntouched) {
    auto filter = make_filter(range(0, 100));

    // No lazy columns are reported, so the rows are cheap and the window does not build.
    run_window(5);

    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
    EXPECT_TRUE(_feedback->enabled());
    EXPECT_TRUE(_feedback->deferred());
    EXPECT_FALSE(_feedback->deferral_refused());
    auto chunk = scan_chunk(4096);
    _scan->eval_runtime_bloom_filters(chunk.get());
    EXPECT_EQ(4096, chunk->num_rows());
    EXPECT_EQ(nullptr, _scan->unique_metrics()->get_info_string("LocalRfFallbackReason"));
}

// Covers: the readers report the cost of their lazy columns after the first window, and a later window builds the
//         filter from the better measurement.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, LaterWindowBuildsTheFilterWhenTheReadersReportTheirCost) {
    auto filter = make_filter(range(0, 100));
    run_window(5);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    ASSERT_EQ(1, filter->evaluations());

    _descriptor.scan_sink()->add_lazy_read(2000L * 5000, 5000);
    // The second window comes after the wait of 256 chunks.
    run_chunks(255, 5);
    EXPECT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    run_chunks(1, 5);

    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->state());
    EXPECT_EQ(2, filter->evaluations());
    EXPECT_NE(nullptr, _descriptor.runtime_filter(0));
    EXPECT_FALSE(_feedback->deferred());
}

// Covers: in the first window every row of the probe finds a pair, so the filter would drop nothing. The share falls
//         in a later window, and the filter is built from that window.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, ChangedShareOfRowsWithAPairInALaterWindowBuildsTheFilter) {
    auto filter = make_filter(range(0, 100));
    run_chunks(64, 50, /*pairs_per_chunk=*/4096);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    EXPECT_STREQ("benefit below cost", filter->decision().reason);
    EXPECT_TRUE(_feedback->deferred());

    run_chunks(256, 50);

    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->state());
    EXPECT_EQ(2, filter->evaluations());
    EXPECT_EQ(DeferredRfTrigger::WINDOW, filter->decision().trigger);
    EXPECT_NE(nullptr, _descriptor.runtime_filter(0));
}

// Covers: the build returns an error. The scan gets it from its feedback to return to the query, the feedback ends,
//         and the chunks that are already out of the scan are not touched.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, FailedBuildGivesTheErrorToTheScan) {
    auto filter = make_filter(range(0, 100), default_scan_progress(), TYPE_UNKNOWN);
    _descriptor.scan_sink()->add_lazy_read(2000L * 5000, 5000);
    ASSERT_FALSE(_feedback->deferral_failed());

    run_window(50);

    EXPECT_EQ(DeferredRfDecider::State::FAILED, filter->state());
    EXPECT_TRUE(_feedback->deferral_failed());
    EXPECT_FALSE(_feedback->deferral_status().ok());
    EXPECT_FALSE(_feedback->enabled());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
    EXPECT_FALSE(filter->holds_keys());
    auto chunk = scan_chunk(4096);
    _scan->eval_runtime_bloom_filters(chunk.get());
    EXPECT_EQ(4096, chunk->num_rows());
}

// Covers: the queue of the scan holds few ranges at the first window, so the filter does not pay off. Ranges arrive,
//         and the next chunk of the scan asks the filter again with the same measurement, without a new window.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, ScanRangesThatArriveLaterBuildTheFilterAtTheNextChunk) {
    Morsels morsels;
    for (int i = 0; i < 10; ++i) morsels.emplace_back(std::make_unique<ScanMorsel>(1, TScanRange()));
    DynamicMorselQueue queue(std::move(morsels), /*has_more=*/true);
    std::vector<MorselPtr> started;
    for (int i = 0; i < 8; ++i) started.push_back(std::move(queue.try_get().value()));

    // So many keys that the rest of the first estimate, two ranges, does not pay for them.
    auto filter = make_filter(range(0, 100000), [&queue](DeferredRfWindow* window) {
        const MorselProgress progress = queue.progress();
        window->morsels_taken = progress.taken();
        window->morsels_remaining = progress.remaining;
        window->ranges_version = progress.version;
    });

    run_window(50);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    EXPECT_STREQ("benefit below cost", filter->decision().reason);
    EXPECT_EQ(1, filter->evaluations());
    EXPECT_TRUE(filter->holds_keys());
    // Chunks go on and the filter is not asked again while the queue has the same ranges.
    run_chunks(10, 50);
    EXPECT_EQ(1, filter->evaluations());

    Morsels more;
    for (int i = 0; i < 90; ++i) more.emplace_back(std::make_unique<ScanMorsel>(1, TScanRange()));
    ASSERT_TRUE(queue.append_morsels(std::move(more)).ok());
    run_chunks(1, 50);

    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->state());
    EXPECT_EQ(2, filter->evaluations());
    EXPECT_NE(nullptr, _descriptor.runtime_filter(0));
    EXPECT_FALSE(filter->holds_keys());
    EXPECT_FALSE(_feedback->deferred());
    EXPECT_TRUE(_feedback->use_filter());
}

// Covers: a scan reads four large files, takes all of them at the start, and the queue has none left. The readers know
//         the rows of their files, so the first window finds the rows that are left and builds the filter.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, LargeFilesTakenAtTheStartBuildTheFilterAtTheFirstWindow) {
    auto filter = make_filter_for_four_large_files(/*readers_know_their_rows=*/true);

    run_window(50);

    EXPECT_EQ(DeferredRfDecider::State::BUILT, filter->state());
    EXPECT_EQ(DeferredRfDecision::RemainingSource::ROW_COUNTS, filter->decision().remaining_source);
    EXPECT_GT(filter->decision().remaining_rows, 100000000);
    EXPECT_NE(nullptr, _descriptor.runtime_filter(0));
}

// Covers: a scan whose readers do not know the rows of their files takes the ranges for read, and the filter waits.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, LargeFilesWithoutARowCountLeaveTheFilterWaiting) {
    auto filter = make_filter_for_four_large_files(/*readers_know_their_rows=*/false);

    run_window(50);

    EXPECT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    EXPECT_STREQ("nothing left to scan", filter->decision().reason);
    EXPECT_EQ(DeferredRfDecision::RemainingSource::RANGES, filter->decision().remaining_source);
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
}

// Covers: the filter waits through many windows that do not pay off until the probe ends, the feedback stays on and
//         measures, nothing is published, and closing the hash table releases the keys.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, FilterThatNeverPaysOffWaitsUntilTheProbeEnds) {
    auto filter = make_filter(range(0, 100));
    for (int window_chunks : {64, 256, 1024, 4096}) {
        run_chunks(window_chunks, 5);
        ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->state());
        ASSERT_TRUE(_feedback->enabled());
        ASSERT_TRUE(_feedback->deferred());
        ASSERT_FALSE(_feedback->deferral_refused());
    }
    EXPECT_EQ(4, filter->evaluations());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
    EXPECT_TRUE(filter->holds_keys());
    ASSERT_EQ(2, _keys->use_count());

    // The hash table is closed.
    filter->cancel();

    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->state());
    EXPECT_FALSE(filter->holds_keys());
    EXPECT_EQ(1, _keys->use_count());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
    // The scan finishes its chunks without the filter.
    run_chunks(2, 5);
    EXPECT_FALSE(_feedback->enabled());
    EXPECT_TRUE(_feedback->deferral_refused());
}

// Covers: the JOIN stops the feedback, for example because its build side spills, while the filter waits.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, StopWhileTheFilterWaitsRefusesItAndReleasesTheKeys) {
    auto filter = make_filter(range(0, 100));
    run_window(5);
    ASSERT_EQ(DeferredRfDecider::State::WAITING, filter->state());
    ASSERT_TRUE(filter->holds_keys());

    _feedback->stop();

    EXPECT_EQ(DeferredRfDecider::State::REFUSED, filter->state());
    EXPECT_FALSE(filter->holds_keys());
    EXPECT_EQ(1, _keys->use_count());
    EXPECT_FALSE(_feedback->enabled());
    EXPECT_EQ(nullptr, _descriptor.runtime_filter(0));
}

// Covers: another driver builds the filter while this one is still measuring.
TEST_F(DeferredLocalRuntimeFilterOperatorTest, FilterBuiltByAnotherDriverIsAppliedByThisOne) {
    auto filter = make_filter(range(0, 100));
    _descriptor.scan_sink()->add_lazy_read(2000L * 5000, 5000);

    for (int i = 0; i < 10; ++i) {
        auto chunk = scan_chunk(4096);
        _scan->eval_runtime_bloom_filters(chunk.get());
        ASSERT_EQ(4096, chunk->num_rows());
        _feedback->observe_lookup(4096, 4096 * 50, 0, 4096 / 20);
    }
    ASSERT_TRUE(_feedback->deferred());

    // The window of another driver decides.
    filter->observe_scan_rows(262144);
    ASSERT_EQ(DeferredRfDecider::State::BUILT, filter->submit(expensive_window()));

    auto chunk = scan_chunk(4096);
    _scan->eval_runtime_bloom_filters(chunk.get());
    EXPECT_EQ(4096, chunk->num_rows());
    _feedback->observe_lookup(4096, 4096 * 50, 0, 4096 / 20);
    EXPECT_FALSE(_feedback->deferred());
    EXPECT_TRUE(_feedback->use_filter());

    chunk = scan_chunk(4096);
    _scan->eval_runtime_bloom_filters(chunk.get());
    EXPECT_LT(chunk->num_rows(), 1000);
    EXPECT_GE(chunk->num_rows(), 410);
}

} // namespace starrocks::pipeline
