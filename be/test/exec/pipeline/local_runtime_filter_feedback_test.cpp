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

#include <string>
#include <vector>

namespace starrocks::pipeline {
namespace {
void chunks(LocalRuntimeFilterFeedback& feedback, int count, int64_t passed, int64_t filter_ns, int64_t lookup_ns) {
    for (int i = 0; i < count; ++i) {
        feedback.observe_filter(4096, passed, filter_ns);
        feedback.observe_lookup(passed, lookup_ns);
    }
}

// A filter that the build side kept back. It records the windows that it gets and decides as the test says.
class FakeDecider final : public DeferredRfDecider {
public:
    explicit FakeDecider(int32_t id) : _id(id) {}

    int32_t filter_id() const override { return _id; }
    State state() const override { return _state; }
    State submit(const DeferredRfWindow& window) override {
        windows.push_back(window);
        if (_state == State::WAITING) _state = on_submit;
        return _state;
    }
    void observe_scan_rows(int64_t rows) override { scan_rows += rows; }
    void cancel() override {
        ++cancels;
        if (_state == State::WAITING) _state = State::REFUSED;
    }
    bool input_changed() const override { return changed; }
    Status error() const override {
        return _state == State::FAILED ? Status::InternalError("build failed") : Status::OK();
    }
    void set_state(State state) { _state = state; }

    State on_submit = State::WAITING;
    bool changed = false;
    std::vector<DeferredRfWindow> windows;
    int64_t scan_rows = 0;
    int cancels = 0;

private:
    int32_t _id;
    State _state = State::WAITING;
};

using State = DeferredRfDecider::State;

// What the scan reports about its ranges. A test changes it while the driver runs.
struct ScanProgress {
    int64_t taken = 3;
    int64_t remaining = 9;
    int64_t version = 0;
};

// A feedback of a driver whose scan has one filter that was kept back.
void defer(LocalRuntimeFilterFeedback& feedback, const std::shared_ptr<FakeDecider>& decider,
           std::shared_ptr<ScanProgress> progress = std::make_shared<ScanProgress>()) {
    feedback.set_deferred_resolver([decider]() { return std::vector<std::shared_ptr<DeferredRfDecider>>{decider}; },
                                   [progress](DeferredRfWindow* window) {
                                       window->morsels_taken = progress->taken;
                                       window->morsels_remaining = progress->remaining;
                                       window->ranges_version = progress->version;
                                   });
    feedback.resolve_deferred_once();
}

// The scan emits a chunk without a filter and the JOIN looks all its rows up at once.
void deferred_chunks(LocalRuntimeFilterFeedback& feedback, int count) {
    for (int i = 0; i < count; ++i) {
        feedback.observe_filter(4096, 4096, 0, 100);
        feedback.observe_lookup(4096, 4096 * 50, 4096 * 5, 4096 / 10);
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

// Covers: while the filter is kept back the driver measures the JOIN without it, the first closed window goes to the
//         filter with everything that the operators measured, and a filter that is built starts the usual
//         measurement of the filter.
TEST(LocalRuntimeFilterFeedbackTest, DeferredWindowGoesToTheFilterAndBuiltFilterStartsTheUsualMeasurement) {
    auto decider = std::make_shared<FakeDecider>(7);
    decider->on_submit = State::BUILT;
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    ASSERT_TRUE(feedback.deferred());
    EXPECT_FALSE(feedback.use_filter());

    deferred_chunks(feedback, 63);
    EXPECT_TRUE(decider->windows.empty());
    EXPECT_TRUE(feedback.deferred());
    deferred_chunks(feedback, 1);

    ASSERT_EQ(1, decider->windows.size());
    const auto& window = decider->windows[0];
    EXPECT_EQ(64, window.chunks);
    EXPECT_EQ(262144, window.rows);
    EXPECT_EQ(262144 * 50, window.lookup_ns);
    EXPECT_EQ(262144 * 5, window.intermediate_ns);
    EXPECT_EQ(64 * 100, window.scan_ns);
    EXPECT_EQ(64 * (4096 / 10), window.probe_output_rows);
    EXPECT_EQ(3, window.morsels_taken);
    EXPECT_EQ(9, window.morsels_remaining);
    EXPECT_EQ(262144, decider->scan_rows);

    EXPECT_TRUE(feedback.enabled());
    EXPECT_FALSE(feedback.deferred());
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_FALSE(feedback.needs_drain());
    EXPECT_EQ(0, feedback.totals().decisions);
    EXPECT_EQ(262144, feedback.totals().input_rows[0]);

    // The window of the deferred phase does not count in the first window with the filter.
    chunks(feedback, 64, 40, 1000, 1000);
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_EQ(1, feedback.totals().decisions);
    EXPECT_EQ(1, decider->windows.size());
}

TEST(LocalRuntimeFilterFeedbackTest, RefusedFilterTurnsTheFeedbackOff) {
    auto decider = std::make_shared<FakeDecider>(7);
    decider->on_submit = State::REFUSED;
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    deferred_chunks(feedback, 64);

    EXPECT_FALSE(feedback.enabled());
    // The refusal is a decision, and the owners of the feedback must not report it as broken accounting.
    EXPECT_TRUE(feedback.deferral_refused());
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_FALSE(feedback.needs_drain());
}

// Covers: a window that did not build the filter, because it is not built yet or another driver is deciding, is
//         followed by a window after the usual wait of the adaptive filters, so the pause of the scan at the end of a
//         window and the short chunk before it happen rarely.
TEST(LocalRuntimeFilterFeedbackTest, FilterThatIsBeingDecidedElsewhereKeepsTheWindowsComing) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    deferred_chunks(feedback, 64);
    ASSERT_EQ(1, decider->windows.size());
    EXPECT_TRUE(feedback.deferred());
    EXPECT_TRUE(feedback.enabled());
    EXPECT_FALSE(feedback.use_filter());

    // The window starts again from nothing, and the wait is 256 chunks.
    deferred_chunks(feedback, 255);
    EXPECT_EQ(1, decider->windows.size());
    deferred_chunks(feedback, 1);
    ASSERT_EQ(2, decider->windows.size());
    EXPECT_EQ(256, decider->windows[1].chunks);
    EXPECT_EQ(256 * 4096, decider->windows[1].rows);

    decider->on_submit = State::BUILT;
    deferred_chunks(feedback, 1024);
    EXPECT_FALSE(feedback.deferred());
    EXPECT_TRUE(feedback.use_filter());
}

// Covers: the waits of a waiting filter are the waits of the adaptive filters: 256 chunks, four times longer while the
//         gain per row stays the same, up to 16384 chunks. A filter that waits does not turn the feedback off.
TEST(LocalRuntimeFilterFeedbackTest, WindowsOfAWaitingFilterFollowTheScheduleOfTheAdaptiveFilters) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    for (int expected : {64, 256, 1024, 4096, 16384, 16384, 16384}) {
        deferred_chunks(feedback, expected);
        ASSERT_EQ(expected, decider->windows.back().chunks);
        ASSERT_TRUE(feedback.enabled());
        ASSERT_TRUE(feedback.deferred());
        ASSERT_FALSE(feedback.deferral_refused());
    }
    EXPECT_EQ(7, decider->windows.size());
    EXPECT_EQ(State::WAITING, decider->state());
    EXPECT_EQ(0, feedback.totals().decisions);
}

// Covers: a window shows a gain per row that differs from the one before, so the wait goes back to 256 chunks.
TEST(LocalRuntimeFilterFeedbackTest, ChangedInputBringsTheWaitBackToTheShortest) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    for (int expected : {64, 256, 1024}) deferred_chunks(feedback, expected);
    ASSERT_EQ(3, decider->windows.size());

    decider->changed = true;
    deferred_chunks(feedback, 4096);
    ASSERT_EQ(4, decider->windows.size());
    EXPECT_EQ(4096, decider->windows[3].chunks);
    // The next window comes after 256 chunks, not after 16384.
    deferred_chunks(feedback, 256);
    ASSERT_EQ(5, decider->windows.size());
    EXPECT_EQ(256, decider->windows[4].chunks);

    decider->changed = false;
    deferred_chunks(feedback, 256);
    ASSERT_EQ(6, decider->windows.size());
    deferred_chunks(feedback, 1024);
    EXPECT_EQ(7, decider->windows.size());
}

// Covers: the scan gets new scan ranges after a window did not build the filter. The filter is asked again with the
//         measurement of that window and the new progress of the scan, and the driver does not wait for the next
//         window to close.
TEST(LocalRuntimeFilterFeedbackTest, NewScanRangesAskTheFilterAgainAtOnce) {
    auto decider = std::make_shared<FakeDecider>(7);
    auto progress = std::make_shared<ScanProgress>();
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider, progress);
    deferred_chunks(feedback, 64);
    ASSERT_EQ(1, decider->windows.size());
    EXPECT_EQ(9, decider->windows[0].morsels_remaining);

    progress->remaining = 90;
    progress->version = 1;
    deferred_chunks(feedback, 1);

    ASSERT_EQ(2, decider->windows.size());
    EXPECT_EQ(DeferredRfTrigger::WINDOW, decider->windows[0].trigger);
    EXPECT_EQ(DeferredRfTrigger::NEW_RANGES, decider->windows[1].trigger);
    EXPECT_EQ(90, decider->windows[1].morsels_remaining);
    EXPECT_EQ(1, decider->windows[1].ranges_version);
    // It is the measurement of the first window, not a new one.
    EXPECT_EQ(decider->windows[0].chunks, decider->windows[1].chunks);
    EXPECT_EQ(decider->windows[0].rows, decider->windows[1].rows);
    EXPECT_EQ(decider->windows[0].lookup_ns, decider->windows[1].lookup_ns);
    EXPECT_EQ(decider->windows[0].probe_output_rows, decider->windows[1].probe_output_rows);
    EXPECT_TRUE(feedback.deferred());

    // The same version does not ask again.
    deferred_chunks(feedback, 10);
    EXPECT_EQ(2, decider->windows.size());

    progress->version = 2;
    decider->on_submit = State::BUILT;
    deferred_chunks(feedback, 1);
    ASSERT_EQ(3, decider->windows.size());
    EXPECT_FALSE(feedback.deferred());
    EXPECT_TRUE(feedback.use_filter());
}

// Covers: before a window has closed the filter has not been asked, so the arrival of ranges has nothing to repeat.
TEST(LocalRuntimeFilterFeedbackTest, NewScanRangesBeforeTheFirstWindowAskNothing) {
    auto decider = std::make_shared<FakeDecider>(7);
    auto progress = std::make_shared<ScanProgress>();
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider, progress);
    deferred_chunks(feedback, 10);
    progress->version = 1;
    deferred_chunks(feedback, 10);
    feedback.request_retry();
    EXPECT_TRUE(decider->windows.empty());
    // The first window takes the progress as it is by then.
    deferred_chunks(feedback, 44);
    ASSERT_EQ(1, decider->windows.size());
    EXPECT_EQ(1, decider->windows[0].ranges_version);
}

TEST(LocalRuntimeFilterFeedbackTest, RetryDoesNothingOnceTheFilterIsDecidedOrTheFeedbackStopped) {
    auto decider = std::make_shared<FakeDecider>(7);
    auto progress = std::make_shared<ScanProgress>();
    decider->on_submit = State::BUILT;
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider, progress);
    deferred_chunks(feedback, 64);
    ASSERT_EQ(1, decider->windows.size());
    ASSERT_FALSE(feedback.deferred());

    progress->version = 1;
    feedback.request_retry();
    deferred_chunks(feedback, 10);
    EXPECT_EQ(1, decider->windows.size());

    auto waiting = std::make_shared<FakeDecider>(8);
    LocalRuntimeFilterFeedback stopped;
    defer(stopped, waiting, progress);
    deferred_chunks(stopped, 64);
    ASSERT_EQ(1, waiting->windows.size());
    stopped.stop();
    progress->version = 2;
    stopped.request_retry();
    EXPECT_EQ(1, waiting->windows.size());
}

// Covers: the build of a filter returns an error. The feedback ends, and the operators of the driver get the error to
//         return to the query.
TEST(LocalRuntimeFilterFeedbackTest, FailedBuildGivesTheErrorToTheDriver) {
    auto decider = std::make_shared<FakeDecider>(7);
    decider->on_submit = State::FAILED;
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    EXPECT_FALSE(feedback.deferral_failed());
    deferred_chunks(feedback, 64);

    EXPECT_TRUE(feedback.deferral_failed());
    EXPECT_FALSE(feedback.enabled());
    // It is not broken accounting either.
    EXPECT_TRUE(feedback.deferral_refused());
    const Status status = feedback.deferral_status();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.message().find("build failed"));
}

// Covers: another driver's build fails, and this driver finds it with its next chunk.
TEST(LocalRuntimeFilterFeedbackTest, FailedBuildOfAnotherDriverIsSeenAtTheNextChunk) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    deferred_chunks(feedback, 10);
    ASSERT_FALSE(feedback.deferral_failed());

    decider->set_state(State::FAILED);
    deferred_chunks(feedback, 1);

    EXPECT_TRUE(feedback.deferral_failed());
    EXPECT_FALSE(feedback.deferral_status().ok());
}

// Covers: another driver builds the filter while this driver has rows in its operators that were looked up without
//         the filter.
TEST(LocalRuntimeFilterFeedbackTest, FilterBuiltElsewhereWaitsForTheRowsInFlight) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    deferred_chunks(feedback, 5);
    feedback.observe_filter(4096, 4096, 0, 100);
    ASSERT_EQ(4096, feedback.outstanding_rows());

    decider->set_state(State::BUILT);
    feedback.observe_filter(4096, 4096, 0, 100);
    EXPECT_EQ(8192, feedback.outstanding_rows());
    // The driver drains the rows before it measures anything with the filter.
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_TRUE(feedback.needs_drain());
    EXPECT_FALSE(feedback.deferred());
    feedback.observe_lookup(4096, 4096 * 50);
    EXPECT_TRUE(feedback.needs_drain());
    feedback.observe_lookup(4096, 4096 * 50);
    EXPECT_FALSE(feedback.needs_drain());
    EXPECT_EQ(0, feedback.outstanding_rows());
    EXPECT_EQ(0, feedback.totals().decisions);

    // The first window with the filter holds only chunks that were filtered.
    chunks(feedback, 64, 40, 1000, 1000);
    EXPECT_EQ(1, feedback.totals().decisions);
    EXPECT_TRUE(feedback.use_filter());
}

TEST(LocalRuntimeFilterFeedbackTest, FilterBuiltBeforeTheFirstChunkIsUsedAtOnce) {
    auto decider = std::make_shared<FakeDecider>(7);
    decider->set_state(State::BUILT);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    EXPECT_FALSE(feedback.deferred());
    EXPECT_TRUE(feedback.use_filter());
    EXPECT_FALSE(feedback.needs_drain());
    chunks(feedback, 64, 40, 1000, 1000);
    EXPECT_EQ(1, feedback.totals().decisions);
    EXPECT_TRUE(decider->windows.empty());
}

TEST(LocalRuntimeFilterFeedbackTest, FilterRefusedBeforeTheFirstChunkTurnsTheFeedbackOff) {
    auto decider = std::make_shared<FakeDecider>(7);
    decider->set_state(State::REFUSED);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    EXPECT_FALSE(feedback.enabled());
    EXPECT_TRUE(feedback.deferral_refused());
}

TEST(LocalRuntimeFilterFeedbackTest, WithoutDeferredFiltersTheUsualMeasurementRuns) {
    LocalRuntimeFilterFeedback feedback;
    feedback.set_deferred_resolver([]() { return std::vector<std::shared_ptr<DeferredRfDecider>>(); }, nullptr);
    feedback.resolve_deferred_once();
    EXPECT_FALSE(feedback.deferred());
    EXPECT_TRUE(feedback.use_filter());
    chunks(feedback, 64, 40, 1000, 1000);
    EXPECT_EQ(1, feedback.totals().decisions);
    EXPECT_FALSE(feedback.awaiting_deferred(7));
}

TEST(LocalRuntimeFilterFeedbackTest, AwaitingDeferredFollowsTheStateOfTheFilter) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    EXPECT_TRUE(feedback.awaiting_deferred(7));
    EXPECT_FALSE(feedback.awaiting_deferred(8));
    decider->set_state(State::REFUSED);
    EXPECT_TRUE(feedback.awaiting_deferred(7));
    decider->set_state(State::BUILT);
    EXPECT_FALSE(feedback.awaiting_deferred(7));
}

TEST(LocalRuntimeFilterFeedbackTest, DeferredWindowNeedsEnoughChunksAndRows) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    for (int i = 0; i < 100; ++i) {
        feedback.observe_filter(10, 10, 0, 1);
        feedback.observe_lookup(10, 100, 0, 1);
    }
    EXPECT_TRUE(decider->windows.empty());
    EXPECT_FALSE(feedback.needs_drain());
    // The window ends only after every row emitted has completed its lookup.
    for (int i = 0; i < 64; ++i) feedback.observe_filter(4096, 4096, 0, 1);
    EXPECT_TRUE(feedback.needs_drain());
    EXPECT_TRUE(decider->windows.empty());
    feedback.observe_lookup(64 * 4096, 1000);
    EXPECT_EQ(1, decider->windows.size());
}

TEST(LocalRuntimeFilterFeedbackTest, BrokenAccountingIsNotTakenForARefusal) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    feedback.observe_lookup(1, 0);
    EXPECT_FALSE(feedback.enabled());
    EXPECT_FALSE(feedback.deferral_refused());
}

// Covers: the JOIN leaves the path that the feedback measures, for example because it spills. A filter that is
//         not decided is refused, because no window will measure it, and the scan goes on without the feedback.
TEST(LocalRuntimeFilterFeedbackTest, StopRefusesTheUndecidedFilterAndTurnsTheFeedbackOff) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    deferred_chunks(feedback, 10);
    ASSERT_TRUE(feedback.deferred());

    feedback.stop();

    EXPECT_EQ(State::REFUSED, decider->state());
    EXPECT_EQ(1, decider->cancels);
    EXPECT_TRUE(feedback.stopped());
    EXPECT_FALSE(feedback.enabled());
    // A stop is neither a refusal by a window nor broken accounting.
    EXPECT_FALSE(feedback.deferral_refused());
    EXPECT_TRUE(decider->windows.empty());
    EXPECT_TRUE(feedback.use_filter());
}

// Covers: a filter that is built stays and is evaluated natively after the stop, also when the feedback had
//         switched it off because it was expensive.
TEST(LocalRuntimeFilterFeedbackTest, StopKeepsABuiltFilterInUse) {
    auto decider = std::make_shared<FakeDecider>(7);
    decider->set_state(State::BUILT);
    LocalRuntimeFilterFeedback feedback;
    defer(feedback, decider);
    chunks(feedback, 64, 1024, 100000, 10000);
    ASSERT_FALSE(feedback.use_filter());

    feedback.stop();

    EXPECT_EQ(State::BUILT, decider->state());
    EXPECT_TRUE(feedback.stopped());
    EXPECT_FALSE(feedback.enabled());
    EXPECT_TRUE(feedback.use_filter());
}

// Covers: the driver drains the operators when a window is complete and rows are still waiting for their lookups.
//         After a stop nobody counts these lookups, so the driver must not be asked to drain any more.
TEST(LocalRuntimeFilterFeedbackTest, StopEndsTheDrain) {
    LocalRuntimeFilterFeedback feedback;
    for (int i = 0; i < 64; ++i) feedback.observe_filter(4096, 1024, 100000);
    ASSERT_TRUE(feedback.needs_drain());
    ASSERT_EQ(65536, feedback.outstanding_rows());

    feedback.stop();

    EXPECT_FALSE(feedback.needs_drain());
    // The lookups that arrive afterwards are not accounted, and they do not turn the feedback on again.
    feedback.observe_lookup(65536, 640000);
    EXPECT_FALSE(feedback.enabled());
    EXPECT_FALSE(feedback.needs_drain());
}

// Covers: the filters are asked for at the first chunk, so a JOIN that spills before that still refuses them.
TEST(LocalRuntimeFilterFeedbackTest, StopBeforeTheFirstChunkRefusesTheFilter) {
    auto decider = std::make_shared<FakeDecider>(7);
    LocalRuntimeFilterFeedback feedback;
    feedback.set_deferred_resolver([decider]() { return std::vector<std::shared_ptr<DeferredRfDecider>>{decider}; },
                                   nullptr);

    feedback.stop();

    EXPECT_EQ(State::REFUSED, decider->state());
    EXPECT_FALSE(feedback.enabled());
    // Another stop changes nothing.
    feedback.stop();
    EXPECT_EQ(1, decider->cancels);
}

} // namespace starrocks::pipeline
