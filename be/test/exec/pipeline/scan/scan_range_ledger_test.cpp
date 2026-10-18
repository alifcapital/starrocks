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

#include "exec/pipeline/scan/scan_range_ledger.h"

#include <gtest/gtest.h>

#include <memory>
#include <thread>
#include <vector>

namespace starrocks::pipeline {

using Range = ScanRangeLedger::Range;

// Covers: a range that was taken and has not read its footer is only bytes, and the ledger has no rows for it.
TEST(ScanRangeLedgerTest, TakenRangeWithoutARowCountIsPendingBytes) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    Range range(ledger, 1000);
    auto snapshot = ledger->snapshot();
    EXPECT_EQ(1000, snapshot.pending_bytes);
    EXPECT_EQ(0, snapshot.known_ranges);
    EXPECT_EQ(0, snapshot.unread_rows);

    // Rows can be read before the reader knows its row count, and they count as read.
    range.update(300, -1);
    snapshot = ledger->snapshot();
    EXPECT_EQ(1000, snapshot.pending_bytes);
    EXPECT_EQ(300, snapshot.rows_read);
    EXPECT_EQ(0, snapshot.unread_rows);

    range.close();
    snapshot = ledger->snapshot();
    EXPECT_EQ(0, snapshot.pending_bytes);
    EXPECT_EQ(300, snapshot.rows_read);
}

// Covers: the footer gives the row count, and the unread rows fall as the range reads.
TEST(ScanRangeLedgerTest, RowCountMovesTheRangeFromPendingBytesToUnreadRows) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    Range range(ledger, 1000);
    range.update(0, 5000);
    auto snapshot = ledger->snapshot();
    EXPECT_EQ(0, snapshot.pending_bytes);
    EXPECT_EQ(1, snapshot.known_ranges);
    EXPECT_EQ(5000, snapshot.known_rows);
    EXPECT_EQ(1000, snapshot.known_bytes);
    EXPECT_EQ(5000, snapshot.unread_rows);

    range.update(1200, 5000);
    snapshot = ledger->snapshot();
    EXPECT_EQ(3800, snapshot.unread_rows);
    EXPECT_EQ(1200, snapshot.rows_read);
    // The same update again changes nothing.
    range.update(1200, 5000);
    EXPECT_EQ(3800, ledger->snapshot().unread_rows);
    EXPECT_EQ(1200, ledger->snapshot().rows_read);

    // A reader that learns its row count after it has read some rows has these rows behind it already.
    Range late(ledger, 500);
    late.update(100, -1);
    late.update(400, 1000);
    snapshot = ledger->snapshot();
    EXPECT_EQ(3800 + 600, snapshot.unread_rows);
    EXPECT_EQ(2, snapshot.known_ranges);
}

// Covers: the rows that a range did not read when it was closed are never read, so they are not left to scan. This is
//         the scan that reached its limit, or a runtime filter that dropped the row groups.
TEST(ScanRangeLedgerTest, ClosingARangeLeavesItsUnreadRowsOutOfTheRowsToScan) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    Range first(ledger, 1000);
    Range second(ledger, 1000);
    first.update(10, 5000);
    second.update(10, 5000);
    EXPECT_EQ(9980, ledger->snapshot().unread_rows);

    first.update(2000, 5000);
    first.close();
    auto snapshot = ledger->snapshot();
    EXPECT_EQ(4990, snapshot.unread_rows);
    // What the closed range read stays in the totals, and so does its row count, for the averages.
    EXPECT_EQ(2010, snapshot.rows_read);
    EXPECT_EQ(2, snapshot.known_ranges);
    EXPECT_EQ(10000, snapshot.known_rows);

    // Closing twice, and updating a closed range, change nothing.
    first.close();
    first.update(3000, 5000);
    EXPECT_EQ(snapshot.unread_rows, ledger->snapshot().unread_rows);
    EXPECT_EQ(snapshot.rows_read, ledger->snapshot().rows_read);

    second.close();
    EXPECT_EQ(0, ledger->snapshot().unread_rows);
}

TEST(ScanRangeLedgerTest, DestructorClosesTheRange) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    {
        Range open(ledger, 700);
        Range known(ledger, 300);
        known.update(5, 100);
        EXPECT_EQ(700, ledger->snapshot().pending_bytes);
        EXPECT_EQ(95, ledger->snapshot().unread_rows);
    }
    const auto snapshot = ledger->snapshot();
    EXPECT_EQ(0, snapshot.pending_bytes);
    EXPECT_EQ(0, snapshot.unread_rows);
}

// Covers: a scan without a ledger writes nothing and does not crash.
TEST(ScanRangeLedgerTest, RangeWithoutALedgerDoesNothing) {
    Range range(nullptr, 1000);
    range.update(10, 100);
    range.close();
}

TEST(ScanRangeLedgerTest, ChunkSourcesOfDifferentThreadsAddUp) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    constexpr int kThreads = 8;
    constexpr int kRangesPerThread = 50;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&]() {
            for (int i = 0; i < kRangesPerThread; ++i) {
                Range range(ledger, 100);
                range.update(0, 1000);
                for (int rows = 100; rows <= 1000; rows += 100) range.update(rows, 1000);
            }
        });
    }
    for (auto& thread : threads) thread.join();
    const auto snapshot = ledger->snapshot();
    EXPECT_EQ(0, snapshot.unread_rows);
    EXPECT_EQ(0, snapshot.pending_bytes);
    EXPECT_EQ(kThreads * kRangesPerThread, snapshot.known_ranges);
    EXPECT_EQ(int64_t(kThreads) * kRangesPerThread * 1000, snapshot.rows_read);
    EXPECT_EQ(int64_t(kThreads) * kRangesPerThread * 1000, snapshot.known_rows);
}

// Covers: nothing is known about a scan whose readers do not know their rows, and the estimate says so.
TEST(ScanRangeLedgerTest, EstimateIsUnknownWithoutARangeThatKnowsItsRows) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    Range range(ledger, 1000);
    range.update(200, -1);
    const auto estimate = estimate_remaining_raw_rows(ledger->snapshot(), 5);
    EXPECT_FALSE(estimate.known);
    EXPECT_EQ(200, estimate.rows_read);
    EXPECT_EQ(0, estimate.total());
}

// Covers: the estimate sums the three kinds of ranges that have rows to read: the opened ones with a row count, the
//         taken ones without it, which it prices by the rows per byte, and the ones that nobody took, which it prices by
//         the rows of a range.
TEST(ScanRangeLedgerTest, EstimateSumsOpenPendingAndNotStartedRanges) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    // Two files with 1000 rows in 100 bytes each; one of them has read 400 rows.
    Range first(ledger, 100);
    Range second(ledger, 100);
    first.update(400, 1000);
    second.update(0, 1000);
    // A file whose footer is not read: 50 bytes.
    Range pending(ledger, 50);

    const auto estimate = estimate_remaining_raw_rows(ledger->snapshot(), 3);

    EXPECT_TRUE(estimate.known);
    EXPECT_EQ(400, estimate.rows_read);
    EXPECT_EQ(600 + 1000, estimate.open_ranges);
    // 50 bytes at 2000 rows per 200 bytes.
    EXPECT_EQ(500, estimate.pending_ranges);
    // 3 ranges at 1000 rows per range.
    EXPECT_EQ(3000, estimate.not_started_ranges);
    EXPECT_EQ(1600 + 500 + 3000, estimate.total());
}

// Covers: four large files that were taken at the start. The queue has none left, and the files have all their rows.
TEST(ScanRangeLedgerTest, EstimateOfLargeFilesTakenAtTheStartIsTheirUnreadRows) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    std::vector<std::unique_ptr<Range>> ranges;
    for (int i = 0; i < 4; ++i) {
        ranges.push_back(std::make_unique<Range>(ledger, 1L << 30));
        ranges.back()->update(60000, 50000000);
    }
    const auto estimate = estimate_remaining_raw_rows(ledger->snapshot(), 0);
    EXPECT_TRUE(estimate.known);
    EXPECT_EQ(4 * (50000000 - 60000), estimate.total());
    EXPECT_EQ(0, estimate.not_started_ranges);
    EXPECT_EQ(0, estimate.pending_ranges);
}

TEST(ScanRangeLedgerTest, RangesWithoutBytesGiveNoRowsPerByte) {
    auto ledger = std::make_shared<ScanRangeLedger>();
    Range known(ledger, 0);
    known.update(0, 1000);
    Range pending(ledger, 500);
    const auto estimate = estimate_remaining_raw_rows(ledger->snapshot(), 0);
    EXPECT_TRUE(estimate.known);
    EXPECT_EQ(0, estimate.pending_ranges);
    EXPECT_EQ(1000, estimate.open_ranges);
}

} // namespace starrocks::pipeline
