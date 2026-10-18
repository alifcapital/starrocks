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

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <utility>

namespace starrocks::pipeline {

// What the scan ranges that were taken from the queue still have to give. A scan that reads a few large files takes all
// of them at the start, so the queue says that nothing is left, while most of the rows are still in the files. A
// chunk source writes the ledger from its IO task as it reads, and the drivers that estimate the rows left to scan read
// it. The ledger counts rows as the storage counts them: the rows that the readers read before any condition of the
// scan is applied.
class ScanRangeLedger {
public:
    struct Snapshot {
        // Rows that the opened ranges with a row count have not read yet.
        int64_t unread_rows = 0;
        // The ranges whose row count was known when they were opened: how many, and their rows and bytes.
        int64_t known_ranges = 0;
        int64_t known_rows = 0;
        int64_t known_bytes = 0;
        // Bytes of the ranges that were taken and whose row count is not known yet, for example because the reader has
        // not read the footer of the file.
        int64_t pending_bytes = 0;
        // Rows that all ranges have read.
        int64_t rows_read = 0;
    };

    Snapshot snapshot() const {
        Snapshot result;
        result.unread_rows = _unread_rows.load(std::memory_order_relaxed);
        result.known_ranges = _known_ranges.load(std::memory_order_relaxed);
        result.known_rows = _known_rows.load(std::memory_order_relaxed);
        result.known_bytes = _known_bytes.load(std::memory_order_relaxed);
        result.pending_bytes = _pending_bytes.load(std::memory_order_relaxed);
        result.rows_read = _rows_read.load(std::memory_order_relaxed);
        return result;
    }

    // One scan range, from the moment a chunk source takes it until the chunk source is closed. One IO task at a time
    // writes it. A range without a ledger, which a scan that has none uses, writes nothing.
    class Range {
    public:
        Range(std::shared_ptr<ScanRangeLedger> ledger, int64_t bytes)
                : _ledger(std::move(ledger)), _bytes(std::max<int64_t>(0, bytes)) {
            if (_ledger == nullptr) {
                _closed = true;
                return;
            }
            _ledger->_pending_bytes.fetch_add(_bytes, std::memory_order_relaxed);
        }
        ~Range() { close(); }

        Range(const Range&) = delete;
        Range& operator=(const Range&) = delete;

        // rows_read is what the range has read so far, expected_rows is what it will read when it is read to its end,
        // or negative when the reader does not know it yet.
        void update(int64_t rows_read, int64_t expected_rows) {
            if (_closed) return;
            auto& ledger = *_ledger;
            const int64_t delta = rows_read - _rows_read;
            if (delta > 0) {
                ledger._rows_read.fetch_add(delta, std::memory_order_relaxed);
                _rows_read = rows_read;
            }
            if (!_known && expected_rows >= 0) {
                _known = true;
                ledger._pending_bytes.fetch_sub(_bytes, std::memory_order_relaxed);
                ledger._known_ranges.fetch_add(1, std::memory_order_relaxed);
                ledger._known_rows.fetch_add(expected_rows, std::memory_order_relaxed);
                ledger._known_bytes.fetch_add(_bytes, std::memory_order_relaxed);
                _expected_rows = expected_rows;
            }
            if (_known) {
                const int64_t unread = std::max<int64_t>(0, _expected_rows - _rows_read);
                ledger._unread_rows.fetch_add(unread - _unread_rows, std::memory_order_relaxed);
                _unread_rows = unread;
            }
        }

        // The rows that the range did not read when it was closed, for example because the scan reached its limit or a
        // runtime filter dropped the row groups, will not be read, so they are not left to scan.
        void close() {
            if (_closed) return;
            _closed = true;
            if (_known) {
                _ledger->_unread_rows.fetch_sub(_unread_rows, std::memory_order_relaxed);
                _unread_rows = 0;
            } else {
                _ledger->_pending_bytes.fetch_sub(_bytes, std::memory_order_relaxed);
            }
        }

    private:
        std::shared_ptr<ScanRangeLedger> _ledger;
        const int64_t _bytes;
        bool _known = false;
        bool _closed = false;
        int64_t _expected_rows = 0;
        int64_t _rows_read = 0;
        int64_t _unread_rows = 0;
    };

private:
    std::atomic<int64_t> _unread_rows{0};
    std::atomic<int64_t> _known_ranges{0};
    std::atomic<int64_t> _known_rows{0};
    std::atomic<int64_t> _known_bytes{0};
    std::atomic<int64_t> _pending_bytes{0};
    std::atomic<int64_t> _rows_read{0};
};

// The rows that the scan has yet to read, as the storage counts them, and where the number comes from.
struct RemainingRawRows {
    // False when no opened range has a row count, so the ledger cannot tell.
    bool known = false;
    // Rows that all ranges have read.
    int64_t rows_read = 0;
    // The rows of the opened ranges that they have not read yet, from the row counts of their files.
    int64_t open_ranges = 0;
    // The rows of the taken ranges whose row count is not known yet: their bytes times the rows per byte of the ranges
    // whose row count is known.
    int64_t pending_ranges = 0;
    // The rows of the ranges that nobody has taken: their number times the rows of a range whose row count is known.
    int64_t not_started_ranges = 0;

    int64_t total() const { return open_ranges + pending_ranges + not_started_ranges; }
};

// We cannot count the rows of a range that is not opened, so we estimate them from the ranges that are: a range whose
// reader knows its rows gives the rows per byte and the rows per range. The result is a lower bound while the queue gets
// more ranges, because it counts only the ranges that the queue has now.
inline RemainingRawRows estimate_remaining_raw_rows(const ScanRangeLedger::Snapshot& ledger,
                                                    int64_t ranges_not_started) {
    RemainingRawRows result;
    result.rows_read = ledger.rows_read;
    if (ledger.known_ranges <= 0) return result;
    result.known = true;
    result.open_ranges = ledger.unread_rows;
    if (ledger.known_bytes > 0 && ledger.pending_bytes > 0) {
        result.pending_ranges =
                static_cast<int64_t>(static_cast<double>(ledger.pending_bytes) *
                                     static_cast<double>(ledger.known_rows) / static_cast<double>(ledger.known_bytes));
    }
    if (ranges_not_started > 0) {
        result.not_started_ranges =
                static_cast<int64_t>(static_cast<double>(ranges_not_started) * static_cast<double>(ledger.known_rows) /
                                     static_cast<double>(ledger.known_ranges));
    }
    return result;
}

} // namespace starrocks::pipeline
