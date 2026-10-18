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

#include <atomic>
#include <cstdint>

namespace starrocks {

// A scan reader reports here what a row that passed all its conditions still costs the reader. A local runtime
// filter that is built after the scan has started uses these numbers to estimate how much work the filter saves
// for every row that it drops. All readers of one filter write to the same sink, so the counters are atomic.
class RuntimeFilterScanSink {
public:
    // The reader spent ns on the lazy columns of rows rows that passed all conditions.
    void add_lazy_read(int64_t ns, int64_t rows) {
        _lazy_read_ns.fetch_add(ns, std::memory_order_relaxed);
        _lazy_read_rows.fetch_add(rows, std::memory_order_relaxed);
    }

    int64_t lazy_read_ns() const { return _lazy_read_ns.load(std::memory_order_relaxed); }
    int64_t lazy_read_rows() const { return _lazy_read_rows.load(std::memory_order_relaxed); }

private:
    std::atomic<int64_t> _lazy_read_ns{0};
    std::atomic<int64_t> _lazy_read_rows{0};
};

} // namespace starrocks
