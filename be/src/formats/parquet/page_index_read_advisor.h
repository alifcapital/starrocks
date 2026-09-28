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
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

namespace starrocks::parquet {

// Scan-local performance feedback, never a row-elimination proof. A skipped
// index still leaves the footer and ordinary row predicates in force.
class PageIndexReadAdvisor {
public:
    bool should_read(const std::string& predicate) {
        std::lock_guard lock(_mutex);
        auto it = _history.find(predicate);
        if (it == _history.end()) return true;
        if (it->second.skip_remaining == 0) return true;
        --it->second.skip_remaining;
        return false;
    }

    void observe(const std::string& predicate, bool removed_rows) {
        std::lock_guard lock(_mutex);
        auto it = _history.find(predicate);
        if (it == _history.end()) {
            // A stream of tightening RF bounds must not grow query memory
            // without limit. Forgetting feedback only enables more index IO.
            if (_history.size() == 64) _history.erase(_history.begin());
            it = _history.emplace(predicate, History{}).first;
        }
        auto& h = it->second;
        if (removed_rows) {
            h = {};
        } else if (++h.misses >= 2) {
            h.skip_budget = std::min(32u, std::max(1u, h.skip_budget * 2));
            h.skip_remaining = h.skip_budget;
            h.misses = 2;
        }
    }

private:
    struct History {
        uint32_t misses = 0;
        uint32_t skip_budget = 0;
        uint32_t skip_remaining = 0;
    };
    std::mutex _mutex;
    std::unordered_map<std::string, History> _history;
};

} // namespace starrocks::parquet
