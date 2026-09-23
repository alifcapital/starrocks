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

#include "exec/pipeline/scan/priority_morsel_queue.h"

#include "exec/pipeline/scan/morsel.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gutil/casts.h"

namespace starrocks::pipeline {

PriorityMorselQueue::Entry PriorityMorselQueue::make_entry(MorselPtr&& morsel) {
    Entry e;
    e.seq = _seq++;
    bool has_null = false;
    bool all_null = false;
    bool has_bound = false;
    int64_t key = 0;

    auto* scan_morsel = down_cast<ScanMorsel*>(morsel.get());
    TScanRange* scan_range = scan_morsel->get_scan_range();
    if (scan_range != nullptr && scan_range->__isset.hdfs_scan_range &&
        scan_range->hdfs_scan_range.__isset.min_max_values) {
        const auto& min_max_values = scan_range->hdfs_scan_range.min_max_values;
        auto it = min_max_values.find(_reorder_slot_id);
        if (it != min_max_values.end()) {
            const TExprMinMaxValue& v = it->second;
            has_null = v.has_null;
            all_null = v.all_null;
            if (!all_null) {
                // Raw int64 endpoint (see class comment): max for DESC, min for ASC.
                if (_desc && v.__isset.max_int_value) {
                    key = v.max_int_value;
                    has_bound = true;
                } else if (!_desc && v.__isset.min_int_value) {
                    key = v.min_int_value;
                    has_bound = true;
                }
            }
        }
    }

    if (_nulls_first && (all_null || has_null)) {
        e.rank = 0; // nulls lead -> serve first
    } else if (has_bound) {
        e.rank = 1;
        e.key = key;
    } else {
        e.rank = 2; // no usable bound -> serve last
    }
    e.morsel = std::move(morsel);
    return e;
}

Status PriorityMorselQueue::append_morsels(Morsels&& morsels) {
    std::lock_guard<std::mutex> l(_mutex);
    const int64_t added = static_cast<int64_t>(morsels.size());
    for (auto& m : morsels) {
        auto entry = make_entry(std::move(m));
        if (entry.rank == 1) {
            _eligible_morsels.fetch_add(1, std::memory_order_relaxed);
        } else {
            _no_bound_morsels.fetch_add(1, std::memory_order_relaxed);
        }
        _set.insert(std::move(entry));
    }
    _size += added;
    return Status::OK();
}

StatusOr<MorselPtr> PriorityMorselQueue::try_get() {
    std::lock_guard<std::mutex> l(_mutex);
    if (_size.load(std::memory_order_relaxed) == 0) {
        return nullptr;
    }
    auto it = _set.begin();
    MorselPtr ret = std::move(it->morsel);
    _set.erase(it);
    _size -= 1;
    if (_ticket_checker != nullptr && ret->has_owner_id() && !ret->is_ticket_checker_entered()) {
        ret->set_ticket_checker_entered(true);
        _ticket_checker->enter(ret->owner_id(), ret->is_last_split());
    }
    return ret;
}

void PriorityMorselQueue::unget(MorselPtr&& morsel) {
    std::lock_guard<std::mutex> l(_mutex);
    _set.insert(make_entry(std::move(morsel)));
    _size += 1;
}

} // namespace starrocks::pipeline
