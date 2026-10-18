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

#include "formats/parquet/narrow_io_planner.h"

#include <algorithm>
#include <utility>

#include "common/config.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "util/runtime_profile.h"

namespace starrocks::parquet {

// The data behind a dictionary page is deferred only if it is at least this many times the coalesce distance. A deferred
// buffer costs a request of its own when a read needs it, and a request is worth the coalesce distance in bytes.
static constexpr int64_t kDictionaryDeferMinDistances = 4;

NarrowIoPlanner::Chunk* NarrowIoPlanner::add_chunk(int64_t begin, int64_t end, int64_t data_begin,
                                                   ComputeNeed compute) {
    Chunk& chunk = _chunks[begin];
    chunk.begin = begin;
    chunk.end = end;
    chunk.data_begin = data_begin;
    chunk.compute = std::move(compute);
    return &chunk;
}

void NarrowIoPlanner::begin_dictionary_read(int64_t chunk_begin, int64_t dict_end, int64_t chunk_end) {
    if (!config::parquet_offset_index_narrow_io_enable || !config::parquet_offset_index_narrow_io_dict_defer_enable ||
        dict_end <= chunk_begin ||
        chunk_end - dict_end < kDictionaryDeferMinDistances * config::io_coalesce_read_max_distance_size) {
        return;
    }
    // A chunk that is being read, or that has a need from a range, keeps it: its dead bytes stay dead.
    auto it = _chunks.find(chunk_begin);
    if (it != _chunks.end() && (it->second.reading || it->second.resolved)) {
        return;
    }
    Chunk& chunk = _chunks[chunk_begin];
    chunk.begin = chunk_begin;
    chunk.end = chunk_end;
    chunk.reading = true;
    chunk.stale = false;
    chunk.resolved = true;
    chunk.dictionary_read = true;
    chunk.need = Need();
    chunk.need.needed.emplace_back(chunk_begin, dict_end - chunk_begin);
    chunk.need.dead_end = chunk_begin;
    chunk.keep = chunk.need.needed;
}

void NarrowIoPlanner::end_dictionary_read(int64_t chunk_begin) {
    auto it = _chunks.find(chunk_begin);
    if (it != _chunks.end() && it->second.dictionary_read) {
        it->second.dictionary_read = false;
        it->second.reading = false;
    }
}

// The reader says what the range needs when the stream asks about the first buffer of the chunk, and the answer serves
// the other buffers of the same range. The work is a pass over the pages of the range, and it is done for a range at
// most once, and only for a range that loads a buffer.
void NarrowIoPlanner::_resolve(Chunk* chunk) {
    chunk->stale = false;
    if (chunk->compute == nullptr) {
        return;
    }
    Need need;
    chunk->compute(&need);
    chunk->need = std::move(need);
    // The pages in front of the end of the range that the reader has not passed yet are dead, unless the range needs them.
    const int64_t from = std::max(chunk->read_until, chunk->data_begin);
    chunk->range_passed = std::max<int64_t>(0, chunk->need.dead_end - from);
    int64_t needed_in_front = 0;
    for (const IORange& run : chunk->need.needed) {
        const int64_t run_begin = std::max(run.offset, from);
        const int64_t run_end = std::min(run.offset + run.size, chunk->need.dead_end);
        if (run_begin < run_end) {
            needed_in_front += run_end - run_begin;
        }
    }
    chunk->range_dead = chunk->range_passed - needed_in_front;
    chunk->keep = chunk->need.needed;
    if (chunk->need.ahead_end > chunk->need.ahead_begin) {
        chunk->keep.emplace_back(chunk->need.ahead_begin, chunk->need.ahead_end - chunk->need.ahead_begin);
    }
    chunk->resolved = true;
}

// We fear that a scan that needs most pages of a chunk pays a request for each range, when each range loads its pages
// with a buffer of its own. So we defer the pages behind a range only while the chunk shows that it has many dead
// bytes: the share of the bytes in front of the range end that the reader jumped over, or that the range does not need,
// is at least parquet_offset_index_narrow_io_defer_min_dead_ratio. The reader counts the pages of every range, also of
// those that the stream serves from loaded buffers and never asks us about, so a scan that needs all pages stops
// deferring after the first few ranges. A chunk that has shown less than the coalesce distance has no evidence yet, and
// it defers.
bool NarrowIoPlanner::_defer_allowed(const Chunk& chunk) const {
    if (chunk.dictionary_read) {
        return true;
    }
    const int64_t passed = std::max<int64_t>(0, chunk.read_until - chunk.data_begin) + chunk.range_passed;
    if (passed <= 0 || passed < config::io_coalesce_read_max_distance_size) {
        return true;
    }
    const double dead_ratio = static_cast<double>(chunk.dead_bytes + chunk.range_dead) / static_cast<double>(passed);
    return dead_ratio >= config::parquet_offset_index_narrow_io_defer_min_dead_ratio;
}

void NarrowIoPlanner::_collect(int64_t offset, int64_t size, bool defer, std::vector<IORange>* ranges) {
    if (!config::parquet_offset_index_narrow_io_enable || _chunks.empty()) {
        return;
    }
    int64_t unused_ns = 0;
    SCOPED_RAW_TIMER(_stats != nullptr ? &_stats->narrow_io_plan_ns : &unused_ns);
    const int64_t end = offset + size;
    int64_t named = 0;
    // The chunks do not overlap. The last chunk that begins at or before the offset can reach into the window, and the
    // chunks that begin inside the window follow it.
    auto it = _chunks.upper_bound(offset);
    if (it != _chunks.begin()) {
        --it;
    }
    for (; it != _chunks.end() && it->first < end; ++it) {
        Chunk& chunk = it->second;
        if (chunk.end <= offset) {
            continue;
        }
        if (chunk.reading && chunk.stale) {
            _resolve(&chunk);
        }
        if (!chunk.resolved) {
            continue;
        }
        // The dead bytes are in front of the end of the last range, and they stay dead when the range is done. The bytes
        // behind it are deferred only while a range is being read, because only then we know what the range needs.
        int64_t from = chunk.begin;
        int64_t to = chunk.need.dead_end;
        if (defer) {
            if (!chunk.reading || !_defer_allowed(chunk)) {
                continue;
            }
            from = chunk.need.dead_end;
            to = chunk.end;
        }
        // Names the part of [first, last) that lies in the window and between from and to.
        auto name = [&](int64_t first, int64_t last) {
            first = std::max({first, from, offset});
            last = std::min({last, to, end});
            if (first < last) {
                ranges->emplace_back(first, last - first);
                named += last - first;
            }
        };
        int64_t cursor = from;
        for (const IORange& run : chunk.keep) {
            if (run.offset >= to) {
                break;
            }
            name(cursor, run.offset);
            cursor = std::max(cursor, run.offset + run.size);
        }
        name(cursor, to);
    }
    if (named > 0 && _stats != nullptr) {
        if (defer) {
            _stats->narrow_io_deferred_bytes += named;
        } else {
            _stats->narrow_io_named_buffers += 1;
            _stats->narrow_io_named_bytes += named;
        }
    }
}

bool NarrowIoPlanner::enabled() const {
    return config::parquet_offset_index_narrow_io_enable;
}

void NarrowIoPlanner::find_skip_ranges(int64_t offset, int64_t size, std::vector<IORange>* ranges) {
    _collect(offset, size, false, ranges);
}

void NarrowIoPlanner::find_defer_ranges(int64_t offset, int64_t size, std::vector<IORange>* ranges) {
    _collect(offset, size, true, ranges);
}

} // namespace starrocks::parquet
