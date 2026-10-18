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

#include <cstdint>
#include <functional>
#include <map>
#include <vector>

#include "io/shared_buffered_input_stream.h"

namespace starrocks {
struct HdfsScannerStats;
} // namespace starrocks

namespace starrocks::parquet {

// A column that skips pages through its offset index jumps over the pages that hold no selected row, but a shared buffer
// that holds the chunk is loaded whole at its first access, and the jumped pages are downloaded in vain. The planner is a
// provider of the stream (SharedBufferedInputStream::plan_load()): before the stream loads a buffer, it asks the planner
// which bytes of the buffer the read range of a column does not need.
// - The pages in front of the end of the range that the range does not need are dead: we expect the reader to move
//   forward in its chunk, so it never reads them again. The planner skips them.
// - The pages behind the end of the range may be needed by a later range. The planner defers them: they stay in a buffer
//   that is not loaded, and the stream asks the planner again when a later range first takes a byte from that buffer.
//   Then the buffer loads only the pages that this range needs. The first bytes behind the last needed page are read
//   ahead with it, because a request costs more than a few pages.
//
// The reader registers a chunk once, and tells the planner when it begins and ends reading a range. The planner asks the
// reader what the range needs only when the stream asks it about a buffer of the chunk, that is, when a buffer is about to
// be loaded from remote storage. A scan whose data cache holds the blocks does not load buffers, so it costs the planner
// nothing but the flags of begin_range() and end_range().
class NarrowIoPlanner final : public io::SharedBufferedInputStream::SkipRangesProvider {
public:
    using IORange = io::SharedBufferedInputStream::IORange;

    // What a read range needs from a chunk.
    struct Need {
        // The byte runs of the chunk that the range needs: the pages with a selected row, and the dictionary page when
        // it is not parsed yet. They are sorted and do not overlap.
        std::vector<IORange> needed;
        // The bytes [ahead_begin, ahead_end) behind the needed runs are read ahead with them. They are empty when
        // ahead_end <= ahead_begin.
        int64_t ahead_begin = 0;
        int64_t ahead_end = 0;
        // The bytes of the chunk in front of this offset that are not needed or read ahead are dead.
        int64_t dead_end = 0;
    };
    using ComputeNeed = std::function<void(Need*)>;

    struct Chunk {
        int64_t begin = 0;
        int64_t end = 0;
        // The first byte of the first data page. The bytes in front of it are the dictionary page, which says nothing about
        // how many data pages the ranges need.
        int64_t data_begin = 0;
        ComputeNeed compute;
        // A range of the chunk is being read.
        bool reading = false;
        // The reader has to be asked what the range needs.
        bool stale = false;
        // `need` and `keep` are the need of the last range that was asked about. The dead bytes of that range stay dead.
        bool resolved = false;
        // The need was set by begin_dictionary_read(), and the planner defers the data behind the dictionary page.
        bool dictionary_read = false;
        Need need;
        // The runs that are neither skipped nor deferred: need.needed and the read ahead.
        std::vector<IORange> keep;
        // We defer only while the chunk shows that it has many dead bytes, so the reader tells the planner what it did in
        // the chunk, at the begin of each range, with one store for each of these: it read or jumped over the data pages in
        // front of `read_until`, and `dead_bytes` of them it jumped over. The ranges that the stream serves from loaded
        // buffers are counted too, because the reader counts them, not the planner.
        int64_t read_until = 0;
        int64_t dead_bytes = 0;
        // The bytes of [read_until, need.dead_end) that the planner looked at for the range, and the part of them that the
        // range does not need. They are set when the planner asks the reader for the need of the range.
        int64_t range_passed = 0;
        int64_t range_dead = 0;
    };

    // `stats` can be null. It must outlive the planner.
    explicit NarrowIoPlanner(HdfsScannerStats* stats) : _stats(stats) {}

    // The chunk [begin, end) of the file, with the data pages from data_begin. `compute` tells what a range needs from it,
    // and it is called only inside the asks of the stream, while a range of the chunk is being read. The returned pointer
    // is valid as long as the planner.
    Chunk* add_chunk(int64_t begin, int64_t end, int64_t data_begin, ComputeNeed compute);
    // The reader begins to read a range of the chunk. `read_until` is the offset of the first page that it has neither read
    // nor jumped over, and `dead_bytes` is how many bytes of the pages in front of it it jumped over.
    void begin_range(Chunk* chunk, int64_t read_until, int64_t dead_bytes) {
        chunk->reading = true;
        chunk->stale = true;
        chunk->read_until = read_until;
        chunk->dead_bytes = dead_bytes;
    }
    void end_range(Chunk* chunk) { chunk->reading = false; }

    // The reader loads the dictionary page of the chunk [chunk_begin, chunk_end) before it knows the pages of the chunk. If
    // the data behind the dictionary page [chunk_begin, dict_end) is large, the buffer that the read of the dictionary
    // page loads defers it, because the reader may not need it (the dictionary can drop the row group), or may need only
    // a few pages.
    void begin_dictionary_read(int64_t chunk_begin, int64_t dict_end, int64_t chunk_end);
    void end_dictionary_read(int64_t chunk_begin);

    bool enabled() const override;
    void find_skip_ranges(int64_t offset, int64_t size, std::vector<IORange>* ranges) override;
    void find_defer_ranges(int64_t offset, int64_t size, std::vector<IORange>* ranges) override;

private:
    // Appends the bytes of the window [offset, offset + size) that the planner names. The skip ranges are the dead bytes
    // of the chunks, and the defer ranges are the bytes behind them.
    void _collect(int64_t offset, int64_t size, bool defer, std::vector<IORange>* ranges);
    void _resolve(Chunk* chunk);
    bool _defer_allowed(const Chunk& chunk) const;

    HdfsScannerStats* const _stats;
    // By the begin of the chunk. The chunks do not overlap, and the pointers to them stay valid.
    std::map<int64_t, Chunk> _chunks;
};

// The stream has one provider, and a row group can have several. The chain asks each of them and joins the answers.
class SkipRangesProviderChain final : public io::SharedBufferedInputStream::SkipRangesProvider {
public:
    void add(io::SharedBufferedInputStream::SkipRangesProvider* provider) {
        if (provider != nullptr) {
            _providers.push_back(provider);
        }
    }
    bool empty() const { return _providers.empty(); }

    // The chain is on while one of its providers is on, and it asks the providers that are on only.
    bool enabled() const override {
        for (const auto* provider : _providers) {
            if (provider->enabled()) {
                return true;
            }
        }
        return false;
    }

    void find_skip_ranges(int64_t offset, int64_t size,
                          std::vector<io::SharedBufferedInputStream::IORange>* ranges) override {
        for (auto* provider : _providers) {
            if (provider->enabled()) {
                provider->find_skip_ranges(offset, size, ranges);
            }
        }
    }

    void find_defer_ranges(int64_t offset, int64_t size,
                           std::vector<io::SharedBufferedInputStream::IORange>* ranges) override {
        for (auto* provider : _providers) {
            if (provider->enabled()) {
                provider->find_defer_ranges(offset, size, ranges);
            }
        }
    }

private:
    std::vector<io::SharedBufferedInputStream::SkipRangesProvider*> _providers;
};

} // namespace starrocks::parquet
