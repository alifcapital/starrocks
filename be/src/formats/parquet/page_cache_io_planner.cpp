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

#include "formats/parquet/page_cache_io_planner.h"

#include <algorithm>
#include <utility>

#include "cache/mem_cache/page_cache.h"
#include "common/config.h"
#include "formats/parquet/page_reader.h"
#include "util/defer_op.h"

namespace starrocks::parquet {

PageCacheIoPlanner::PageCacheIoPlanner(StoragePageCache* cache, const std::string& file_key)
        : _cache(cache), _key(PageReader::make_page_cache_key(file_key)) {}

void PageCacheIoPlanner::add_chunk(PageCacheIoChunk chunk) {
    if (chunk.pages != nullptr) {
        _chunks.emplace_back(std::move(chunk));
        _chunks_sorted = false;
    }
}

void PageCacheIoPlanner::_sort_chunks() {
    if (!_chunks_sorted) {
        std::stable_sort(_chunks.begin(), _chunks.end(),
                         [](const PageCacheIoChunk& a, const PageCacheIoChunk& b) { return a.begin < b.begin; });
        _chunks_sorted = true;
    }
}

bool PageCacheIoPlanner::_page_cached(uint64_t offset) {
    PageReader::set_page_cache_key_offset(&_key, offset);
    return _cache->contains(_key);
}

// The read of an offset index is a remote read, and we fear that a scan whose pages are not in the page cache pays it
// for nothing. We expect a file that the page cache holds to have a page of some chunk of the row group in the cache,
// so we read the offset indexes only when the page cache holds one of the pages whose offsets we know without them:
// the first page of a chunk (the dictionary page when there is one), and the first data page after a dictionary page.
// The other pages are located by the offset index only, which is the read that the gate protects. A scan that selects
// pages can skip both known pages of every chunk, and then the gate stays closed and the chunks load as they did
// before: we lose the saving of the plan, not the bytes. We look once.
bool PageCacheIoPlanner::_may_load_page_layouts() {
    if (!_gate_checked) {
        _gate_checked = true;
        for (const PageCacheIoChunk& chunk : _chunks) {
            if (_page_cached(chunk.begin) ||
                (chunk.first_data_page > chunk.begin && _page_cached(chunk.first_data_page))) {
                _gate_open = true;
                break;
            }
        }
    }
    return _gate_open;
}

bool PageCacheIoPlanner::enabled() const {
    return config::parquet_page_cache_aware_io_enable;
}

void PageCacheIoPlanner::find_skip_ranges(int64_t offset, int64_t size,
                                          std::vector<io::SharedBufferedInputStream::IORange>* ranges) {
    if (_busy || !config::parquet_page_cache_aware_io_enable) {
        return;
    }
    _busy = true;
    DeferOp reset_busy([this] { _busy = false; });
    _sort_chunks();

    // We append a page to the range before it when they touch, so a run of cached pages is one range.
    auto append = [ranges](int64_t page_offset, int64_t page_size) {
        if (!ranges->empty() && ranges->back().offset + ranges->back().size == page_offset) {
            ranges->back().size += page_size;
        } else {
            ranges->emplace_back(page_offset, page_size);
        }
    };

    const int64_t end = offset + size;
    // the first chunk that ends after the offset
    auto chunk = std::partition_point(_chunks.begin(), _chunks.end(),
                                      [offset](const PageCacheIoChunk& c) { return c.end <= offset; });
    for (; chunk != _chunks.end() && chunk->begin < end; ++chunk) {
        if (chunk->end <= offset) {
            continue;
        }
        const std::vector<PageSpan>* pages = chunk->pages(false);
        if (pages == nullptr && _may_load_page_layouts()) {
            pages = chunk->pages(true);
        }
        if (pages == nullptr) {
            continue;
        }
        // the first page that ends after the offset
        auto page = std::partition_point(pages->begin(), pages->end(),
                                         [offset](const PageSpan& span) { return span.offset + span.size <= offset; });
        for (; page != pages->end() && page->offset < end; ++page) {
            if (page->size > 0 && page->offset >= 0 && _page_cached(static_cast<uint64_t>(page->offset))) {
                append(page->offset, page->size);
            }
        }
    }
}

} // namespace starrocks::parquet
