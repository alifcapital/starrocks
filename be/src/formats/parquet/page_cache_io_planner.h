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
#include <string>
#include <vector>

#include "io/shared_buffered_input_stream.h"

namespace starrocks {
class StoragePageCache;
} // namespace starrocks

namespace starrocks::parquet {

// The bytes [offset, offset + size) that a page takes in the file, header included.
struct PageSpan {
    int64_t offset = 0;
    int64_t size = 0;
};

// A column chunk that the planner looks at.
struct PageCacheIoChunk {
    // The chunk takes the bytes [begin, end) of the file, and begin is the offset of its first page.
    int64_t begin = 0;
    int64_t end = 0;
    // The offset of the first data page when a dictionary page comes before it, otherwise -1. The footer gives the
    // offsets of these two pages without the offset index.
    int64_t first_data_page = -1;
    // The pages of the chunk in the order of the file, the dictionary page first when the chunk has one. It returns
    // nullptr when the reader of the chunk does not know the pages. The reader knows them from the offset index of the
    // chunk. With allow_load the reader may read the offset index to know them, and without it the reader answers from
    // what it holds already.
    std::function<const std::vector<PageSpan>*(bool allow_load)> pages;
};

// We expect the page cache to serve a page that it holds, so the reader does not read the stream for that page. We fear
// that a shared buffer of the stream, which holds a column chunk or several, loads the pages that the page cache holds
// together with the pages that it misses, and then the page cache saves the decompression of a page but no remote
// bytes. So before the stream loads a buffer, the planner looks in the page cache for the pages of the chunks that the
// buffer holds, and names the pages that it finds, and the stream loads the runs of the other bytes only.
// The answer is a hint: the page cache can evict a page after the answer, and then the reader does not find the page in
// a buffer and reads it with a direct read.
class PageCacheIoPlanner final : public io::SharedBufferedInputStream::SkipRangesProvider {
public:
    // `file_key` is the key of the file in the page cache, as the page reader builds it.
    PageCacheIoPlanner(StoragePageCache* cache, const std::string& file_key);

    void add_chunk(PageCacheIoChunk chunk);

    // False while parquet_page_cache_aware_io_enable is off. The stream does not plan a buffer then.
    bool enabled() const override;

    void find_skip_ranges(int64_t offset, int64_t size,
                          std::vector<io::SharedBufferedInputStream::IORange>* ranges) override;

private:
    bool _page_cached(uint64_t offset);
    bool _may_load_page_layouts();
    void _sort_chunks();

    StoragePageCache* const _cache;
    // The key of a page: the key of the file, then the offset of the page. The offset is set for each lookup.
    std::string _key;
    // Sorted by the offset of the chunk once the first answer is due. The chunks of a row group do not overlap.
    std::vector<PageCacheIoChunk> _chunks;
    bool _chunks_sorted = true;
    bool _gate_checked = false;
    bool _gate_open = false;
    // True while find_skip_ranges() runs. A reader that loads an offset index for us reads the stream, and the stream
    // asks us about the buffer of the index, which holds no page.
    bool _busy = false;
};

} // namespace starrocks::parquet
