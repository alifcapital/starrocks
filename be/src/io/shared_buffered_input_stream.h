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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "io/seekable_input_stream.h"

namespace starrocks::io {

class SharedBufferedInputStream : public SeekableInputStream {
public:
    struct IORange {
        IORange(const int64_t offset, const int64_t size, const bool is_active = true)
                : offset(offset), size(size), is_active(is_active) {}
        int64_t offset;
        int64_t size;
        bool is_active = true;
        bool operator<(const IORange& x) const { return offset < x.offset; }
    };
    struct CoalesceOptions {
        static constexpr int64_t MB = 1024 * 1024;
        int64_t max_dist_size = 1 * MB;
        int64_t max_buffer_size = 8 * MB;
    };
    struct SharedBuffer {
        // request range
        int64_t raw_offset;
        int64_t raw_size;
        // request range after alignment
        int64_t offset;
        int64_t size;
        int64_t ref_count;
        // The buffer is built from a single io range larger than max_buffer_size, or from a part of such a
        // buffer (see split_shared_buffer). We keep the flag so that we can count how many bytes we load and use
        // for such buffers.
        bool large = false;
        // A large io range is cut into consecutive buffers (pieces), and split_shared_buffer() can cut a piece
        // further. The flag is set for a buffer whose bytes end where the bytes of the next buffer of the same
        // large io range begin, so a read that crosses the end of this buffer can continue in the next one. It is
        // false when a hole follows the buffer, because the caller reads the hole by other means.
        bool has_next_piece = false;
        // The bytes of the buffer. A buffer is loaded when the capacity of the vector is not 0, and get_bytes() sets it
        // only after the read of the bytes succeeded.
        std::vector<uint8_t> buffer;
        // True for a buffer that split_shared_buffer() made, and for a buffer whose blocks the data cache has answered
        // for already. The caller has chosen the bytes that the buffer covers, so no one needs to choose them again.
        bool split = false;
        // True when the skip ranges provider (see SkipRangesProvider) was asked about the bytes of the buffer. We ask
        // once, because the answer is a hint and a second ask costs the probes again for the bytes that the first
        // answer left in the buffer. The new buffers of plan_load() are asked when a read first takes a byte from
        // them, except the one run with the bytes of the read that triggered the plan, which keeps the flag. We fear
        // that a buffer holds the bytes of other columns, which the provider did not know when it was asked, and that
        // it knows more when a later read first takes a byte from them, for example the range of the next column.
        bool planned = false;
        // True when the data cache refused to write a block of the buffer because too many writes were in flight (see
        // datacache_populate_stop_at_busy). The reads of the buffer write its blocks in parts, so the flag stops the
        // writes of the later parts too.
        bool populate_refused = false;
        void align(int64_t align_size, int64_t file_size);
        std::string debug_string() const;
    };
    using SharedBufferPtr = std::shared_ptr<SharedBuffer>;

    // Some bytes of a buffer need not be loaded with the rest of it. A provider names them before the stream loads the
    // buffer for the first time (see plan_load()), in two ways:
    // - skip: the reader gets these bytes from a faster place (the Parquet page cache holds a page in memory and does not
    //   read it from the stream), or it never reads them again (the pages of a chunk in front of its read range). We fear
    //   that the buffer loads them for nothing, so the stream cuts them out of the buffer for good.
    // - defer: the reader does not need these bytes for the read that loads the buffer, but a later read may need them.
    //   We fear that the buffer loads them for nothing too, but we cannot cut them out, so the stream keeps them as
    //   buffers that are not loaded, and asks the provider again when a later read first takes a byte from them.
    class SkipRangesProvider {
    public:
        virtual ~SkipRangesProvider() = default;
        // False when the provider would name nothing, for example because its switch is off. The stream then does not
        // ask, does not time the call and does not mark the buffer as asked, so a provider that is off costs one
        // virtual call for a buffer, and it plans the buffers that load after it is on again.
        virtual bool enabled() const { return true; }
        // Appends to `ranges` the parts of [offset, offset + size) to skip. The ranges can come in any order and can
        // overlap. The answer is a hint: a byte that the provider names can be needed later, and then a direct read gets
        // it.
        virtual void find_skip_ranges(int64_t offset, int64_t size, std::vector<IORange>* ranges) = 0;
        // Appends to `ranges` the parts of [offset, offset + size) to defer. The ranges can come in any order and can
        // overlap. A byte that the provider names as skip and as defer is skipped. By default the provider defers
        // nothing.
        virtual void find_defer_ranges(int64_t offset, int64_t size, std::vector<IORange>* ranges) {}
    };

    SharedBufferedInputStream(std::shared_ptr<SeekableInputStream> stream, std::string filename, size_t file_size);
    ~SharedBufferedInputStream() override = default;

    Status seek(int64_t position) override {
        _offset = position;
        return _stream->seek(position);
    }
    StatusOr<int64_t> position() override { return _offset; }
    StatusOr<int64_t> read(void* data, int64_t count) override;
    // A read that no buffer covers is read directly, with one request. The exception is a read that lies inside a cut
    // span, which is a part of a buffer that plan_load() cut out because the reader got it elsewhere (the page cache)
    // and the answer went stale (the page cache evicted the page). We fear that every page of the span costs two
    // requests then, one for the header and one for the body, and that a smaller cache evicts a run of pages. So we
    // read the span from the read to its end (at most max_buffer_size, and not into the next buffer) with one request
    // into a new buffer, and the following pages of the span are served from it.
    Status read_at_fully(int64_t offset, void* out, int64_t count) override;
    // read_at_fully() for a caller that reads whole blocks of the data cache: [offset, offset + count) begins at a block
    // border, and `need_offset` is the first byte in it that the reader asked for. A buffer or a run of pieces serves
    // the read only if its io range holds `need_offset` (see find_shared_buffer_holding()).
    Status read_blocks_at_fully(int64_t need_offset, int64_t offset, void* out, int64_t count);
    StatusOr<int64_t> get_size() override;
    Status skip(int64_t count) override {
        _offset += count;
        return _stream->skip(count);
    }

    StatusOr<SharedBufferPtr> find_shared_buffer(size_t offset, size_t count);
    // The buffer whose io range holds `need_offset` and which covers [offset, offset + count). A read of whole blocks
    // looks up its buffer with it, because with the alignment the buffer of another io range can cover the blocks too.
    StatusOr<SharedBufferPtr> find_shared_buffer_holding(int64_t need_offset, int64_t offset, int64_t count);
    // We want a caller that knows that some bytes of `sb` are not worth loading (for example, the data cache holds
    // them) to load the rest of `sb` as before: each run of bytes with one request, when a read first needs it.
    // So this function replaces `sb`, which is not loaded yet, by one shared buffer for each of `ranges`. No new
    // buffer covers the bytes of `sb` outside `ranges`, so find_shared_buffer() fails for them and the caller reads
    // them by other means.
    // `ranges` are sorted by offset, do not overlap, and lie inside [sb->offset, sb->offset + sb->size).
    // `is_final` is the value of SharedBuffer::split of the new buffers. A caller that chose the bytes by one reason
    // only, and expects another reason to cut the buffers further, passes false.
    // On an error nothing changes. On success `sb` is not registered any more and the caller does not use it again.
    Status split_shared_buffer(const SharedBufferPtr& sb, const std::vector<IORange>& ranges, bool is_final = true);

    void set_skip_ranges_provider(SkipRangesProvider* provider) { _skip_ranges_provider = provider; }
    SkipRangesProvider* skip_ranges_provider() const { return _skip_ranges_provider; }
    // We want a buffer not to load the bytes that the provider names, because the reader gets them elsewhere or does not
    // need them now. So when a read is about to load `sb`, the caller of this function asks the provider about `sb` once,
    // and the function splits `sb` into the runs of bytes to load, with split_shared_buffer().
    // - The bytes of [need_offset, need_offset + need_size) are always in a run: the read that triggers the load needs
    //   them, even if the provider names them.
    // - The bytes that the provider skips are in no new buffer, so find_shared_buffer() fails for them. A read of them is
    //   served by one read of the cut span, or goes direct.
    // - The run with the bytes of the read is planned, and the provider is not asked about it again. The other new
    //   buffers, the runs and the buffers of the deferred bytes, are not planned, so the provider is asked about each of
    //   them when a read first takes a byte from it.
    // - With an align size the runs keep to the borders of the blocks, so a block that holds a byte to load is loaded
    //   whole, as the data cache expects. Skipped and deferred ranges give up their partial blocks.
    // - We expect a request to cost about as much as 1 MB of download (io_coalesce_read_max_distance_size), so two runs
    //   with a gap of at most that size share a buffer and the gap is loaded, and a merged buffer is not larger than
    //   the coalesce buffer size. This is the rule of the data cache split too. A deferred range of at most that size
    //   next to a run is loaded with the run, because a buffer of its own costs a request when a read needs it.
    // - The new buffers keep SharedBuffer::split of `sb`, so a data cache that has answered for the blocks of `sb` is
    //   not asked again, and a data cache that has not is asked about the new buffers.
    // - The bytes that stay out of the new buffers are remembered as cut spans. A read that falls into a cut span
    //   because the provider's answer went stale is served by one read of the span (see read_at_fully()). The deferred
    //   bytes are buffers, so they are not cut spans, and a recovered span never covers them.
    // The time that the function takes, including the reads that the provider makes, is added to skip_ranges_timer().
    // Returns true when `sb` is not registered any more, and the caller looks for the buffer of its read again.
    // Otherwise it returns false and `sb` stays as it is: there is no provider or it is off, `sb` was asked or is
    // loaded already, the provider names nothing, or the split failed (then the buffer is loaded whole, which costs
    // bytes but returns the right data).
    bool plan_load(const SharedBufferPtr& sb, int64_t need_offset, int64_t need_size);
    // find_shared_buffer needs one buffer for the whole range. A read inside a large io range can cross the
    // border of two pieces, so we also accept a range that consecutive pieces cover together. The pieces are
    // returned in the order of offset, `pieces` can be nullptr when the caller only checks the coverage.
    Status find_shared_pieces(size_t offset, size_t count, std::vector<SharedBufferPtr>* pieces);
    // Get bytes from shared buffer or remote storage, when the shared_buffer is not NULL, the function
    // will use it directely instead of finding it repeatedly.
    Status get_bytes(const uint8_t** buffer, size_t offset, size_t count, SharedBufferPtr shared_buffer);

    StatusOr<std::unique_ptr<NumericStatistics>> get_numeric_statistics() override {
        return _stream->get_numeric_statistics();
    }

    IoStatsSnapshot get_io_stats_snapshot() const override;

    Status set_io_ranges(const std::vector<IORange>& ranges, bool coalesce_lazy_column = true);
    void release_to_offset(int64_t offset);
    void release();
    void set_coalesce_options(const CoalesceOptions& options) { _options = options; }
    const CoalesceOptions& coalesce_options() const { return _options; }
    void set_align_size(int64_t size) { _align_size = size; }

    int64_t shared_io_count() const { return _shared_io_count; }
    int64_t shared_io_bytes() const { return _shared_io_bytes; }
    int64_t shared_align_io_bytes() const { return _shared_align_io_bytes; }
    int64_t shared_io_timer() const { return _shared_io_timer; }
    // Loads of the large shared buffers (see SharedBuffer::large): the number of loads, the bytes loaded
    // and the bytes the readers asked for from them.
    int64_t large_io_count() const { return _large_io_count; }
    int64_t large_io_bytes() const { return _large_io_bytes; }
    int64_t large_used_bytes() const { return _large_used_bytes; }
    // The bytes that buffers did not load, and the buffers that plan_load() split, because the provider named bytes
    // that the reader gets elsewhere. The named bytes are all the bytes inside the buffers that the provider named:
    // the buffers load some of them anyway (the bytes of the read that loads the buffer, the gaps below the coalesce
    // distance, and the parts of the blocks of the data cache), so they are never fewer than the bytes not loaded. A
    // buffer that the provider is asked about again names its bytes again.
    int64_t skip_ranges_bytes() const { return _skip_ranges_bytes; }
    int64_t skip_ranges_named_bytes() const { return _skip_ranges_named_bytes; }
    int64_t skip_ranges_split_count() const { return _skip_ranges_split_count; }
    // The buffers that plan_load() asked the provider about, and the time that these calls took in nanoseconds. The
    // time includes the time of the provider, so it includes a read that the provider makes through the stream.
    int64_t skip_ranges_plan_count() const { return _skip_ranges_plan_count; }
    int64_t skip_ranges_timer() const { return _skip_ranges_timer; }
    // The reads that fell into a cut span and loaded the rest of the span, and the bytes of these loads.
    int64_t skip_ranges_recover_count() const { return _skip_ranges_recover_count; }
    int64_t skip_ranges_recover_bytes() const { return _skip_ranges_recover_bytes; }
    // The bytes that plan_load() kept in buffers that are not loaded because the provider deferred them, and the
    // buffers that it made for them. A deferred buffer is asked again, so the same bytes can be counted again.
    int64_t skip_ranges_deferred_bytes() const { return _skip_ranges_deferred_bytes; }
    int64_t skip_ranges_deferred_count() const { return _skip_ranges_deferred_count; }
    int64_t direct_io_count() const { return _direct_io_count; }
    int64_t direct_io_bytes() const { return _direct_io_bytes; }
    int64_t direct_io_timer() const { return _direct_io_timer; }
    int64_t estimated_mem_usage() const { return _estimated_mem_usage; }
    // each SharedBuffer may contain several ranges, the return the ref sum
    int64_t current_range_ref_sum() const;

    StatusOr<std::string_view> peek(int64_t count) override;
    const std::string& filename() const override { return _filename; }
    bool is_cache_hit() const override { return false; }
    StatusOr<std::string_view> peek_shared_buffer(int64_t count, SharedBufferPtr* shared_buffer);

private:
    void _update_estimated_mem_usage();
    Status _sort_and_check_overlap(std::vector<IORange>& ranges);
    void _merge_small_ranges(const std::vector<IORange>& ranges);
    void _add_large_range(const IORange& range);
    Status _set_io_ranges_all_columns(const std::vector<IORange>& ranges);
    Status _set_io_ranges_active_and_lazy_columns(const std::vector<IORange>& ranges);
    // plan_load() for each piece of a read that crosses the border of two pieces. Returns false when the pieces do not
    // cover the read any more.
    bool _plan_pieces(std::vector<SharedBufferPtr>* pieces, size_t offset, size_t count);
    // True when the io range of one of the pieces holds `need_offset`.
    static bool _pieces_hold(const std::vector<SharedBufferPtr>& pieces, int64_t need_offset);
    // read_at_fully() and read_blocks_at_fully(). A negative `need_offset` looks up the buffers by `offset`.
    Status _read_at_fully(int64_t need_offset, int64_t offset, void* out, int64_t count);
    // Remembers the bytes of the raw range of `sb` that none of `ranges` keeps, for _recover_cut_span().
    void _record_cut_spans(const SharedBuffer& sb, const std::vector<IORange>& ranges);
    // Returns a new buffer for a read that lies inside a cut span, or nullptr.
    SharedBufferPtr _recover_cut_span(int64_t offset, int64_t count);
    const std::shared_ptr<SeekableInputStream> _stream;
    const std::string _filename;
    std::map<int64_t, SharedBufferPtr> _map;
    // The cut spans: the key is the end of a span and the value is its begin. The spans do not overlap and no buffer
    // covers them.
    std::map<int64_t, int64_t> _cut_spans;
    CoalesceOptions _options;
    int64_t _offset = 0;
    int64_t _file_size = 0;
    int64_t _shared_io_count = 0;
    int64_t _shared_io_bytes = 0;
    int64_t _shared_align_io_bytes = 0;
    int64_t _shared_io_timer = 0;
    int64_t _large_io_count = 0;
    int64_t _large_io_bytes = 0;
    // Sum of the `count` of every read served from a large buffer. If the readers ask for the same bytes
    // twice, we count them twice.
    int64_t _large_used_bytes = 0;
    int64_t _skip_ranges_bytes = 0;
    int64_t _skip_ranges_named_bytes = 0;
    int64_t _skip_ranges_split_count = 0;
    int64_t _skip_ranges_plan_count = 0;
    int64_t _skip_ranges_timer = 0;
    int64_t _skip_ranges_recover_count = 0;
    int64_t _skip_ranges_recover_bytes = 0;
    int64_t _skip_ranges_deferred_bytes = 0;
    int64_t _skip_ranges_deferred_count = 0;
    int64_t _direct_io_count = 0;
    int64_t _direct_io_bytes = 0;
    int64_t _direct_io_timer = 0;
    int64_t _align_size = 0;
    int64_t _estimated_mem_usage = 0;
    SkipRangesProvider* _skip_ranges_provider = nullptr;
};

} // namespace starrocks::io
