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
#include <cmath>
#include <cstring>
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/utils.h"
#include "gutil/strings/substitute.h"
#include "types/int256.h"
#include "util/bit_stream_utils.h"
#include "util/bit_util.h"
#include "util/coding.h"
#include "util/faststring.h"
#include "util/raw_container.h"
#include "util/slice.h"

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace starrocks::parquet {

static constexpr int kBooleanBitPackedBitWidth = 1;

template <typename T>
class PlainEncoder final : public Encoder {
public:
    PlainEncoder() = default;
    ~PlainEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        size_t copy_bytes = count * SIZE_OF_TYPE;
        _buffer.append(vals, copy_bytes);
        return Status::OK();
    }

    Slice build() override { return {_buffer.data(), _buffer.size()}; }

    std::string to_string() const override { return fmt::format("PlainEncoder<{}>", typeid(T).name()); }

private:
    enum { SIZE_OF_TYPE = sizeof(T) };

    faststring _buffer;
};

template <>
class PlainEncoder<Slice> final : public Encoder {
public:
    PlainEncoder() = default;
    ~PlainEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        const auto* slices = (const Slice*)vals;
        for (int i = 0; i < count; ++i) {
            put_fixed32_le(&_buffer, static_cast<uint32_t>(slices[i].size));
            _buffer.append(slices[i].data, slices[i].size);
        }
        return Status::OK();
    }

    Slice build() override { return {_buffer.data(), _buffer.size()}; }

    std::string to_string() const override { return "PlainEncoder<Slice>"; }

private:
    faststring _buffer;
};

template <>
class PlainEncoder<bool> final : public Encoder {
public:
    PlainEncoder() : _bit_writer(&_buffer) {}
    ~PlainEncoder() override = default;

    Status append(const uint8_t* vals, size_t count) override {
        // TODO(@DorianZheng) Plain boolean encoder is using by UT currently, optimize it in the future.
        for (int i = 0; i < count; i++) {
            _bit_writer.PutValue(vals[i], kBooleanBitPackedBitWidth);
        }
        return Status::OK();
    }

    Slice build() override {
        _bit_writer.Flush();
        return {_buffer.data(), _buffer.size()};
    }

    std::string to_string() const override { return "PlainEncoder<bool>"; }

private:
    faststring _buffer;
    BitWriter _bit_writer;
};

template <typename T>
class PlainDecoder final : public Decoder {
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

    std::string to_string() const override { return fmt::format("PlainDecoder<{}>", typeid(T).name()); }

    // The buffer is a min/max statistic from the footer or the page index, and its length is not ours to
    // trust: we copy sizeof(T) bytes, so a shorter buffer would be read past its end.
    static Status decode(const std::string& buffer, T* value) {
        if (UNLIKELY(buffer.size() < sizeof(T))) {
            return Status::Corruption(
                    strings::Substitute("A statistic value of $0 bytes is shorter than the $1 bytes of its type",
                                        buffer.size(), sizeof(T)));
        }
        memcpy(value, buffer.data(), sizeof(T));
        return Status::OK();
    }

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        size_t max_fetch = 0;
        RETURN_IF_ERROR(_get_values_size(count, "read", &max_fetch));
        auto n = dst->append_numbers(_data.data + _offset, max_fetch);
        DCHECK_EQ(count, n);
        _offset += max_fetch;
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        size_t fetch_size = 0;
        RETURN_IF_ERROR(_get_values_size(values_to_skip, "skip", &fetch_size));
        _offset += fetch_size;
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        size_t max_fetch = 0;
        RETURN_IF_ERROR(_get_values_size(count, "read", &max_fetch));
        memcpy(dst, _data.data + _offset, max_fetch);
        _offset += max_fetch;
        return Status::OK();
    }

private:
    // Returns in `bytes` the size of `count` values, and checks that the rest of the page holds them.
    // We are afraid that the product wraps when count is large, and then a wrapped size passes the
    // comparison with the page size and the caller reads outside the page. So we detect the wrap first.
    Status _get_values_size(size_t count, const char* action, size_t* bytes) const {
        if (UNLIKELY(__builtin_mul_overflow(count, static_cast<size_t>(SIZE_OF_TYPE), bytes) || *bytes > _data.size ||
                     _offset > _data.size - *bytes)) {
            return Status::InternalError(strings::Substitute(
                    "going to $0 out-of-bounds data, offset=$1,count=$2,size=$3", action, _offset, count, _data.size));
        }
        return Status::OK();
    }

    enum { SIZE_OF_TYPE = sizeof(T) };

    Slice _data;
    size_t _offset = 0;
};

template <>
class PlainDecoder<Slice> final : public Decoder {
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

    std::string to_string() const override { return "PlainDecoder<Slice>"; }

    static Status decode(const std::string& buffer, Slice* value) {
        value->data = const_cast<char*>(buffer.data());
        value->size = buffer.size();
        return Status::OK();
    }

    void set_num_values(size_t num_values) override { _page_num_values = num_values; }

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        _max_filter_share = max_filter_share(avg_value_width(data.size, _page_num_values));
        _page_num_values = 0;
        return Status::OK();
    }

    // The average width of a value of a page, without its length prefix, from the size of the page and the
    // number of its values. The number includes the NULL values, which have no bytes, so we expect the result to
    // be at most the real width, and a page with many NULL values is taken for a page of narrower values. This
    // is the safe side: the filter is then used less often. It is 0 when the number is not known.
    static size_t avg_value_width(size_t page_size, size_t num_values) {
        if (num_values == 0) {
            return 0;
        }
        const size_t bytes_per_value = page_size / num_values;
        return bytes_per_value > sizeof(uint32_t) ? bytes_per_value - sizeof(uint32_t) : 0;
    }

    // The largest share of the rows of a read range that the filter may keep for the filtered decode to be faster
    // than the decode of all values followed by Column::filter_range(), by the average width of a value. The filtered
    // decode reads the length prefix of every value and copies the selected values one by one, where the plain
    // decode copies all values together. We expect the filtered decode to win when it skips enough copies, and
    // a skipped copy saves more bytes the wider the value is. We measured the share where both take the same
    // time, on batches of 4096 rows with the kept rows at random, with 0% and 10% NULL, into a nullable and a
    // not nullable column:
    //    8 bytes: 10% to 15%     32 bytes: about 20%     128 bytes: 50% and more     512 bytes: 90% and more
    // We take 12%, 20%, 55% and 90% at these widths. The widths are a factor of 4 apart and we have not measured
    // between them, so between two points we interpolate linearly in the logarithm of the width. Below 8 and above
    // 512 bytes we keep the nearest point. A range just above the share, where the filtered decode starts to lose,
    // takes the plain decode.
    static double max_filter_share(size_t avg_width) {
        // The widths 8, 32, 128 and 512 have the logarithms 3, 5, 7 and 9, which are two apart.
        static constexpr double kShares[] = {0.12, 0.20, 0.55, 0.90};
        if (avg_width <= 8) {
            return kShares[0];
        }
        if (avg_width >= 512) {
            return kShares[3];
        }
        const double position = (std::log2(static_cast<double>(avg_width)) - 3.0) / 2.0;
        const size_t segment = std::min<size_t>(static_cast<size_t>(position), 2);
        const double fraction = position - static_cast<double>(segment);
        return kShares[segment] + fraction * (kShares[segment + 1] - kShares[segment]);
    }

    Status next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type, Column* dst,
                                 const FilterData* filter) override {
        const uint8_t* __restrict is_nulls = null_infos.nulls_data();
        // fill null data
        DCHECK(dst->is_nullable());
        size_t null_cnt = null_infos.num_nulls;
        if (dst->is_nullable()) {
            NullColumn* null_column = down_cast<NullableColumn*>(dst)->null_column_raw_ptr();
            auto& null_data = null_column->get_data();
            size_t prev_num_rows = null_data.size();
            raw::stl_vector_resize_uninitialized(&null_data, count + prev_num_rows);
            uint8_t* __restrict__ dst_nulls = null_data.data() + prev_num_rows;
            memcpy(dst_nulls, is_nulls, count);
            down_cast<NullableColumn*>(dst)->set_has_null(null_cnt > 0);
        }

        if (_use_filter(filter, _max_filter_share)) {
            return _next_batch_with_nulls_filtered(count, count - null_cnt, is_nulls, filter, dst);
        }

        size_t max_size = 0;
        size_t read_count = count - null_cnt;
        // Reusable members rather than VLAs: a data page may hold ~1M values and
        // StoredColumnReaderImpl::_read() hands the whole page over in one call, so stack
        // arrays sized by read_count overflow the thread stack.
        _temp_lengths.resize(read_count + 1);
        _temp_datas.resize(read_count + 1);
        uint32_t* lengths = _temp_lengths.data();
        char** datas = _temp_datas.data();
        size_t i = 0;
        size_t cursor = _offset;
        // We fear a corrupt page: a length prefix or a value that does not fit in the page would make us read
        // and copy memory after the page. So the loop condition checks that the prefix fits, and the value
        // is checked once its length is known.
        for (i = 0; (i < read_count) & (cursor + sizeof(uint32_t) <= _data.size); ++i) {
            uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + cursor);
            cursor += sizeof(int32_t);
            if (UNLIKELY(length > _data.size - cursor)) {
                return _out_of_bounds_error(cursor, count);
            }
            datas[i] = _data.data + cursor;
            cursor += length;
            lengths[i] = length;
            max_size = max_size > length ? max_size : length;
        }

        _offset = cursor;
        if (i < read_count) {
            return Status::InternalError(fmt::format("error: null_cnt {} i:{} read_cnt:{}", null_cnt, i, read_count));
        }
        // fill offset data
        auto* binary_column = ColumnHelper::get_binary_column(dst);
        {
            auto& offsets = binary_column->get_offset();
            auto& bytes = binary_column->get_bytes();
            size_t prev_offsets = offsets.size();
            raw::stl_vector_resize_uninitialized(&offsets, count + prev_offsets);
            size_t offset = bytes.size();
            size_t cnt = 0;
            for (size_t i = 0; i < count; ++i) {
                offset += is_nulls[i] ? 0 : lengths[cnt++];
                offsets[prev_offsets + i] = offset;
            }

            binary_column->get_bytes().reserve(offset);
        }

        if (read_count == 0) {
            return Status::OK();
        }

        // fill bytes data
        max_size = std::max(BitUtil::next_power_of_two(max_size), 8L);
        if (datas[read_count - 1] - _data.data + max_size <= _data.size) {
            binary_column->append_bytes_overflow(datas, lengths, read_count, max_size);
            DCHECK_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
        } else {
            binary_column->append_bytes(datas, lengths, read_count);
            DCHECK_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
        }

        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        size_t num_decoded = 0;
        if (dst->is_nullable()) {
            down_cast<NullableColumn*>(dst)->null_column_raw_ptr()->append_default(count);
        }

#define CHECK_DECODING_BOUND                                     \
    if (UNLIKELY(num_decoded < count || _offset > _data.size)) { \
        return _out_of_bounds_error(_offset, count);             \
    }

        if (_use_filter(filter, _max_filter_share)) {
            auto* binary_column = ColumnHelper::get_binary_column(dst);
            // We fear a reader that looks at every non-NULL value of the column, for example the lookup in a
            // global dictionary: it would fail on an empty value that a dropped row gets. So when the column is
            // nullable we mark a dropped row as NULL, as the dictionary decoder does. The null column has
            // already got `count` default entries, which are the last ones. A column that is not nullable keeps
            // the empty value: such a column is not looked up before the dropped rows are removed.
            NullableColumn* nullable_dst = dst->is_nullable() ? down_cast<NullableColumn*>(dst) : nullptr;
            uint8_t* dst_nulls = nullptr;
            if (nullable_dst != nullptr) {
                auto& null_data = nullable_dst->null_column_raw_ptr()->get_data();
                dst_nulls = null_data.data() + null_data.size() - count;
            }
            size_t num_dropped = 0;
            // We fear a corrupt page: a length that does not fit in the page would make us copy memory after
            // it. So the loop condition checks that the prefix fits and the value is checked before the copy.
            while (num_decoded < count && _offset + sizeof(int32_t) <= _data.size) {
                uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
                _offset += sizeof(int32_t);
                if (UNLIKELY(length > _data.size - _offset)) {
                    return _out_of_bounds_error(_offset, count);
                }
                if (filter[num_decoded]) {
                    binary_column->append(Slice(_data.data + _offset, length));
                } else {
                    binary_column->append_default();
                    if (dst_nulls != nullptr) {
                        dst_nulls[num_decoded] = 1;
                        num_dropped++;
                    }
                }
                _offset += length;
                num_decoded++;
            }
            CHECK_DECODING_BOUND
            if (num_dropped > 0) {
                nullable_dst->set_has_null(true);
            }
        } else {
            auto slices_data = std::make_unique_for_overwrite<uint8_t[]>(count * sizeof(Slice));
            Slice* slices = reinterpret_cast<Slice*>(slices_data.get());
            size_t max_size = 0;
            // The slices only point into the page here, and CHECK_DECODING_BOUND runs before they are used. The
            // loop condition checks that the prefix fits, so a corrupt length ends the loop with _offset
            // after the page.
            while (num_decoded < count && _offset + sizeof(int32_t) <= _data.size) {
                uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
                _offset += sizeof(int32_t);
                slices[num_decoded] = {_data.data + _offset, length};
                _offset += length;
                max_size = std::max<size_t>(max_size, length);
                num_decoded++;
            }
            CHECK_DECODING_BOUND
            if (num_decoded == 0) {
                return Status::OK();
            }
            bool ret = false;
            // when last slices offset + max_size > _data.size, there is overflow on reading
            max_size = std::max(BitUtil::next_power_of_two(max_size), 8L);
            if (slices[count - 1].data - _data.data + max_size <= _data.size) {
                ret = ColumnHelper::get_binary_column(dst)->append_strings_overflow(slices, num_decoded, max_size);
            } else {
                ret = ColumnHelper::get_binary_column(dst)->append_strings(slices, num_decoded);
            }

            if (UNLIKELY(!ret)) {
                return Status::InternalError("PlainDecoder append strings to column failed");
            }
        }

        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        size_t num_decoded = 0;
        while (num_decoded < values_to_skip && _offset + sizeof(int32_t) <= _data.size) {
            uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
            _offset += sizeof(int32_t);
            _offset += length;
            num_decoded++;
        }
        // unlikely happened
        if (UNLIKELY(num_decoded < values_to_skip || _offset > _data.size)) {
            return Status::InternalError(
                    strings::Substitute("going to skip out-of-bounds data, offset=$0,size=$1", _offset, _data.size));
        }
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        auto* slices = reinterpret_cast<Slice*>(dst);

        size_t num_decoded = 0;
        while (num_decoded < count && _offset + sizeof(int32_t) <= _data.size) {
            uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + _offset);
            _offset += sizeof(int32_t);
            slices[num_decoded] = Slice(_data.data + _offset, length);
            _offset += length;
            num_decoded++;
        }
        // never happend
        if (num_decoded < count || _offset > _data.size) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, offset=$0,count=$1,size=$2", _offset, count, _data.size));
        }
        return Status::OK();
    }

private:
    Status _out_of_bounds_error(size_t offset, size_t count) const {
        return Status::InternalError(strings::Substitute("going to read out-of-bounds data, offset=$0,count=$1,size=$2",
                                                         offset, count, _data.size));
    }

    // We expect a pushed-down filter to keep few rows, so we copy the bytes of the selected rows only.
    // We still read the length prefix of every non-null value, because it is the only way to find where
    // the next value starts. A NULL row and a row that the filter drops both get an empty value. We fear a
    // reader that looks at every non-NULL value of the column, for example the lookup in a global dictionary:
    // it would fail on an empty value that is not NULL. So we also mark a dropped row as NULL, as the
    // dictionary decoder does. The caller has already written the NULL flags of this batch to the last
    // `count` entries of the null column.
    // The filter has one byte per row of the batch, NULL rows included.
    Status _next_batch_with_nulls_filtered(size_t count, size_t read_count, const uint8_t* __restrict is_nulls,
                                           const FilterData* __restrict filter, Column* dst) {
        auto* binary_column = ColumnHelper::get_binary_column(dst);
        auto& offsets = binary_column->get_offset();
        auto& bytes = binary_column->get_bytes();
        const size_t prev_offsets = offsets.size();
        raw::stl_vector_resize_uninitialized(&offsets, count + prev_offsets);

        NullableColumn* nullable_dst = dst->is_nullable() ? down_cast<NullableColumn*>(dst) : nullptr;
        uint8_t* dst_nulls = nullptr;
        if (nullable_dst != nullptr) {
            auto& null_data = nullable_dst->null_column_raw_ptr()->get_data();
            dst_nulls = null_data.data() + null_data.size() - count;
        }
        size_t num_dropped = 0;

        _temp_lengths.resize(read_count + 1);
        _temp_datas.resize(read_count + 1);
        uint32_t* lengths = _temp_lengths.data();
        char** datas = _temp_datas.data();

        size_t cursor = _offset;
        size_t end_offset = bytes.size();
        size_t num_selected = 0;
        size_t max_size = 0;
        for (size_t i = 0; i < count; ++i) {
            if (!is_nulls[i]) {
                if (UNLIKELY(cursor + sizeof(uint32_t) > _data.size)) {
                    return _out_of_bounds_error(cursor, count);
                }
                uint32_t length = decode_fixed32_le(reinterpret_cast<const uint8_t*>(_data.data) + cursor);
                cursor += sizeof(uint32_t);
                if (UNLIKELY(length > _data.size - cursor)) {
                    return _out_of_bounds_error(cursor, count);
                }
                if (filter[i]) {
                    datas[num_selected] = _data.data + cursor;
                    lengths[num_selected] = length;
                    num_selected++;
                    end_offset += length;
                    max_size = max_size > length ? max_size : length;
                } else if (dst_nulls != nullptr) {
                    dst_nulls[i] = 1;
                    num_dropped++;
                }
                cursor += length;
            }
            offsets[prev_offsets + i] = end_offset;
        }
        _offset = cursor;
        if (num_dropped > 0) {
            nullable_dst->set_has_null(true);
        }

        bytes.reserve(end_offset);
        binary_column->invalidate_slice_cache();
        if (num_selected == 0) {
            return Status::OK();
        }

        // The overflow copy reads max_size bytes from the start of every selected value. We fear reading
        // past the end of the page when the last selected value is close to it, so we use the exact copy there.
        max_size = std::max(BitUtil::next_power_of_two(max_size), 8L);
        if (datas[num_selected - 1] - _data.data + max_size <= _data.size) {
            binary_column->append_bytes_overflow(datas, lengths, num_selected, max_size);
        } else {
            binary_column->append_bytes(datas, lengths, num_selected);
        }
        DCHECK_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
        return Status::OK();
    }

    Slice _data;
    size_t _offset = 0;
    // The number of values that the reader announced for the next page, and the filter share that the width of
    // the values of the current page allows. A page of an unknown width gets the share of the narrowest values.
    size_t _page_num_values = 0;
    double _max_filter_share = max_filter_share(0);
    std::vector<uint32_t> _temp_lengths;
    std::vector<char*> _temp_datas;
};

// plain encoding for boolean type is stored as `Bit Packed`, `LSB` first format
// more details refer to: https://github.com/apache/parquet-format/blob/master/Encodings.md#PLAIN
template <>
class PlainDecoder<bool> final : public Decoder {
public:
    PlainDecoder() = default;
    ~PlainDecoder() override = default;

    std::string to_string() const override { return "PlainDecoder<bool>"; }

    Status set_data(const Slice& data) override {
        _batched_bit_reader.reset(reinterpret_cast<const uint8_t*>(data.data), data.size);
        _decoded_values_buffer.reset();
        _bits_left = static_cast<uint64_t>(data.size) * 8;
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        RETURN_IF_ERROR(_check_values_left(count));
        auto original_size = dst->size();
        dst->resize(original_size + count);
        auto num_unpacked_values = unpack_batch(count, dst->mutable_raw_data() + original_size);
        if (num_unpacked_values < count) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, count=$0,num_unpacked_values=$1", count, num_unpacked_values));
        }
        _bits_left -= count;
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        RETURN_IF_ERROR(_check_values_left(values_to_skip));
        //TODO(Smith) still heavy work load
        // resize() — see encoding_bss.h: reserve() leaves storage uninitialised,
        // writing through data() is UB.
        _skip_buffer.resize(values_to_skip);
        return next_batch(values_to_skip, _skip_buffer.data());
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        RETURN_IF_ERROR(_check_values_left(count));
        auto num_unpacked_values = unpack_batch(count, dst);
        if (num_unpacked_values < count) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, count=$0,num_unpacked_values=$1", count, num_unpacked_values));
        }
        _bits_left -= count;
        return Status::OK();
    }

private:
    // A value takes one bit. The count comes from the page header and the caller, and the caller sizes its
    // buffer from it, so we stop a count that the page cannot hold before any buffer is sized.
    Status _check_values_left(size_t count) const {
        if (UNLIKELY(count > _bits_left)) {
            return Status::InternalError(strings::Substitute(
                    "going to read out-of-bounds data, count=$0,values left=$1", count, _bits_left));
        }
        return Status::OK();
    }

    uint64_t _bits_left = 0;

    static const int kBitPackedBatchSize = 32;
    static const int kBitPackedDefaultValue = 8;

    BatchedBitReader _batched_bit_reader;
    std::vector<uint8_t> _skip_buffer;

    std::unique_ptr<uint8_t[]> _decoded_values_buffer;
    std::size_t _decoded_buffer_size;
    std::size_t _decoded_values_size;
    std::size_t _decoded_values_offset;

    std::size_t read_decoded_values(std::size_t num_values, uint8_t* v) {
        if (_decoded_values_buffer != nullptr && _decoded_values_offset < _decoded_values_size) {
            auto read_count = std::min(num_values, _decoded_values_size - _decoded_values_offset);
            memcpy(v, &_decoded_values_buffer[_decoded_values_offset], read_count * sizeof(uint8_t));
            _decoded_values_offset += read_count;
            return read_count;
        } else {
            return 0;
        }
    }

    void unpack_round_up_num_values(int bit_width, std::size_t num_values) {
        auto round_up_num_values = BitUtil::round_up_numi32(static_cast<uint32_t>(num_values)) << 5;
        if (_decoded_values_buffer == nullptr || _decoded_buffer_size < round_up_num_values) {
            _decoded_values_buffer = std::make_unique<uint8_t[]>(round_up_num_values);
            _decoded_buffer_size = round_up_num_values;
        }
        _decoded_values_size = _batched_bit_reader.unpack_batch(bit_width, static_cast<int>(round_up_num_values),
                                                                _decoded_values_buffer.get());
        _decoded_values_offset = 0;
    }

    // BatchedBitReader::unpack_batch may drop trailing bits, for safety, we should decode 32*n value a batch
    // and buffer it.
    std::size_t unpack_batch(std::size_t num_values, uint8_t* v) {
        // if _decoded_values_buffer has remaining unread values, read it first
        auto read_count = read_decoded_values(num_values, v);
        if (read_count == num_values) {
            return num_values;
        }
        num_values -= read_count;

        // If 'num_values' is a multiple of 32 or 'bit_width' * 'num_values' must be a multiple of 8.
        // We will not drop any trailing bits.
        if (num_values % kBitPackedBatchSize == 0 ||
            (num_values * kBooleanBitPackedBitWidth) % kBitPackedDefaultValue == 0) {
            read_count += _batched_bit_reader.unpack_batch(kBooleanBitPackedBitWidth, static_cast<int>(num_values),
                                                           v + read_count);
        } else {
            // Else, read round up number to 32 of values and buffer it
            unpack_round_up_num_values(kBooleanBitPackedBitWidth, num_values);
            read_count += read_decoded_values(num_values, v + read_count);
        }

        return read_count;
    }
};

// Fixed length byte array encoder and decoder
class FLBAPlainEncoder final : public Encoder {
public:
    FLBAPlainEncoder() = default;
    ~FLBAPlainEncoder() override = default;

    std::string to_string() const override { return "FLBAPlainEncoder"; }

    Status append(const uint8_t* vals, size_t count) override {
        if (count == 0) return Status::OK();

        const auto* slices = (const Slice*)vals;
        _buffer.reserve(_buffer.size() + count * slices[0].size);
        for (int i = 0; i < count; ++i) {
            DCHECK_EQ(slices[0].size, slices[i].size);
            _buffer.append(slices[i].data, slices[i].size);
        }
        return Status::OK();
    }

    Slice build() override { return {_buffer.data(), _buffer.size()}; }

private:
    faststring _buffer;
};

class FLBAPlainDecoder final : public Decoder {
public:
    FLBAPlainDecoder() = default;
    ~FLBAPlainDecoder() override = default;

    std::string to_string() const override { return "FLBAPlainDecoder"; }

    static Status decode(const std::string& buffer, Slice* value) {
        value->data = const_cast<char*>(buffer.data());
        value->size = buffer.size();
        return Status::OK();
    }

    Status set_type_length(int32_t type_length) override {
        if (UNLIKELY(!ParquetUtils::is_valid_flba_type_length(type_length))) {
            _type_length = 0;
            return Status::Corruption(
                    strings::Substitute("Invalid type length $0 of FIXED_LEN_BYTE_ARRAY, expect a value in [1, $1]",
                                        type_length, ParquetUtils::MAX_FLBA_TYPE_LENGTH));
        }
        _type_length = type_length;
        return Status::OK();
    }

    Status set_data(const Slice& data) override {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        size_t bytes = 0;
        RETURN_IF_ERROR(_get_values_size(count, "read", &bytes));
        auto ret = dst->append_continuous_fixed_length_strings(_data.data + _offset, count, _type_length);
        if (UNLIKELY(!ret)) {
            return Status::InternalError("FLBAPlainDecoder append strings to column failed");
        }
        _offset += bytes;
        return Status::OK();
    }

    Status next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type, Column* dst,
                                 const FilterData* filter) override {
        if (null_infos.num_ranges <= 1) {
            return Decoder::next_batch_with_nulls(count, null_infos, content_type, dst, filter);
        }

        const uint8_t* __restrict is_nulls = null_infos.nulls_data();
        DCHECK(dst->is_nullable());
        auto* nullable_column = down_cast<NullableColumn*>(dst);
        size_t null_cnt = null_infos.num_nulls;
        size_t read_count = count - null_cnt;
        // check the page holds the values before we change the column
        size_t read_bytes_size = 0;
        RETURN_IF_ERROR(_get_values_size(read_count, "read", &read_bytes_size));
        // fill null columns
        _next_null_column(count, null_infos, nullable_column);
        // fill data columns
        auto* binary_column = down_cast<BinaryColumn*>(nullable_column->data_column_raw_ptr());

        auto& bytes = binary_column->get_bytes();
        size_t offset = bytes.size();
        {
            // fill byte columns
            size_t prev_bytes_size = bytes.size();

            bytes.resize(prev_bytes_size + read_bytes_size);
            memcpy(bytes.data() + prev_bytes_size, _data.data + _offset, read_bytes_size);
            _offset += read_bytes_size;
        }
        auto& offsets = binary_column->get_offset();
        size_t prev_offsets = offsets.size();
        raw::stl_vector_resize_uninitialized(&offsets, count + prev_offsets);
        {
            // fill offset columns
            for (size_t i = 0; i < count; ++i) {
                offset += is_nulls[i] ? 0 : _type_length;
                offsets[prev_offsets + i] = offset;
            }
        }
        DCHECK_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());

        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        size_t bytes = 0;
        RETURN_IF_ERROR(_get_values_size(values_to_skip, "skip", &bytes));
        _offset += bytes;
        return Status::OK();
    }

    Status next_batch(size_t count, uint8_t* dst) override {
        size_t bytes = 0;
        RETURN_IF_ERROR(_get_values_size(count, "read", &bytes));

        auto* slices = reinterpret_cast<Slice*>(dst);
        size_t i = 0;

#ifdef __AVX2__
        static_assert(sizeof(Slice) == sizeof(int128_t));
        __m256i fixed_length = _mm256_set1_epi64x(_type_length);
        __m256i inc = _mm256_set1_epi64x(_type_length * 4);
        __m256i offsets = _mm256_setr_epi64x(0, _type_length, _type_length * 2, _type_length * 3);
        __m256i cur = _mm256_set1_epi64x((uint64_t)(_data.data + _offset));
        cur = _mm256_add_epi64(cur, offsets);
        for (; i + 4 <= count; i += 4) {
            // Interleave (ptr,len) lanes to materialise 4 Slice{ptr,len} structs.
            // Replaces clang-only __builtin_shufflevector with portable AVX2 intrinsics.
            __m256i unpacklo = _mm256_unpacklo_epi64(cur, fixed_length); // [ptr0, len, ptr2, len]
            __m256i unpackhi = _mm256_unpackhi_epi64(cur, fixed_length); // [ptr1, len, ptr3, len]
            __m256i lo = _mm256_permute2x128_si256(unpacklo, unpackhi, 0x20);
            __m256i hi = _mm256_permute2x128_si256(unpacklo, unpackhi, 0x31);

            _mm256_storeu_si256(reinterpret_cast<__m256i*>(&slices[i]), lo);
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(&slices[i + 2]), hi);

            cur = _mm256_add_epi64(cur, inc);
        }
        _offset += i * _type_length;

#endif

        for (; i < count; ++i) {
            slices[i] = Slice(_data.data + _offset, _type_length);
            _offset += _type_length;
        }

        return Status::OK();
    }

private:
    // Returns in `bytes` the size of `count` values, and checks that the rest of the page holds them.
    // We are afraid that the product of the type length and count wraps: a wrapped size passes the
    // comparison with the page size and the caller reads outside the page. So we detect the wrap first,
    // and compare the size without adding to the offset.
    Status _get_values_size(size_t count, const char* action, size_t* bytes) const {
        *bytes = 0;
        if (count == 0) {
            return Status::OK();
        }
        if (UNLIKELY(_type_length == 0)) {
            return Status::Corruption("FIXED_LEN_BYTE_ARRAY type length is not set");
        }
        if (UNLIKELY(__builtin_mul_overflow(_type_length, count, bytes) || *bytes > _data.size ||
                     _offset > _data.size - *bytes)) {
            return Status::InternalError(strings::Substitute(
                    "going to $0 out-of-bounds data, offset=$1,count=$2,size=$3", action, _offset, count, _data.size));
        }
        return Status::OK();
    }

    Slice _data;
    size_t _type_length = 0;
    size_t _offset = 0;
};

} // namespace starrocks::parquet
