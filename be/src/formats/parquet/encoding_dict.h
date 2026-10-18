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

#include <map>
#include <memory>
#include <vector>

#include "column/column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "common/status.h"
#include "formats/parquet/encoding.h"
#include "simd/expand.h"
#include "simd/rle_simd.h"
#include "simd/simd.h"
#include "util/coding.h"
#include "util/cpu_info.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace {
// Single-pass min/max bounds check for dictionary indices. Reads the unsigned
// indices through a signed reinterpret to feed SIMD min/max; any value
// >= INT32_MAX surfaces as a negative min, which is also "out of bounds" for
// any realistic dictionary, so the signed view is correct.
inline bool indices_out_of_bounds(const uint32_t* indices, int32_t count, size_t dict_size) {
    if (count <= 0) return false;
    int32_t min_idx, max_idx;
    starrocks::simd_minmax_int32(reinterpret_cast<const int32_t*>(indices), count, min_idx, max_idx);
    return min_idx < 0 || static_cast<size_t>(max_idx) >= dict_size;
}
} // anonymous namespace

namespace starrocks::parquet {

template <typename T>
class DictEncoder final : public Encoder {
public:
    DictEncoder() = default;
    ~DictEncoder() override = default;

    std::string to_string() const override { return fmt::format("DictEncoder<{}>", typeid(T).name()); }

    Status append(const uint8_t* vals, size_t count) override {
        const T* ptr = (const T*)vals;
        for (int i = 0; i < count; ++i) {
            int index = 0;
            auto it = _dict.find(*ptr);
            if (it == _dict.end()) {
                _dict[*ptr] = _num_dict;
                _dict_vector.push_back(*ptr);
                index = _num_dict++;
            } else {
                index = it->second;
            }
            _indexes.push_back(index);
            ptr++;
        }
        return Status::OK();
    }

    Slice build() override {
        // TODO(zc): modify RLE encoder to avoid another copy
        uint8_t bit_width = BitUtil::log2(_num_dict);
        faststring encoded_data;
        RleEncoder<int32_t> rle_encoder(&encoded_data, bit_width);
        for (auto index : _indexes) {
            rle_encoder.Put(index);
        }
        int length = rle_encoder.Flush();
        _buffer.append(&bit_width, 1);
        _buffer.append(encoded_data.data(), length);
        return {_buffer.data(), _buffer.size()};
    }

    Status encode_dict(Encoder* dict_encoder, size_t* num_dicts) override {
        RETURN_IF_ERROR(dict_encoder->append((const uint8_t*)&_dict_vector[0], _num_dict));
        *num_dicts = _num_dict;
        return Status::OK();
    }

private:
    size_t _num_dict{};
    std::map<T, int> _dict; // not unordered_map: T=Slice has no std::hash specialisation.
    std::vector<T> _dict_vector;
    std::vector<int> _indexes;
    faststring _buffer;
};

class CacheAwareDictDecoder : public Decoder {
public:
    CacheAwareDictDecoder() { _dict_size_threshold = CpuInfo::get_l2_cache_size(); }
    ~CacheAwareDictDecoder() override = default;

    std::string to_string() const override { return "CacheAwareDictDecoder"; }

    Status next_batch(size_t count, ColumnContentType content_type, Column* dst, const FilterData* filter) override {
        switch (content_type) {
        case DICT_CODE: {
            FixedLengthColumn<int32_t>* data_column;
            if (dst->is_nullable()) {
                auto nullable_column = down_cast<NullableColumn*>(dst);
                nullable_column->null_column_raw_ptr()->append_default(count);
                data_column = down_cast<FixedLengthColumn<int32_t>*>(nullable_column->data_column_raw_ptr());
            } else {
                data_column = down_cast<FixedLengthColumn<int32_t>*>(dst);
            }
            size_t cur_size = data_column->size();
            data_column->resize_uninitialized(cur_size + count);
            int32_t* __restrict__ data = data_column->get_data().data() + cur_size;
            auto decoded_num = _rle_batch_reader.GetBatch(reinterpret_cast<uint32_t*>(data), count);
            if (decoded_num < count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }
            // The consumers of the codes index the dictionary with them, so we reject a code that the
            // dictionary does not have here, where all the codes of the batch are real values.
            if (UNLIKELY(indices_out_of_bounds(reinterpret_cast<const uint32_t*>(data), count,
                                               _get_num_dict_values()))) {
                return Status::Corruption("Dictionary code not in dictionary bounds");
            }
            break;
        }
        case VALUE: {
#ifndef BE_TEST
            if (!_filter_gate_open()) {
                filter = nullptr;
            }
#endif // BE_TEST
            return _next_batch_value(count, dst, _select_filter(filter));
        }
        default:
            return Status::NotSupported("read type not supported");
        }
        return Status::OK();
    }

    // The decision is made here, once for the batch. A path that reads the batch run by run passes the filter of
    // each run to next_batch(), which makes the same decision again, because it depends on the share of the read
    // range and not on the run.
    Status next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type, Column* dst,
                                 const FilterData* filter) override {
        if (!_filter_gate_open()) {
            filter = nullptr;
        }
        return _do_next_batch_with_nulls(count, null_infos, content_type, dst, _select_filter(filter));
    }

    // `src_data` holds the `num_non_nulls` values of the batch and must have one more readable element after them.
    // We fear a batch that ends with NULL rows: SIMD::Expand::expand_load copies the next source element for
    // every row, NULL rows included, so it reads the element after the last value. A caller has to allocate that
    // element, or the read goes past the buffer.
    template <class DataType>
    void assign_data_with_nulls(size_t count, size_t num_non_nulls, const uint8_t* nulls, const DataType* src_data,
                                DataType* dst_data) {
        // opt branch for process sparse column
        if (num_non_nulls < count / 10) {
            size_t cnt = 0;
            size_t i = 0;
#ifdef __AVX2__
            for (i = 0; i + 32 <= count; i += 32) {
                // Load the next 32 elements of is_nulls into a mask
                __m256i loaded = _mm256_loadu_si256((__m256i*)&nulls[i]);
                int mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(loaded, _mm256_setzero_si256()));
                phmap::priv::BitMask<uint32_t, 32> bitmask(mask);
                for (auto idx : bitmask) {
                    dst_data[i + idx] = src_data[cnt++];
                }
            }
#endif
            // Tail: guard the load so we don't read past the last valid src element
            // when the trailing batch ends with nulls.
            for (; i < count; ++i) {
                if (!nulls[i]) {
                    dst_data[i] = src_data[cnt++];
                }
            }
            DCHECK_EQ(cnt, num_non_nulls) << "count:" << count << " null_cnt:" << count - num_non_nulls;
        } else {
            SIMD::Expand::expand_load(dst_data, src_data, nulls, count);
        }
    }

    Status next_dict_code_batch_with_nulls(size_t count, size_t cur_size, const NullInfos& null_infos, Column* dst) {
        size_t null_cnt = null_infos.num_nulls;
        auto nullable_column = down_cast<NullableColumn*>(dst);

        size_t read_count = count - null_cnt;
        Int32Column* data_column = down_cast<Int32Column*>(nullable_column->data_column_raw_ptr());
        // resize data
        data_column->resize_uninitialized(cur_size + count);
        int32_t* __restrict__ data = data_column->get_data().data() + cur_size;

        if (read_count == 0) {
            return Status::OK();
        }
        // Reuse the _indexes member buffer rather than a VLA — large batches blow the stack.
        _indexes.resize(read_count + 1);
        auto decoded_num = _rle_batch_reader.GetBatch(_indexes.data(), read_count);
        if (decoded_num < read_count) {
            return Status::InternalError("didn't get enough data from dict-decoder");
        }
        // only the codes of the non-NULL rows are in _indexes here, the slots of the NULL rows are not set
        if (UNLIKELY(indices_out_of_bounds(_indexes.data(), read_count, _get_num_dict_values()))) {
            return Status::Corruption("Dictionary code not in dictionary bounds");
        }

        assign_data_with_nulls(count, read_count, null_infos.nulls_data(), reinterpret_cast<int32_t*>(_indexes.data()),
                               data);

        return Status::OK();
    }

protected:
    // The number of dictionary values that set_dict asks the decoder of the dictionary page for at a time.
    static constexpr size_t kDictDecodeStep = 1 << 16;

    virtual size_t _get_dict_size() const = 0;
    // The number of values of the dictionary, where _get_dict_size is its size in bytes.
    virtual size_t _get_num_dict_values() const = 0;
    // The largest share of the rows of a read range that the filter may keep for the filtered decode to be faster,
    // see _use_filter(). It depends on the type of the values and, for strings, on their width.
    virtual double _max_filter_share() const = 0;

    // We measured that with a dictionary inside the L2 cache the filtered decode is slower than the plain one
    // for numbers (2 to 11 times) and for most strings, because the lookups hit the cache and the filtered decode
    // adds a branch per row. So only a dictionary larger than the cache may gain from the filter. The switch
    // turns the filter off for all dictionary decoders.
    bool _filter_gate_open() const {
        return _get_dict_size() > _dict_size_threshold && config::parquet_cache_aware_dict_decoder_enable;
    }

    // Returns `filter` when the decoder should use it, and nullptr when it should not.
    const FilterData* _select_filter(const FilterData* filter) const {
        return _use_filter(filter, _max_filter_share()) ? filter : nullptr;
    }

    virtual Status _next_batch_value(size_t count, Column* dst, const FilterData* filter) = 0;
    virtual Status _do_next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type,
                                             Column* dst, const FilterData* filter) = 0;
    RleBatchDecoder<uint32_t> _rle_batch_reader;
    std::vector<uint32_t> _indexes;

private:
    size_t _dict_size_threshold = 0;
};

// TODO(zc): support read run later. however should add more interface to Column first
template <typename T>
class DictDecoder final : public CacheAwareDictDecoder {
public:
    DictDecoder() = default;
    ~DictDecoder() override = default;

    std::string to_string() const override { return fmt::format("DictDecoder<{}>", typeid(T).name()); }

    // initialize dictionary
    Status set_dict(int chunk_size, size_t num_values, Decoder* decoder) override {
        _indexes.resize(chunk_size);
        // num_values comes from the page header, and a corrupt header can claim far more values than the
        // page holds. We want the memory we allocate to follow the values that the decoder returns, so we
        // fill the dictionary in steps, and the decoder fails on the first step that the page cannot serve.
        _dict.clear();
        for (size_t done = 0; done < num_values;) {
            const size_t step = std::min(kDictDecodeStep, num_values - done);
            _dict.resize(done + step);
            RETURN_IF_ERROR(decoder->next_batch(step, reinterpret_cast<uint8_t*>(_dict.data() + done)));
            done += step;
        }
        return Status::OK();
    }

    Status set_data(const Slice& data) override {
        if (data.size > 0) {
            uint8_t bit_width = *data.data;
            // PARQUET-2115: [C++] Parquet dictionary bit widths are limited to 32 bits
            // https://github.com/apache/arrow/pull/12274/files
            if (PREDICT_FALSE(bit_width > 32)) {
                return Status::Corruption("bit width is larger than 32");
            }
            _rle_batch_reader = RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data.data) + 1,
                                                          static_cast<int>(data.size) - 1, bit_width);
        } else {
            return Status::Corruption("input encoded data size is 0");
        }
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        auto ret = _rle_batch_reader.SkipBatch(values_to_skip);
        if (UNLIKELY(ret != values_to_skip)) {
            return Status::InternalError("rle skip error, not enough values");
        }
        return Status::OK();
    }

    Status _do_next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type,
                                     Column* dst, const FilterData* filter) override {
        DCHECK(dst->is_nullable());
        if (null_infos.num_ranges <= 2) {
            return Decoder::next_batch_with_nulls(count, null_infos, content_type, dst, filter);
        }
        size_t cur_size = dst->size();
        _next_null_column(count, null_infos, down_cast<NullableColumn*>(dst));

        switch (content_type) {
        case DICT_CODE: {
            return next_dict_code_batch_with_nulls(count, cur_size, null_infos, dst);
        }
        case VALUE: {
            return next_value_batch_with_nulls(count, cur_size, null_infos, dst, filter);
        }
        default:
            return Status::NotSupported("read type not supported");
        }
        return Status::OK();
    }

    Status next_value_batch_with_nulls(size_t count, size_t cur_size, const NullInfos& null_infos, Column* dst,
                                       const FilterData* filter) {
        DCHECK(dst->is_nullable());
        const uint8_t* __restrict is_nulls = null_infos.nulls_data();
        // assign null infos
        size_t null_cnt = null_infos.num_nulls;
        auto nullable_column = down_cast<NullableColumn*>(dst);
        FixedLengthColumn<T>* data_column = down_cast<FixedLengthColumn<T>*>(nullable_column->data_column_raw_ptr());
        // resize data
        data_column->resize_uninitialized(cur_size + count);
        T* __restrict__ data = data_column->get_data().data() + cur_size;

        size_t read_count = count - null_cnt;

        if (read_count == 0) {
            return Status::OK();
        }

        if (filter) {
            _indexes.resize(read_count);
            auto decoded_num = _rle_batch_reader.GetBatch(_indexes.data(), read_count);
            if (decoded_num < read_count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }

            if (UNLIKELY(indices_out_of_bounds(_indexes.data(), read_count, _dict.size()))) {
                return Status::InternalError("Index not in dictionary bounds");
            }

            // We skip the dictionary lookup of a row the filter drops. We fear a reader that looks at every row
            // before the dropped rows are removed, for example a converter that multiplies each value by a
            // scale factor: it would read memory that nobody wrote and overflow on it. So a dropped row gets
            // zero, and it is also marked NULL, as the Slice specialization does, so that a reader which skips
            // NULL rows skips it. `_next_null_column` has already written the NULL flags of this batch.
            uint8_t* null_data = nullable_column->null_column_raw_ptr()->get_data().data() + cur_size;
            size_t cnt = 0;
            size_t num_dropped = 0;
            for (size_t i = 0; i < count; i++) {
                if (is_nulls[i]) {
                    continue;
                }
                if (filter[i]) {
                    data[i] = _dict[_indexes[cnt]];
                } else {
                    data[i] = T{};
                    null_data[i] = 1;
                    num_dropped++;
                }
                cnt++;
            }
            if (num_dropped > 0) {
                nullable_column->set_has_null(true);
            }
        } else {
            // assign_data_with_nulls reads one element after the last value when the batch ends with NULL rows,
            // so the buffer holds one more element than the values.
            _temp_read_data.resize(read_count + 1);
            auto ret =
                    _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), _temp_read_data.data(), read_count);
            if (UNLIKELY(ret <= 0)) {
                return Status::InternalError("DictDecoder GetBatchWithDict failed");
            }

            assign_data_with_nulls(count, read_count, null_infos.nulls_data(), _temp_read_data.data(), data);
        }

        return Status::OK();
    }

private:
    size_t _get_dict_size() const override { return _dict.size() * SIZE_OF_TYPE; }
    size_t _get_num_dict_values() const override { return _dict.size(); }

    // With a dictionary larger than the L2 cache, we measured the share where the filtered decode and the plain
    // decode take the same time at 17% without NULL and at about 10% with 10% NULL, for 4 and 8 byte values. We use
    // 15%. The filtered decode gains below it in both cases, except in a range with NULL rows between 10% and 15%,
    // where it loses a little, and it loses above it in both cases. A limit of 17% would lose more in the range
    // with NULL rows, and a limit of 10% would leave the gain between 10% and 17% unused when there is no NULL.
    double _max_filter_share() const override { return 0.15; }

    Status _next_batch_value(size_t count, Column* dst, const FilterData* filter) override {
        FixedLengthColumn<T>* data_column /* = nullptr */;
        if (dst->is_nullable()) {
            auto nullable_column = down_cast<NullableColumn*>(dst);
            nullable_column->null_column_raw_ptr()->append_default(count);
            data_column = down_cast<FixedLengthColumn<T>*>(nullable_column->data_column_raw_ptr());
        } else {
            data_column = down_cast<FixedLengthColumn<T>*>(dst);
        }

        size_t cur_size = data_column->size();
        data_column->resize_uninitialized(cur_size + count);
        T* __restrict__ data = data_column->get_data().data() + cur_size;

        if (filter) {
            _indexes.resize(count);
            auto decoded_num = _rle_batch_reader.GetBatch(_indexes.data(), count);
            if (decoded_num < count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }

            if (UNLIKELY(indices_out_of_bounds(_indexes.data(), count, _dict.size()))) {
                return Status::InternalError("Index not in dictionary bounds");
            }

            // A dropped row gets zero and, in a nullable column, a NULL flag, for the reason given in
            // next_value_batch_with_nulls. The null column has just got `count` default entries, which are the
            // last ones.
            uint8_t* null_data = nullptr;
            if (dst->is_nullable()) {
                auto& null_flags = down_cast<NullableColumn*>(dst)->null_column_raw_ptr()->get_data();
                null_data = null_flags.data() + null_flags.size() - count;
            }
            size_t num_dropped = 0;
            for (size_t i = 0; i < count; i++) {
                if (filter[i]) {
                    data[i] = _dict[_indexes[i]];
                } else {
                    data[i] = T{};
                    if (null_data != nullptr) {
                        null_data[i] = 1;
                    }
                    num_dropped++;
                }
            }
            if (null_data != nullptr && num_dropped > 0) {
                down_cast<NullableColumn*>(dst)->set_has_null(true);
            }
        } else {
            auto ret = _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), data, count);
            if (UNLIKELY(ret <= 0)) {
                return Status::InternalError("DictDecoder<> GetBatchWithDict failed");
            }
        }

        return Status::OK();
    }

    enum { SIZE_OF_TYPE = sizeof(T) };

    std::vector<T> _dict;
    std::vector<T> _temp_read_data;
};

class FixedSliceArray {
public:
    const Slice* data() const { return reinterpret_cast<Slice*>(_data.get()); }
    Slice* data() { return reinterpret_cast<Slice*>(_data.get()); }
    size_t size() const { return _size; }

    Slice& operator[](size_t idx) { return data()[idx]; }

    void resize(size_t size) {
        _data = std::make_unique_for_overwrite<uint8_t[]>(size * sizeof(Slice));
        _size = size;
    }

private:
    std::unique_ptr<uint8_t[]> _data;
    size_t _size{};
};

// Unlike the fixed-width specialization, a row skipped because a pushed-down filter excludes it
// cannot just be left with a garbage value: a BinaryColumn row that is not written contributes no
// bytes at all. Consumers of a FIXED_LEN_BYTE_ARRAY column may walk the bytes with a fixed stride
// instead of loading an offset per row -- BinaryToDecimalConverter does -- which is only valid
// under this layout invariant:
//
//     a non-NULL row occupies exactly `type_length` bytes, a NULL row occupies none.
//
// So the excluded rows are marked NULL rather than left NOT NULL. Leaving them NOT NULL creates a
// third state, zero bytes but not NULL, that the invariant does not cover: such a consumer would
// consume `type_length` bytes for the row anyway, decode every following row -- including the rows
// that survive the filter -- from a misaligned offset, and read past the end of the buffer. The
// caller drops the excluded rows immediately after this, so the NULL is never observable, and
// marking it also lets those consumers skip the excluded rows entirely.
template <>
class DictDecoder<Slice> final : public CacheAwareDictDecoder {
public:
    DictDecoder() = default;
    ~DictDecoder() override = default;

    std::string to_string() const override { return fmt::format("DictDecoder<Slice>"); }

    Status set_dict(int chunk_size, size_t num_values, Decoder* decoder) override {
        // num_values comes from the page header, and a corrupt header can claim far more values than the
        // page holds. We want the memory we allocate to follow the values that the decoder returns, so we
        // take the values in steps, copy each step into _dict_data, and let the decoder fail on the first
        // step that the page cannot serve.
        std::vector<Slice> step_slices;
        std::vector<size_t> lengths;
        _dict_data.clear();
        _max_value_length = 0;
        for (size_t done = 0; done < num_values;) {
            const size_t step = std::min(kDictDecodeStep, num_values - done);
            step_slices.resize(step);
            RETURN_IF_ERROR(decoder->next_batch(step, reinterpret_cast<uint8_t*>(step_slices.data())));

            size_t step_length = 0;
            for (const Slice& slice : step_slices) {
                step_length += slice.size;
            }
            size_t offset = _dict_data.size();
            raw::stl_vector_resize_uninitialized(&_dict_data, offset + step_length);
            for (const Slice& slice : step_slices) {
                if (slice.size > 0) {
                    memcpy(&_dict_data[offset], slice.data, slice.size);
                }
                offset += slice.size;
                lengths.push_back(slice.size);
                _max_value_length = std::max(_max_value_length, slice.size);
            }
            done += step;
        }

        // reserve enough memory to use append_strings_overflow
        const size_t total_length = _dict_data.size();
        _avg_value_length = num_values > 0 ? total_length / num_values : 0;
        raw::stl_vector_resize_uninitialized(&_dict_data, total_length + Column::APPEND_OVERFLOW_MAX_SIZE);

        // _dict_data does not move any more, so the values can point into it
        _dict.resize(num_values);
        size_t offset = 0;
        for (size_t i = 0; i < num_values; ++i) {
            _dict[i].data = reinterpret_cast<char*>(&_dict_data[offset]);
            _dict[i].size = lengths[i];
            offset += lengths[i];
        }

        return Status::OK();
    }

    Status get_dict_values(Column* column) override {
        auto ret = column->append_strings_overflow(_dict.data(), _dict.size(), _max_value_length);
        if (UNLIKELY(!ret)) {
            return Status::InternalError("DictDecoder append strings to column failed");
        }
        return Status::OK();
    }

    Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) override {
        const auto null_data = nulls.immutable_null_column_data();
        bool has_null = nulls.has_null();
        bool all_null = false;

        if (has_null) {
            size_t count = SIMD::count_nonzero(null_data);
            all_null = (count == null_data.size());
        }
        if (all_null) {
            column->append_default(null_data.size());
            return Status::OK();
        }

        // dict codes size and column size HAVE TO BE EXACTLY SAME.
        if (UNLIKELY(dict_codes.size() != null_data.size())) {
            return Status::InternalError("The number of dictionary codes differs from the number of rows");
        }
        FixedSliceArray slices;
        slices.resize(dict_codes.size());
        const size_t dict_size = _dict.size();
        if (!has_null) {
            if (UNLIKELY(indices_out_of_bounds(reinterpret_cast<const uint32_t*>(dict_codes.data()),
                                               static_cast<int32_t>(dict_codes.size()), dict_size))) {
                return Status::Corruption("Dictionary code not in dictionary bounds");
            }
            for (size_t i = 0; i < dict_codes.size(); i++) {
                slices[i] = _dict[dict_codes[i]];
            }
        } else {
            // A NULL row has no dictionary code: the slot of the code is not set. We want it to hold no
            // byte in the column, as a NULL row does when the values are read directly, because a consumer
            // such as the decimal converter walks the bytes of a fixed-width column with a stride and
            // counts only the rows that are not NULL. So a NULL row gets an empty value, and only the codes
            // of the rows that are not NULL are looked up.
            const Slice empty_value(reinterpret_cast<const char*>(_dict_data.data()), 0);
            const uint8_t* null_data_ptr = null_data.data();
            for (size_t i = 0; i < dict_codes.size(); i++) {
                if (null_data_ptr[i]) {
                    slices[i] = empty_value;
                    continue;
                }
                if (UNLIKELY(static_cast<uint32_t>(dict_codes[i]) >= dict_size)) {
                    return Status::Corruption("Dictionary code not in dictionary bounds");
                }
                slices[i] = _dict[dict_codes[i]];
            }
        }

        bool ret = column->append_strings_overflow(slices.data(), slices.size(), _max_value_length);

        if (UNLIKELY(!ret)) {
            return Status::InternalError("DictDecoder append strings to column failed");
        }
        return Status::OK();
    }

    Status set_data(const Slice& data) override {
        if (data.size > 0) {
            uint8_t bit_width = *data.data;
            // PARQUET-2115: [C++] Parquet dictionary bit widths are limited to 32 bits
            // https://github.com/apache/arrow/pull/12274/files
            if (PREDICT_FALSE(bit_width > 32)) {
                return Status::Corruption("bit width is larger than 32");
            }
            _rle_batch_reader = RleBatchDecoder<uint32_t>(reinterpret_cast<uint8_t*>(data.data) + 1,
                                                          static_cast<int>(data.size) - 1, bit_width);
        } else {
            return Status::Corruption("input encoded data size is 0");
        }
        return Status::OK();
    }

    Status skip(size_t values_to_skip) override {
        auto ret = _rle_batch_reader.SkipBatch(values_to_skip);
        if (UNLIKELY(ret != values_to_skip)) {
            return Status::InternalError("rle skip error, not enough values");
        }
        return Status::OK();
    }

private:
    size_t _get_dict_size() const override { return _dict_data.size(); }
    size_t _get_num_dict_values() const override { return _dict.size(); }

    // With a dictionary larger than the L2 cache, we measured the share where the filtered decode and the plain
    // decode take the same time at 16% to 20% for values of 8 and 32 bytes and at about 30% for 128 bytes. The
    // filtered decode skips the lookup and the copy of a dropped value, so we expect it to gain more on wide
    // values. We use 20%, and 30% when the average value of the dictionary is wider than 64 bytes.
    double _max_filter_share() const override { return _avg_value_length > 64 ? 0.3 : 0.2; }

    Status _do_next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type,
                                     Column* dst, const FilterData* filter) override {
        DCHECK(dst->is_nullable());
        if (null_infos.num_ranges <= 1) {
            return Decoder::next_batch_with_nulls(count, null_infos, content_type, dst, filter);
        }
        size_t cur_size = dst->size();
        _next_null_column(count, null_infos, down_cast<NullableColumn*>(dst));

        switch (content_type) {
        case DICT_CODE: {
            return next_dict_code_batch_with_nulls(count, cur_size, null_infos, dst);
        }
        case VALUE: {
            return next_value_batch_with_nulls(count, cur_size, null_infos, dst, filter);
        }
        default:
            return Status::NotSupported("read type not supported");
        }
        return Status::OK();
    }

    Status next_value_batch_with_nulls(size_t count, size_t cur_size, const NullInfos& null_infos, Column* dst,
                                       const FilterData* filter) {
        DCHECK(dst->is_nullable());
        const uint8_t* __restrict is_nulls = null_infos.nulls_data();
        // assign null infos
        size_t null_cnt = null_infos.num_nulls;
        // resize data
        auto* data_column = ColumnHelper::get_data_column(dst);
        if (UNLIKELY(!data_column->is_binary())) {
            return Status::InternalError("DictDecoder<Slice> expected a binary destination column");
        }
        auto* binary_column = down_cast<BinaryColumn*>(data_column);
        size_t read_count = count - null_cnt;

        if (read_count == 0) {
            binary_column->append_default(count);
            return Status::OK();
        }

        if (filter) {
            _indexes.resize(read_count);
            auto decoded_num = _rle_batch_reader.GetBatch(_indexes.data(), read_count);
            if (decoded_num < read_count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }
            if (UNLIKELY(indices_out_of_bounds(_indexes.data(), read_count, _dict.size()))) {
                return Status::InternalError("Index not in dictionary bounds");
            }
            // `_next_null_column` has already written this batch's NULL flags from the definition
            // levels; the rows the filter excludes are added on top, so that a row which
            // contributes no bytes is always marked NULL (see the class comment).
            uint8_t* null_data = down_cast<NullableColumn*>(dst)->null_column_raw_ptr()->get_data().data() + cur_size;
            size_t cnt = 0;
            size_t num_excluded = 0;
            for (int i = 0; i < count; ++i) {
                if (filter[i] && !is_nulls[i]) {
                    binary_column->append(_dict[_indexes[cnt]]);
                } else {
                    binary_column->append_default();
                    if (!is_nulls[i]) {
                        null_data[i] = 1;
                        ++num_excluded;
                    }
                }
                cnt += !is_nulls[i];
            }
            if (num_excluded > 0) {
                down_cast<NullableColumn*>(dst)->set_has_null(true);
            }

        } else {
            auto& bytes = binary_column->get_bytes();
            size_t offset = bytes.size();

            _slices.resize(read_count);
            auto ret = _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), _slices.data(), read_count);
            if (UNLIKELY(ret <= 0)) {
                return Status::InternalError("DictDecoder<Slice> GetBatchWithDict failed");
            }

            _temp_lengths.resize(read_count + 1);
            _temp_datas.resize(read_count + 1);
            uint32_t* lengths = _temp_lengths.data();
            char** datas = _temp_datas.data();

            for (size_t i = 0; i < read_count; ++i) {
                datas[i] = _slices[i].data;
                lengths[i] = _slices[i].size;
            }

            // relocate offsets
            auto& offsets = binary_column->get_offset();
            size_t prev_offsets = offsets.size();
            size_t cnt = 0;
            raw::stl_vector_resize_uninitialized(&offsets, count + prev_offsets);
            for (size_t i = 0; i < count; ++i) {
                offset += is_nulls[i] ? 0 : lengths[cnt++];
                offsets[prev_offsets + i] = offset;
            }

            if (read_count == 0) {
                return Status::OK();
            }
            binary_column->append_bytes_overflow(datas, lengths, read_count, _max_value_length);
            DCHECK_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
        }

        return Status::OK();
    }

    Status _next_batch_value(size_t count, Column* dst, const FilterData* filter) override {
        if (filter) {
            _indexes.resize(count);
            auto decoded_num = _rle_batch_reader.GetBatch(_indexes.data(), count);
            if (decoded_num < count) {
                return Status::InternalError("didn't get enough data from dict-decoder");
            }
            if (UNLIKELY(indices_out_of_bounds(_indexes.data(), count, _dict.size()))) {
                return Status::InternalError("Index not in dictionary bounds");
            }
            uint8_t* null_data = nullptr;
            if (dst->is_nullable()) {
                auto* null_column = down_cast<NullableColumn*>(dst)->null_column_raw_ptr();
                size_t null_base = null_column->size();
                null_column->append_default(count);
                null_data = null_column->get_data().data() + null_base;
            }
            auto* data_column = ColumnHelper::get_data_column(dst);
            if (UNLIKELY(!data_column->is_binary())) {
                return Status::InternalError("DictDecoder<Slice> expected a binary destination column");
            }
            auto* binary_column = down_cast<BinaryColumn*>(data_column);
            size_t num_excluded = 0;
            for (int i = 0; i < count; ++i) {
                if (filter[i]) {
                    binary_column->append(_dict[_indexes[i]]);
                } else {
                    // Skipping the dictionary lookup means this row contributes no bytes, so it
                    // must be marked NULL to keep the layout invariant (see the class comment).
                    binary_column->append_default();
                    if (null_data != nullptr) {
                        null_data[i] = 1;
                    }
                    ++num_excluded;
                }
            }
            if (num_excluded > 0 && null_data != nullptr) {
                down_cast<NullableColumn*>(dst)->set_has_null(true);
            }
        } else {
            _slices.resize(count);
            auto ret = _rle_batch_reader.GetBatchWithDict(_dict.data(), _dict.size(), _slices.data(), count);
            if (UNLIKELY(ret <= 0)) {
                return Status::InternalError("DictDecoder<Slice> GetBatchWithDict failed");
            }
            ret = dst->append_strings_overflow(_slices.data(), _slices.size(), _max_value_length);
            if (UNLIKELY(!ret)) {
                return Status::InternalError("DictDecoder append strings to column failed");
            }
        }
        return Status::OK();
    }

    enum { SIZE_OF_DICT_CODE_TYPE = sizeof(int32_t) };

    std::vector<uint8_t> _dict_data;
    FixedSliceArray _dict;
    FixedSliceArray _slices;
    size_t _max_value_length = 0;
    // The average length of a value of the dictionary.
    size_t _avg_value_length = 0;
    std::vector<uint32_t> _temp_lengths;
    std::vector<char*> _temp_datas;
};

} // namespace starrocks::parquet
