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
#include <functional>
#include <memory>

#include "column/vectorized_fwd.h"
#include "common/compiler_util.h"
#include "common/status.h"
#include "gen_cpp/parquet_types.h"
#include "utils.h"

namespace starrocks {

class Slice;
class Column;
class NullableColumn;

} // namespace starrocks

namespace starrocks::parquet {

class Encoder {
public:
    Encoder() = default;
    virtual ~Encoder() = default;

    virtual Status append(const uint8_t* vals, size_t count) = 0;

    virtual Slice build() = 0;

    virtual Status encode_dict(Encoder* dict_encoder, size_t* num_dicts) {
        return Status::NotSupported("encode_dict is not supported");
    }

    // used to set fixed length for FLBA
    virtual void set_type_length(int32_t type_length) {}

    virtual std::string to_string() const { return "Encoder"; }
};

// The reader offers every decoder the filter of the rows that the query keeps, and each decoder decides whether
// to use it. A decoder that uses the filter does not decode the value of a row that the filter drops: such a row
// gets NULL when the destination is nullable, and a zero or an empty value when it is not. A decoder that does not
// use the filter decodes every row, so a dropped row holds its real value. The caller removes the dropped rows
// afterwards, so it has to accept both.
enum class FilterMode : uint8_t {
    // The decoder uses the filter when it expects the filtered decode to be faster, by the share of the rows of
    // the read range that the filter keeps (see Decoder::set_filter_share()).
    AUTO,
    // The decoder uses every filter it is offered, if it can use a filter at all. Tests and benchmarks set it to
    // see the filtered decode at any share.
    ALWAYS,
};

class Decoder {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    virtual std::string to_string() const { return "Decoder"; }

    // Tells the decoder how many values the next page holds, NULL values included. The reader calls it before
    // set_data(). A decoder that chooses how to decode by the width of a value takes the width from the size of
    // the page and this number, so it does not have to scan the page. The other decoders ignore it, and 0 or no
    // call means that the number is not known.
    virtual void set_num_values(size_t num_values) {}

    // Tells the decoder the share of the rows that the filter of the current read range keeps, from 0 to 1. The
    // reader counts the rows once for the range, and tells every decoder that the range uses before the decoder
    // decodes, so a decoder does not count the filter and all the batches and runs of the range get the same
    // answer. A decoder that can use a filter compares the share with the largest share at which a filtered decode
    // still pays off, see _use_filter(). A decoder that is not told the share takes it as 1, a filter that keeps
    // every row.
    void set_filter_share(double share) { _filter_share = share; }

    virtual Status set_dict(int chunk_size, size_t num_values, Decoder* decoder) {
        return Status::NotSupported("set_dict is not supported");
    }

    virtual Status get_dict_values(Column* column) { return Status::NotSupported("get_dict_values is not supported"); }

    virtual Status get_dict_values(const Buffer<int32_t>& dict_codes, const NullableColumn& nulls, Column* column) {
        return Status::NotSupported("get_dict_values is not supported");
    }

    // Used to set the fixed length of FLBA. The decoders of FLBA return Corruption for a length that
    // a real file cannot have, and the other decoders ignore it.
    virtual Status set_type_length(int32_t type_length) { return Status::OK(); }

    // Set a new page to decode.
    virtual Status set_data(const Slice& data) = 0;

    // For history reason, decoder don't known how many elements encoded in one page.
    // Caller must assure that no out-of-bounds access.
    // It will return ERROR if caller wants to read out-of-bound data.
    virtual Status next_batch(size_t count, ColumnContentType content_type, Column* dst,
                              const FilterData* filter = nullptr) = 0;
    virtual Status next_batch_with_nulls(size_t count, const NullInfos& null_infos, ColumnContentType content_type,
                                         Column* dst, const FilterData* filter);

    virtual Status skip(size_t values_to_skip) = 0;

    // Currently, this function is only used to read dictionary values.
    virtual Status next_batch(size_t count, uint8_t* dst) {
        return Status::NotSupported("next_batch is not supported");
    }

protected:
    void _next_null_column(size_t count, const NullInfos& null_infos, NullableColumn* dst);

    // A NULL value in a batch for a column that cannot hold NULL means that the file contradicts the schema of the
    // table, for example a required field of an Iceberg table that was written with NULL values. We fear that going
    // on would leave the column shorter than the read range and out of step with the other columns of the chunk,
    // because a column that is not nullable has no place for a NULL row. So the decoders that take NULL rows call
    // this first and give back its error. The decoder knows neither the file nor the column, and the caller that
    // does adds them. A batch without NULL rows is fine for a column that is not nullable.
    Status _check_nulls_fit(const NullInfos& null_infos, const Column* dst, size_t count) const;

    // Cuts `dst` back to `size` rows: the data, the offsets of a binary column, the null column of a nullable
    // column, and its has_null flag.
    static void _truncate(Column* dst, size_t size);

    // Runs `append`, which adds the rows of a batch to `dst`, and gives `dst` back as it was when `append` returns an
    // error. We fear a page that is short or corrupt in the middle of a batch: the rows that were appended before
    // the error were not decoded, the offsets of a binary column may not match its bytes, and the next use of the
    // column reads garbage. A decoder that fails after it changed the column uses this, or checks the whole batch
    // against the page before it changes the column. The position of the decoder is not part of this.
    template <typename Append>
    static Status _append_or_rollback(Column* dst, Append&& append) {
        const size_t size = dst->size();
        Status status = append();
        if (UNLIKELY(!status.ok())) {
            _truncate(dst, size);
        }
        return status;
    }

    // Tells whether to use `filter`. The decoder passes the largest share of the rows that the filter may keep for
    // the filtered decode to pay off, `max_share`. We expect the filtered decode to be slower than the plain one
    // when the filter keeps most of the rows, because it works row by row and gains only on the rows that it
    // drops. So we use the filter when the share that the reader told is below `max_share`. The share is the same
    // for the whole read range, so every call of the range decides alike, whatever its size. A decoder in
    // FilterMode::ALWAYS uses any filter.
    bool _use_filter(const FilterData* filter, double max_share) const {
        return filter != nullptr && (_filter_mode == FilterMode::ALWAYS || _filter_share < max_share);
    }

    // The share of the rows of the read range that the filter keeps.
    double _filter_share = 1.0;

#ifdef BE_TEST
    // The unit tests give a decoder filters of any share and expect it to use them, so a test build starts with
    // ALWAYS. The tests of the decision itself set AUTO and the share.
    FilterMode _filter_mode = FilterMode::ALWAYS;
#else
    FilterMode _filter_mode = FilterMode::AUTO;
#endif
};

class EncodingInfo {
public:
    // Get EncodingInfo for TypeInfo and EncodingTypePB
    static Status get(tparquet::Type::type type, tparquet::Encoding::type encoding, const EncodingInfo** info);

    Status create_decoder(std::unique_ptr<Decoder>* decoder) const { return _create_decoder_func(decoder); }

    Status create_encoder(std::unique_ptr<Encoder>* encoder) const { return _create_encoder_func(encoder); }

    tparquet::Type::type type() const { return _type; }
    tparquet::Encoding::type encoding() const { return _encoding; }

private:
    friend class EncodingInfoResolver;

    template <typename TypeEncodingTraits>
    explicit EncodingInfo(TypeEncodingTraits traits);

    using CreateDecoderFunc = std::function<Status(std::unique_ptr<Decoder>*)>;
    CreateDecoderFunc _create_decoder_func;
    using CreateEncoderFunc = std::function<Status(std::unique_ptr<Encoder>*)>;
    CreateEncoderFunc _create_encoder_func;

    tparquet::Type::type _type;
    tparquet::Encoding::type _encoding;
};

} // namespace starrocks::parquet
