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

#include "formats/parquet/encoding.h"

#include <gtest/gtest.h>

#include <limits>
#include <random>
#include <set>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/types.h"
#include "util/byte_stream_split.h"

namespace starrocks::parquet {
class ParquetEncodingTest : public testing::Test {
public:
    ParquetEncodingTest() = default;
    ~ParquetEncodingTest() override = default;
};

template <typename T, bool is_dictionary>
struct DecoderChecker {
    static void check(const std::vector<T>& values, const Slice& encoded_data, Decoder* decoder) {
        Status st = Status::OK();
        if (!is_dictionary) {
            // normal read
            {
                std::vector<T> checks(values.size());
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok()) << st.to_string();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_FALSE(st.ok()) << st.to_string();
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                std::vector<T> checks(remain_values);
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(remain_values, (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok()) << st.to_string();
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], checks[i]);
                }

                // out-of-bounds skip
                st = decoder->skip(2);
                ASSERT_FALSE(st.ok()) << st.to_string();
            }
        }
        if (true) {
            // normal read
            {
                auto column = starrocks::FixedLengthColumn<T>::create();

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const T* check = (const T*)column->raw_data();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], *check);
                    check++;
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                auto column = starrocks::FixedLengthColumn<T>::create();
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const T* check = (const T*)column->raw_data();
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->skip(2);
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
        }
        if (true) {
            // read
            {
                auto data_column = starrocks::FixedLengthColumn<T>::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const T* check = (const T*)column->data_column()->raw_data();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], *check);
                    check++;
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
            // read+skip
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                auto data_column = starrocks::FixedLengthColumn<T>::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const T* check = (const T*)column->data_column()->raw_data();
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->skip(2);
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
        }
    }
};

template <bool is_dictionary>
struct DecoderChecker<Slice, is_dictionary> {
    static void check(const std::vector<Slice>& values, const Slice& encoded_data, Decoder* decoder) {
        Status st = Status::OK();
        if (!is_dictionary) {
            // read
            {
                std::vector<Slice> checks(values.size());
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok()) << st.to_string();
                for (int i = 0; i < values.size(); ++i) {
                    ASSERT_EQ(values[i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->next_batch(values.size(), (uint8_t*)&checks[0]);
                ASSERT_FALSE(st.ok()) << st.to_string();
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                std::vector<Slice> checks(remain_values);
                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, (uint8_t*)&checks[0]);
                ASSERT_TRUE(st.ok()) << st.to_string();
                for (int i = 0; i < remain_values; ++i) {
                    ASSERT_EQ(values[values_to_skip + i], checks[i]);
                }

                // out-of-bounds access
                st = decoder->skip(2);
                ASSERT_FALSE(st.ok()) << st.to_string();
            }
        }
        if (true) {
            // read
            {
                auto column = starrocks::BinaryColumn::create();

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto* check = (const Slice*)column->raw_data();
                for (auto value : values) {
                    ASSERT_EQ(value, *check);
                    check++;
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
            // skip+read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                auto column = starrocks::BinaryColumn::create();

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto* check = (const Slice*)column->raw_data();
                for (size_t i = 0; i < remain_values; i++) {
                    EXPECT_EQ(values[values_to_skip + i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->skip(2);
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
        }
        if (true) {
            // read
            {
                auto data_column = starrocks::BinaryColumn::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto* check = (const Slice*)column->data_column()->raw_data();
                for (auto value : values) {
                    ASSERT_EQ(value, *check);
                    check++;
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->next_batch(values.size(), ColumnContentType::VALUE, column.get());
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
            // skip + read
            {
                size_t values_to_skip = values.size() / 2;
                size_t remain_values = values.size() - values_to_skip;

                auto data_column = starrocks::BinaryColumn::create();
                auto column = NullableColumn::create(std::move(data_column), NullColumn::create());

                st = decoder->set_data(encoded_data);
                ASSERT_TRUE(st.ok()) << st.to_string();
                st = decoder->skip(values_to_skip);
                st = decoder->next_batch(remain_values, ColumnContentType::VALUE, column.get());
                ASSERT_TRUE(st.ok()) << st.to_string();

                const auto* check = (const Slice*)column->data_column()->raw_data();
                for (size_t i = 0; i < remain_values; i++) {
                    EXPECT_EQ(values[values_to_skip + i], check[i]);
                }

                if (!is_dictionary) {
                    // out-of-bounds access
                    st = decoder->skip(2);
                    ASSERT_FALSE(st.ok()) << st.to_string();
                }
            }
        }
    }
};

TEST_F(ParquetEncodingTest, FindNoneExistedEncoding) {
    const EncodingInfo* enc_info = nullptr;
    auto st = EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::RLE_DICTIONARY, &enc_info);
    EXPECT_FALSE(st.ok());
}

TEST_F(ParquetEncodingTest, Int32) {
    std::vector<int32_t> values;
    for (int i = 0; i < 20; i++) {
        values.push_back(i);
    }

    const EncodingInfo* plain_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 20);
        ASSERT_TRUE(st.ok()) << st.to_string();

        DecoderChecker<int32_t, false>::check(values, encoder->build(), decoder.get());
    }

    const EncodingInfo* dict_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 20);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        dict_decoder->set_data(dict_encoder->build());

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok()) << st.to_string();

        DecoderChecker<int32_t, true>::check(values, encoder->build(), decoder.get());
    }
}

namespace {

template <typename T>
constexpr T kDictSentinel = static_cast<T>(-12345);

// The encoder owns the page memory, so it has to outlive the decoder.
template <tparquet::Type::type PT>
void make_numeric_dict_page(const std::vector<typename PhysicalTypeTraits<PT>::CppType>& values,
                            std::unique_ptr<Encoder>* encoder, std::unique_ptr<Decoder>* decoder) {
    const EncodingInfo* plain_encoding = nullptr;
    const EncodingInfo* dict_encoding = nullptr;
    (void)EncodingInfo::get(PT, tparquet::Encoding::PLAIN, &plain_encoding);
    (void)EncodingInfo::get(PT, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    ASSERT_TRUE(dict_encoding != nullptr);

    Status st = dict_encoding->create_encoder(encoder);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = (*encoder)->append(reinterpret_cast<const uint8_t*>(values.data()), values.size());
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::unique_ptr<Encoder> dict_encoder;
    st = plain_encoding->create_encoder(&dict_encoder);
    ASSERT_TRUE(st.ok()) << st.to_string();
    size_t num_dicts = 0;
    st = (*encoder)->encode_dict(dict_encoder.get(), &num_dicts);
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::unique_ptr<Decoder> dict_decoder;
    st = plain_encoding->create_decoder(&dict_decoder);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = dict_decoder->set_data(dict_encoder->build());
    ASSERT_TRUE(st.ok()) << st.to_string();

    st = dict_encoding->create_decoder(decoder);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = (*decoder)->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = (*decoder)->set_data((*encoder)->build());
    ASSERT_TRUE(st.ok()) << st.to_string();
}

// A decoder appends rows to a column without initializing them, so a row it does not write keeps whatever the
// memory held. We fill the capacity of the column with a sentinel before the decoding, so that such a row shows
// the sentinel and the test does not depend on what the allocator returns.
template <typename T>
auto make_sentinel_data_column(size_t capacity) {
    auto column = FixedLengthColumn<T>::create();
    auto& data = column->get_data();
    data.resize(capacity, kDictSentinel<T>);
    data.clear();
    return column;
}

NullInfos make_dict_null_infos(const std::vector<uint8_t>& nulls) {
    NullInfos null_infos;
    null_infos.reset_with_capacity(nulls.size());
    size_t num_ranges = nulls.empty() ? 0 : 1;
    for (size_t i = 0; i < nulls.size(); ++i) {
        null_infos.nulls_data()[i] = nulls[i];
        null_infos.num_nulls += nulls[i];
        if (i > 0 && nulls[i] != nulls[i - 1]) {
            num_ranges++;
        }
    }
    null_infos.num_ranges = num_ranges;
    return null_infos;
}

// Checks the rows `[prev_rows, prev_rows + nulls.size())` of `dst`. `values` are the values of the rows that are
// not NULL in the page, in page order. A row the filter drops has to be zero, and NULL when `dst` is nullable.
// A row that is NULL in the page has to be NULL. The value of such a row is not defined, so it is not checked.
template <typename T>
void verify_dict_batch(Column* dst, size_t prev_rows, const std::vector<uint8_t>& nulls,
                       const std::vector<uint8_t>* filter, const std::vector<T>& values) {
    const size_t count = nulls.size();
    ASSERT_EQ(prev_rows + count, dst->size());
    NullableColumn* nullable = dst->is_nullable() ? down_cast<NullableColumn*>(dst) : nullptr;
    const Column* data_column = nullable != nullptr ? nullable->data_column_raw_ptr() : dst;
    ASSERT_EQ(prev_rows + count, data_column->size());
    if (nullable != nullptr) {
        ASSERT_EQ(prev_rows + count, nullable->null_column_raw_ptr()->size());
    }
    const T* data = reinterpret_cast<const T*>(data_column->raw_data());

    size_t next_value = 0;
    bool expect_null_row = false;
    for (size_t i = 0; i < count; ++i) {
        const size_t row = prev_rows + i;
        if (nulls[i]) {
            ASSERT_TRUE(nullable != nullptr && nullable->is_null(row)) << "row " << row;
            expect_null_row = true;
            continue;
        }
        const T expected = values[next_value++];
        if (filter != nullptr && !(*filter)[i]) {
            if (nullable != nullptr) {
                ASSERT_TRUE(nullable->is_null(row)) << "row " << row;
                expect_null_row = true;
            }
            ASSERT_EQ(T(0), data[row]) << "row " << row;
        } else {
            if (nullable != nullptr) {
                ASSERT_FALSE(nullable->is_null(row)) << "row " << row;
            }
            ASSERT_EQ(expected, data[row]) << "row " << row;
        }
    }
    ASSERT_EQ(values.size(), next_value);
    if (expect_null_row) {
        ASSERT_TRUE(nullable->has_null());
    }
}

// Decodes `count` rows of a page without NULL rows, the way the reader does for a required column or for a page
// that has no nulls, appends them to `dst` and checks them.
template <typename T>
void decode_and_verify_dict_batch(Decoder* decoder, const std::vector<T>& page_values, size_t* page_pos, size_t count,
                                  const std::vector<uint8_t>* filter, Column* dst) {
    const size_t prev_rows = dst->size();
    Status st = decoder->next_batch(count, ColumnContentType::VALUE, dst, filter != nullptr ? filter->data() : nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const std::vector<T> values(page_values.begin() + *page_pos, page_values.begin() + *page_pos + count);
    *page_pos += count;
    ASSERT_NO_FATAL_FAILURE(verify_dict_batch<T>(dst, prev_rows, std::vector<uint8_t>(count, 0), filter, values));
}

// Decodes one batch of a page with NULL rows, appends it to `dst` and checks it. The page holds the values of the
// rows that are not NULL only.
template <typename T>
void decode_and_verify_dict_batch_with_nulls(Decoder* decoder, const std::vector<T>& page_values, size_t* page_pos,
                                             const std::vector<uint8_t>& nulls, const std::vector<uint8_t>* filter,
                                             Column* dst) {
    const size_t prev_rows = dst->size();
    NullInfos null_infos = make_dict_null_infos(nulls);
    Status st = decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst,
                                               filter != nullptr ? filter->data() : nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();
    const size_t num_values = nulls.size() - null_infos.num_nulls;
    const std::vector<T> values(page_values.begin() + *page_pos, page_values.begin() + *page_pos + num_values);
    *page_pos += num_values;
    ASSERT_NO_FATAL_FAILURE(verify_dict_batch<T>(dst, prev_rows, nulls, filter, values));
}

template <typename T>
std::vector<T> make_dict_page_values(size_t count) {
    std::vector<T> values;
    for (size_t i = 0; i < count; ++i) {
        values.push_back(static_cast<T>((i * 3 % 5 + 1) * 100));
    }
    return values;
}

template <tparquet::Type::type PT>
void check_dict_filter_without_nulls(bool nullable_dst) {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    const std::vector<T> values = make_dict_page_values<T>(24);
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_numeric_dict_page<PT>(values, &encoder, &decoder));

    auto nullable_column = NullableColumn::create(make_sentinel_data_column<T>(values.size()), NullColumn::create());
    auto plain_column = make_sentinel_data_column<T>(values.size());
    Column* dst = nullable_dst ? static_cast<Column*>(nullable_column.get()) : static_cast<Column*>(plain_column.get());
    size_t page_pos = 0;

    // Most rows are dropped, as it is when the filter is selective. The first and the last row are dropped too.
    {
        const std::vector<uint8_t> filter = {0, 1, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0};
        ASSERT_NO_FATAL_FAILURE(decode_and_verify_dict_batch<T>(decoder.get(), values, &page_pos, 12, &filter, dst));
    }
    // No filter: the batch after a filtered one has to start at the right value, and nothing is dropped.
    ASSERT_NO_FATAL_FAILURE(decode_and_verify_dict_batch<T>(decoder.get(), values, &page_pos, 4, nullptr, dst));
    // Every row is dropped, but the decoder still has to move over all the values.
    {
        const std::vector<uint8_t> filter(4, 0);
        ASSERT_NO_FATAL_FAILURE(decode_and_verify_dict_batch<T>(decoder.get(), values, &page_pos, 4, &filter, dst));
    }
    ASSERT_EQ(20U, dst->size());

    // Every row is kept: nothing is dropped, so no row may become NULL.
    auto kept_column = NullableColumn::create(make_sentinel_data_column<T>(4), NullColumn::create());
    {
        const std::vector<uint8_t> filter(4, 1);
        ASSERT_NO_FATAL_FAILURE(
                decode_and_verify_dict_batch<T>(decoder.get(), values, &page_pos, 4, &filter, kept_column.get()));
        ASSERT_FALSE(kept_column->has_null());
    }
    ASSERT_EQ(values.size(), page_pos);
}

struct DictBatchWithNulls {
    std::vector<uint8_t> nulls;
    // Empty: the batch is decoded without a filter.
    std::vector<uint8_t> filter;
};

template <tparquet::Type::type PT>
void check_dict_filter_with_nulls() {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    const std::vector<DictBatchWithNulls> batches = {
            // More than two runs of NULL rows: the decoder reads the values of the whole batch at once. A kept
            // row, a dropped row, a NULL row that is kept and a NULL row that is dropped are all there.
            {{0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 1}, {1, 1, 0, 1, 0, 0, 1, 0, 1, 1, 0, 0}},
            // No filter: the batch after a filtered one has to start at the right value.
            {{0, 1, 0, 1, 0}, {}},
            // Every row is dropped.
            {{1, 0, 0, 1, 0, 0}, {0, 0, 0, 0, 0, 0}},
            // Two runs or less: the decoder reads one run of values at a time. A run of NULL rows first.
            {{1, 1, 0, 0, 0, 0, 0, 0}, {1, 1, 1, 0, 0, 1, 1, 0}},
            // A run of NULL rows last.
            {{0, 0, 0, 0, 1, 1}, {0, 1, 0, 1, 1, 0}},
    };
    size_t num_rows = 0;
    size_t num_values = 0;
    for (const auto& batch : batches) {
        num_rows += batch.nulls.size();
        for (uint8_t is_null : batch.nulls) {
            num_values += !is_null;
        }
    }
    const std::vector<T> values = make_dict_page_values<T>(num_values);
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_numeric_dict_page<PT>(values, &encoder, &decoder));
    // next_batch_with_nulls() hands the filter to the decoder only for a dictionary bigger than the L2 cache, so
    // the threshold is dropped to reach the filtered path with a small dictionary.
    down_cast<CacheAwareDictDecoder*>(decoder.get())->_dict_size_threshold = 0;

    auto dst = NullableColumn::create(make_sentinel_data_column<T>(num_rows), NullColumn::create());
    size_t page_pos = 0;
    for (const auto& batch : batches) {
        ASSERT_NO_FATAL_FAILURE(
                decode_and_verify_dict_batch_with_nulls<T>(decoder.get(), values, &page_pos, batch.nulls,
                                                           batch.filter.empty() ? nullptr : &batch.filter, dst.get()));
    }
    ASSERT_EQ(num_rows, dst->size());
    ASSERT_EQ(values.size(), page_pos);
}

// Every third row is NULL and the batch ends with a run of NULL rows. There are enough values for the vectorized
// loop of expand_load and for its scalar tail, which runs over the trailing NULL rows.
std::vector<uint8_t> make_nulls_with_trailing_nulls() {
    std::vector<uint8_t> nulls(37, 0);
    for (size_t i = 0; i < nulls.size(); ++i) {
        nulls[i] = (i % 3 == 1) || i + 6 >= nulls.size();
    }
    return nulls;
}

size_t count_non_null_rows(const std::vector<uint8_t>& nulls) {
    size_t num = 0;
    for (uint8_t is_null : nulls) {
        num += !is_null;
    }
    return num;
}

// We fear that expand_load reads the source element after the last value when a batch ends with NULL rows. So the
// buffer that holds the values of the batch is made as small as the values: a build with ASAN reports the read,
// and without a sanitizer the check of the buffer size fails if the decoder does not allocate the extra element.
template <tparquet::Type::type PT>
void check_dict_value_batch_ending_with_nulls() {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    const std::vector<uint8_t> nulls = make_nulls_with_trailing_nulls();
    const size_t num_values = count_non_null_rows(nulls);
    const std::vector<T> values = make_dict_page_values<T>(num_values);
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_numeric_dict_page<PT>(values, &encoder, &decoder));

    auto* dict_decoder = down_cast<DictDecoder<T>*>(decoder.get());
    dict_decoder->_temp_read_data.assign(num_values, T(0));
    dict_decoder->_temp_read_data.shrink_to_fit();

    auto dst = NullableColumn::create(make_sentinel_data_column<T>(nulls.size()), NullColumn::create());
    size_t page_pos = 0;
    ASSERT_NO_FATAL_FAILURE(
            decode_and_verify_dict_batch_with_nulls<T>(decoder.get(), values, &page_pos, nulls, nullptr, dst.get()));
    ASSERT_EQ(num_values, page_pos);
    ASSERT_TRUE(dst->has_null());
    ASSERT_EQ(num_values + 1, dict_decoder->_temp_read_data.size());
}

// The dictionary codes of the same batch go through expand_load too.
template <tparquet::Type::type PT>
void check_dict_code_batch_ending_with_nulls() {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    const std::vector<uint8_t> nulls = make_nulls_with_trailing_nulls();
    const size_t num_values = count_non_null_rows(nulls);
    const std::vector<T> values = make_dict_page_values<T>(num_values);
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_numeric_dict_page<PT>(values, &encoder, &decoder));

    // The encoder gives a code to a value when it first sees it.
    std::map<T, int32_t> codes;
    for (const T& value : values) {
        codes.emplace(value, static_cast<int32_t>(codes.size()));
    }

    auto dst = NullableColumn::create(FixedLengthColumn<int32_t>::create(), NullColumn::create());
    NullInfos null_infos = make_dict_null_infos(nulls);
    Status st =
            decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::DICT_CODE, dst.get(), nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(nulls.size(), dst->size());
    ASSERT_TRUE(dst->has_null());
    const int32_t* data = reinterpret_cast<const int32_t*>(dst->data_column_raw_ptr()->raw_data());
    size_t next_value = 0;
    for (size_t i = 0; i < nulls.size(); ++i) {
        if (nulls[i]) {
            ASSERT_TRUE(dst->is_null(i)) << "row " << i;
        } else {
            ASSERT_FALSE(dst->is_null(i)) << "row " << i;
            ASSERT_EQ(codes.at(values[next_value++]), data[i]) << "row " << i;
        }
    }
    ASSERT_EQ(num_values, next_value);
}

} // namespace

// The decoder skips the dictionary lookup of a row the filter drops. We fear that it leaves such a row as it finds
// it: the memory of the column is not initialized, so the row holds garbage and is not NULL. A converter that
// reads every row before the dropped rows are removed, for example one that multiplies each value by a scale
// factor, would then read garbage and overflow on it. So a dropped row has to be zero, and NULL as well when the
// column is nullable.
TEST_F(ParquetEncodingTest, NumericDictFilterDroppedRowsAreDefinedWithoutNulls) {
    for (bool nullable_dst : {true, false}) {
        ASSERT_NO_FATAL_FAILURE(check_dict_filter_without_nulls<tparquet::Type::INT32>(nullable_dst));
        ASSERT_NO_FATAL_FAILURE(check_dict_filter_without_nulls<tparquet::Type::INT64>(nullable_dst));
        ASSERT_NO_FATAL_FAILURE(check_dict_filter_without_nulls<tparquet::Type::FLOAT>(nullable_dst));
        ASSERT_NO_FATAL_FAILURE(check_dict_filter_without_nulls<tparquet::Type::DOUBLE>(nullable_dst));
    }
}

// The same for a page that has NULL rows: a row that is NULL in the page keeps its NULL flag, a row that the
// filter drops becomes NULL, and the values of the other rows are read from the right place of the page.
TEST_F(ParquetEncodingTest, NumericDictFilterDroppedRowsAreDefinedWithNulls) {
    ASSERT_NO_FATAL_FAILURE(check_dict_filter_with_nulls<tparquet::Type::INT32>());
    ASSERT_NO_FATAL_FAILURE(check_dict_filter_with_nulls<tparquet::Type::INT64>());
    ASSERT_NO_FATAL_FAILURE(check_dict_filter_with_nulls<tparquet::Type::FLOAT>());
    ASSERT_NO_FATAL_FAILURE(check_dict_filter_with_nulls<tparquet::Type::DOUBLE>());
}

// A batch of a nullable column that ends with NULL rows, decoded without a filter, so the values are expanded
// to their rows by expand_load. The values and the NULL rows have to be right whatever the width of the value.
TEST_F(ParquetEncodingTest, NumericDictBatchEndingWithNulls) {
    ASSERT_NO_FATAL_FAILURE(check_dict_value_batch_ending_with_nulls<tparquet::Type::INT32>());
    ASSERT_NO_FATAL_FAILURE(check_dict_value_batch_ending_with_nulls<tparquet::Type::INT64>());
    ASSERT_NO_FATAL_FAILURE(check_dict_value_batch_ending_with_nulls<tparquet::Type::FLOAT>());
    ASSERT_NO_FATAL_FAILURE(check_dict_value_batch_ending_with_nulls<tparquet::Type::DOUBLE>());
    ASSERT_NO_FATAL_FAILURE(check_dict_code_batch_ending_with_nulls<tparquet::Type::INT32>());
    ASSERT_NO_FATAL_FAILURE(check_dict_code_batch_ending_with_nulls<tparquet::Type::INT64>());
    ASSERT_NO_FATAL_FAILURE(check_dict_code_batch_ending_with_nulls<tparquet::Type::FLOAT>());
    ASSERT_NO_FATAL_FAILURE(check_dict_code_batch_ending_with_nulls<tparquet::Type::DOUBLE>());
}

TEST_F(ParquetEncodingTest, String) {
    std::vector<std::string> values;
    for (int i = 0; i < 20; i++) {
        values.push_back(std::to_string(i));
    }

    std::vector<Slice> slices;
    for (int i = 0; i < 20; ++i) {
        slices.emplace_back(values[i]);
    }

    const EncodingInfo* plain_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&slices[0]), 20);
        ASSERT_TRUE(st.ok()) << st.to_string();

        DecoderChecker<Slice, false>::check(slices, encoder->build(), decoder.get());
    }
    const EncodingInfo* dict_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)&slices[0], 20);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = dict_decoder->set_data(dict_encoder->build());
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok()) << st.to_string();

        DecoderChecker<Slice, true>::check(slices, encoder->build(), decoder.get());
    }
}

TEST_F(ParquetEncodingTest, FixedString) {
    std::vector<std::string> values;
    for (int i = 100; i < 200; i++) {
        values.push_back(std::to_string(i));
    }

    std::vector<Slice> slices;
    for (int i = 100; i < 200; ++i) {
        slices.emplace_back(values[i - 100]);
    }

    const EncodingInfo* plain_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);

        st = encoder->append(reinterpret_cast<uint8_t*>(&slices[0]), 100);
        ASSERT_TRUE(st.ok()) << st.to_string();

        decoder->set_type_length(3);
        DecoderChecker<Slice, false>::check(slices, encoder->build(), decoder.get());
    }
    const EncodingInfo* dict_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(dict_encoding != nullptr);
    // dict
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = dict_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = dict_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)&slices[0], 100);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary encoder
        std::unique_ptr<Encoder> dict_encoder;
        st = plain_encoding->create_encoder(&dict_encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        size_t num_dicts = 0;
        st = encoder->encode_dict(dict_encoder.get(), &num_dicts);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // construct dictionary decoder
        std::unique_ptr<Decoder> dict_decoder;
        st = plain_encoding->create_decoder(&dict_decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = dict_decoder->set_data(dict_encoder->build());
        ASSERT_TRUE(st.ok()) << st.to_string();
        dict_decoder->set_type_length(3);

        st = decoder->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get());
        ASSERT_TRUE(st.ok()) << st.to_string();

        DecoderChecker<Slice, true>::check(slices, encoder->build(), decoder.get());
    }
}

TEST_F(ParquetEncodingTest, Boolean) {
    std::vector<uint8_t> values;
    for (int i = 0; i < 32; i++) {
        values.push_back(i % 3 == 0);
    }

    const EncodingInfo* plain_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    // plain
    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = plain_encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = plain_encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        // decode without buffer
        st = encoder->append(reinterpret_cast<uint8_t*>(&values[0]), 32);
        ASSERT_TRUE(st.ok()) << st.to_string();
        DecoderChecker<uint8_t, false>::check(values, encoder->build(), decoder.get());

        // decode with buffer
        values.resize(31);
        DecoderChecker<uint8_t, false>::check(values, encoder->build(), decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaBinaryPacked) {
    auto fn = []<tparquet::Type::type PT>(int rep, int n, int seed) {
        std::cout << "running DeltaBinaryPacked test for type: " << PT << ", rep: " << rep << ", n: " << n
                  << ", seed: " << seed << std::endl;
        using T = typename PhysicalTypeTraits<PT>::CppType;
        std::vector<T> values;
        auto gen = std::mt19937(seed);
        std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());

        if (seed == -1) {
            values.resize(rep * n);
            std::fill(values.begin(), values.end(), dist(gen));
        } else {
            for (int rep = 0; rep < 10; rep++) {
                values.push_back(std::numeric_limits<T>::max());
                values.push_back(std::numeric_limits<T>::min());
                for (int i = 0; i < n; i++) {
                    values.push_back(dist(gen));
                }
                values.push_back(std::numeric_limits<T>::max());
                values.push_back(std::numeric_limits<T>::min());
            }
        }

        // unaligned access.
        T* buffer = values.data();
        size_t size = values.size();
        {
            buffer += 3;
            size -= 3;
        }

        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(PT, tparquet::Encoding::DELTA_BINARY_PACKED, &encoding);
        ASSERT_TRUE(encoding != nullptr);

        {
            Status st = Status::OK();
            std::unique_ptr<Decoder> decoder;
            st = encoding->create_decoder(&decoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            std::unique_ptr<Encoder> encoder;
            st = encoding->create_encoder(&encoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            st = encoder->append((const uint8_t*)buffer, size);
            ASSERT_TRUE(st.ok()) << st.to_string();

            // simple verification.
            Slice encoded_data = encoder->build();
            std::vector<T> check(size);
            st = decoder->set_data(encoded_data);
            ASSERT_TRUE(st.ok()) << st.to_string();
            st = decoder->next_batch(size, (uint8_t*)(check.data()));
            ASSERT_TRUE(st.ok()) << st.to_string();

            for (int i = 0; i < check.size(); i++) {
                ASSERT_EQ(check[i], buffer[i]);
            }

            // enhanced verification.
            DecoderChecker<T, false>::check(check, encoded_data, decoder.get());
        }
    };

    fn.operator()<tparquet::Type::INT32>(10, 8, -1);
    fn.operator()<tparquet::Type::INT32>(10, 8, 0);
    fn.operator()<tparquet::Type::INT32>(10, 31, 0);
    fn.operator()<tparquet::Type::INT32>(10, 127, 0);
    fn.operator()<tparquet::Type::INT32>(10, 255, 0);

    fn.operator()<tparquet::Type::INT64>(10, 8, -1);
    fn.operator()<tparquet::Type::INT64>(10, 8, 0);
    fn.operator()<tparquet::Type::INT64>(10, 31, 0);
    fn.operator()<tparquet::Type::INT64>(10, 127, 0);
    fn.operator()<tparquet::Type::INT64>(10, 255, 0);
}

TEST_F(ParquetEncodingTest, DeltaLengthByteArrayNonFixedSizeString) {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; i++) {
        strings.push_back(std::to_string(i));
    }

    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaLengthByteArrayFixedSizeString) {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; i++) {
        strings.push_back(fmt::format("{:0>4}", i));
    }

    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = encoding->create_decoder(&decoder);
        decoder->set_type_length(4);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaByteArrayNonFixedSizeStringDebug) {
    // examples from https://parquet.apache.org/docs/file-format/data-pages/encodings/#delta-strings-delta_byte_array--7
    std::vector<std::string> strings = {"axis", "axle", "babble", "babyhood"};
    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    {
        std::unique_ptr<Decoder> decoder;
        auto st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            std::cout << "check[" << i << "]=" << check[i].to_string() << std::endl;
        }

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }

        // initialize with wrong data.
        for (int i = 0; i < check.size(); i++) {
            check[i].data = (char*)(&decoder);
            check[i].size = 32;
        }
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            std::cout << "check[" << i << "]=" << check[i].to_string() << std::endl;
        }

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaByteArrayNonFixedSizeString) {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; i++) {
        strings.push_back(std::to_string(i));
    }

    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        std::unique_ptr<Decoder> decoder;
        auto st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, DeltaByteArrayFixedSizeString) {
    std::vector<std::string> strings;
    for (int i = 0; i < 1000; i++) {
        strings.push_back(fmt::format("{:0>4}", i));
    }

    std::vector<Slice> values;
    for (const auto& s : strings) {
        values.emplace_back(s);
    }

    const EncodingInfo* encoding = nullptr;
    EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::DELTA_BYTE_ARRAY, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        std::unique_ptr<Decoder> decoder;
        auto st = encoding->create_decoder(&decoder);
        decoder->set_type_length(4);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        std::vector<Slice> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        // enhanced verification.
        DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
    }
}

TEST_F(ParquetEncodingTest, ByteStreamSplitInt32Debug) {
    using T = int32_t;
    // examples from https://parquet.apache.org/docs/file-format/data-pages/encodings/#byte-stream-split-byte_stream_split--9
    std::vector<T> values = {
            (int)0xDDCCBBAA,
            (int)0x33221100,
            (int)0xD6C5B4A3,
    };
    uint8_t expected[12] = {0xAA, 0x00, 0xA3, 0xBB, 0x11, 0xB4, 0xCC, 0x22, 0xC5, 0xDD, 0x33, 0xD6};

    {
        uint8_t encoded[12];
        ByteStreamSplitUtil::ByteStreamSplitEncode((uint8_t*)(&values[0]), 4, 3, encoded);
        for (int i = 0; i < 12; i++) {
            ASSERT_EQ(encoded[i], expected[i]);
        }
        uint8_t decoded[12];
        ByteStreamSplitUtil::ByteStreamSplitDecode(encoded, 4, 3, 3, decoded);
        for (int i = 0; i < 12; i++) {
            ASSERT_EQ(decoded[i], *((uint8_t*)(values.data()) + i));
        }
    }

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::BYTE_STREAM_SPLIT, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    {
        Status st = Status::OK();
        std::unique_ptr<Decoder> decoder;
        st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        std::unique_ptr<Encoder> encoder;
        st = encoding->create_encoder(&encoder);
        ASSERT_TRUE(st.ok()) << st.to_string();

        st = encoder->append((uint8_t*)(&values[0]), values.size());
        ASSERT_TRUE(st.ok()) << st.to_string();

        // simple verification.
        Slice encoded_data = encoder->build();
        ASSERT_EQ(encoded_data.size, 12);

        for (int i = 0; i < 12; i++) {
            ASSERT_EQ((uint8_t)encoded_data.data[i], expected[i]);
        }

        std::vector<T> check(values.size());
        st = decoder->set_data(encoded_data);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
        ASSERT_TRUE(st.ok()) << st.to_string();

        for (int i = 0; i < check.size(); i++) {
            ASSERT_EQ(check[i], values[i]);
        }
    }
}

TEST_F(ParquetEncodingTest, ByteStreamSplitNonFLBA) {
    auto fn = []<tparquet::Type::type PT>(int rep, int n, int seed) {
        std::cout << "running ByteStreamSplitInteger test for type: " << PT << ", rep: " << rep << ", n: " << n
                  << ", seed: " << seed << std::endl;
        using T = typename PhysicalTypeTraits<PT>::CppType;
        std::vector<T> values;
        std::mt19937 gen(seed);
        for (int rep = 0; rep < 10; rep++) {
            values.push_back(std::numeric_limits<T>::max());
            values.push_back(std::numeric_limits<T>::min());
            values.push_back(std::numeric_limits<T>::lowest());
            if constexpr (PT == tparquet::Type::INT32 || PT == tparquet::Type::INT64) {
                for (int i = 0; i < n; i++) {
                    std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
                    values.push_back(dist(gen));
                }
            } else {
                std::uniform_real_distribution<T> dist(std::numeric_limits<T>::lowest(), std::numeric_limits<T>::max());
                for (int i = 0; i < n; i++) {
                    values.push_back(dist(gen));
                }
            }
            values.push_back(std::numeric_limits<T>::lowest());
            values.push_back(std::numeric_limits<T>::max());
            values.push_back(std::numeric_limits<T>::min());
        }

        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(PT, tparquet::Encoding::BYTE_STREAM_SPLIT, &encoding);
        ASSERT_TRUE(encoding != nullptr);

        {
            Status st = Status::OK();
            std::unique_ptr<Decoder> decoder;
            st = encoding->create_decoder(&decoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            std::unique_ptr<Encoder> encoder;
            st = encoding->create_encoder(&encoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            st = encoder->append((uint8_t*)(&values[0]), values.size());
            ASSERT_TRUE(st.ok()) << st.to_string();

            // simple verification.
            Slice encoded_data = encoder->build();
            std::vector<T> check(values.size());
            st = decoder->set_data(encoded_data);
            ASSERT_TRUE(st.ok()) << st.to_string();
            st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
            ASSERT_TRUE(st.ok()) << st.to_string();

            for (int i = 0; i < check.size(); i++) {
                ASSERT_EQ(check[i], values[i]);
            }

            // enhanced verification.
            DecoderChecker<T, false>::check(values, encoded_data, decoder.get());
        }
    };

    fn.operator()<tparquet::Type::INT32>(10, 8, 0);
    fn.operator()<tparquet::Type::INT32>(10, 31, 0);
    fn.operator()<tparquet::Type::INT32>(10, 127, 0);
    fn.operator()<tparquet::Type::INT32>(10, 255, 0);

    fn.operator()<tparquet::Type::INT64>(10, 8, 0);
    fn.operator()<tparquet::Type::INT64>(10, 31, 0);
    fn.operator()<tparquet::Type::INT64>(10, 127, 0);
    fn.operator()<tparquet::Type::INT64>(10, 255, 0);

    fn.operator()<tparquet::Type::FLOAT>(10, 8, 0);
    fn.operator()<tparquet::Type::FLOAT>(10, 31, 0);
    fn.operator()<tparquet::Type::FLOAT>(10, 127, 0);
    fn.operator()<tparquet::Type::FLOAT>(10, 255, 0);

    fn.operator()<tparquet::Type::DOUBLE>(10, 8, 0);
    fn.operator()<tparquet::Type::DOUBLE>(10, 31, 0);
    fn.operator()<tparquet::Type::DOUBLE>(10, 127, 0);
    fn.operator()<tparquet::Type::DOUBLE>(10, 255, 0);
}

TEST_F(ParquetEncodingTest, ByteStreamSplitFLBA) {
    auto f = [](int byte_width, int num_values) {
        std::cout << "running ByteStreamSplitFLBA test for byte_width: " << byte_width << ", N: " << num_values
                  << std::endl;
        const int SIZE = byte_width * num_values;
        std::vector<uint8_t> bytes_data(SIZE);

        for (int i = 0; i < SIZE; i++) {
            bytes_data[i] = (uint8_t)(i % 256);
        }

        std::vector<uint8_t> expected(SIZE);
        int idx = 0;
        for (int i = 0; i < byte_width; i++) {
            for (int j = 0; j < num_values; j++) {
                expected[idx++] = bytes_data[j * byte_width + i];
            }
        }

        std::vector<uint8_t> encoded(SIZE);
        ByteStreamSplitUtil::ByteStreamSplitEncode(bytes_data.data(), byte_width, num_values, encoded.data());
        for (int i = 0; i < SIZE; i++) {
            ASSERT_EQ(encoded[i], expected[i]);
        }
        std::vector<uint8_t> decoded(SIZE);
        ByteStreamSplitUtil::ByteStreamSplitDecode(encoded.data(), byte_width, num_values, num_values, decoded.data());
        for (int i = 0; i < SIZE; i++) {
            ASSERT_EQ(decoded[i], bytes_data[i]);
        }

        std::vector<Slice> values;
        for (int i = 0; i < num_values; i++) {
            values.emplace_back(bytes_data.data() + i * byte_width, byte_width);
        }

        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::BYTE_STREAM_SPLIT, &encoding);
        ASSERT_TRUE(encoding != nullptr);

        {
            Status st = Status::OK();
            std::unique_ptr<Encoder> encoder;
            st = encoding->create_encoder(&encoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            encoder->set_type_length(byte_width);
            st = encoder->append((uint8_t*)(&values[0]), values.size());
            ASSERT_TRUE(st.ok()) << st.to_string();

            // simple verification.
            Slice encoded_data = encoder->build();
            ASSERT_EQ(encoded_data.size, expected.size());
            ASSERT_EQ(encoded_data.size, encoded.size());
            for (int i = 0; i < SIZE; i++) {
                ASSERT_EQ((uint8_t)encoded_data.data[i], expected[i]);
                ASSERT_EQ((uint8_t)encoded_data.data[i], encoded[i]);
            }

            std::unique_ptr<Decoder> decoder;
            st = encoding->create_decoder(&decoder);
            ASSERT_TRUE(st.ok()) << st.to_string();
            decoder->set_type_length(byte_width);

            // real all.
            st = decoder->set_data(encoded_data);
            ASSERT_TRUE(st.ok()) << st.to_string();
            std::vector<Slice> check(values.size());
            st = decoder->next_batch(num_values, (uint8_t*)(&check[0]));
            ASSERT_TRUE(st.ok()) << st.to_string();
            for (int i = 0; i < check.size(); i++) {
                ASSERT_EQ(check[i], values[i]);
            }
        }

        {
            Status st = Status::OK();
            std::unique_ptr<Decoder> decoder;
            st = encoding->create_decoder(&decoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            decoder->set_type_length(byte_width);

            std::unique_ptr<Encoder> encoder;
            st = encoding->create_encoder(&encoder);
            ASSERT_TRUE(st.ok()) << st.to_string();

            encoder->set_type_length(byte_width);

            st = encoder->append((uint8_t*)(&values[0]), values.size());
            ASSERT_TRUE(st.ok()) << st.to_string();

            // simple verification.
            Slice encoded_data = encoder->build();
            std::vector<Slice> check(values.size());
            st = decoder->set_data(encoded_data);
            ASSERT_TRUE(st.ok()) << st.to_string();
            st = decoder->next_batch(values.size(), (uint8_t*)(&check[0]));
            ASSERT_TRUE(st.ok()) << st.to_string();

            for (int i = 0; i < check.size(); i++) {
                ASSERT_EQ(check[i], values[i]);
            }

            // enhanced verification.
            DecoderChecker<Slice, false>::check(values, encoded_data, decoder.get());
        }
    };

    f(5, 3);
    f(5, 10);
    f(5, 31);
    f(5, 127);
    f(5, 255);

    f(31, 3);
    f(31, 10);
    f(31, 31);
    f(31, 127);
    f(31, 255);
}

// A DELTA_BINARY_PACKED page header is entirely file-controlled, so a corrupt one must come back
// as a Status. Throwing here escapes the scan worker thread, which has no handler in any build
// type, and aborts the BE process.
TEST_F(ParquetEncodingTest, DeltaBinaryPackedCorruptHeader) {
    // The header fields are ULEB128: values_per_block, mini_blocks_per_block, total_value_count
    // and zigzag(first_value). Here values_per_block is 128 in both cases.
    // 128 / 1000 == 0, i.e. more miniblocks than values.
    const std::vector<uint8_t> zero_values_per_mini_block = {0x80, 0x01, 0xe8, 0x07, 0x00, 0x00};
    // 128 / 3 == 42, which is not a multiple of 32.
    const std::vector<uint8_t> unaligned_mini_block = {0x80, 0x01, 0x03, 0x00, 0x00};

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    for (const auto& header : {zero_values_per_mini_block, unaligned_mini_block}) {
        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
        Status st = decoder->set_data(Slice(reinterpret_cast<const char*>(header.data()), header.size()));
        ASSERT_TRUE(st.is_corruption()) << st;
    }
}

// The batch size handed to a decoder is not the chunk size: StoredColumnReaderImpl::_read() passes
// everything left in the current data page in one call, so a page holding ~1M values produces a
// ~1M-value batch. Scratch space sized by that batch must live on the heap; the VLAs this path used
// to declare needed ~12MB of stack and killed the BE with a SIGSEGV raised inside the decoder.
TEST_F(ParquetEncodingTest, PlainByteArrayLargeBatchWithNulls) {
    constexpr size_t kCount = 1 << 20;
    constexpr size_t kNullStride = 8;
    const std::string value("abcdefgh");

    NullInfos null_infos;
    null_infos.reset_with_capacity(kCount);
    uint8_t* nulls = null_infos.nulls_data();
    null_infos.num_nulls = 0;
    for (size_t i = 0; i < kCount; ++i) {
        nulls[i] = (i % kNullStride) == 0;
        null_infos.num_nulls += nulls[i];
    }
    null_infos.num_ranges = kCount / 2;
    const size_t read_count = kCount - null_infos.num_nulls;

    const EncodingInfo* plain_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &plain_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);

    std::vector<Slice> slices(read_count, Slice(value));
    std::unique_ptr<Encoder> encoder;
    Status st = plain_encoding->create_encoder(&encoder);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = encoder->append(reinterpret_cast<uint8_t*>(slices.data()), slices.size());
    ASSERT_TRUE(st.ok()) << st.to_string();

    std::unique_ptr<Decoder> decoder;
    st = plain_encoding->create_decoder(&decoder);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = decoder->set_data(encoder->build());
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    st = decoder->next_batch_with_nulls(kCount, null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(kCount, dst->size());
    ASSERT_TRUE(dst->is_null(0));
    ASSERT_FALSE(dst->is_null(1));
    ASSERT_FALSE(dst->is_null(kCount - 1));
    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());
    ASSERT_EQ(read_count * value.size(), binary_column->get_bytes().size());
    ASSERT_EQ(value, binary_column->get_slice(1).to_string());
}

namespace {

NullInfos make_plain_null_infos(const std::vector<uint8_t>& nulls) {
    NullInfos null_infos;
    null_infos.reset_with_capacity(nulls.size());
    size_t num_ranges = nulls.empty() ? 0 : 1;
    for (size_t i = 0; i < nulls.size(); ++i) {
        null_infos.nulls_data()[i] = nulls[i];
        null_infos.num_nulls += nulls[i];
        if (i > 0 && nulls[i] != nulls[i - 1]) {
            num_ranges++;
        }
    }
    null_infos.num_ranges = num_ranges;
    return null_infos;
}

// The encoder owns the page memory, so it has to outlive the decoder.
void make_plain_byte_array_page(const std::vector<std::string>& values, std::unique_ptr<Encoder>* encoder,
                                std::unique_ptr<Decoder>* decoder) {
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);

    std::vector<Slice> slices;
    for (const auto& value : values) {
        slices.emplace_back(value);
    }
    Status st = encoding->create_encoder(encoder);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = (*encoder)->append(reinterpret_cast<uint8_t*>(slices.data()), slices.size());
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = encoding->create_decoder(decoder);
    ASSERT_TRUE(st.ok()) << st.to_string();
    st = (*decoder)->set_data((*encoder)->build());
    ASSERT_TRUE(st.ok()) << st.to_string();
}

// Decodes one batch and appends it to `dst`. `values` are the non-null values of the batch in page order.
// A NULL row and a row the filter drops must both end up as an empty NULL value, and a NULL row must stay
// NULL whatever the filter says. `dst` may already hold rows of earlier batches.
void decode_and_check_plain_batch(Decoder* decoder, const std::vector<uint8_t>& nulls,
                                  const std::vector<std::string>& values, const std::vector<uint8_t>* filter,
                                  NullableColumn* dst) {
    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());
    const size_t prev_rows = dst->size();
    const size_t prev_bytes = binary_column->get_bytes().size();

    NullInfos null_infos = make_plain_null_infos(nulls);
    Status st = decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst,
                                               filter != nullptr ? filter->data() : nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(prev_rows + nulls.size(), dst->size());
    ASSERT_EQ(prev_rows + nulls.size() + 1, binary_column->get_offset().size());
    size_t next_value = 0;
    size_t expected_bytes = 0;
    bool expect_null_row = false;
    for (size_t i = 0; i < nulls.size(); ++i) {
        std::string expected;
        bool expected_null = nulls[i] != 0;
        if (!nulls[i]) {
            const std::string& value = values[next_value++];
            if (filter == nullptr || (*filter)[i]) {
                expected = value;
            } else {
                expected_null = true;
            }
        }
        ASSERT_EQ(expected_null, dst->is_null(prev_rows + i)) << "row " << i;
        ASSERT_EQ(expected, binary_column->get_slice(prev_rows + i).to_string()) << "row " << i;
        expected_bytes += expected.size();
        expect_null_row |= expected_null;
    }
    if (expect_null_row) {
        ASSERT_TRUE(dst->has_null());
    }
    ASSERT_EQ(values.size(), next_value);
    ASSERT_EQ(prev_bytes + expected_bytes, binary_column->get_bytes().size());
    ASSERT_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
}

// Decodes `values.size()` rows from a page without NULL rows, the way the reader does for a required column or
// for a page that has no nulls, and appends them to `dst`. A row the filter drops gets no bytes. It is NULL
// when `dst` is nullable and an empty value when it is not.
void decode_and_check_plain_batch_without_nulls(Decoder* decoder, const std::vector<std::string>& values,
                                                const std::vector<uint8_t>* filter, Column* dst) {
    auto* binary_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(dst));
    const size_t prev_rows = dst->size();
    const size_t prev_bytes = binary_column->get_bytes().size();

    Status st = decoder->next_batch(values.size(), ColumnContentType::VALUE, dst,
                                    filter != nullptr ? filter->data() : nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(prev_rows + values.size(), dst->size());
    ASSERT_EQ(prev_rows + values.size() + 1, binary_column->get_offset().size());
    size_t expected_bytes = 0;
    bool expect_null_row = false;
    for (size_t i = 0; i < values.size(); ++i) {
        const bool dropped = filter != nullptr && !(*filter)[i];
        const std::string expected = dropped ? std::string() : values[i];
        if (dst->is_nullable()) {
            ASSERT_EQ(dropped, dst->is_null(prev_rows + i)) << "row " << i;
        }
        ASSERT_EQ(expected, binary_column->get_slice(prev_rows + i).to_string()) << "row " << i;
        expected_bytes += expected.size();
        expect_null_row |= dropped;
    }
    if (dst->is_nullable() && expect_null_row) {
        ASSERT_TRUE(dst->has_null());
    }
    ASSERT_EQ(prev_bytes + expected_bytes, binary_column->get_bytes().size());
    ASSERT_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
}

} // namespace

// The filter has one byte per row of the batch, NULL rows included. We fear that the decoder copies the bytes
// of the rows the filter drops: they are never read, but a big string column pays for the copy. So we check
// that the column holds the selected values only, and that the batches after a filtered one still start at the
// right value.
TEST_F(ParquetEncodingTest, PlainByteArrayWithNullsAndFilter) {
    const std::vector<std::string> values = {
            "alpha", "b",     "",     "gamma-gamma",         "delta", std::string(200, 'x'),
            "zeta",  "theta", "iota", std::string(130, 'k'), "kappa", "mu"};
    auto batch_values = [&](size_t from, size_t to) {
        return std::vector<std::string>(values.begin() + from, values.begin() + to);
    };
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_plain_byte_array_page(values, &encoder, &decoder));
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());

    // Rows 2, 5 and 7 hold a value the filter drops, so they must become NULL. Row 4 is a NULL row the filter
    // drops. Row 3 is a kept empty value, so it must stay not NULL. The last selected value is followed by more
    // values in the page.
    {
        const std::vector<uint8_t> nulls = {0, 1, 0, 0, 1, 0, 0, 0, 1, 0};
        const std::vector<uint8_t> filter = {1, 1, 0, 1, 0, 0, 1, 0, 1, 1};
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch(decoder.get(), nulls, batch_values(0, 7), &filter, dst.get()));
        // "alpha" + "" + "delta" + "zeta"
        ASSERT_EQ(14U, binary_column->get_bytes().size());
    }
    // No filter: the batch after a filtered one has to start at "theta".
    {
        const std::vector<uint8_t> nulls = {0, 1, 0};
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch(decoder.get(), nulls, batch_values(7, 9), nullptr, dst.get()));
        ASSERT_EQ(23U, binary_column->get_bytes().size());
    }
    // The last selected value is the last value of the page.
    {
        const std::vector<uint8_t> nulls = {0, 0, 1, 0};
        const std::vector<uint8_t> filter = {0, 1, 0, 1};
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch(decoder.get(), nulls, batch_values(9, 12), &filter, dst.get()));
        // "kappa" + "mu"
        ASSERT_EQ(30U, binary_column->get_bytes().size());
    }
    ASSERT_EQ(17U, dst->size());
}

// Every row is dropped: the column must get no bytes at all and every row must be NULL, but the decoder still
// has to move over all the values, or the next batch reads garbage.
TEST_F(ParquetEncodingTest, PlainByteArrayWithNullsAndFilterDropsAll) {
    const std::vector<std::string> values = {"alpha", "b",     "",    "gamma-gamma", "delta", std::string(200, 'x'),
                                             "zeta",  "theta", "iota"};
    auto batch_values = [&](size_t from, size_t to) {
        return std::vector<std::string>(values.begin() + from, values.begin() + to);
    };
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_plain_byte_array_page(values, &encoder, &decoder));
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());

    {
        const std::vector<uint8_t> nulls = {0, 1, 0, 0, 1, 0, 0, 0, 1, 0};
        const std::vector<uint8_t> filter(nulls.size(), 0);
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch(decoder.get(), nulls, batch_values(0, 7), &filter, dst.get()));
        ASSERT_EQ(0U, binary_column->get_bytes().size());
    }
    // A batch of NULL rows has no value in the page, so the decoder must not move at all.
    {
        const std::vector<uint8_t> nulls = {1, 1, 1};
        const std::vector<uint8_t> filter = {1, 1, 1};
        ASSERT_NO_FATAL_FAILURE(decode_and_check_plain_batch(decoder.get(), nulls, {}, &filter, dst.get()));
        ASSERT_EQ(0U, binary_column->get_bytes().size());
    }
    {
        const std::vector<uint8_t> nulls = {0, 1, 0};
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch(decoder.get(), nulls, batch_values(7, 9), nullptr, dst.get()));
        ASSERT_EQ(9U, binary_column->get_bytes().size());
    }
}

// Every row is selected, so the result has to be the same as without a filter. This also reaches the copy of
// a value longer than 128 bytes.
TEST_F(ParquetEncodingTest, PlainByteArrayWithNullsAndFilterKeepsAll) {
    const std::vector<std::string> values = {"alpha", "b",     "",    "gamma-gamma", "delta", std::string(200, 'x'),
                                             "zeta",  "theta", "iota"};
    auto batch_values = [&](size_t from, size_t to) {
        return std::vector<std::string>(values.begin() + from, values.begin() + to);
    };
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_plain_byte_array_page(values, &encoder, &decoder));
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());

    {
        const std::vector<uint8_t> nulls = {0, 1, 0, 0, 1, 0, 0, 0, 1, 0};
        const std::vector<uint8_t> filter(nulls.size(), 1);
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch(decoder.get(), nulls, batch_values(0, 7), &filter, dst.get()));
        ASSERT_EQ(226U, binary_column->get_bytes().size());
    }
    {
        const std::vector<uint8_t> nulls = {0, 0};
        const std::vector<uint8_t> filter(nulls.size(), 1);
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch(decoder.get(), nulls, batch_values(7, 9), &filter, dst.get()));
        ASSERT_EQ(235U, binary_column->get_bytes().size());
    }
}

// The global dictionary reader (LowRowsColumnReader::fill_dst_column) looks up every non-NULL row of the column
// it has just read, before the filter of that read is applied, and fails with GlobalDictNotMatch when a value is
// not in the dictionary. So a row that the filter drops must be NULL: an empty value that is not NULL is not in
// the dictionary. The batch has no NULL row, so only the filter can make the column know about NULL rows.
TEST_F(ParquetEncodingTest, PlainByteArrayWithNullsAndFilterDroppedRowsAreNull) {
    const std::vector<std::string> values = {"red", "green", "blue", "green"};
    const std::set<std::string> dict = {"red", "green", "blue"};
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_plain_byte_array_page(values, &encoder, &decoder));
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());

    const std::vector<uint8_t> nulls = {0, 0, 0, 0};
    const std::vector<uint8_t> filter = {1, 0, 1, 0};
    ASSERT_NO_FATAL_FAILURE(decode_and_check_plain_batch(decoder.get(), nulls, values, &filter, dst.get()));

    ASSERT_TRUE(dst->has_null());
    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());
    size_t num_not_null = 0;
    for (size_t i = 0; i < dst->size(); ++i) {
        if (!dst->is_null(i)) {
            ASSERT_EQ(1U, dict.count(binary_column->get_slice(i).to_string())) << "row " << i;
            num_not_null++;
        }
    }
    ASSERT_EQ(2U, num_not_null);
}

// A required column and a page without nulls are read with next_batch(). The global dictionary reader looks up
// every non-NULL row of the nullable column it has just read, before the filter of that read is applied, so a
// row that the filter drops must be NULL and must not leak its bytes. The dictionary has neither an empty string
// nor the long value that the filter drops.
TEST_F(ParquetEncodingTest, PlainByteArrayWithoutNullsAndFilterDroppedRowsAreNull) {
    const std::vector<std::string> values = {"red", std::string(200, 'x'), "blue", "green", "red", "blue", "green",
                                             "red"};
    const std::set<std::string> dict = {"red", "green", "blue"};
    auto batch_values = [&](size_t from, size_t to) {
        return std::vector<std::string>(values.begin() + from, values.begin() + to);
    };
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_plain_byte_array_page(values, &encoder, &decoder));
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());

    {
        const std::vector<uint8_t> filter = {1, 0, 1, 0};
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch_without_nulls(decoder.get(), batch_values(0, 4), &filter, dst.get()));
        ASSERT_TRUE(dst->has_null());
        // "red" + "blue"
        ASSERT_EQ(7U, binary_column->get_bytes().size());
    }
    // No filter: the batch after a filtered one has to start at the right value, and nothing is NULL in it.
    {
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch_without_nulls(decoder.get(), batch_values(4, 6), nullptr, dst.get()));
        ASSERT_EQ(14U, binary_column->get_bytes().size());
        ASSERT_FALSE(dst->is_null(4));
        ASSERT_FALSE(dst->is_null(5));
    }
    // Every row is dropped.
    {
        const std::vector<uint8_t> filter = {0, 0};
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch_without_nulls(decoder.get(), batch_values(6, 8), &filter, dst.get()));
        ASSERT_EQ(14U, binary_column->get_bytes().size());
        ASSERT_TRUE(dst->is_null(6));
        ASSERT_TRUE(dst->is_null(7));
    }

    ASSERT_EQ(8U, dst->size());
    size_t num_not_null = 0;
    for (size_t i = 0; i < dst->size(); ++i) {
        if (!dst->is_null(i)) {
            ASSERT_EQ(1U, dict.count(binary_column->get_slice(i).to_string())) << "row " << i;
            num_not_null++;
        }
    }
    ASSERT_EQ(4U, num_not_null);
}

// A column that is not nullable cannot hold a NULL row and is not looked up before the dropped rows are removed,
// so a dropped row keeps being an empty value there.
TEST_F(ParquetEncodingTest, PlainByteArrayWithoutNullsAndFilterNotNullableDestination) {
    const std::vector<std::string> values = {"red", std::string(200, 'x'), "blue", "green", "red"};
    auto batch_values = [&](size_t from, size_t to) {
        return std::vector<std::string>(values.begin() + from, values.begin() + to);
    };
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_plain_byte_array_page(values, &encoder, &decoder));
    auto dst = BinaryColumn::create();

    {
        const std::vector<uint8_t> filter = {1, 0, 1, 0};
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch_without_nulls(decoder.get(), batch_values(0, 4), &filter, dst.get()));
        ASSERT_EQ(7U, dst->get_bytes().size());
    }
    {
        ASSERT_NO_FATAL_FAILURE(
                decode_and_check_plain_batch_without_nulls(decoder.get(), batch_values(4, 5), nullptr, dst.get()));
        ASSERT_EQ(10U, dst->get_bytes().size());
    }
    ASSERT_EQ(5U, dst->size());
    ASSERT_FALSE(dst->has_null());
}

// The page is file data. A length prefix that does not fit in the page must come back as a Status, because the
// filtered path parses the prefix of every value itself.
TEST_F(ParquetEncodingTest, PlainByteArrayWithNullsAndFilterTruncatedPage) {
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_plain_byte_array_page({"abc"}, &encoder, &decoder));
    const Slice page = encoder->build();
    ASSERT_EQ(7U, page.size);

    const std::vector<uint8_t> nulls = {0};
    const std::vector<uint8_t> filter = {1};
    NullInfos null_infos = make_plain_null_infos(nulls);
    // The first page misses 2 bytes of the value, the second one misses a part of the length prefix.
    for (size_t truncated_size : {page.size - 2, static_cast<size_t>(2)}) {
        ASSERT_TRUE(decoder->set_data(Slice(page.data, truncated_size)).ok());
        auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        Status st = decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(),
                                                   filter.data());
        ASSERT_FALSE(st.ok()) << "truncated_size=" << truncated_size;
    }
}

} // namespace starrocks::parquet
