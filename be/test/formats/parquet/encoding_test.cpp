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

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/config.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/types.h"
#include "formats/parquet/utils.h"
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

        ASSERT_TRUE(decoder->set_type_length(3).ok());
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
        ASSERT_TRUE(dict_decoder->set_type_length(3).ok());

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
        ASSERT_TRUE(decoder->set_type_length(4).ok());
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
        ASSERT_TRUE(decoder->set_type_length(4).ok());
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
            ASSERT_TRUE(decoder->set_type_length(byte_width).ok());

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

            ASSERT_TRUE(decoder->set_type_length(byte_width).ok());

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

namespace {

struct RawPlainPage {
    std::string name;
    std::string bytes;
    bool valid;
};

void append_le32(std::string* page, uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        page->push_back(static_cast<char>((value >> (8 * i)) & 0xff));
    }
}

// PLAIN BYTE_ARRAY pages written by hand: two valid ones, and pages that are cut inside a length prefix or a
// value, or that hold a length which is far bigger than the page.
std::vector<RawPlainPage> make_raw_plain_pages() {
    std::string two_values;
    append_le32(&two_values, 3);
    two_values += "abc";
    append_le32(&two_values, 3);
    two_values += "def";

    std::string two_empty_values;
    append_le32(&two_empty_values, 0);
    append_le32(&two_empty_values, 0);

    std::string huge_first_length;
    append_le32(&huge_first_length, 0xFFFFFFF0U);
    huge_first_length += "abcd";

    std::string huge_second_length = two_values.substr(0, 7);
    append_le32(&huge_second_length, 0xFFFFFFFFU);
    huge_second_length += "def";

    return {{"two values", two_values, true},
            {"two empty values at the end of the page", two_empty_values, true},
            {"cut inside the first length prefix", two_values.substr(0, 2), false},
            {"cut inside the second length prefix", two_values.substr(0, 9), false},
            {"cut inside the second value", two_values.substr(0, 12), false},
            {"huge first length", huge_first_length, false},
            {"huge second length", huge_second_length, false}};
}

// Runs `decode` on a new decoder for each page, which holds two values. A valid page has to decode, and any
// other page has to give an error. The page is copied to a buffer of exactly its size, so that a read after
// its end is a heap overflow for ASAN.
void check_plain_pages(const std::function<Status(Decoder*)>& decode) {
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    for (const auto& page : make_raw_plain_pages()) {
        std::vector<char> buffer(page.bytes.begin(), page.bytes.end());
        std::unique_ptr<Decoder> decoder;
        Status st = encoding->create_decoder(&decoder);
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decoder->set_data(Slice(buffer.data(), buffer.size()));
        ASSERT_TRUE(st.ok()) << st.to_string();
        st = decode(decoder.get());
        if (page.valid) {
            ASSERT_TRUE(st.ok()) << page.name << ": " << st.to_string();
        } else {
            ASSERT_FALSE(st.ok()) << page.name;
        }
    }
}

MutableColumnPtr make_plain_binary_column(bool nullable) {
    if (nullable) {
        return NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    }
    return BinaryColumn::create();
}

} // namespace

// A page is file data, so a length prefix or a value that does not fit in the page has to give an error in
// every decoding path, and no path may read after the page.
TEST_F(ParquetEncodingTest, PlainByteArrayBoundsNextBatchWithNulls) {
    ASSERT_NO_FATAL_FAILURE(check_plain_pages([](Decoder* decoder) -> Status {
        const std::vector<uint8_t> nulls = {0, 1, 0};
        NullInfos null_infos = make_plain_null_infos(nulls);
        auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        return decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
    }));
}

// The values that the filter drops are parsed too, so they have to be checked as well.
TEST_F(ParquetEncodingTest, PlainByteArrayBoundsNextBatchWithNullsAndFilter) {
    for (int selected : {1, 0}) {
        ASSERT_NO_FATAL_FAILURE(check_plain_pages([selected](Decoder* decoder) -> Status {
            const std::vector<uint8_t> nulls = {0, 1, 0};
            const std::vector<uint8_t> filter(nulls.size(), static_cast<uint8_t>(selected));
            NullInfos null_infos = make_plain_null_infos(nulls);
            auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
            return decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(),
                                                  filter.data());
        }));
    }
}

TEST_F(ParquetEncodingTest, PlainByteArrayBoundsNextBatch) {
    for (bool nullable : {true, false}) {
        ASSERT_NO_FATAL_FAILURE(check_plain_pages([nullable](Decoder* decoder) -> Status {
            auto dst = make_plain_binary_column(nullable);
            return decoder->next_batch(2, ColumnContentType::VALUE, dst.get(), nullptr);
        }));
    }
}

// The filtered path copies a value to the column as soon as it parses it.
TEST_F(ParquetEncodingTest, PlainByteArrayBoundsNextBatchWithFilter) {
    for (bool nullable : {true, false}) {
        for (int selected : {1, 0}) {
            ASSERT_NO_FATAL_FAILURE(check_plain_pages([nullable, selected](Decoder* decoder) -> Status {
                const std::vector<uint8_t> filter(2, static_cast<uint8_t>(selected));
                auto dst = make_plain_binary_column(nullable);
                return decoder->next_batch(2, ColumnContentType::VALUE, dst.get(), filter.data());
            }));
        }
    }
}

TEST_F(ParquetEncodingTest, PlainByteArrayBoundsSkip) {
    ASSERT_NO_FATAL_FAILURE(check_plain_pages([](Decoder* decoder) -> Status { return decoder->skip(2); }));
}

// This path reads the values of a dictionary page.
TEST_F(ParquetEncodingTest, PlainByteArrayBoundsNextBatchToSlices) {
    ASSERT_NO_FATAL_FAILURE(check_plain_pages([](Decoder* decoder) -> Status {
        std::vector<Slice> slices(2);
        return decoder->next_batch(2, reinterpret_cast<uint8_t*>(slices.data()));
    }));
}

namespace {
// The type lengths that a real file cannot have: zero, negative, and wider than the limit.
std::vector<int32_t> invalid_flba_type_lengths() {
    return {0,
            -1,
            -4,
            std::numeric_limits<int32_t>::min(),
            ParquetUtils::MAX_FLBA_TYPE_LENGTH + 1,
            std::numeric_limits<int32_t>::max()};
}

std::unique_ptr<Decoder> create_test_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding_type) {
    const EncodingInfo* encoding = nullptr;
    EXPECT_TRUE(EncodingInfo::get(type, encoding_type, &encoding).ok());
    std::unique_ptr<Decoder> decoder;
    EXPECT_TRUE(encoding->create_decoder(&decoder).ok());
    return decoder;
}
} // namespace

// The type length comes from the file footer. A decoder that gets a length no real file has must return
// an error and must not read with a stale or default length afterwards.
TEST_F(ParquetEncodingTest, FLBAPlainInvalidTypeLength) {
    auto decoder = create_test_decoder(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::PLAIN);
    std::string page(64, 'x');
    Slice out[4];

    // a decoder that never got a type length cannot read
    ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());
    ASSERT_FALSE(decoder->next_batch(1, reinterpret_cast<uint8_t*>(out)).ok());
    ASSERT_FALSE(decoder->skip(1).ok());

    for (int32_t type_length : invalid_flba_type_lengths()) {
        ASSERT_TRUE(decoder->set_type_length(4).ok());
        Status st = decoder->set_type_length(type_length);
        ASSERT_TRUE(st.is_corruption()) << type_length << " " << st.to_string();

        ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());
        ASSERT_FALSE(decoder->next_batch(1, reinterpret_cast<uint8_t*>(out)).ok()) << type_length;
        ASSERT_FALSE(decoder->skip(1).ok()) << type_length;
    }

    ASSERT_TRUE(decoder->set_type_length(1).ok());
    ASSERT_TRUE(decoder->set_type_length(ParquetUtils::MAX_FLBA_TYPE_LENGTH).ok());
}

// type_length * count wraps around to a small number for the pairs below. The decoder has to return an
// error before it writes any value: `out` holds only 4 values, and the page is 64 bytes.
TEST_F(ParquetEncodingTest, FLBAPlainCountOverflow) {
    struct Case {
        int32_t type_length;
        size_t count;
    };
    // 4 * 2^62 and 2^20 * 2^44 are 2^64, which wraps to 0; 3 * (2^64 / 3 + 1) wraps to 2.
    const std::vector<Case> cases = {{4, size_t(1) << 62},
                                     {ParquetUtils::MAX_FLBA_TYPE_LENGTH, size_t(1) << 44},
                                     {3, std::numeric_limits<size_t>::max() / 3 + 1}};
    std::string page(64, 'x');

    for (const auto& c : cases) {
        auto decoder = create_test_decoder(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::PLAIN);
        ASSERT_TRUE(decoder->set_type_length(c.type_length).ok());
        ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());

        Slice out[4];
        Status st = decoder->next_batch(c.count, reinterpret_cast<uint8_t*>(out));
        ASSERT_FALSE(st.ok()) << c.type_length << " " << c.count;

        st = decoder->skip(c.count);
        ASSERT_FALSE(st.ok()) << c.type_length << " " << c.count;

        auto column = BinaryColumn::create();
        st = decoder->next_batch(c.count, ColumnContentType::VALUE, column.get(), nullptr);
        ASSERT_FALSE(st.ok()) << c.type_length << " " << c.count;
        ASSERT_EQ(0U, column->size());

        // more nulls than rows: count - num_nulls wraps to a huge number of values to read
        NullInfos null_infos;
        null_infos.reset_with_capacity(2);
        memset(null_infos.nulls_data(), 0, 2);
        null_infos.num_nulls = 3;
        null_infos.num_ranges = 2;
        auto nullable_column = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        st = decoder->next_batch_with_nulls(2, null_infos, ColumnContentType::VALUE, nullable_column.get(), nullptr);
        ASSERT_FALSE(st.ok()) << c.type_length;
        ASSERT_EQ(0U, nullable_column->size());

        // the failed calls did not move the read position
        if (c.type_length == 4) {
            std::vector<Slice> values(16);
            st = decoder->next_batch(16, reinterpret_cast<uint8_t*>(values.data()));
            ASSERT_TRUE(st.ok()) << st.to_string();
            ASSERT_EQ(page.data(), values[0].data);
            ASSERT_EQ(page.data() + 60, values[15].data);
            ASSERT_EQ(4U, values[15].size);
            ASSERT_FALSE(decoder->skip(1).ok());
        }
    }
}

TEST_F(ParquetEncodingTest, ByteStreamSplitFLBAInvalidTypeLength) {
    auto decoder = create_test_decoder(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::BYTE_STREAM_SPLIT);
    std::string page(8, 'x');

    // set_data divides by the width, so a missing width must come back as an error, not as a division by zero
    ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).is_corruption());

    for (int32_t type_length : invalid_flba_type_lengths()) {
        ASSERT_TRUE(decoder->set_type_length(4).ok());
        Status st = decoder->set_type_length(type_length);
        ASSERT_TRUE(st.is_corruption()) << type_length << " " << st.to_string();
        ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).is_corruption()) << type_length;
    }

    ASSERT_TRUE(decoder->set_type_length(4).ok());
    ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());
}

TEST_F(ParquetEncodingTest, DeltaLengthByteArrayFLBAInvalidTypeLength) {
    auto decoder =
            create_test_decoder(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY);
    for (int32_t type_length : invalid_flba_type_lengths()) {
        Status st = decoder->set_type_length(type_length);
        ASSERT_TRUE(st.is_corruption()) << type_length << " " << st.to_string();
    }
    ASSERT_TRUE(decoder->set_type_length(4).ok());
}

// Only the FLBA decoders use the type length, so the decoders of the other types ignore any value.
TEST_F(ParquetEncodingTest, TypeLengthIgnoredByOtherDecoders) {
    for (int32_t type_length : invalid_flba_type_lengths()) {
        ASSERT_TRUE(create_test_decoder(tparquet::Type::INT32, tparquet::Encoding::PLAIN)
                            ->set_type_length(type_length)
                            .ok());
        ASSERT_TRUE(create_test_decoder(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN)
                            ->set_type_length(type_length)
                            .ok());
        ASSERT_TRUE(create_test_decoder(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY)
                            ->set_type_length(type_length)
                            .ok());
        ASSERT_TRUE(create_test_decoder(tparquet::Type::INT32, tparquet::Encoding::BYTE_STREAM_SPLIT)
                            ->set_type_length(type_length)
                            .ok());
    }
}

// type_length is not the only factor of a size that wraps: the count of values is too. For these counts
// count * sizeof(value) is 2^64, i.e. 0, and a decoder that multiplies without a check reads outside the
// page. The decoder has to return an error, and the 16 bytes of `out` must stay unwritten.
TEST_F(ParquetEncodingTest, PlainCountOverflow) {
    std::string page(16, 'x');
    for (auto [type, value_size] : std::vector<std::pair<tparquet::Type::type, size_t>>{
                 {tparquet::Type::INT32, 4}, {tparquet::Type::INT64, 8}, {tparquet::Type::DOUBLE, 8}}) {
        auto decoder = create_test_decoder(type, tparquet::Encoding::PLAIN);
        ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());
        const size_t wrap_count = std::numeric_limits<size_t>::max() / value_size + 1;

        uint8_t out[16] = {};
        ASSERT_FALSE(decoder->next_batch(wrap_count, out).ok());
        for (uint8_t byte : out) {
            ASSERT_EQ(0, byte);
        }
        ASSERT_FALSE(decoder->skip(wrap_count).ok());

        auto column = FixedLengthColumn<int64_t>::create();
        ASSERT_FALSE(decoder->next_batch(wrap_count, ColumnContentType::VALUE, column.get(), nullptr).ok());
        ASSERT_EQ(0U, column->size());

        // the position did not move, and the whole page is still readable
        ASSERT_TRUE(decoder->next_batch(16 / value_size, out).ok());
    }
}

// Each boolean takes one bit, so the count of a read is bounded by the bits of the page. The decoder must
// return an error for a larger count before it sizes any buffer from the count.
TEST_F(ParquetEncodingTest, PlainBooleanCountLargerThanThePage) {
    auto decoder = create_test_decoder(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN);
    std::string page(4, '\xff');
    ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());

    const size_t huge_count = size_t(1) << 40;
    auto column = FixedLengthColumn<uint8_t>::create();
    ASSERT_FALSE(decoder->next_batch(huge_count, ColumnContentType::VALUE, column.get(), nullptr).ok());
    ASSERT_EQ(0U, column->size());
    ASSERT_FALSE(decoder->skip(huge_count).ok());
    uint8_t out[40] = {};
    ASSERT_FALSE(decoder->next_batch(33, out).ok());

    // 32 bits are 32 values
    ASSERT_TRUE(decoder->skip(4).ok());
    ASSERT_TRUE(decoder->next_batch(28, out).ok());
    ASSERT_FALSE(decoder->next_batch(1, out).ok());
}

// The header of a DELTA_BINARY_PACKED page declares the number of values, and the decoders of the delta
// encodings size their buffers from it. A header that declares values that the page cannot hold must be
// rejected when the page is set, before any buffer is sized.
TEST_F(ParquetEncodingTest, DeltaHeaderDeclaresMoreValuesThanThePageHolds) {
    // ULEB128 fields: values_per_block = 128, mini_blocks_per_block = 4, total_value_count, zigzag(first_value) = 0
    auto make_page = [](const std::vector<uint8_t>& total_value_count, size_t body_size) {
        std::string page = {'\x80', '\x01', '\x04'};
        page.append(total_value_count.begin(), total_value_count.end());
        page.push_back('\x00');
        page.append(body_size, '\x00');
        return page;
    };
    const std::vector<uint8_t> max_count = {0xff, 0xff, 0xff, 0xff, 0x0f}; // 2^32 - 1
    const std::vector<uint8_t> count_1000 = {0xe8, 0x07};

    for (auto [type, encoding] : std::vector<std::pair<tparquet::Type::type, tparquet::Encoding::type>>{
                 {tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED},
                 {tparquet::Type::INT64, tparquet::Encoding::DELTA_BINARY_PACKED},
                 {tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY},
                 {tparquet::Type::BYTE_ARRAY, tparquet::Encoding::DELTA_BYTE_ARRAY},
                 {tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::DELTA_BYTE_ARRAY}}) {
        // 1000 values need 8 blocks of at least 5 bytes each (a minimum delta and 4 bit widths)
        for (const auto& page : {make_page(max_count, 0), make_page(max_count, 100), make_page(count_1000, 0),
                                 make_page(count_1000, 39)}) {
            auto decoder = create_test_decoder(type, encoding);
            Status st = decoder->set_data(Slice(page.data(), page.size()));
            ASSERT_TRUE(st.is_corruption()) << type << " " << encoding << " " << st.to_string();
        }
    }

    // a page of exactly the least size is accepted
    auto decoder = create_test_decoder(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED);
    std::string page = make_page(count_1000, 40);
    ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());
}

namespace {

// A PLAIN BYTE_ARRAY page of `num_rows` values of `width` bytes. With `announce` the decoder is told how many
// values the page holds before the page is set, as the reader does, so it knows the width of a value. The decoder
// decides by the share that it is told, as it does in production.
void make_plain_page_of_width(size_t num_rows, size_t width, bool announce, std::vector<std::string>* values,
                              std::unique_ptr<Encoder>* encoder, std::unique_ptr<Decoder>* decoder) {
    values->clear();
    for (size_t i = 0; i < num_rows; ++i) {
        values->push_back(std::string(width, static_cast<char>('a' + i % 26)));
    }
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::vector<Slice> slices;
    for (const auto& value : *values) {
        slices.emplace_back(value);
    }
    ASSERT_TRUE(encoding->create_encoder(encoder).ok());
    ASSERT_TRUE((*encoder)->append(reinterpret_cast<uint8_t*>(slices.data()), slices.size()).ok());
    ASSERT_TRUE(encoding->create_decoder(decoder).ok());
    (*decoder)->_filter_mode = FilterMode::AUTO;
    if (announce) {
        (*decoder)->set_num_values(num_rows);
    }
    ASSERT_TRUE((*decoder)->set_data((*encoder)->build()).ok());
}

// Decodes the rows of `values` with next_batch() and a filter that keeps the first `num_kept` rows, after the
// decoder is told that the filter keeps `share` of the rows of the range, and checks the column. A decoder that
// uses the filter makes each dropped row NULL, and a decoder that does not has every value. `used` tells which one
// happened. The share is a parameter because the decoder does not count the filter: the reader does.
void decode_plain_batch_and_tell(Decoder* decoder, const std::vector<std::string>& values, size_t num_kept,
                                 double share, bool* used) {
    std::vector<uint8_t> filter(values.size(), 0);
    std::fill_n(filter.begin(), num_kept, 1);
    decoder->set_filter_share(share);
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Status st = decoder->next_batch(values.size(), ColumnContentType::VALUE, dst.get(), filter.data());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(values.size(), dst->size());
    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());
    *used = dst->has_null();
    for (size_t i = 0; i < values.size(); ++i) {
        if (i < num_kept || !*used) {
            ASSERT_FALSE(dst->is_null(i)) << "row " << i;
            ASSERT_EQ(values[i], binary_column->get_slice(i).to_string()) << "row " << i;
        } else {
            ASSERT_TRUE(dst->is_null(i)) << "row " << i;
            ASSERT_EQ(0U, binary_column->get_slice(i).size) << "row " << i;
        }
    }
}

} // namespace

TEST_F(ParquetEncodingTest, DecoderStartsWithTheFilterModeOfTheBuild) {
    PlainDecoder<Slice> plain;
    DictDecoder<int32_t> dict;
    EXPECT_EQ(FilterMode::ALWAYS, plain._filter_mode);
    EXPECT_EQ(FilterMode::ALWAYS, dict._filter_mode);
    // A decoder that is not told the share takes it as a filter that keeps every row.
    EXPECT_DOUBLE_EQ(1.0, plain._filter_share);
    EXPECT_DOUBLE_EQ(1.0, dict._filter_share);
    plain.set_filter_share(0.25);
    EXPECT_DOUBLE_EQ(0.25, plain._filter_share);
}

TEST_F(ParquetEncodingTest, PlainByteArrayFilterShareTable) {
    using PlainSliceDecoder = PlainDecoder<Slice>;
    constexpr double kEps = 1e-9;
    // The measured points.
    EXPECT_NEAR(0.12, PlainSliceDecoder::max_filter_share(8), kEps);
    EXPECT_NEAR(0.20, PlainSliceDecoder::max_filter_share(32), kEps);
    EXPECT_NEAR(0.55, PlainSliceDecoder::max_filter_share(128), kEps);
    EXPECT_NEAR(0.90, PlainSliceDecoder::max_filter_share(512), kEps);
    // The nearest point below 8 and above 512 bytes, and for an unknown width.
    for (size_t width : {0, 1, 2, 7}) {
        EXPECT_NEAR(0.12, PlainSliceDecoder::max_filter_share(width), kEps) << width;
    }
    for (size_t width : {513, 1024, 4096, 1 << 20}) {
        EXPECT_NEAR(0.90, PlainSliceDecoder::max_filter_share(width), kEps) << width;
    }
    // The middle of two points in the logarithm of the width is the middle of their shares.
    EXPECT_NEAR(0.16, PlainSliceDecoder::max_filter_share(16), kEps);
    EXPECT_NEAR(0.375, PlainSliceDecoder::max_filter_share(64), kEps);
    EXPECT_NEAR(0.725, PlainSliceDecoder::max_filter_share(256), kEps);
    // Other widths are on the line between their points: 100 bytes is 82% of the way from 32 to 128 bytes in the
    // logarithm.
    EXPECT_NEAR(0.20 + 0.35 * (std::log2(100.0) - 5.0) / 2.0, PlainSliceDecoder::max_filter_share(100), kEps);
    // The share never falls when the width grows, and stays between the points.
    double previous = 0;
    for (size_t width = 0; width <= 2048; ++width) {
        const double share = PlainSliceDecoder::max_filter_share(width);
        EXPECT_GE(share, previous) << width;
        EXPECT_GE(share, 0.12 - kEps) << width;
        EXPECT_LE(share, 0.90 + kEps) << width;
        previous = share;
    }

    // The page of 100 values of 8 bytes holds 12 bytes for each, the length prefix included.
    EXPECT_EQ(8U, PlainSliceDecoder::avg_value_width(1200, 100));
    EXPECT_EQ(0U, PlainSliceDecoder::avg_value_width(1200, 0));
    // A page that is shorter than the prefixes of its values, which only a corrupt page or NULL values give.
    EXPECT_EQ(0U, PlainSliceDecoder::avg_value_width(100, 100));
    EXPECT_EQ(0U, PlainSliceDecoder::avg_value_width(0, 100));
}

// The decoder uses the filter when the share that it is told is below the share of the width of the values of the
// page, and the shares of the cases are a little below and a little above the share of the width. The page has 200
// rows, and the filter keeps the first 100 of them whatever the share is, so a row is dropped when the filter is
// used.
TEST_F(ParquetEncodingTest, PlainByteArrayUsesFilterByWidthAndShare) {
    struct Case {
        size_t width;
        double share;
        bool expect_used;
    };
    const std::vector<Case> cases = {
            // the nearest point below 8 bytes, 12%
            {5, 0.11, true},
            {5, 0.13, false},
            {8, 0.11, true},
            {8, 0.13, false},
            // between the points, on the line in the logarithm of the width
            {15, 0.15, true},
            {15, 0.165, false},
            {16, 0.15, true},
            {16, 0.17, false},
            {32, 0.19, true},
            {32, 0.21, false},
            {64, 0.36, true},
            {64, 0.39, false},
            {100, 0.46, true},
            {100, 0.52, false},
            {128, 0.53, true},
            {128, 0.57, false},
            {200, 0.64, true},
            {200, 0.69, false},
            {256, 0.70, true},
            {256, 0.75, false},
            // from 512 bytes, 90%
            {512, 0.88, true},
            {512, 0.92, false},
            {1024, 0.88, true},
            {1024, 0.92, false},
            {4096, 0.88, true},
            {4096, 0.92, false},
            {512, 1.0, false},
            {512, 0.0, true},
    };
    for (const Case& c : cases) {
        SCOPED_TRACE("width " + std::to_string(c.width) + ", share " + std::to_string(c.share));
        std::vector<std::string> values;
        std::unique_ptr<Encoder> encoder;
        std::unique_ptr<Decoder> decoder;
        ASSERT_NO_FATAL_FAILURE(make_plain_page_of_width(200, c.width, true, &values, &encoder, &decoder));
        bool used = false;
        ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), values, 100, c.share, &used));
        ASSERT_EQ(c.expect_used, used);
    }
}

// A page that the reader did not describe has no known width, so the decoder takes the share of the narrowest
// values. The description is for one page: the next page starts without it.
TEST_F(ParquetEncodingTest, PlainByteArrayFilterShareNeedsTheNumberOfValuesOfThePage) {
    std::vector<std::string> values;
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    bool used = false;

    // 512 byte values, half of the rows kept: used when the width is known, not used when it is not.
    ASSERT_NO_FATAL_FAILURE(make_plain_page_of_width(100, 512, true, &values, &encoder, &decoder));
    ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), values, 50, 0.5, &used));
    ASSERT_TRUE(used);

    // The same decoder gets the same page again, now without the number of values.
    ASSERT_TRUE(decoder->set_data(encoder->build()).ok());
    ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), values, 50, 0.5, &used));
    ASSERT_FALSE(used);
    // Few kept rows are still worth it for any width.
    ASSERT_TRUE(decoder->set_data(encoder->build()).ok());
    ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), values, 5, 0.05, &used));
    ASSERT_TRUE(used);

    ASSERT_NO_FATAL_FAILURE(make_plain_page_of_width(100, 512, false, &values, &encoder, &decoder));
    ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), values, 50, 0.5, &used));
    ASSERT_FALSE(used);
}

// The decoder does not count the filter. It decides by the share that it is told, whatever the filter keeps in the
// batch it decodes, and a decoder that is not told takes the filter as one that keeps every row.
TEST_F(ParquetEncodingTest, PlainByteArrayDecidesByTheShareItIsTold) {
    std::vector<std::string> values;
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    // 32 byte values use the filter below 20%.
    ASSERT_NO_FATAL_FAILURE(make_plain_page_of_width(200, 32, true, &values, &encoder, &decoder));
    bool used = false;
    const std::vector<std::string> first(values.begin(), values.begin() + 100);
    const std::vector<std::string> second(values.begin() + 100, values.end());

    // The filter of the batch keeps 50%, the range keeps 5%: used. Then the filter keeps 5% and the range 5%: used.
    ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), first, 50, 0.05, &used));
    ASSERT_TRUE(used);
    ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), second, 5, 0.05, &used));
    ASSERT_TRUE(used);

    // The filter of the batch keeps 5%, the range keeps 60%: not used.
    decoder->set_num_values(values.size());
    ASSERT_TRUE(decoder->set_data(encoder->build()).ok());
    ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), first, 5, 0.6, &used));
    ASSERT_FALSE(used);
    ASSERT_NO_FATAL_FAILURE(decode_plain_batch_and_tell(decoder.get(), second, 50, 0.6, &used));
    ASSERT_FALSE(used);

    // A decoder that is not told the share does not use the filter.
    ASSERT_NO_FATAL_FAILURE(make_plain_page_of_width(100, 32, true, &values, &encoder, &decoder));
    std::vector<uint8_t> filter(values.size(), 0);
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    ASSERT_TRUE(decoder->next_batch(values.size(), ColumnContentType::VALUE, dst.get(), filter.data()).ok());
    ASSERT_FALSE(dst->has_null());
}

namespace {

// A batch with NULL rows. The page holds a value for every second row, and the filter keeps every NULL row and
// every fourth value. The decoder decides by the share that it is told, and the NULL rows do not change that.
void check_plain_share_with_nulls(double share, bool expect_used) {
    SCOPED_TRACE("share " + std::to_string(share));
    constexpr size_t kNumRows = 400;
    // 200 values of 8 bytes, which use the filter below 12%. The reader announces the 400 rows of the page, NULL
    // rows included.
    std::vector<std::string> values;
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_plain_page_of_width(kNumRows / 2, 8, false, &values, &encoder, &decoder));
    decoder->set_num_values(kNumRows);
    ASSERT_TRUE(decoder->set_data(encoder->build()).ok());
    decoder->set_filter_share(share);

    std::vector<uint8_t> nulls(kNumRows);
    std::vector<uint8_t> filter(kNumRows);
    size_t num_values_seen = 0;
    for (size_t i = 0; i < kNumRows; ++i) {
        nulls[i] = i % 2;
        filter[i] = nulls[i] ? 1 : (num_values_seen++ % 4 == 0);
    }
    NullInfos null_infos = make_plain_null_infos(nulls);
    auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
    Status st =
            decoder->next_batch_with_nulls(kNumRows, null_infos, ColumnContentType::VALUE, dst.get(), filter.data());
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(kNumRows, dst->size());

    auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());
    bool used = false;
    size_t next_value = 0;
    for (size_t i = 0; i < kNumRows; ++i) {
        if (nulls[i]) {
            ASSERT_TRUE(dst->is_null(i)) << "row " << i;
            continue;
        }
        const std::string& value = values[next_value++];
        if (filter[i] || !dst->is_null(i)) {
            ASSERT_FALSE(dst->is_null(i)) << "row " << i;
            ASSERT_EQ(value, binary_column->get_slice(i).to_string()) << "row " << i;
        } else {
            used = true;
        }
    }
    ASSERT_EQ(expect_used, used);
}

} // namespace

TEST_F(ParquetEncodingTest, PlainByteArrayWithNullsDecidesByTheShareItIsTold) {
    ASSERT_NO_FATAL_FAILURE(check_plain_share_with_nulls(0.09, true));
    ASSERT_NO_FATAL_FAILURE(check_plain_share_with_nulls(0.0, true));
    ASSERT_NO_FATAL_FAILURE(check_plain_share_with_nulls(0.11, true));
    ASSERT_NO_FATAL_FAILURE(check_plain_share_with_nulls(0.13, false));
    ASSERT_NO_FATAL_FAILURE(check_plain_share_with_nulls(1.0, false));
}

namespace {

// The rows of a page of a numeric dictionary decoder that has NULL rows or not. `used` tells from the column
// whether the filter was used: a row with a value that the filter drops is NULL then. The column is checked in
// both cases against what the decoder promises.
template <typename T>
void decode_numeric_dict_batch_and_tell(Decoder* decoder, const std::vector<T>& page_values, size_t* page_pos,
                                        const std::vector<uint8_t>& nulls, const std::vector<uint8_t>& filter,
                                        bool with_null_infos, Column* dst, bool* used) {
    const size_t prev_rows = dst->size();
    NullInfos null_infos = make_dict_null_infos(nulls);
    Status st = with_null_infos ? decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE,
                                                                 dst, filter.data())
                                : decoder->next_batch(nulls.size(), ColumnContentType::VALUE, dst, filter.data());
    ASSERT_TRUE(st.ok()) << st.to_string();
    const size_t num_values = nulls.size() - null_infos.num_nulls;
    const std::vector<T> values(page_values.begin() + *page_pos, page_values.begin() + *page_pos + num_values);
    *page_pos += num_values;

    auto* nullable = down_cast<NullableColumn*>(dst);
    *used = false;
    for (size_t i = 0; i < nulls.size(); ++i) {
        if (!nulls[i] && !filter[i] && nullable->is_null(prev_rows + i)) {
            *used = true;
        }
    }
    ASSERT_NO_FATAL_FAILURE(verify_dict_batch<T>(dst, prev_rows, nulls, *used ? &filter : nullptr, values));
}

// The NULL rows of a batch of 200 rows: none, every fourth row, or a run of 60 NULL rows first.
std::vector<uint8_t> make_dict_test_nulls(int null_pattern) {
    std::vector<uint8_t> nulls(200, 0);
    for (size_t i = 0; i < nulls.size(); ++i) {
        if (null_pattern == 1) {
            nulls[i] = i % 4 == 3;
        } else if (null_pattern == 2) {
            nulls[i] = i < 60;
        }
    }
    return nulls;
}

// A filter that keeps every NULL row and every third value, so the filter drops rows whatever the share is.
std::vector<uint8_t> make_dict_test_filter(const std::vector<uint8_t>& nulls) {
    std::vector<uint8_t> filter(nulls.size());
    size_t num_values_seen = 0;
    for (size_t i = 0; i < nulls.size(); ++i) {
        filter[i] = nulls[i] ? 1 : (num_values_seen++ % 3 == 0);
    }
    return filter;
}

// A dictionary decoder of numbers uses the filter when the share that it is told is below 15%. The gate on the
// size of the dictionary is open, and the batch is decoded in one go or run by run, and both decide by the share
// of the range.
template <tparquet::Type::type PT>
void check_numeric_dict_filter_share() {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    struct Case {
        // The NULL rows of the batch, see make_dict_test_nulls(). Pattern 2 has a run of NULL rows first, so the
        // base class decodes the run of values on its own.
        int null_pattern;
        double share;
        bool expect_used;
    };
    const std::vector<Case> cases = {{0, 0.14, true}, {0, 0.16, false}, {0, 0.0, true},  {0, 1.0, false},
                                     {1, 0.14, true}, {1, 0.16, false}, {2, 0.14, true}, {2, 0.16, false}};
    for (const Case& c : cases) {
        SCOPED_TRACE("null pattern " + std::to_string(c.null_pattern) + ", share " + std::to_string(c.share));
        const std::vector<uint8_t> nulls = make_dict_test_nulls(c.null_pattern);
        const size_t num_values = count_non_null_rows(nulls);
        const std::vector<T> values = make_dict_page_values<T>(num_values);
        std::unique_ptr<Encoder> encoder;
        std::unique_ptr<Decoder> decoder;
        ASSERT_NO_FATAL_FAILURE(make_numeric_dict_page<PT>(values, &encoder, &decoder));
        decoder->_filter_mode = FilterMode::AUTO;
        decoder->set_filter_share(c.share);
        down_cast<CacheAwareDictDecoder*>(decoder.get())->_dict_size_threshold = 0;

        const std::vector<uint8_t> filter = make_dict_test_filter(nulls);
        auto dst = NullableColumn::create(make_sentinel_data_column<T>(nulls.size()), NullColumn::create());
        size_t page_pos = 0;
        bool used = false;
        ASSERT_NO_FATAL_FAILURE(decode_numeric_dict_batch_and_tell<T>(decoder.get(), values, &page_pos, nulls, filter,
                                                                      c.null_pattern != 0, dst.get(), &used));
        ASSERT_EQ(c.expect_used, used);
        ASSERT_EQ(values.size(), page_pos);
    }
}

} // namespace

TEST_F(ParquetEncodingTest, NumericDictDecidesByTheShareItIsTold) {
    ASSERT_NO_FATAL_FAILURE(check_numeric_dict_filter_share<tparquet::Type::INT32>());
    ASSERT_NO_FATAL_FAILURE(check_numeric_dict_filter_share<tparquet::Type::INT64>());
    ASSERT_NO_FATAL_FAILURE(check_numeric_dict_filter_share<tparquet::Type::FLOAT>());
    ASSERT_NO_FATAL_FAILURE(check_numeric_dict_filter_share<tparquet::Type::DOUBLE>());
}

// The decoder does not look at the share while the dictionary fits in the cache: the gate is closed and the
// filter is ignored, even when the share is 0.
TEST_F(ParquetEncodingTest, NumericDictIgnoresFilterWhileGateIsClosed) {
    const std::vector<uint8_t> nulls(100, 0);
    const std::vector<int32_t> values = make_dict_page_values<int32_t>(100);
    std::unique_ptr<Encoder> encoder;
    std::unique_ptr<Decoder> decoder;
    ASSERT_NO_FATAL_FAILURE(make_numeric_dict_page<tparquet::Type::INT32>(values, &encoder, &decoder));
    decoder->_filter_mode = FilterMode::AUTO;
    decoder->set_filter_share(0.0);
    ASSERT_GT(down_cast<CacheAwareDictDecoder*>(decoder.get())->_dict_size_threshold, 0U);

    const std::vector<uint8_t> filter(100, 0);
    auto dst = NullableColumn::create(make_sentinel_data_column<int32_t>(100), NullColumn::create());
    // next_batch_with_nulls() has no bypass for tests, and a page without NULL rows reaches it only from a test.
    NullInfos null_infos = make_dict_null_infos(nulls);
    ASSERT_TRUE(
            decoder->next_batch_with_nulls(100, null_infos, ColumnContentType::VALUE, dst.get(), filter.data()).ok());
    ASSERT_FALSE(dst->has_null());
    const int32_t* data = reinterpret_cast<const int32_t*>(dst->data_column_raw_ptr()->raw_data());
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_EQ(values[i], data[i]) << "row " << i;
    }
}

namespace {

// A dictionary of 10 strings of `width` bytes and a page of `num_rows` rows over it.
void make_string_dict_page(size_t num_rows, size_t width, std::vector<std::string>* values,
                           std::unique_ptr<Encoder>* encoder, std::unique_ptr<Decoder>* decoder) {
    std::vector<std::string> dict_values;
    for (size_t i = 0; i < 10; ++i) {
        dict_values.push_back(std::string(width, static_cast<char>('a' + i)));
    }
    values->clear();
    std::vector<Slice> slices;
    for (size_t i = 0; i < num_rows; ++i) {
        values->push_back(dict_values[i % dict_values.size()]);
    }
    for (const auto& value : *values) {
        slices.emplace_back(value);
    }

    const EncodingInfo* plain_encoding = nullptr;
    const EncodingInfo* dict_encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::PLAIN, &plain_encoding);
    (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, tparquet::Encoding::RLE_DICTIONARY, &dict_encoding);
    ASSERT_TRUE(plain_encoding != nullptr);
    ASSERT_TRUE(dict_encoding != nullptr);
    ASSERT_TRUE(dict_encoding->create_encoder(encoder).ok());
    ASSERT_TRUE((*encoder)->append(reinterpret_cast<uint8_t*>(slices.data()), slices.size()).ok());

    std::unique_ptr<Encoder> dict_encoder;
    ASSERT_TRUE(plain_encoding->create_encoder(&dict_encoder).ok());
    size_t num_dicts = 0;
    ASSERT_TRUE((*encoder)->encode_dict(dict_encoder.get(), &num_dicts).ok());
    std::unique_ptr<Decoder> dict_decoder;
    ASSERT_TRUE(plain_encoding->create_decoder(&dict_decoder).ok());
    ASSERT_TRUE(dict_decoder->set_data(dict_encoder->build()).ok());

    ASSERT_TRUE(dict_encoding->create_decoder(decoder).ok());
    ASSERT_TRUE((*decoder)->set_dict(config::vector_chunk_size, num_dicts, dict_decoder.get()).ok());
    ASSERT_TRUE((*decoder)->set_data((*encoder)->build()).ok());
    (*decoder)->_filter_mode = FilterMode::AUTO;
    down_cast<CacheAwareDictDecoder*>(decoder->get())->_dict_size_threshold = 0;
}

} // namespace

// The dictionary decoder of strings uses the filter when the share that it is told is below 20%, and below 30%
// when the average value of the dictionary is wider than 64 bytes.
TEST_F(ParquetEncodingTest, StringDictDecidesByWidthAndTheShareItIsTold) {
    struct Case {
        size_t width;
        // Every fourth row is NULL, or none.
        bool with_nulls;
        double share;
        bool expect_used;
    };
    const std::vector<Case> cases = {{8, false, 0.19, true},   {8, false, 0.21, false},  {64, false, 0.19, true},
                                     {64, false, 0.21, false}, {64, false, 0.29, false}, {65, false, 0.29, true},
                                     {65, false, 0.31, false}, {200, false, 0.29, true}, {200, false, 0.31, false},
                                     {8, true, 0.19, true},    {8, true, 0.21, false},   {65, true, 0.29, true},
                                     {65, true, 0.31, false}};
    for (const Case& c : cases) {
        SCOPED_TRACE("width " + std::to_string(c.width) + ", with NULL " + std::to_string(c.with_nulls) + ", share " +
                     std::to_string(c.share));
        const std::vector<uint8_t> nulls = make_dict_test_nulls(c.with_nulls ? 1 : 0);
        const size_t num_values = count_non_null_rows(nulls);
        std::vector<std::string> values;
        std::unique_ptr<Encoder> encoder;
        std::unique_ptr<Decoder> decoder;
        ASSERT_NO_FATAL_FAILURE(make_string_dict_page(num_values, c.width, &values, &encoder, &decoder));
        decoder->set_filter_share(c.share);

        const std::vector<uint8_t> filter = make_dict_test_filter(nulls);
        auto dst = NullableColumn::create(BinaryColumn::create(), NullColumn::create());
        NullInfos null_infos = make_dict_null_infos(nulls);
        Status st = c.with_nulls
                            ? decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE,
                                                             dst.get(), filter.data())
                            : decoder->next_batch(nulls.size(), ColumnContentType::VALUE, dst.get(), filter.data());
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(nulls.size(), dst->size());

        auto* binary_column = down_cast<BinaryColumn*>(dst->data_column_raw_ptr());
        bool used = false;
        size_t next_value = 0;
        for (size_t i = 0; i < nulls.size(); ++i) {
            if (nulls[i]) {
                ASSERT_TRUE(dst->is_null(i)) << "row " << i;
                continue;
            }
            const std::string& value = values[next_value++];
            if (filter[i] || !dst->is_null(i)) {
                ASSERT_FALSE(dst->is_null(i)) << "row " << i;
                ASSERT_EQ(value, binary_column->get_slice(i).to_string()) << "row " << i;
            } else {
                used = true;
                ASSERT_EQ(0U, binary_column->get_slice(i).size) << "row " << i;
            }
        }
        ASSERT_EQ(c.expect_used, used);
    }
}

namespace {

// The NULL patterns of a batch: leading, trailing and long runs of NULL rows, alternating rows, a single row,
// and batches that end with NULL rows. Each pattern is a name and the NULL flag of every row.
std::vector<std::pair<std::string, std::vector<uint8_t>>> make_null_patterns() {
    std::vector<std::pair<std::string, std::vector<uint8_t>>> patterns;
    auto add = [&patterns](const std::string& name, size_t count, const std::function<bool(size_t)>& is_null) {
        std::vector<uint8_t> nulls(count);
        for (size_t i = 0; i < count; ++i) {
            nulls[i] = is_null(i);
        }
        patterns.emplace_back(name, std::move(nulls));
    };
    add("no NULL", 100, [](size_t) { return false; });
    add("all NULL", 100, [](size_t) { return true; });
    add("leading NULL run", 100, [](size_t i) { return i < 40; });
    add("trailing NULL run", 100, [](size_t i) { return i >= 60; });
    add("alternating", 101, [](size_t i) { return i % 2 == 1; });
    add("alternating from NULL", 101, [](size_t i) { return i % 2 == 0; });
    add("every third", 100, [](size_t i) { return i % 3 == 2; });
    add("long runs", 200, [](size_t i) { return (i / 30) % 2 == 0; });
    add("one value", 100, [](size_t i) { return i != 57; });
    add("one NULL", 100, [](size_t i) { return i == 57; });
    add("last row is the only value", 100, [](size_t i) { return i != 99; });
    add("single NULL row", 1, [](size_t) { return true; });
    add("single value row", 1, [](size_t) { return false; });
    add("ends with NULL rows", 37, [](size_t i) { return i % 3 == 1 || i + 6 >= 37; });
    add("big batch", 5000, [](size_t i) { return i % 7 == 3 || (i >= 2500 && i < 2520); });
    return patterns;
}

template <typename T>
T make_plain_value(size_t i) {
    if constexpr (std::is_same_v<T, int96_t>) {
        int96_t value;
        value.lo = 1000003ULL * (i + 1);
        value.hi = static_cast<uint32_t>(i + 7);
        return value;
    } else if constexpr (std::is_floating_point_v<T>) {
        return static_cast<T>(i) * static_cast<T>(1.5) + static_cast<T>(0.25);
    } else {
        return static_cast<T>(1000 + i * 7);
    }
}

// Decodes two batches that have the NULL rows `nulls` with next_batch_with_nulls(), and then `tail_values` values
// without NULL rows, from one PLAIN page. The page is a copy of its exact size, so a decoder that reads after
// its end is a heap overflow for ASAN. With `count_ranges` the NullInfos tell the number of runs of NULL rows, and
// without it the number is 0, which means that it is not known. A filter that drops every row is passed with
// `with_filter`: the decoder does not use it, and the dropped rows have to hold their values.
template <tparquet::Type::type PT>
void check_plain_numeric_batches_with_nulls(const std::vector<uint8_t>& nulls, bool count_ranges, size_t tail_values,
                                            bool with_filter) {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    constexpr size_t kNumBatches = 2;
    const size_t num_values = count_non_null_rows(nulls);
    std::vector<T> page_values;
    for (size_t i = 0; i < kNumBatches * num_values + tail_values; ++i) {
        page_values.push_back(make_plain_value<T>(i));
    }

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(PT, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Encoder> encoder;
    ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
    if (!page_values.empty()) {
        ASSERT_TRUE(encoder->append(reinterpret_cast<const uint8_t*>(page_values.data()), page_values.size()).ok());
    }
    const Slice built = encoder->build();
    const std::vector<char> page(built.data, built.data + built.size);
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
    ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());

    const std::vector<uint8_t> filter(nulls.size(), 0);
    auto dst = NullableColumn::create(FixedLengthColumn<T>::create(), NullColumn::create());
    size_t next_value = 0;
    for (size_t batch = 0; batch < kNumBatches; ++batch) {
        const size_t prev_rows = dst->size();
        NullInfos null_infos = make_dict_null_infos(nulls);
        if (!count_ranges) {
            null_infos.num_ranges = 0;
        }
        Status st = decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(),
                                                   with_filter ? filter.data() : nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(prev_rows + nulls.size(), dst->size());
        ASSERT_EQ(dst->size(), dst->null_column_raw_ptr()->size());

        const T* data = reinterpret_cast<const T*>(dst->data_column_raw_ptr()->raw_data());
        for (size_t i = 0; i < nulls.size(); ++i) {
            const size_t row = prev_rows + i;
            if (nulls[i]) {
                ASSERT_TRUE(dst->is_null(row)) << "row " << row;
            } else {
                ASSERT_FALSE(dst->is_null(row)) << "row " << row;
                ASSERT_TRUE(page_values[next_value] == data[row]) << "row " << row;
                next_value++;
            }
        }
    }
    ASSERT_EQ(dst->has_null(), num_values < nulls.size());

    // The decoder is at the end of the values of the batches.
    auto tail = FixedLengthColumn<T>::create();
    ASSERT_TRUE(decoder->next_batch(tail_values, ColumnContentType::VALUE, tail.get(), nullptr).ok());
    const T* tail_data = reinterpret_cast<const T*>(tail->raw_data());
    for (size_t i = 0; i < tail_values; ++i) {
        ASSERT_TRUE(page_values[next_value + i] == tail_data[i]) << "tail value " << i;
    }
    ASSERT_FALSE(decoder->next_batch(1, ColumnContentType::VALUE, tail.get(), nullptr).ok());
}

template <tparquet::Type::type PT>
void check_plain_numeric_with_nulls() {
    for (const auto& [name, nulls] : make_null_patterns()) {
        for (bool count_ranges : {true, false}) {
            for (size_t tail_values : {0, 3}) {
                for (bool with_filter : {false, true}) {
                    SCOPED_TRACE("pattern '" + name + "', count_ranges " + std::to_string(count_ranges) +
                                 ", tail values " + std::to_string(tail_values) + ", filter " +
                                 std::to_string(with_filter));
                    ASSERT_NO_FATAL_FAILURE(
                            check_plain_numeric_batches_with_nulls<PT>(nulls, count_ranges, tail_values, with_filter));
                }
            }
        }
    }
}

} // namespace

// A nullable batch of a PLAIN page of numbers holds the values of the rows that are not NULL. The decoder spreads
// them over their rows, and the values have to be the same for every pattern of NULL rows and every width.
TEST_F(ParquetEncodingTest, PlainNumericWithNulls) {
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_with_nulls<tparquet::Type::INT32>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_with_nulls<tparquet::Type::INT64>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_with_nulls<tparquet::Type::FLOAT>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_with_nulls<tparquet::Type::DOUBLE>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_with_nulls<tparquet::Type::INT96>());
}

// The page is file data. A batch that needs more values than the page holds has to give an error before it
// changes the column or moves the decoder, and a number of NULL rows above the number of rows is a mistake of the
// caller.
TEST_F(ParquetEncodingTest, PlainNumericWithNullsBounds) {
    for (auto type : {tparquet::Type::INT32, tparquet::Type::INT64, tparquet::Type::FLOAT, tparquet::Type::DOUBLE,
                      tparquet::Type::INT96}) {
        SCOPED_TRACE("type " + std::to_string(type));
        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(type, tparquet::Encoding::PLAIN, &encoding);
        ASSERT_TRUE(encoding != nullptr);
        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(encoding->create_decoder(&decoder).ok());

        size_t value_size = 4;
        if (type == tparquet::Type::INT64 || type == tparquet::Type::DOUBLE) {
            value_size = 8;
        } else if (type == tparquet::Type::INT96) {
            value_size = 12;
        }
        // The batch of 100 rows has 50 values, and the page holds 49. The page is a buffer of its exact size.
        const std::vector<char> page(49 * value_size, 1);
        ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());

        std::vector<uint8_t> nulls(100);
        for (size_t i = 0; i < nulls.size(); ++i) {
            nulls[i] = i % 2;
        }
        NullInfos null_infos = make_dict_null_infos(nulls);
        auto dst = NullableColumn::create(FixedLengthColumn<int64_t>::create(), NullColumn::create());
        // The column type does not matter, because the decoder does not reach the column.
        Status st =
                decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_FALSE(st.ok());
        ASSERT_EQ(0U, dst->size());
        ASSERT_EQ(0U, dst->null_column_raw_ptr()->size());

        // The decoder did not move: the 49 values of the page can still be read.
        uint8_t out[49 * 12];
        ASSERT_TRUE(decoder->next_batch(49, out).ok());

        // A count that makes the size of the values wrap is an error, and the NULL flags are not read.
        ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());
        NullInfos wrap_infos;
        wrap_infos.reset_with_capacity(1);
        wrap_infos.nulls_data()[0] = 1;
        wrap_infos.num_nulls = 1;
        wrap_infos.num_ranges = 0;
        const size_t wrap_count = std::numeric_limits<size_t>::max() / value_size + 2;
        ASSERT_FALSE(
                decoder->next_batch_with_nulls(wrap_count, wrap_infos, ColumnContentType::VALUE, dst.get(), nullptr)
                        .ok());
        ASSERT_EQ(0U, dst->size());

        // More NULL rows than rows.
        NullInfos bad_infos = make_dict_null_infos(nulls);
        bad_infos.num_nulls = nulls.size() + 1;
        ASSERT_FALSE(
                decoder->next_batch_with_nulls(nulls.size(), bad_infos, ColumnContentType::VALUE, dst.get(), nullptr)
                        .ok());
        ASSERT_EQ(0U, dst->size());
    }
}

namespace {

bool plain_bool_value(size_t i) {
    return (i * 5) % 7 < 3;
}

// The same as check_plain_numeric_batches_with_nulls() for the bits of a PLAIN BOOLEAN page. The batches have
// different numbers of values, so the bits that the unpacking buffers between two calls are used.
void check_plain_bool_batches_with_nulls(const std::vector<std::vector<uint8_t>>& batches, bool count_ranges,
                                         size_t tail_values) {
    size_t total_values = tail_values;
    for (const auto& nulls : batches) {
        total_values += count_non_null_rows(nulls);
    }
    std::vector<uint8_t> page_values(total_values);
    for (size_t i = 0; i < total_values; ++i) {
        page_values[i] = plain_bool_value(i);
    }

    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Encoder> encoder;
    ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
    ASSERT_TRUE(encoder->append(page_values.data(), page_values.size()).ok());
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
    ASSERT_TRUE(decoder->set_data(encoder->build()).ok());

    auto dst = NullableColumn::create(FixedLengthColumn<uint8_t>::create(), NullColumn::create());
    size_t next_value = 0;
    for (const auto& nulls : batches) {
        const size_t prev_rows = dst->size();
        NullInfos null_infos = make_dict_null_infos(nulls);
        if (!count_ranges) {
            null_infos.num_ranges = 0;
        }
        Status st =
                decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(prev_rows + nulls.size(), dst->size());
        ASSERT_EQ(dst->size(), dst->null_column_raw_ptr()->size());
        const uint8_t* data = dst->data_column_raw_ptr()->raw_data();
        for (size_t i = 0; i < nulls.size(); ++i) {
            const size_t row = prev_rows + i;
            if (nulls[i]) {
                ASSERT_TRUE(dst->is_null(row)) << "row " << row;
            } else {
                ASSERT_FALSE(dst->is_null(row)) << "row " << row;
                ASSERT_EQ(page_values[next_value], data[row]) << "row " << row;
                next_value++;
            }
        }
    }

    auto tail = FixedLengthColumn<uint8_t>::create();
    ASSERT_TRUE(decoder->next_batch(tail_values, ColumnContentType::VALUE, tail.get(), nullptr).ok());
    for (size_t i = 0; i < tail_values; ++i) {
        ASSERT_EQ(page_values[next_value + i], tail->raw_data()[i]) << "tail value " << i;
    }
}

} // namespace

TEST_F(ParquetEncodingTest, PlainBooleanWithNulls) {
    const auto patterns = make_null_patterns();
    // One batch for each pattern, and then every pattern after the previous one in the same page.
    std::vector<std::vector<std::vector<uint8_t>>> cases;
    std::vector<std::vector<uint8_t>> all_patterns;
    for (const auto& pattern : patterns) {
        cases.push_back({pattern.second});
        all_patterns.push_back(pattern.second);
    }
    cases.push_back(all_patterns);
    for (size_t c = 0; c < cases.size(); ++c) {
        for (bool count_ranges : {true, false}) {
            for (size_t tail_values : {0, 5}) {
                SCOPED_TRACE("case " + std::to_string(c) + ", count_ranges " + std::to_string(count_ranges) +
                             ", tail values " + std::to_string(tail_values));
                ASSERT_NO_FATAL_FAILURE(check_plain_bool_batches_with_nulls(cases[c], count_ranges, tail_values));
            }
        }
    }
}

// A batch that needs more bits than the page holds gives an error and leaves the column as it was.
TEST_F(ParquetEncodingTest, PlainBooleanWithNullsBounds) {
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
    const std::string page(1, '\xff');
    ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());

    std::vector<uint8_t> nulls(40);
    for (size_t i = 0; i < nulls.size(); ++i) {
        nulls[i] = i % 2;
    }
    NullInfos null_infos = make_dict_null_infos(nulls);
    auto dst = NullableColumn::create(FixedLengthColumn<uint8_t>::create(), NullColumn::create());
    ASSERT_FALSE(decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(), nullptr)
                         .ok());
    ASSERT_EQ(0U, dst->size());
    ASSERT_EQ(0U, dst->null_column_raw_ptr()->size());

    // A count that the page cannot hold is rejected before the decoder sizes a buffer from it, and the NULL
    // flags are not read.
    NullInfos huge_infos;
    huge_infos.reset_with_capacity(1);
    huge_infos.nulls_data()[0] = 1;
    huge_infos.num_nulls = 1;
    huge_infos.num_ranges = 0;
    ASSERT_FALSE(
            decoder->next_batch_with_nulls(size_t(1) << 40, huge_infos, ColumnContentType::VALUE, dst.get(), nullptr)
                    .ok());
    ASSERT_EQ(0U, dst->size());

    // More NULL rows than rows.
    NullInfos bad_infos = make_dict_null_infos(nulls);
    bad_infos.num_nulls = nulls.size() + 1;
    ASSERT_FALSE(
            decoder->next_batch_with_nulls(nulls.size(), bad_infos, ColumnContentType::VALUE, dst.get(), nullptr).ok());
    ASSERT_EQ(0U, dst->size());
}

namespace {

// The NULL rows of the batches below: every second row, so there are many runs and the decoders would take the
// paths that write the NULL flags to a null column.
std::vector<uint8_t> make_alternating_nulls() {
    std::vector<uint8_t> nulls(41);
    for (size_t i = 0; i < nulls.size(); ++i) {
        nulls[i] = i % 2;
    }
    return nulls;
}

// A column that is not nullable has no place for a NULL row, and a NULL value in the page for it means that the file
// contradicts the table. The decoder gives Corruption and leaves the column and its own position as they were.
void check_nulls_are_corruption(Decoder* decoder, const std::vector<uint8_t>& nulls, bool count_ranges, Column* dst) {
    NullInfos null_infos = make_dict_null_infos(nulls);
    if (!count_ranges) {
        null_infos.num_ranges = 0;
    }
    Status st = decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst, nullptr);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_EQ(0U, dst->size());
}

// A batch without NULL rows is fine for a column that is not nullable, and starts at the first value of the page.
NullInfos make_batch_without_nulls(size_t count) {
    return make_dict_null_infos(std::vector<uint8_t>(count, 0));
}

// A PLAIN page of numbers read into a column that is not nullable.
template <tparquet::Type::type PT>
void check_plain_numeric_to_non_nullable_column() {
    using T = typename PhysicalTypeTraits<PT>::CppType;
    const std::vector<uint8_t> nulls = make_alternating_nulls();
    const size_t num_values = count_non_null_rows(nulls);
    std::vector<T> page_values;
    for (size_t i = 0; i < num_values; ++i) {
        page_values.push_back(make_plain_value<T>(i));
    }
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(PT, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Encoder> encoder;
    ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
    ASSERT_TRUE(encoder->append(reinterpret_cast<const uint8_t*>(page_values.data()), page_values.size()).ok());
    const Slice built = encoder->build();
    const std::vector<char> page(built.data, built.data + built.size);
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());

    for (bool count_ranges : {true, false}) {
        ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());
        auto dst = FixedLengthColumn<T>::create();
        ASSERT_NO_FATAL_FAILURE(check_nulls_are_corruption(decoder.get(), nulls, count_ranges, dst.get()));

        NullInfos null_infos = make_batch_without_nulls(num_values);
        Status st =
                decoder->next_batch_with_nulls(num_values, null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(num_values, dst->size());
        const T* data = reinterpret_cast<const T*>(dst->raw_data());
        for (size_t i = 0; i < num_values; ++i) {
            ASSERT_TRUE(page_values[i] == data[i]) << "value " << i;
        }
    }
}

} // namespace

TEST_F(ParquetEncodingTest, NullsForNonNullableColumnArePlainNumericCorruption) {
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_to_non_nullable_column<tparquet::Type::INT32>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_to_non_nullable_column<tparquet::Type::INT64>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_to_non_nullable_column<tparquet::Type::FLOAT>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_to_non_nullable_column<tparquet::Type::DOUBLE>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_to_non_nullable_column<tparquet::Type::INT96>());
}

TEST_F(ParquetEncodingTest, NullsForNonNullableColumnArePlainBooleanCorruption) {
    const std::vector<uint8_t> nulls = make_alternating_nulls();
    const size_t num_values = count_non_null_rows(nulls);
    std::vector<uint8_t> page_values(num_values);
    for (size_t i = 0; i < num_values; ++i) {
        page_values[i] = plain_bool_value(i);
    }
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::BOOLEAN, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Encoder> encoder;
    ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
    ASSERT_TRUE(encoder->append(page_values.data(), page_values.size()).ok());
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());

    for (bool count_ranges : {true, false}) {
        ASSERT_TRUE(decoder->set_data(encoder->build()).ok());
        auto dst = FixedLengthColumn<uint8_t>::create();
        ASSERT_NO_FATAL_FAILURE(check_nulls_are_corruption(decoder.get(), nulls, count_ranges, dst.get()));

        NullInfos null_infos = make_batch_without_nulls(num_values);
        Status st =
                decoder->next_batch_with_nulls(num_values, null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(num_values, dst->size());
        for (size_t i = 0; i < num_values; ++i) {
            ASSERT_EQ(page_values[i], dst->raw_data()[i]) << "value " << i;
        }
    }
}

TEST_F(ParquetEncodingTest, NullsForNonNullableColumnAreDictionaryCorruption) {
    const std::vector<uint8_t> nulls = make_alternating_nulls();
    const size_t num_values = count_non_null_rows(nulls);
    // numbers
    {
        const std::vector<int32_t> values = make_dict_page_values<int32_t>(num_values);
        std::unique_ptr<Encoder> encoder;
        std::unique_ptr<Decoder> decoder;
        ASSERT_NO_FATAL_FAILURE(make_numeric_dict_page<tparquet::Type::INT32>(values, &encoder, &decoder));
        auto dst = FixedLengthColumn<int32_t>::create();
        ASSERT_NO_FATAL_FAILURE(check_nulls_are_corruption(decoder.get(), nulls, true, dst.get()));

        NullInfos null_infos = make_batch_without_nulls(num_values);
        Status st =
                decoder->next_batch_with_nulls(num_values, null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(num_values, dst->size());
        const int32_t* data = reinterpret_cast<const int32_t*>(dst->raw_data());
        for (size_t i = 0; i < num_values; ++i) {
            ASSERT_EQ(values[i], data[i]) << "value " << i;
        }
    }
    // strings
    {
        std::vector<std::string> values;
        std::unique_ptr<Encoder> encoder;
        std::unique_ptr<Decoder> decoder;
        ASSERT_NO_FATAL_FAILURE(make_string_dict_page(num_values, 5, &values, &encoder, &decoder));
        auto dst = BinaryColumn::create();
        ASSERT_NO_FATAL_FAILURE(check_nulls_are_corruption(decoder.get(), nulls, true, dst.get()));

        NullInfos null_infos = make_batch_without_nulls(num_values);
        Status st =
                decoder->next_batch_with_nulls(num_values, null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(num_values, dst->size());
        for (size_t i = 0; i < num_values; ++i) {
            ASSERT_EQ(values[i], dst->get_slice(i).to_string()) << "value " << i;
        }
    }
}

TEST_F(ParquetEncodingTest, NullsForNonNullableColumnAreFixedLengthByteArrayCorruption) {
    constexpr size_t kTypeLength = 4;
    const std::vector<uint8_t> nulls = make_alternating_nulls();
    const size_t num_values = count_non_null_rows(nulls);
    std::vector<std::string> values;
    std::vector<Slice> slices;
    for (size_t i = 0; i < num_values; ++i) {
        values.push_back(std::string(kTypeLength, static_cast<char>('a' + i % 26)));
    }
    for (const auto& value : values) {
        slices.emplace_back(value);
    }
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::PLAIN, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Encoder> encoder;
    ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
    ASSERT_TRUE(encoder->append(reinterpret_cast<uint8_t*>(slices.data()), slices.size()).ok());
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
    ASSERT_TRUE(decoder->set_type_length(kTypeLength).ok());
    ASSERT_TRUE(decoder->set_data(encoder->build()).ok());

    auto dst = BinaryColumn::create();
    ASSERT_NO_FATAL_FAILURE(check_nulls_are_corruption(decoder.get(), nulls, true, dst.get()));
    NullInfos null_infos = make_batch_without_nulls(num_values);
    Status st = decoder->next_batch_with_nulls(num_values, null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(num_values, dst->size());
    for (size_t i = 0; i < num_values; ++i) {
        ASSERT_EQ(values[i], dst->get_slice(i).to_string()) << "value " << i;
    }
}

// The PLAIN decoder of strings, which has its own path, and the delta and byte stream split decoders, which use the
// base class: they all give the same error.
TEST_F(ParquetEncodingTest, NullsForNonNullableColumnArePlainStringAndBaseClassCorruption) {
    const std::vector<uint8_t> nulls = make_alternating_nulls();
    const size_t num_values = count_non_null_rows(nulls);
    // PLAIN strings
    {
        std::vector<std::string> values;
        std::unique_ptr<Encoder> encoder;
        std::unique_ptr<Decoder> decoder;
        ASSERT_NO_FATAL_FAILURE(make_plain_page_of_width(num_values, 5, false, &values, &encoder, &decoder));
        auto dst = BinaryColumn::create();
        ASSERT_NO_FATAL_FAILURE(check_nulls_are_corruption(decoder.get(), nulls, true, dst.get()));

        NullInfos null_infos = make_batch_without_nulls(num_values);
        Status st =
                decoder->next_batch_with_nulls(num_values, null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(num_values, dst->size());
        for (size_t i = 0; i < num_values; ++i) {
            ASSERT_EQ(values[i], dst->get_slice(i).to_string()) << "value " << i;
        }
    }
    // delta binary packed and byte stream split numbers
    std::vector<int32_t> values;
    for (size_t i = 0; i < num_values; ++i) {
        values.push_back(static_cast<int32_t>(1000 + i * 7));
    }
    for (auto encoding_type : {tparquet::Encoding::DELTA_BINARY_PACKED, tparquet::Encoding::BYTE_STREAM_SPLIT}) {
        SCOPED_TRACE("encoding " + std::to_string(encoding_type));
        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(tparquet::Type::INT32, encoding_type, &encoding);
        ASSERT_TRUE(encoding != nullptr);
        std::unique_ptr<Encoder> encoder;
        ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
        ASSERT_TRUE(encoder->append(reinterpret_cast<const uint8_t*>(values.data()), values.size()).ok());
        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
        ASSERT_TRUE(decoder->set_data(encoder->build()).ok());

        auto dst = FixedLengthColumn<int32_t>::create();
        ASSERT_NO_FATAL_FAILURE(check_nulls_are_corruption(decoder.get(), nulls, true, dst.get()));
        NullInfos null_infos = make_batch_without_nulls(num_values);
        Status st =
                decoder->next_batch_with_nulls(num_values, null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_TRUE(st.ok()) << st.to_string();
        ASSERT_EQ(num_values, dst->size());
        const int32_t* data = reinterpret_cast<const int32_t*>(dst->raw_data());
        for (size_t i = 0; i < num_values; ++i) {
            ASSERT_EQ(values[i], data[i]) << "value " << i;
        }
    }
}

// The base class gives the error from the flags too, when the number of NULL rows that the caller gave is wrong.
TEST_F(ParquetEncodingTest, BaseClassRefusesNullFlagsForNonNullableColumn) {
    const std::vector<uint8_t> nulls = make_alternating_nulls();
    std::vector<int32_t> values(count_non_null_rows(nulls), 7);
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Encoder> encoder;
    ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
    ASSERT_TRUE(encoder->append(reinterpret_cast<const uint8_t*>(values.data()), values.size()).ok());
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
    ASSERT_TRUE(decoder->set_data(encoder->build()).ok());

    NullInfos null_infos = make_dict_null_infos(nulls);
    null_infos.num_nulls = 0;
    auto dst = FixedLengthColumn<int32_t>::create();
    Status st = decoder->next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
}

namespace {

// What a column holds, to see that a failed read changed nothing.
struct ColumnSnapshot {
    size_t size = 0;
    bool has_null = false;
    std::vector<std::string> items;
};

ColumnSnapshot take_snapshot(Column* column) {
    ColumnSnapshot snapshot;
    snapshot.size = column->size();
    snapshot.has_null = column->is_nullable() && down_cast<NullableColumn*>(column)->has_null();
    for (size_t i = 0; i < column->size(); ++i) {
        snapshot.items.push_back(column->debug_item(i));
    }
    return snapshot;
}

// A decoder that fails in the middle of a batch has to give the column back as it was: the same rows, the same
// null column and has_null, and for a binary column offsets that match its bytes. A destructor of a binary column
// checks the last one in a debug build, and a release build would read garbage.
void expect_unchanged(Column* column, const ColumnSnapshot& before) {
    ASSERT_EQ(before.size, column->size());
    ASSERT_EQ(before.has_null, column->is_nullable() && down_cast<NullableColumn*>(column)->has_null());
    ASSERT_EQ(before.items.size(), column->size());
    for (size_t i = 0; i < column->size(); ++i) {
        ASSERT_EQ(before.items[i], column->debug_item(i)) << "row " << i;
    }
    Column* data_column = column;
    if (column->is_nullable()) {
        auto* nullable = down_cast<NullableColumn*>(column);
        ASSERT_EQ(before.size, nullable->null_column_raw_ptr()->size());
        data_column = nullable->data_column_raw_ptr();
    }
    ASSERT_EQ(before.size, data_column->size());
    if (data_column->is_binary()) {
        auto* binary_column = down_cast<BinaryColumn*>(data_column);
        ASSERT_EQ(before.size + 1, binary_column->get_offset().size());
        ASSERT_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
    }
}

// Three values and then a length prefix that is far beyond the page.
std::string make_plain_string_page_with_corrupt_tail(const std::vector<std::string>& values) {
    std::vector<Slice> slices;
    for (const auto& value : values) {
        slices.emplace_back(value);
    }
    PlainEncoder<Slice> encoder;
    (void)encoder.append(reinterpret_cast<const uint8_t*>(slices.data()), slices.size());
    const Slice built = encoder.build();
    std::string page(built.data, built.size);
    append_le32(&page, 1000);
    page += "abc";
    return page;
}

// Reads a good batch, then a batch whose last value is the corrupt tail, and checks that the failure leaves the
// column and the position of the decoder as they were.
void check_plain_string_batch_rolls_back(bool nullable, bool with_nulls, bool use_filter) {
    SCOPED_TRACE("nullable " + std::to_string(nullable) + ", with nulls " + std::to_string(with_nulls) + ", filter " +
                 std::to_string(use_filter));
    const std::vector<std::string> values = {"alpha", "bb", "gamma"};
    const std::string page_string = make_plain_string_page_with_corrupt_tail(values);
    // The page is a buffer of its exact size, so that a read after its end is a heap overflow for ASAN.
    const std::vector<char> page(page_string.begin(), page_string.end());
    PlainDecoder<Slice> decoder;
    decoder._filter_mode = FilterMode::ALWAYS;
    ASSERT_TRUE(decoder.set_data(Slice(page.data(), page.size())).ok());
    MutableColumnPtr dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), nullable);

    const bool has_nulls = with_nulls && nullable;
    auto read = [&](const std::vector<uint8_t>& nulls) {
        std::vector<uint8_t> filter(nulls.size());
        for (size_t i = 0; i < filter.size(); ++i) {
            filter[i] = i % 2 == 0;
        }
        const FilterData* filter_data = use_filter ? filter.data() : nullptr;
        if (with_nulls) {
            NullInfos null_infos = make_plain_null_infos(nulls);
            return decoder.next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(),
                                                 filter_data);
        }
        return decoder.next_batch(nulls.size(), ColumnContentType::VALUE, dst.get(), filter_data);
    };

    // "alpha" and "bb"
    ASSERT_TRUE(read(has_nulls ? std::vector<uint8_t>{1, 0, 0} : std::vector<uint8_t>{0, 0}).ok());
    const ColumnSnapshot before = take_snapshot(dst.get());

    // "gamma" and the corrupt tail
    Status st = read(has_nulls ? std::vector<uint8_t>{0, 1, 0} : std::vector<uint8_t>{0, 0});
    ASSERT_FALSE(st.ok());
    ASSERT_NO_FATAL_FAILURE(expect_unchanged(dst.get(), before));

    // The decoder is where it was before the failed batch, so "gamma" can still be read.
    ASSERT_TRUE(decoder.next_batch(1, ColumnContentType::VALUE, dst.get(), nullptr).ok());
    ASSERT_EQ(before.size + 1, dst->size());
    ASSERT_EQ("gamma", ColumnHelper::get_binary_column(dst.get())->get_slice(before.size).to_string());
}

} // namespace

TEST_F(ParquetEncodingTest, PlainStringBatchThatFailsInTheMiddleLeavesTheColumnAsItWas) {
    for (bool nullable : {true, false}) {
        for (bool with_nulls : {false, true}) {
            for (bool use_filter : {false, true}) {
                ASSERT_NO_FATAL_FAILURE(check_plain_string_batch_rolls_back(nullable, with_nulls, use_filter));
            }
        }
    }
}

namespace {

// A PLAIN decoder of numbers reads a batch of a page that has the values of the rows that are not NULL, and the
// page has fewer values than the second batch needs.
template <typename T>
void check_plain_numeric_batch_rolls_back() {
    std::vector<T> values;
    for (size_t i = 0; i < 4; ++i) {
        values.push_back(make_plain_value<T>(i));
    }
    PlainEncoder<T> encoder;
    ASSERT_TRUE(encoder.append(reinterpret_cast<const uint8_t*>(values.data()), values.size()).ok());
    const Slice built = encoder.build();
    const std::vector<char> page(built.data, built.data + built.size);
    PlainDecoder<T> decoder;
    ASSERT_TRUE(decoder.set_data(Slice(page.data(), page.size())).ok());

    auto dst = NullableColumn::create(FixedLengthColumn<T>::create(), NullColumn::create());
    // two values and a NULL row
    NullInfos first = make_dict_null_infos({0, 1, 0});
    ASSERT_TRUE(decoder.next_batch_with_nulls(3, first, ColumnContentType::VALUE, dst.get(), nullptr).ok());
    const ColumnSnapshot before = take_snapshot(dst.get());

    // The page has two values left. The batches need three. One has a run of NULL rows first, so the base class
    // appends the NULL rows before it meets the end of the page. The others are filled in one pass, and the count
    // of runs is known for one of them and not for the other.
    const std::vector<std::vector<uint8_t>> batches = {{1, 1, 1, 0, 0, 0}, {0, 1, 0, 1, 0}, {1, 1, 1, 0, 0, 0}};
    for (size_t i = 0; i < batches.size(); ++i) {
        NullInfos null_infos = make_dict_null_infos(batches[i]);
        if (i == 2) {
            null_infos.num_ranges = 0;
        }
        Status st = decoder.next_batch_with_nulls(batches[i].size(), null_infos, ColumnContentType::VALUE, dst.get(),
                                                  nullptr);
        ASSERT_FALSE(st.ok()) << "batch " << i;
        ASSERT_NO_FATAL_FAILURE(expect_unchanged(dst.get(), before));
    }

    // The two values that are left can still be read.
    NullInfos last = make_dict_null_infos({0, 0});
    ASSERT_TRUE(decoder.next_batch_with_nulls(2, last, ColumnContentType::VALUE, dst.get(), nullptr).ok());
    const T* data = reinterpret_cast<const T*>(dst->data_column_raw_ptr()->raw_data());
    ASSERT_TRUE(values[2] == data[before.size]);
    ASSERT_TRUE(values[3] == data[before.size + 1]);
}

} // namespace

TEST_F(ParquetEncodingTest, PlainNumericBatchThatFailsLeavesTheColumnAsItWas) {
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_batch_rolls_back<int32_t>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_batch_rolls_back<int64_t>());
    ASSERT_NO_FATAL_FAILURE(check_plain_numeric_batch_rolls_back<double>());
}

TEST_F(ParquetEncodingTest, PlainBooleanBatchThatFailsLeavesTheColumnAsItWas) {
    // 8 bits in the page.
    std::vector<uint8_t> values(8);
    for (size_t i = 0; i < values.size(); ++i) {
        values[i] = plain_bool_value(i);
    }
    PlainEncoder<bool> encoder;
    ASSERT_TRUE(encoder.append(values.data(), values.size()).ok());
    PlainDecoder<bool> decoder;
    ASSERT_TRUE(decoder.set_data(encoder.build()).ok());

    auto dst = NullableColumn::create(FixedLengthColumn<uint8_t>::create(), NullColumn::create());
    NullInfos first = make_dict_null_infos({0, 1, 0});
    ASSERT_TRUE(decoder.next_batch_with_nulls(3, first, ColumnContentType::VALUE, dst.get(), nullptr).ok());
    const ColumnSnapshot before = take_snapshot(dst.get());

    // 6 bits are left. The first batch has a run of NULL rows and then 7 values, so the base class appends the NULL
    // rows before it meets the end of the page. The second one is filled in one pass.
    std::vector<uint8_t> leading_run = {1, 1, 1, 0, 0, 0, 0, 0, 0, 0};
    std::vector<uint8_t> alternating(15);
    for (size_t i = 0; i < alternating.size(); ++i) {
        alternating[i] = i % 2;
    }
    for (const auto& nulls : {leading_run, alternating}) {
        NullInfos null_infos = make_dict_null_infos(nulls);
        Status st =
                decoder.next_batch_with_nulls(nulls.size(), null_infos, ColumnContentType::VALUE, dst.get(), nullptr);
        ASSERT_FALSE(st.ok());
        ASSERT_NO_FATAL_FAILURE(expect_unchanged(dst.get(), before));
    }
    // A batch without NULL rows that needs 7 values.
    auto plain = FixedLengthColumn<uint8_t>::create();
    ASSERT_FALSE(decoder.next_batch(7, ColumnContentType::VALUE, plain.get(), nullptr).ok());
    ASSERT_EQ(0U, plain->size());
}

namespace {

constexpr size_t kNumDictValues = 5;

// The page of dictionary codes: the bit width, then the runs. The codes are one literal group of 8 values.
std::string make_literal_codes_page(const std::vector<int32_t>& codes, int bit_width) {
    faststring runs;
    RleEncoder<int32_t> encoder(&runs, bit_width);
    for (int32_t code : codes) {
        encoder.Put(code);
    }
    encoder.Flush();
    std::string page(1, static_cast<char>(bit_width));
    page.append(reinterpret_cast<const char*>(runs.data()), runs.length());
    return page;
}

// The page of dictionary codes with one repeated run: `count` times the code.
std::string make_repeated_codes_page(int32_t code, size_t count, int bit_width) {
    faststring runs;
    RleEncoder<int32_t> encoder(&runs, bit_width);
    encoder.Put(code, count);
    encoder.Flush();
    std::string page(1, static_cast<char>(bit_width));
    page.append(reinterpret_cast<const char*>(runs.data()), runs.length());
    return page;
}

// A dictionary decoder over 5 values. DictDecoder<int32_t> and DictDecoder<Slice> differ in the dictionary page.
template <typename T>
Status init_dict_decoder(DictDecoder<T>* decoder, const std::string& codes_page, std::string* dict_page_storage) {
    if constexpr (std::is_same_v<T, Slice>) {
        std::vector<std::string> values;
        std::vector<Slice> slices;
        for (size_t i = 0; i < kNumDictValues; ++i) {
            values.push_back("value" + std::to_string(i));
        }
        for (const auto& value : values) {
            slices.emplace_back(value);
        }
        PlainEncoder<Slice> encoder;
        RETURN_IF_ERROR(encoder.append(reinterpret_cast<const uint8_t*>(slices.data()), slices.size()));
        const Slice built = encoder.build();
        dict_page_storage->assign(built.data, built.size);
    } else {
        std::vector<int32_t> values;
        for (size_t i = 0; i < kNumDictValues; ++i) {
            values.push_back(static_cast<int32_t>(100 + i));
        }
        PlainEncoder<int32_t> encoder;
        RETURN_IF_ERROR(encoder.append(reinterpret_cast<const uint8_t*>(values.data()), values.size()));
        const Slice built = encoder.build();
        dict_page_storage->assign(built.data, built.size);
    }
    PlainDecoder<T> dict_values;
    RETURN_IF_ERROR(dict_values.set_data(Slice(dict_page_storage->data(), dict_page_storage->size())));
    RETURN_IF_ERROR(decoder->set_dict(8, kNumDictValues, &dict_values));
    return decoder->set_data(Slice(codes_page.data(), codes_page.size()));
}

// Reads a good batch of two values, then a batch that fails after the decoder resized the column: a code that the
// dictionary does not have, or a page with fewer codes than the batch needs. The column has to be as it was.
template <typename T>
void check_dict_batch_rolls_back(ColumnContentType content_type, bool nullable, bool with_nulls, bool use_filter,
                                 bool short_page) {
    SCOPED_TRACE("codes " + std::to_string(content_type == ColumnContentType::DICT_CODE) + ", nullable " +
                 std::to_string(nullable) + ", with nulls " + std::to_string(with_nulls) + ", filter " +
                 std::to_string(use_filter) + ", short page " + std::to_string(short_page));
    // The literal group holds 1, 2, 3 and then the code 9, which a dictionary of 5 values does not have.
    const std::string codes_page =
            short_page ? make_repeated_codes_page(1, 4, 3) : make_literal_codes_page({1, 2, 3, 9, 2, 1, 1, 1}, 4);
    DictDecoder<T> decoder;
    std::string dict_page;
    ASSERT_TRUE(init_dict_decoder(&decoder, codes_page, &dict_page).ok());
    // The filter is used for a dictionary of any size.
    decoder._dict_size_threshold = 0;

    // The codes are 32-bit numbers, and the values of the strings are strings.
    LogicalType column_type = TYPE_INT;
    if (content_type == ColumnContentType::VALUE && std::is_same_v<T, Slice>) {
        column_type = TYPE_VARCHAR;
    }
    MutableColumnPtr dst = ColumnHelper::create_column(TypeDescriptor(column_type), nullable);

    const bool has_nulls = with_nulls && nullable;
    auto read = [&](const std::vector<uint8_t>& nulls) {
        std::vector<uint8_t> filter(nulls.size());
        for (size_t i = 0; i < filter.size(); ++i) {
            filter[i] = i % 2 == 0;
        }
        const FilterData* filter_data = use_filter ? filter.data() : nullptr;
        if (with_nulls) {
            NullInfos null_infos = make_dict_null_infos(nulls);
            return decoder.next_batch_with_nulls(nulls.size(), null_infos, content_type, dst.get(), filter_data);
        }
        return decoder.next_batch(nulls.size(), content_type, dst.get(), filter_data);
    };

    // the codes 1 and 2
    ASSERT_TRUE(read(has_nulls ? std::vector<uint8_t>{1, 0, 0} : std::vector<uint8_t>{0, 0}).ok());
    const ColumnSnapshot before = take_snapshot(dst.get());

    // the codes 3 and 9, or 4 values of a page that has two left
    std::vector<uint8_t> nulls = has_nulls ? std::vector<uint8_t>{0, 1, 0} : std::vector<uint8_t>{0, 0};
    if (short_page) {
        nulls = has_nulls ? std::vector<uint8_t>{0, 1, 0, 1, 0, 0} : std::vector<uint8_t>{0, 0, 0, 0};
    }
    ASSERT_FALSE(read(nulls).ok());
    ASSERT_NO_FATAL_FAILURE(expect_unchanged(dst.get(), before));
}

} // namespace

TEST_F(ParquetEncodingTest, DictionaryBatchThatFailsInTheMiddleLeavesTheColumnAsItWas) {
    for (ColumnContentType content_type : {ColumnContentType::VALUE, ColumnContentType::DICT_CODE}) {
        for (bool nullable : {true, false}) {
            for (bool with_nulls : {false, true}) {
                for (bool use_filter : {false, true}) {
                    for (bool short_page : {false, true}) {
                        ASSERT_NO_FATAL_FAILURE(check_dict_batch_rolls_back<int32_t>(content_type, nullable, with_nulls,
                                                                                     use_filter, short_page));
                        ASSERT_NO_FATAL_FAILURE(check_dict_batch_rolls_back<Slice>(content_type, nullable, with_nulls,
                                                                                   use_filter, short_page));
                    }
                }
            }
        }
    }
}

namespace {

// Reads two values of a page, then a batch that asks for more values than the page has, and checks that the
// column is as it was and that the values that are left can be read. The decoders of these encodings check the
// count before they change the column, or cut the column back.
void check_count_beyond_page_leaves_column(tparquet::Type::type type, tparquet::Encoding::type encoding_type,
                                           const Slice& page, const std::vector<std::string>& expected_tail,
                                           int32_t type_length, Column* dst) {
    SCOPED_TRACE("type " + std::to_string(type) + ", encoding " + std::to_string(encoding_type));
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(type, encoding_type, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Decoder> decoder;
    ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
    ASSERT_TRUE(decoder->set_type_length(type_length).ok());
    ASSERT_TRUE(decoder->set_data(page).ok());
    ASSERT_TRUE(decoder->next_batch(2, ColumnContentType::VALUE, dst, nullptr).ok());
    const ColumnSnapshot before = take_snapshot(dst);

    ASSERT_FALSE(decoder->next_batch(expected_tail.size() + 1, ColumnContentType::VALUE, dst, nullptr).ok());
    ASSERT_NO_FATAL_FAILURE(expect_unchanged(dst, before));
    const size_t too_many = expected_tail.size() + 1;
    NullInfos null_infos = make_dict_null_infos(std::vector<uint8_t>(too_many, 0));
    ASSERT_FALSE(decoder->next_batch_with_nulls(too_many, null_infos, ColumnContentType::VALUE, dst, nullptr).ok());
    ASSERT_NO_FATAL_FAILURE(expect_unchanged(dst, before));

    ASSERT_TRUE(decoder->next_batch(expected_tail.size(), ColumnContentType::VALUE, dst, nullptr).ok());
    ASSERT_EQ(before.size + expected_tail.size(), dst->size());
    for (size_t i = 0; i < expected_tail.size(); ++i) {
        ASSERT_EQ(expected_tail[i], dst->debug_item(before.size + i)) << "tail " << i;
    }
}

} // namespace

// The decoders that check the count before they change the column.
TEST_F(ParquetEncodingTest, CountBeyondThePageLeavesTheColumnAsItWas) {
    // numbers
    std::vector<int32_t> int_values;
    for (size_t i = 0; i < 6; ++i) {
        int_values.push_back(static_cast<int32_t>(1000 + i * 7));
    }
    const std::vector<std::string> int_tail = {"1014", "1021", "1028", "1035"};
    for (auto encoding_type :
         {tparquet::Encoding::PLAIN, tparquet::Encoding::DELTA_BINARY_PACKED, tparquet::Encoding::BYTE_STREAM_SPLIT}) {
        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(tparquet::Type::INT32, encoding_type, &encoding);
        ASSERT_TRUE(encoding != nullptr);
        std::unique_ptr<Encoder> encoder;
        ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
        ASSERT_TRUE(encoder->append(reinterpret_cast<const uint8_t*>(int_values.data()), int_values.size()).ok());
        const Slice built = encoder->build();
        const std::vector<char> page(built.data, built.data + built.size);
        for (bool nullable : {true, false}) {
            MutableColumnPtr dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), nullable);
            ASSERT_NO_FATAL_FAILURE(check_count_beyond_page_leaves_column(
                    tparquet::Type::INT32, encoding_type, Slice(page.data(), page.size()), int_tail, 0, dst.get()));
        }
    }

    // strings
    std::vector<std::string> strings = {"red", "green", "blue", "yellow", "black", "white"};
    std::vector<Slice> slices;
    for (const auto& value : strings) {
        slices.emplace_back(value);
    }
    const std::vector<std::string> string_tail = {"'blue'", "'yellow'", "'black'", "'white'"};
    for (auto encoding_type : {tparquet::Encoding::PLAIN, tparquet::Encoding::DELTA_LENGTH_BYTE_ARRAY,
                               tparquet::Encoding::DELTA_BYTE_ARRAY}) {
        const EncodingInfo* encoding = nullptr;
        (void)EncodingInfo::get(tparquet::Type::BYTE_ARRAY, encoding_type, &encoding);
        ASSERT_TRUE(encoding != nullptr);
        std::unique_ptr<Encoder> encoder;
        ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
        ASSERT_TRUE(encoder->append(reinterpret_cast<const uint8_t*>(slices.data()), slices.size()).ok());
        const Slice built = encoder->build();
        const std::vector<char> page(built.data, built.data + built.size);
        for (bool nullable : {true, false}) {
            MutableColumnPtr dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), nullable);
            ASSERT_NO_FATAL_FAILURE(check_count_beyond_page_leaves_column(tparquet::Type::BYTE_ARRAY, encoding_type,
                                                                          Slice(page.data(), page.size()), string_tail,
                                                                          0, dst.get()));
        }
    }

    // fixed length strings of 4 bytes
    std::vector<std::string> flba_values = {"aaaa", "bbbb", "cccc", "dddd", "eeee", "ffff"};
    std::vector<Slice> flba_slices;
    for (const auto& value : flba_values) {
        flba_slices.emplace_back(value);
    }
    const std::vector<std::string> flba_tail = {"'cccc'", "'dddd'", "'eeee'", "'ffff'"};
    const EncodingInfo* flba = nullptr;
    (void)EncodingInfo::get(tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::PLAIN, &flba);
    ASSERT_TRUE(flba != nullptr);
    std::unique_ptr<Encoder> flba_encoder;
    ASSERT_TRUE(flba->create_encoder(&flba_encoder).ok());
    ASSERT_TRUE(flba_encoder->append(reinterpret_cast<const uint8_t*>(flba_slices.data()), flba_slices.size()).ok());
    const Slice flba_built = flba_encoder->build();
    const std::vector<char> flba_page(flba_built.data, flba_built.data + flba_built.size);
    for (bool nullable : {true, false}) {
        MutableColumnPtr dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), nullable);
        ASSERT_NO_FATAL_FAILURE(check_count_beyond_page_leaves_column(
                tparquet::Type::FIXED_LEN_BYTE_ARRAY, tparquet::Encoding::PLAIN,
                Slice(flba_page.data(), flba_page.size()), flba_tail, 4, dst.get()));
    }
}

// A delta page that ends inside the blocks fails after the decoder resized the column and decoded some of the
// rows, so the decoder cuts the column back.
TEST_F(ParquetEncodingTest, DeltaBinaryPackedBatchThatFailsInTheMiddleLeavesTheColumnAsItWas) {
    std::mt19937 gen(7);
    std::vector<int32_t> values(400);
    for (auto& value : values) {
        value = static_cast<int32_t>(gen() >> 1);
    }
    const EncodingInfo* encoding = nullptr;
    (void)EncodingInfo::get(tparquet::Type::INT32, tparquet::Encoding::DELTA_BINARY_PACKED, &encoding);
    ASSERT_TRUE(encoding != nullptr);
    std::unique_ptr<Encoder> encoder;
    ASSERT_TRUE(encoding->create_encoder(&encoder).ok());
    ASSERT_TRUE(encoder->append(reinterpret_cast<const uint8_t*>(values.data()), values.size()).ok());
    const Slice built = encoder->build();
    // The first block and a part of the second one.
    const std::vector<char> page(built.data, built.data + built.size / 2);

    for (bool nullable : {true, false}) {
        std::unique_ptr<Decoder> decoder;
        ASSERT_TRUE(encoding->create_decoder(&decoder).ok());
        ASSERT_TRUE(decoder->set_data(Slice(page.data(), page.size())).ok());
        MutableColumnPtr dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), nullable);
        ASSERT_TRUE(decoder->next_batch(10, ColumnContentType::VALUE, dst.get(), nullptr).ok());
        const ColumnSnapshot before = take_snapshot(dst.get());
        ASSERT_FALSE(decoder->next_batch(390, ColumnContentType::VALUE, dst.get(), nullptr).ok());
        ASSERT_NO_FATAL_FAILURE(expect_unchanged(dst.get(), before));
    }
}

} // namespace starrocks::parquet
