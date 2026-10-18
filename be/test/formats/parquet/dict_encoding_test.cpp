#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "formats/parquet/encoding.h"
#include "formats/parquet/encoding_dict.h"
#include "formats/parquet/encoding_plain.h"
#include "formats/parquet/utils.h"
#include "runtime/mem_pool.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "types/logical_type.h"
#include "util/slice.h"

namespace starrocks::parquet {
template <LogicalType LT>
class FakeDictDecoder final : public Decoder {
public:
    Status set_data(const Slice& data) override { throw std::runtime_error("not supported function set_data"); }
    Status skip(size_t values_to_skip) override { throw std::runtime_error("not supported skip"); }
    Status next_batch(size_t count, ColumnContentType content_type, Column* dst,
                      const FilterData* filter = nullptr) override {
        throw std::runtime_error("not supported skip");
    }
    Status next_batch(size_t count, uint8_t* dst) override {
        using RT = RunTimeCppType<LT>;
        auto* spec_dst = reinterpret_cast<RT*>(dst);
        for (size_t i = 0; i < count; ++i) {
            spec_dst[i] = i;
        }
        return Status::OK();
    }
};

template <>
class FakeDictDecoder<TYPE_VARCHAR> final : public Decoder {
public:
    Status set_data(const Slice& data) override { throw std::runtime_error("not supported function set_data"); }
    Status skip(size_t values_to_skip) override { throw std::runtime_error("not supported skip"); }
    Status next_batch(size_t count, ColumnContentType content_type, Column* dst,
                      const FilterData* filter = nullptr) override {
        throw std::runtime_error("not supported skip");
    }
    Status next_batch(size_t count, uint8_t* dst) override {
        auto* spec_dst = reinterpret_cast<Slice*>(dst);
        for (size_t i = 0; i < count; ++i) {
            auto data = std::to_string(i);
            Slice slice = Slice(_pool.allocate(data.size()), data.size());
            memcpy(slice.data, data.data(), data.size());
            spec_dst[i] = slice;
        }
        return Status::OK();
    }

private:
    MemPool _pool;
};

static Slice unquote(Slice slice) {
    if ((slice.starts_with("\"") && slice.ends_with("\"")) || (slice.starts_with("'") && slice.ends_with("'"))) {
        slice.remove_prefix(1);
        slice.remove_suffix(1);
    }
    return slice;
}

#define EXPECTED_UNQUOTE(lhs, rhs) EXPECT_EQ(unquote(lhs), rhs)

template <LogicalType DICT_TYPE, LogicalType TARGET_TYPE>
void dict_encoding_test() {
    using DICT_CXX_TYPE = RunTimeCppType<DICT_TYPE>;
    using TARGET_CXX_TYPE = RunTimeCppType<TARGET_TYPE>;
    faststring fs;
    RleEncoder<DICT_CXX_TYPE> encoder(&fs, 32);
    {
        for (size_t i = 0; i < 4096; ++i) {
            encoder.Put(i % 9 + 1, 10);
        }
    }

    DictDecoder<TARGET_CXX_TYPE> decoder;
    FakeDictDecoder<TARGET_TYPE> inner_decoder;
    faststring fs2;
    fs2.resize(fs.length() + 1);
    fs2.data()[0] = 32;
    memcpy(fs2.data() + 1, fs.data(), fs.length());
    ASSERT_OK(decoder.set_data(Slice(fs2.data(), fs2.length())));
    ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

    // read dict code
    size_t chunk_size = 4095;
    NullInfos infos;
    infos.reset_with_capacity(chunk_size);
    {
        // interleave
        infos.num_nulls = 0;
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = i % 2;
            infos.num_nulls += infos.nulls_data()[i];
        }
        infos.num_ranges = chunk_size / 2;
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        EXPECTED_UNQUOTE(dst->debug_item(0), "1");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "1");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(),
                                                filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "7");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "7");
        EXPECT_EQ(dst->size(), chunk_size);
    }

    {
        // sparse
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = 1;
        }
        infos.nulls_data()[0] = 0;
        infos.nulls_data()[1000] = 0;
        infos.nulls_data()[2000] = 0;
        infos.nulls_data()[3000] = 0;
        infos.nulls_data()[4000] = 0;

        infos.num_nulls = chunk_size - 5;
        infos.num_ranges = chunk_size / 2;
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(DICT_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        EXPECTED_UNQUOTE(dst->debug_item(0), "5");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        TypeDescriptor type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "6");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        decoder._dict_size_threshold = 0;
        TypeDescriptor type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x01, chunk_size);
        filter[1] = 0;
        filter[1000] = 0;
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "6");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2000), "6");
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        // all filtered
        decoder._dict_size_threshold = 0;
        TypeDescriptor type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        memset(filter.get(), 0x00, chunk_size);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECT_EQ(dst->size(), chunk_size);
    }
    {
        // all null
        for (size_t i = 0; i < chunk_size; ++i) {
            infos.nulls_data()[i] = 1;
        }
        infos.num_nulls = chunk_size;
        TypeDescriptor type_desc = TypeDescriptor(TARGET_TYPE);
        auto dst = ColumnHelper::create_column(type_desc, true);
        auto filter = std::make_unique<uint8_t[]>(chunk_size);
        ASSERT_OK(decoder.next_batch_with_nulls(chunk_size, infos, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECTED_UNQUOTE(dst->debug_item(0), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(1), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(2), "NULL");
        EXPECT_EQ(dst->size(), chunk_size);
    }
}

TEST(DictEncodingReadTest, BasicTest) {
    constexpr LogicalType TARGET_TYPE = LogicalType::TYPE_INT;
    constexpr LogicalType DICT_TYPE = LogicalType::TYPE_INT;
    dict_encoding_test<DICT_TYPE, TARGET_TYPE>();
}

TEST(DictEncodingReadTest, BinaryPageTest) {
    constexpr LogicalType TARGET_TYPE = LogicalType::TYPE_VARCHAR;
    constexpr LogicalType DICT_TYPE = LogicalType::TYPE_INT;
    dict_encoding_test<DICT_TYPE, TARGET_TYPE>();
}

// Build a ready-to-read DictDecoder<Slice> backed by an int-keyed dictionary, mirroring the setup
// in dict_encoding_test().
static void setup_slice_dict_decoder(DictDecoder<Slice>* decoder, FakeDictDecoder<TYPE_VARCHAR>* inner_decoder,
                                     faststring* backing) {
    faststring fs;
    RleEncoder<int32_t> encoder(&fs, 32);
    for (size_t i = 0; i < 4096; ++i) {
        encoder.Put(i % 9 + 1, 10);
    }
    backing->resize(fs.length() + 1);
    backing->data()[0] = 32;
    memcpy(backing->data() + 1, fs.data(), fs.length());
    ASSERT_OK(decoder->set_data(Slice(backing->data(), backing->length())));
    ASSERT_OK(decoder->set_dict(10, 10, inner_decoder));
}

// DictDecoder<Slice> must reject a non-binary destination column instead of blindly down-casting it
// to BinaryColumn. This guards the path that could otherwise let a temporary Int32 dict-code column
// reach a string slot. Exercises both the filtered _next_batch_value() path and the
// next_value_batch_with_nulls() path.
TEST(DictEncodingReadTest, BinaryDestinationTypeGuard) {
    constexpr size_t count = 100;

    // 1. _next_batch_value() with a filter: a binary destination succeeds.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        filter[0] = 0;
        ASSERT_OK(decoder.next_batch(count, ColumnContentType::VALUE, dst.get(), filter.get()));
        EXPECT_EQ(dst.get()->size(), count);
    }

    // 2. _next_batch_value() with a filter: a non-binary destination is rejected.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        auto st = decoder.next_batch(count, ColumnContentType::VALUE, dst.get(), filter.get());
        ASSERT_FALSE(st.ok());
    }

    // 3. next_value_batch_with_nulls(): a non-binary destination is rejected.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        NullInfos infos;
        infos.reset_with_capacity(count);
        infos.num_nulls = 0;
        for (size_t i = 0; i < count; ++i) {
            infos.nulls_data()[i] = i % 2;
            infos.num_nulls += infos.nulls_data()[i];
        }
        // num_ranges > 2 routes to next_value_batch_with_nulls() rather than the row-by-row fallback.
        infos.num_ranges = count / 2;

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        auto st = decoder.next_batch_with_nulls(count, infos, ColumnContentType::VALUE, dst.get(), filter.get());
        ASSERT_FALSE(st.ok());
    }
}

// A dictionary-encoded data page: one bit-width byte, then RLE/bit-packed runs. A single
// repeated run covers every value, so the page is a handful of bytes however many values it
// claims -- which is what keeps a multi-million-value test cheap. (Building the same page with
// RleEncoder::Put would not: it costs seconds per 100k values.)
static void build_repeated_run_page(faststring* page, uint32_t code, size_t num_values, int bit_width = 32) {
    auto put_byte = [page](uint32_t b) {
        auto byte = static_cast<uint8_t>(b);
        page->append(&byte, 1);
    };

    put_byte(bit_width);
    // Run indicator, ULEB128 of (num_values << 1); lsb 0 marks a repeated run.
    uint64_t indicator = static_cast<uint64_t>(num_values) << 1;
    for (; indicator >= 0x80; indicator >>= 7) {
        put_byte((indicator & 0x7f) | 0x80);
    }
    put_byte(indicator);
    // The repeated value, ceil(bit_width / 8) bytes, little endian.
    for (int i = 0; i < (bit_width + 7) / 8; ++i) {
        put_byte(code >> (8 * i));
    }
}

// The batch size handed to a decoder is not the chunk size: StoredColumnReaderImpl::_read() passes
// everything left in the current data page in one call, so a page holding millions of values
// produces a batch that big. Scratch space sized by that batch must live on the heap; the VLAs
// these paths used to declare killed the BE with a SIGSEGV raised inside the decoder.
template <LogicalType DICT_TYPE, LogicalType TARGET_TYPE>
static void large_batch_with_nulls_test() {
    using TARGET_CXX_TYPE = RunTimeCppType<TARGET_TYPE>;
    // 2^21 values: the removed VLAs wanted 8MB for the dict codes and the fixed-width values,
    // and 24MB for the slices, against the 8MB stack a scan thread has.
    constexpr size_t kCount = 1 << 21;
    constexpr size_t kNullStride = 8;
    constexpr uint32_t kCode = 3;

    faststring page;
    build_repeated_run_page(&page, kCode, kCount);

    NullInfos infos;
    infos.reset_with_capacity(kCount);
    infos.num_nulls = 0;
    for (size_t i = 0; i < kCount; ++i) {
        infos.nulls_data()[i] = (i % kNullStride) == 0;
        infos.num_nulls += infos.nulls_data()[i];
    }
    // num_ranges > 2 routes to the batched paths rather than the row-by-row fallback.
    infos.num_ranges = kCount / 2;

    // The dictionary holds "0".."9", every code in the page is kCode, and FakeDictDecoder makes
    // dictionary entry i render as "i" for both the int and the string dictionary.
    auto check = [&](const auto& dst) {
        EXPECT_EQ(dst->size(), kCount);
        EXPECTED_UNQUOTE(dst->debug_item(0), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(kNullStride), "NULL");
        EXPECTED_UNQUOTE(dst->debug_item(1), "3");
        EXPECTED_UNQUOTE(dst->debug_item(kCount - 1), "3");
    };

    {
        // dict codes
        DictDecoder<TARGET_CXX_TYPE> decoder;
        FakeDictDecoder<TARGET_TYPE> inner_decoder;
        ASSERT_OK(decoder.set_data(Slice(page.data(), page.length())));
        ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

        auto dst = ColumnHelper::create_column(TypeDescriptor(DICT_TYPE), true);
        ASSERT_OK(decoder.next_batch_with_nulls(kCount, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr));
        check(dst);
    }
    {
        // values
        DictDecoder<TARGET_CXX_TYPE> decoder;
        FakeDictDecoder<TARGET_TYPE> inner_decoder;
        ASSERT_OK(decoder.set_data(Slice(page.data(), page.length())));
        ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

        auto dst = ColumnHelper::create_column(TypeDescriptor(TARGET_TYPE), true);
        ASSERT_OK(decoder.next_batch_with_nulls(kCount, infos, ColumnContentType::VALUE, dst.get(), nullptr));
        check(dst);
    }
}

TEST(DictEncodingReadTest, LargeBatchWithNulls) {
    large_batch_with_nulls_test<LogicalType::TYPE_INT, LogicalType::TYPE_INT>();
    large_batch_with_nulls_test<LogicalType::TYPE_INT, LogicalType::TYPE_VARCHAR>();
}

// A row that a pushed-down filter excludes is never written, so it contributes no bytes to the
// BinaryColumn. It must therefore come back NULL, keeping the layout invariant that a non-NULL row
// occupies exactly the value width and a NULL row occupies nothing. Leaving such a row NOT NULL
// creates a third state that consumers walking FIXED_LEN_BYTE_ARRAY bytes with a fixed stride --
// BinaryToDecimalConverter does -- cannot represent: they consume the value width for it anyway,
// decode every following row (including the rows that survive the filter) from a misaligned offset,
// and read past the end of the buffer.
TEST(DictEncodingReadTest, FilterExcludedRowsAreMarkedNull) {
    constexpr size_t count = 100;
    // FakeDictDecoder<TYPE_VARCHAR> builds the dictionary from std::to_string(i), i in [0, 10),
    // so every dictionary value is exactly one byte wide.
    constexpr size_t kValueLen = 1;

    // 1. _next_batch_value(): no NULLs of its own, only filter-excluded rows.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        const std::vector<size_t> excluded = {3, 50, 99};
        for (size_t i : excluded) {
            filter[i] = 0;
        }
        ASSERT_OK(decoder.next_batch(count, ColumnContentType::VALUE, dst.get(), filter.get()));

        auto* nullable = down_cast<NullableColumn*>(dst.get());
        ASSERT_EQ(count, nullable->size());
        EXPECT_TRUE(nullable->has_null());
        for (size_t i = 0; i < count; ++i) {
            const bool is_excluded = std::find(excluded.begin(), excluded.end(), i) != excluded.end();
            EXPECT_EQ(is_excluded, nullable->is_null(i)) << "row " << i;
        }
        // The rows that survive still decode: rows [0, 10) all map to dictionary index 1.
        EXPECTED_UNQUOTE(dst->debug_item(0), "1");
        EXPECTED_UNQUOTE(dst->debug_item(4), "1");

        // The invariant itself: only the surviving rows occupy bytes.
        auto* binary = down_cast<BinaryColumn*>(nullable->data_column_raw_ptr());
        EXPECT_EQ((count - excluded.size()) * kValueLen, binary->get_bytes().size());
        EXPECT_EQ(binary->get_bytes().size(), binary->get_offset().back());
    }

    // 2. next_value_batch_with_nulls(): real NULLs and filter-excluded rows together.
    {
        DictDecoder<Slice> decoder;
        FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
        faststring backing;
        setup_slice_dict_decoder(&decoder, &inner_decoder, &backing);
        // next_batch_with_nulls() has no BE_TEST bypass, so drop the threshold to force the
        // filtered path regardless of how small the test dictionary is.
        decoder._dict_size_threshold = 0;

        NullInfos infos;
        infos.reset_with_capacity(count);
        infos.num_nulls = 0;
        for (size_t i = 0; i < count; ++i) {
            infos.nulls_data()[i] = (i % 10 == 0);
            infos.num_nulls += infos.nulls_data()[i];
        }
        // num_ranges > 1 routes to next_value_batch_with_nulls() rather than the row-by-row fallback.
        infos.num_ranges = count / 2;

        auto filter = std::make_unique<uint8_t[]>(count);
        memset(filter.get(), 0x01, count);
        // Two non-NULL rows, plus one row that is already NULL: the latter must not be counted twice.
        const std::vector<size_t> excluded = {3, 57};
        for (size_t i : excluded) {
            filter[i] = 0;
        }
        filter[10] = 0;

        auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_VARCHAR), true);
        ASSERT_OK(decoder.next_batch_with_nulls(count, infos, ColumnContentType::VALUE, dst.get(), filter.get()));

        auto* nullable = down_cast<NullableColumn*>(dst.get());
        ASSERT_EQ(count, nullable->size());
        EXPECT_TRUE(nullable->has_null());
        for (size_t i = 0; i < count; ++i) {
            const bool is_excluded = std::find(excluded.begin(), excluded.end(), i) != excluded.end();
            EXPECT_EQ(infos.nulls_data()[i] != 0 || is_excluded, nullable->is_null(i)) << "row " << i;
        }

        auto* binary = down_cast<BinaryColumn*>(nullable->data_column_raw_ptr());
        const size_t surviving = count - infos.num_nulls - excluded.size();
        EXPECT_EQ(surviving * kValueLen, binary->get_bytes().size());
        EXPECT_EQ(binary->get_bytes().size(), binary->get_offset().back());
    }
}

// The number of values in a dictionary page comes from its header. A corrupt header can claim far more
// values than the page holds, and set_dict must then return the error of the decoder of the page without
// allocating memory for the claim.
TEST(DictEncodingReadTest, SetDictWithMoreValuesThanThePageHolds) {
    const std::vector<size_t> claims = {9, size_t(1) << 20, size_t(1) << 40, std::numeric_limits<size_t>::max()};

    {
        std::vector<int32_t> values(8);
        std::iota(values.begin(), values.end(), 0);
        PlainEncoder<int32_t> encoder;
        ASSERT_OK(encoder.append(reinterpret_cast<const uint8_t*>(values.data()), values.size()));
        Slice page = encoder.build();

        for (size_t claim : claims) {
            PlainDecoder<int32_t> plain;
            ASSERT_OK(plain.set_data(page));
            DictDecoder<int32_t> decoder;
            Status st = decoder.set_dict(10, claim, &plain);
            ASSERT_FALSE(st.ok()) << claim;
            ASSERT_LE(decoder._dict.capacity(), 2 * DictDecoder<int32_t>::kDictDecodeStep) << claim;
        }
    }
    {
        std::vector<std::string> strings = {"a", "bc", "def"};
        std::vector<Slice> slices(strings.begin(), strings.end());
        PlainEncoder<Slice> encoder;
        ASSERT_OK(encoder.append(reinterpret_cast<const uint8_t*>(slices.data()), slices.size()));
        Slice page = encoder.build();

        for (size_t claim : claims) {
            PlainDecoder<Slice> plain;
            ASSERT_OK(plain.set_data(page));
            DictDecoder<Slice> decoder;
            Status st = decoder.set_dict(10, claim, &plain);
            ASSERT_FALSE(st.ok()) << claim;
            ASSERT_LE(decoder._dict_data.capacity(), size_t(1) << 20) << claim;
        }
    }
}

// A dictionary that is longer than one step of set_dict, and an empty one.
TEST(DictEncodingReadTest, SetDictInSeveralSteps) {
    {
        const size_t num_values = 3 * DictDecoder<int32_t>::kDictDecodeStep + 5;
        std::vector<int32_t> values(num_values);
        for (size_t i = 0; i < num_values; ++i) {
            values[i] = static_cast<int32_t>(i * 3);
        }
        PlainEncoder<int32_t> encoder;
        ASSERT_OK(encoder.append(reinterpret_cast<const uint8_t*>(values.data()), values.size()));

        PlainDecoder<int32_t> plain;
        ASSERT_OK(plain.set_data(encoder.build()));
        DictDecoder<int32_t> decoder;
        ASSERT_OK(decoder.set_dict(10, num_values, &plain));
        ASSERT_EQ(num_values, decoder._dict.size());
        ASSERT_EQ(0, memcmp(values.data(), decoder._dict.data(), num_values * sizeof(int32_t)));
    }
    {
        const size_t num_values = 2 * DictDecoder<Slice>::kDictDecodeStep + 7;
        std::vector<std::string> strings;
        for (size_t i = 0; i < num_values; ++i) {
            strings.push_back("v" + std::to_string(i));
        }
        std::vector<Slice> slices(strings.begin(), strings.end());
        PlainEncoder<Slice> encoder;
        ASSERT_OK(encoder.append(reinterpret_cast<const uint8_t*>(slices.data()), slices.size()));

        PlainDecoder<Slice> plain;
        ASSERT_OK(plain.set_data(encoder.build()));
        DictDecoder<Slice> decoder;
        ASSERT_OK(decoder.set_dict(10, num_values, &plain));

        auto column = BinaryColumn::create();
        ASSERT_OK(decoder.get_dict_values(column.get()));
        ASSERT_EQ(num_values, column->size());
        for (size_t i = 0; i < num_values; ++i) {
            ASSERT_EQ(strings[i], column->get_slice(i).to_string()) << i;
        }
    }
    {
        PlainEncoder<int32_t> int_encoder;
        PlainDecoder<int32_t> int_plain;
        ASSERT_OK(int_plain.set_data(int_encoder.build()));
        DictDecoder<int32_t> int_decoder;
        ASSERT_OK(int_decoder.set_dict(10, 0, &int_plain));
        ASSERT_EQ(0U, int_decoder._dict.size());

        PlainEncoder<Slice> slice_encoder;
        PlainDecoder<Slice> slice_plain;
        ASSERT_OK(slice_plain.set_data(slice_encoder.build()));
        DictDecoder<Slice> slice_decoder;
        ASSERT_OK(slice_decoder.set_dict(10, 0, &slice_plain));
        ASSERT_EQ(0U, slice_decoder._dict.size());
    }
}

// The consumers of dictionary codes index the dictionary with them. A code that the dictionary does not
// have must come back as an error when the codes are read, whether they are read with or without NULLs.
template <typename DecoderType, LogicalType DICT_VALUE_TYPE>
static void dict_code_out_of_bounds_test() {
    constexpr size_t kCount = 100;
    // FakeDictDecoder makes a dictionary of 10 values, so the codes 0..9 are the valid ones.
    const std::vector<std::pair<uint32_t, bool>> codes = {{0, true}, {9, true}, {10, false}, {0xffffffffU, false}};

    NullInfos infos;
    infos.reset_with_capacity(kCount);
    infos.num_nulls = 0;
    for (size_t i = 0; i < kCount; ++i) {
        infos.nulls_data()[i] = (i % 4) == 0;
        infos.num_nulls += infos.nulls_data()[i];
    }
    // num_ranges > 2 routes to next_dict_code_batch_with_nulls() rather than the row-by-row fallback.
    infos.num_ranges = kCount / 2;

    for (const auto& [code, valid] : codes) {
        faststring page;
        build_repeated_run_page(&page, code, kCount);
        {
            DecoderType decoder;
            FakeDictDecoder<DICT_VALUE_TYPE> inner_decoder;
            ASSERT_OK(decoder.set_data(Slice(page.data(), page.length())));
            ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));
            auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
            Status st = decoder.next_batch(kCount, ColumnContentType::DICT_CODE, dst.get(), nullptr);
            ASSERT_EQ(valid, st.ok()) << code << " " << st.to_string();
            if (!valid) {
                ASSERT_TRUE(st.is_corruption()) << st.to_string();
            }
        }
        {
            DecoderType decoder;
            FakeDictDecoder<DICT_VALUE_TYPE> inner_decoder;
            ASSERT_OK(decoder.set_data(Slice(page.data(), page.length())));
            ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));
            auto dst = ColumnHelper::create_column(TypeDescriptor(TYPE_INT), true);
            Status st = decoder.next_batch_with_nulls(kCount, infos, ColumnContentType::DICT_CODE, dst.get(), nullptr);
            ASSERT_EQ(valid, st.ok()) << code << " " << st.to_string();
            if (!valid) {
                ASSERT_TRUE(st.is_corruption()) << st.to_string();
            }
        }
    }
}

TEST(DictEncodingReadTest, DictCodeOutOfDictionaryBounds) {
    dict_code_out_of_bounds_test<DictDecoder<int32_t>, TYPE_INT>();
    dict_code_out_of_bounds_test<DictDecoder<Slice>, TYPE_VARCHAR>();
}

// get_dict_values looks the dictionary up with the codes it is given. A NULL row has no code (its slot is
// not set), so it must not be looked up, and it must hold no byte in the column: the bytes of the rows that
// are not NULL have to start where the previous row ended, as they do when the values are read directly.
TEST(DictEncodingReadTest, GetDictValuesSkipsNullRows) {
    DictDecoder<Slice> decoder;
    FakeDictDecoder<TYPE_VARCHAR> inner_decoder;
    ASSERT_OK(decoder.set_dict(10, 10, &inner_decoder));

    auto make_nulls = [](const Buffer<int32_t>& codes, const std::vector<uint8_t>& is_null) {
        auto data = Int32Column::create();
        auto null_column = NullColumn::create();
        for (size_t i = 0; i < codes.size(); ++i) {
            data->append(codes[i]);
            null_column->append(is_null[i]);
        }
        auto nullable = NullableColumn::create(std::move(data), std::move(null_column));
        nullable->update_has_null();
        return nullable;
    };

    // The codes in the slots of the NULL rows (0 and 3) are garbage, and one is not even in the dictionary.
    {
        Buffer<int32_t> codes = {7777, 3, 5, -1, 9};
        auto nulls = make_nulls(codes, {1, 0, 0, 1, 0});
        auto dst = BinaryColumn::create();
        ASSERT_OK(decoder.get_dict_values(codes, *nulls, dst.get()));
        ASSERT_EQ(5U, dst->size());
        ASSERT_EQ(3U, dst->get_bytes().size());
        const std::vector<std::string> expected = {"", "3", "5", "", "9"};
        for (size_t i = 0; i < expected.size(); ++i) {
            ASSERT_EQ(expected[i], dst->get_slice(i).to_string()) << i;
        }
    }
    // a code of a row that is not NULL must be in the dictionary
    for (int32_t bad_code : {10, -1, std::numeric_limits<int32_t>::max()}) {
        Buffer<int32_t> codes = {7777, 3, bad_code, -1, 9};
        auto nulls = make_nulls(codes, {1, 0, 0, 1, 0});
        auto dst = BinaryColumn::create();
        Status st = decoder.get_dict_values(codes, *nulls, dst.get());
        ASSERT_TRUE(st.is_corruption()) << bad_code << " " << st.to_string();

        // the same without a NULL row
        Buffer<int32_t> codes_without_null = {3, bad_code};
        auto nulls_without_null = make_nulls(codes_without_null, {0, 0});
        st = decoder.get_dict_values(codes_without_null, *nulls_without_null, dst.get());
        ASSERT_TRUE(st.is_corruption()) << bad_code << " " << st.to_string();
    }
    // a dictionary of no value cannot serve a row that is not NULL
    {
        DictDecoder<Slice> empty_decoder;
        FakeDictDecoder<TYPE_VARCHAR> empty_inner;
        ASSERT_OK(empty_decoder.set_dict(10, 0, &empty_inner));
        Buffer<int32_t> codes = {0, 0};
        auto nulls = make_nulls(codes, {1, 0});
        auto dst = BinaryColumn::create();
        ASSERT_TRUE(empty_decoder.get_dict_values(codes, *nulls, dst.get()).is_corruption());
    }
}

} // namespace starrocks::parquet
