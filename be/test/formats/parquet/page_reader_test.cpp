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

#include "formats/parquet/page_reader.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "cache/cache_options.h"
#include "cache/datacache.h"
#include "cache/mem_cache/lrucache_engine.h"
#include "cache/mem_cache/page_cache.h"
#include "column/column_helper.h"
#include "common/config.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/page_cache_io_planner.h"
#include "formats/parquet/parquet_test_util/recording_input_stream.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/stored_column_reader.h"
#include "fs/fs_memory.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "io/string_input_stream.h"
#include "runtime/types.h"
#include "testutil/assert.h"
#include "util/thrift_util.h"
namespace starrocks::parquet {

class ParquetPageReaderTest : public testing::Test {
public:
    ParquetPageReaderTest() = default;
    ~ParquetPageReaderTest() override = default;
};

TEST_F(ParquetPageReaderTest, Normal) {
    std::string buffer;
    HdfsScannerStats stats;

    // page 0
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 100;
        page_header.compressed_page_size = 100;
        page_header.data_page_header.num_values = 10;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t page_1_size = buffer.size();
    // page 1
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 200;
        page_header.compressed_page_size = 300;
        page_header.data_page_header.num_values = 20;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    io::SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    ColumnReaderOptions opts;
    opts.stats = &stats;
    PageReader reader(&stream, 0, total_size, 30, opts, tparquet::CompressionCodec::ZSTD);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    // read out-of-page
    reader.seek_to_offset(total_size);
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

TEST_F(ParquetPageReaderTest, ExtraBytes) {
    std::string buffer;
    HdfsScannerStats stats;

    // page 0
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 100;
        page_header.compressed_page_size = 100;
        page_header.data_page_header.num_values = 10;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }

    size_t page_1_size = buffer.size();
    // page 1
    {
        tparquet::PageHeader page_header;
        page_header.type = tparquet::PageType::DATA_PAGE;
        page_header.uncompressed_page_size = 200;
        page_header.compressed_page_size = 300;
        page_header.data_page_header.num_values = 20;

        ThriftSerializer ser(true, 100);
        uint32_t len = 0;
        uint8_t* header_ser = nullptr;
        CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
        buffer.append((char*)header_ser, len);

        buffer.resize(buffer.size() + page_header.compressed_page_size);
    }
    size_t page_2_size = buffer.size();

    size_t extra_nbytes = 10;
    buffer.resize(buffer.size() + extra_nbytes);
    size_t total_size = buffer.size();

    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");

    io::SharedBufferedInputStream stream(file.stream(), file.filename(), file.get_size().value());

    ColumnReaderOptions opts;
    opts.stats = &stats;
    PageReader reader(&stream, 0, total_size, 30, opts, tparquet::CompressionCodec::ZSTD);

    // read page 1
    auto st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(100, reader.current_header()->uncompressed_page_size);

    // read page 2
    reader.seek_to_offset(page_1_size);
    st = reader.next_header();
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(200, reader.current_header()->uncompressed_page_size);

    // read out-of-page
    ASSERT_TRUE(reader.seek_to_offset(page_2_size).ok());
    st = reader.next_header();
    ASSERT_FALSE(st.ok());
}

namespace {

// A page is the serialized header followed by `body_size` bytes.
std::string make_page(const tparquet::PageHeader& page_header, size_t body_size) {
    ThriftSerializer ser(true, 100);
    uint32_t len = 0;
    uint8_t* header_ser = nullptr;
    CHECK(ser.serialize(&page_header, &len, &header_ser).ok());
    std::string page(reinterpret_cast<char*>(header_ser), len);
    page.resize(page.size() + body_size);
    return page;
}

tparquet::PageHeader make_data_page_header(int32_t num_values) {
    tparquet::DataPageHeader data_page_header;
    data_page_header.num_values = num_values;
    data_page_header.encoding = tparquet::Encoding::PLAIN;
    data_page_header.definition_level_encoding = tparquet::Encoding::RLE;
    data_page_header.repetition_level_encoding = tparquet::Encoding::RLE;

    tparquet::PageHeader page_header;
    page_header.type = tparquet::PageType::DATA_PAGE;
    page_header.uncompressed_page_size = 64;
    page_header.compressed_page_size = 64;
    page_header.__set_data_page_header(data_page_header);
    return page_header;
}

tparquet::PageHeader make_data_page_v2_header(int32_t num_values, int32_t def_levels_length,
                                              int32_t rep_levels_length) {
    tparquet::DataPageHeaderV2 data_page_header;
    data_page_header.num_values = num_values;
    data_page_header.num_nulls = 0;
    data_page_header.num_rows = num_values;
    data_page_header.encoding = tparquet::Encoding::PLAIN;
    data_page_header.definition_levels_byte_length = def_levels_length;
    data_page_header.repetition_levels_byte_length = rep_levels_length;

    tparquet::PageHeader page_header;
    page_header.type = tparquet::PageType::DATA_PAGE_V2;
    page_header.uncompressed_page_size = 64;
    page_header.compressed_page_size = 64;
    page_header.__set_data_page_header_v2(data_page_header);
    return page_header;
}

tparquet::PageHeader make_dictionary_page_header(int32_t num_values, int32_t page_size = 64,
                                                 tparquet::Encoding::type encoding = tparquet::Encoding::PLAIN) {
    tparquet::DictionaryPageHeader dictionary_page_header;
    dictionary_page_header.num_values = num_values;
    dictionary_page_header.encoding = encoding;

    tparquet::PageHeader page_header;
    page_header.type = tparquet::PageType::DICTIONARY_PAGE;
    page_header.uncompressed_page_size = page_size;
    page_header.compressed_page_size = page_size;
    page_header.__set_dictionary_page_header(dictionary_page_header);
    return page_header;
}

// Holds a page in memory and reads it with a PageReader.
class PageFixture {
public:
    PageFixture(std::string buffer, tparquet::CompressionCodec::type codec)
            : _file(std::make_shared<io::StringInputStream>(std::string(buffer)), "string-file"),
              _stream(_file.stream(), _file.filename(), _file.get_size().value()) {
        _opts.stats = &_stats;
        _opts.file_meta_data = &_file_meta_data;
        _reader = std::make_unique<PageReader>(&_stream, 0, buffer.size(), 10, _opts, codec);
    }

    PageReader* reader() { return _reader.get(); }

private:
    HdfsScannerStats _stats;
    FileMetaData _file_meta_data;
    RandomAccessFile _file;
    io::SharedBufferedInputStream _stream;
    ColumnReaderOptions _opts;
    std::unique_ptr<PageReader> _reader;
};

} // namespace

// The sizes and counts of a page header are signed in the file but we use them as sizes. A negative
// one must come back as an error from the header, before any buffer is sized with it.
TEST_F(ParquetPageReaderTest, NegativePageSize) {
    for (auto sizes : std::vector<std::pair<int32_t, int32_t>>{{-1, 64},
                                                               {64, -1},
                                                               {std::numeric_limits<int32_t>::min(), 64},
                                                               {64, std::numeric_limits<int32_t>::min()}}) {
        auto page_header = make_data_page_header(10);
        page_header.compressed_page_size = sizes.first;
        page_header.uncompressed_page_size = sizes.second;
        PageFixture fixture(make_page(page_header, 64), tparquet::CompressionCodec::ZSTD);
        Status st = fixture.reader()->next_header();
        ASSERT_TRUE(st.is_corruption()) << sizes.first << " " << sizes.second << " " << st.to_string();
    }
}

TEST_F(ParquetPageReaderTest, NegativeNumValues) {
    for (const auto& page_header :
         {make_data_page_header(-1), make_data_page_v2_header(-1, 0, 0), make_dictionary_page_header(-1),
          make_data_page_header(std::numeric_limits<int32_t>::min()),
          make_dictionary_page_header(std::numeric_limits<int32_t>::min())}) {
        PageFixture fixture(make_page(page_header, 64), tparquet::CompressionCodec::ZSTD);
        Status st = fixture.reader()->next_header();
        ASSERT_TRUE(st.is_corruption()) << st.to_string();
    }

    // the same pages with a count that is valid are read
    for (const auto& page_header :
         {make_data_page_header(10), make_data_page_v2_header(10, 0, 0), make_dictionary_page_header(10)}) {
        PageFixture fixture(make_page(page_header, 64), tparquet::CompressionCodec::ZSTD);
        Status st = fixture.reader()->next_header();
        ASSERT_TRUE(st.ok()) << st.to_string();
    }
}

TEST_F(ParquetPageReaderTest, NegativeLevelsLengthInDataPageV2) {
    for (const auto& page_header : {make_data_page_v2_header(10, -1, 0), make_data_page_v2_header(10, 0, -1)}) {
        PageFixture fixture(make_page(page_header, 64), tparquet::CompressionCodec::ZSTD);
        Status st = fixture.reader()->next_header();
        ASSERT_TRUE(st.is_corruption()) << st.to_string();
    }
}

// The levels of a data page v2 are copied before the rest of the page is decompressed, and the copy is as
// long as the sum of the two lengths in the header. The sum must not wrap, and must fit the page.
TEST_F(ParquetPageReaderTest, LevelsLengthLongerThanDataPageV2) {
    const int32_t max_length = std::numeric_limits<int32_t>::max();
    for (auto lengths :
         std::vector<std::pair<int32_t, int32_t>>{{max_length, max_length}, {max_length, 1}, {40, 40}, {65, 0}}) {
        auto page_header = make_data_page_v2_header(10, lengths.first, lengths.second);
        PageFixture fixture(make_page(page_header, 64), tparquet::CompressionCodec::ZSTD);
        ASSERT_TRUE(fixture.reader()->next_header().ok());
        auto data = fixture.reader()->read_and_decompress_page_data();
        ASSERT_FALSE(data.ok()) << lengths.first << " " << lengths.second;
        ASSERT_TRUE(data.status().is_corruption()) << data.status().to_string();
    }
}

// A page header lies inside its page. When the offset index gives the size of the page, the reader asks the stream for
// the bytes of that page and no more, so the bytes of the next page stay unread. The dictionary page is the bytes
// before the first page of the offset index, and the read of its header stops there. If the offset index names a page
// that is shorter than its header, the reader reads more bytes and still returns the header. A position that the index
// does not list as the start of a page does not limit the read, unless the position is before the first listed page:
// by the rule above that position is the dictionary page, whatever the index lists after it.
TEST_F(ParquetPageReaderTest, HeaderReadStaysInsideThePageOfTheOffsetIndex) {
    // an optional dictionary page and three data pages, each a header and 20 bytes of body
    auto make_chunk = [](bool with_dict, std::string* buffer, std::vector<tparquet::PageLocation>* locations) {
        if (with_dict) {
            *buffer += make_page(make_dictionary_page_header(5, 20), 20);
        }
        for (int i = 0; i < 3; ++i) {
            auto page_header = make_data_page_header(10);
            page_header.compressed_page_size = 20;
            page_header.uncompressed_page_size = 20;
            std::string page = make_page(page_header, 20);
            tparquet::PageLocation location;
            location.offset = static_cast<int64_t>(buffer->size());
            location.compressed_page_size = static_cast<int32_t>(page.size());
            location.first_row_index = i * 10;
            locations->push_back(location);
            *buffer += page;
        }
    };

    // Reads the header at `start` and returns whether the stream gave out a byte at or after `boundary`.
    auto read_header_at = [](const std::string& buffer, const std::vector<tparquet::PageLocation>* page_locations,
                             int64_t start, int64_t boundary, tparquet::PageType::type expected_type) {
        RecordingInputStream stream(buffer);
        HdfsScannerStats stats;
        ColumnReaderOptions opts;
        opts.stats = &stats;
        PageReader reader(&stream, 0, buffer.size(), 30, opts, tparquet::CompressionCodec::UNCOMPRESSED);
        reader.set_page_locations(page_locations);
        EXPECT_TRUE(reader.seek_to_offset(start).ok());
        Status st = reader.next_header();
        EXPECT_TRUE(st.ok()) << st.to_string();
        EXPECT_EQ(expected_type, reader.current_header()->type);
        EXPECT_EQ(20, reader.current_header()->compressed_page_size);
        return stream.touched(boundary, buffer.size());
    };

    // the headers of the data pages that have a page after them: the read stops at the end of the page
    {
        std::string buffer;
        std::vector<tparquet::PageLocation> locations;
        make_chunk(false, &buffer, &locations);
        const auto type = tparquet::PageType::DATA_PAGE;

        // the same index with every page one byte later than it is
        std::vector<tparquet::PageLocation> shifted = locations;
        for (auto& location : shifted) {
            location.offset += 1;
        }

        for (size_t i = 0; i + 1 < locations.size(); ++i) {
            SCOPED_TRACE("page " + std::to_string(i));
            const int64_t start = locations[i].offset;
            const int64_t next_page_begin = locations[i + 1].offset;

            // without the offset index the header read takes the bytes after the page
            ASSERT_TRUE(read_header_at(buffer, nullptr, start, next_page_begin, type));
            ASSERT_FALSE(read_header_at(buffer, &locations, start, next_page_begin, type));

            // a size that is shorter than the header: the reader reads more and returns the header
            std::vector<tparquet::PageLocation> too_short = locations;
            too_short[i].compressed_page_size = 1;
            ASSERT_FALSE(read_header_at(buffer, &too_short, start, next_page_begin, type));

            if (i == 0) {
                // We expect the reader to take the bytes before the first page of the index for the dictionary page,
                // also when the index is wrong about it. Here that page is one byte long, so the first read is short
                // of the header, and the reader grows the read until the header parses. It stays short of the next
                // page.
                ASSERT_FALSE(read_header_at(buffer, &shifted, start, next_page_begin, type));
            } else {
                // a page that the offset index gives at another position than the reader is at does not limit the read
                ASSERT_TRUE(read_header_at(buffer, &shifted, start, next_page_begin, type));
            }
        }
    }

    // the header of the dictionary page: the read stops at the first data page
    {
        std::string buffer;
        std::vector<tparquet::PageLocation> locations;
        make_chunk(true, &buffer, &locations);
        const int64_t first_page_begin = locations[0].offset;
        const auto type = tparquet::PageType::DICTIONARY_PAGE;

        ASSERT_TRUE(read_header_at(buffer, nullptr, 0, first_page_begin, type));
        ASSERT_FALSE(read_header_at(buffer, &locations, 0, first_page_begin, type));
    }
}

// next_header() ends the chunk by the values of the headers that it has read. A reader that jumps over pages has not read
// their headers, so it sets the values before the page that it jumps to, and then the chunk ends after the last page.
// Without it the count stays below the values of the chunk, and the reader that goes on gets a corrupt header.
TEST_F(ParquetPageReaderTest, EndOfFileAfterAJumpNeedsTheValuesRead) {
    // three pages of 10 values
    std::string buffer;
    std::vector<int64_t> offsets;
    for (int i = 0; i < 3; ++i) {
        offsets.push_back(static_cast<int64_t>(buffer.size()));
        buffer += make_page(make_data_page_header(10), 64);
    }

    // jumps to the last page, reads its header, and asks for the page after it
    auto read_past_the_last_page = [&](bool set_values_read) -> Status {
        RecordingInputStream stream(buffer);
        HdfsScannerStats stats;
        ColumnReaderOptions opts;
        opts.stats = &stats;
        PageReader reader(&stream, 0, buffer.size(), 30, opts, tparquet::CompressionCodec::UNCOMPRESSED);
        RETURN_IF_ERROR(reader.seek_to_offset(offsets[2]));
        if (set_values_read) {
            reader.set_num_values_read(20);
        }
        RETURN_IF_ERROR(reader.next_header());
        RETURN_IF_ERROR(reader.next_page());
        return reader.next_header();
    };

    Status st = read_past_the_last_page(true);
    ASSERT_TRUE(st.is_end_of_file()) << st.to_string();
    st = read_past_the_last_page(false);
    ASSERT_FALSE(st.ok());
    ASSERT_FALSE(st.is_end_of_file()) << st.to_string();
}

class ParquetColumnChunkReaderTest : public testing::Test {
protected:
    // Reads the dictionary page of a column chunk that is one dictionary page of `body_size` zero bytes.
    static Status load_dictionary(tparquet::Type::type type, int32_t type_length,
                                  const tparquet::PageHeader& page_header, size_t body_size) {
        std::string buffer = make_page(page_header, body_size);

        HdfsScannerStats stats;
        FileMetaData file_meta;
        RandomAccessFile file(std::make_shared<io::StringInputStream>(std::string(buffer)), "string-file");
        ColumnReaderOptions opts;
        opts.stats = &stats;
        opts.file = &file;
        opts.file_meta_data = &file_meta;

        tparquet::ColumnChunk chunk;
        chunk.meta_data.type = type;
        chunk.meta_data.codec = tparquet::CompressionCodec::UNCOMPRESSED;
        chunk.meta_data.num_values = 1000;
        chunk.meta_data.total_compressed_size = buffer.size();
        chunk.meta_data.data_page_offset = 0;
        chunk.meta_data.__set_dictionary_page_offset(0);

        ColumnChunkReader reader(0, 0, type_length, &chunk, opts);
        RETURN_IF_ERROR(reader.init(4096));
        return reader.load_dictionary_page();
    }
};

// A PLAIN value takes a known minimum of bytes, so a dictionary page cannot hold more values than that
// allows. The reader must reject such a page before it sizes the dictionary from the count in the header.
TEST_F(ParquetColumnChunkReaderTest, DictionaryPageHoldsFewerValuesThanDeclared) {
    for (auto encoding : {tparquet::Encoding::PLAIN, tparquet::Encoding::PLAIN_DICTIONARY}) {
        // INT32 takes 4 bytes
        Status st = load_dictionary(tparquet::Type::INT32, 0, make_dictionary_page_header(1000, 8, encoding), 8);
        ASSERT_TRUE(st.is_corruption()) << st.to_string();
        st = load_dictionary(tparquet::Type::INT32, 0, make_dictionary_page_header(3, 8, encoding), 8);
        ASSERT_TRUE(st.is_corruption()) << st.to_string();
        // BYTE_ARRAY takes at least 4 bytes for the length
        st = load_dictionary(tparquet::Type::BYTE_ARRAY, 0, make_dictionary_page_header(5, 16, encoding), 16);
        ASSERT_TRUE(st.is_corruption()) << st.to_string();
        // FIXED_LEN_BYTE_ARRAY takes the type length
        st = load_dictionary(tparquet::Type::FIXED_LEN_BYTE_ARRAY, 16, make_dictionary_page_header(2, 16, encoding),
                             16);
        ASSERT_TRUE(st.is_corruption()) << st.to_string();
        // the largest count of the type that fits in the page
        for (const auto& [type, type_length, num_values, page_size] :
             std::vector<std::tuple<tparquet::Type::type, int32_t, int32_t, int32_t>>{
                     {tparquet::Type::INT32, 0, 2, 8},
                     {tparquet::Type::INT64, 0, 2, 16},
                     {tparquet::Type::BYTE_ARRAY, 0, 4, 16},
                     {tparquet::Type::FIXED_LEN_BYTE_ARRAY, 16, 1, 16}}) {
            st = load_dictionary(type, type_length, make_dictionary_page_header(num_values, page_size, encoding),
                                 page_size);
            ASSERT_TRUE(st.ok()) << st.to_string();
        }
    }
}

// A caller asks for the dictionary of a chunk that is dictionary encoded. If the first page of the chunk is a data
// page, the reader returns an error, and does not read the header of the page and leave the page behind.
TEST_F(ParquetColumnChunkReaderTest, DictionaryOfAChunkWithoutADictionaryPageIsAnError) {
    const std::string buffer = make_page(make_data_page_header(10), 64);

    HdfsScannerStats stats;
    FileMetaData file_meta;
    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::string(buffer)), "string-file");
    ColumnReaderOptions opts;
    opts.stats = &stats;
    opts.file = &file;
    opts.file_meta_data = &file_meta;

    tparquet::ColumnChunk chunk;
    chunk.meta_data.type = tparquet::Type::INT32;
    chunk.meta_data.codec = tparquet::CompressionCodec::UNCOMPRESSED;
    chunk.meta_data.num_values = 10;
    chunk.meta_data.total_compressed_size = buffer.size();
    chunk.meta_data.data_page_offset = 0;

    ColumnChunkReader reader(0, 0, 0, &chunk, opts);
    ASSERT_TRUE(reader.init(4096).ok());
    MutableColumnPtr column = ColumnHelper::create_column(TYPE_INT_DESC, false);
    Status st = reader.get_dict_values(column.get());
    ASSERT_TRUE(st.is_corruption()) << st.to_string();
    ASSERT_NE(std::string::npos, st.message().find("string-file")) << st.message();
}

// The schema and the column chunk of a column both declare its physical type, and the reader takes the
// decoders from the chunk but the type length and the converter from the schema. A file where the two
// differ must be rejected when the reader of the column is created, naming the column and the file.
TEST_F(ParquetColumnChunkReaderTest, ChunkTypeDiffersFromSchema) {
    std::string buffer(64, '\0');
    HdfsScannerStats stats;
    FileMetaData file_meta;
    RandomAccessFile file(std::make_shared<io::StringInputStream>(std::move(buffer)), "string-file");
    ColumnReaderOptions opts;
    opts.stats = &stats;
    opts.file = &file;
    opts.file_meta_data = &file_meta;
    opts.chunk_size = 4096;

    ParquetField field{};
    field.name = "price";
    field.physical_type = tparquet::Type::INT32;

    tparquet::ColumnChunk chunk;
    chunk.meta_data.codec = tparquet::CompressionCodec::UNCOMPRESSED;
    chunk.meta_data.num_values = 10;
    chunk.meta_data.total_compressed_size = 64;
    chunk.meta_data.data_page_offset = 0;

    std::unique_ptr<StoredColumnReader> reader;
    for (auto chunk_type : {tparquet::Type::BOOLEAN, tparquet::Type::INT64, tparquet::Type::FIXED_LEN_BYTE_ARRAY}) {
        chunk.meta_data.type = chunk_type;
        Status st = StoredColumnReader::create(opts, &field, &chunk, &reader);
        ASSERT_TRUE(st.is_corruption()) << st.to_string();
        ASSERT_NE(std::string::npos, st.message().find("'price'")) << st.message();
        ASSERT_NE(std::string::npos, st.message().find("string-file")) << st.message();
        ASSERT_TRUE(reader == nullptr);
    }

    chunk.meta_data.type = tparquet::Type::INT32;
    Status st = StoredColumnReader::create(opts, &field, &chunk, &reader);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(reader != nullptr);
}

// A column chunk of six data pages A to F, each a header and a body of its own letter, read twice through a page reader
// that uses the page cache. The first read fills the page cache. Before the second read the page cache loses some of
// the pages, and the shared buffer of the stream loads only the runs of these pages, when the planner of the page cache
// aware IO is on.
class ParquetPageCacheIoTest : public testing::Test {
public:
    static constexpr int kPages = 6;
    static constexpr size_t kBodySize = 4000;
    static constexpr const char* kFileName = "page-cache-io-file";
    static constexpr int64_t kModificationTime = 1000;

    void SetUp() override {
        _saved_distance = config::io_coalesce_read_max_distance_size;
        _saved_enable = config::parquet_page_cache_aware_io_enable;
        // the runs of pages are far smaller than the default distance, so we make every cached page a gap
        config::io_coalesce_read_max_distance_size = 0;
        config::parquet_page_cache_aware_io_enable = true;

        MemCacheOptions options{.mem_space_size = 100 * 1024 * 1024};
        _engine = std::make_shared<LRUCacheEngine>();
        ASSERT_OK(_engine->init(options));
        _page_cache = std::make_shared<StoragePageCache>(_engine.get());
        _prev_page_cache = DataCache::GetInstance()->page_cache_ptr();
        DataCache::GetInstance()->set_page_cache(_page_cache);

        for (int i = 0; i < kPages; ++i) {
            auto header = make_data_page_header(10);
            header.compressed_page_size = kBodySize;
            header.uncompressed_page_size = kBodySize;
            std::string page = make_page(header, kBodySize);
            std::fill(page.end() - kBodySize, page.end(), static_cast<char>('a' + i));
            tparquet::PageLocation location;
            location.offset = _chunk.size();
            location.compressed_page_size = page.size();
            location.first_row_index = i * 10;
            _locations.push_back(location);
            _spans.push_back(PageSpan{location.offset, location.compressed_page_size});
            _chunk += page;
        }

        _file = std::make_unique<RandomAccessFile>(std::make_shared<io::StringInputStream>(std::string(_chunk)),
                                                   kFileName);
        _opts.stats = &_stats;
        _opts.file = _file.get();
        _opts.file_meta_data = &_file_meta;
        _opts.use_file_pagecache = true;
        _opts.modification_time = kModificationTime;
        _opts.file_size = _chunk.size();
        _opts.datacache_options = &_datacache_options;
    }

    void TearDown() override {
        config::io_coalesce_read_max_distance_size = _saved_distance;
        config::parquet_page_cache_aware_io_enable = _saved_enable;
        DataCache::GetInstance()->set_page_cache(_prev_page_cache);
    }

protected:
    // A shared stream over a recording copy of the chunk.
    struct Scan {
        std::shared_ptr<RecordingInputStream> remote;
        std::unique_ptr<io::SharedBufferedInputStream> stream;
        std::unique_ptr<PageCacheIoPlanner> planner;
    };

    // With a planner the stream does not load the pages that the page cache holds. has_layout: the column of the chunk
    // knows the pages of the chunk.
    void open_scan(Scan* scan, bool with_planner, bool has_layout = true) {
        scan->remote = std::make_shared<RecordingInputStream>(_chunk);
        scan->stream = std::make_unique<io::SharedBufferedInputStream>(scan->remote, kFileName, _chunk.size());
        ASSERT_OK(scan->stream->set_io_ranges({io::SharedBufferedInputStream::IORange(0, _chunk.size())}));
        if (with_planner) {
            scan->planner = std::make_unique<PageCacheIoPlanner>(
                    _page_cache.get(),
                    ParquetUtils::get_file_cache_key(CacheType::PAGE, kFileName, kModificationTime, _chunk.size()));
            PageCacheIoChunk chunk;
            chunk.begin = 0;
            chunk.end = _chunk.size();
            chunk.pages = [this, has_layout](bool) { return has_layout ? &_spans : nullptr; };
            scan->planner->add_chunk(chunk);
            scan->stream->set_skip_ranges_provider(scan->planner.get());
        }
    }

    // Reads the pages of the chunk one after the other, as the column reader does, and checks the bytes of each page.
    // before_page(i) runs before the page i is read.
    void read_pages(io::SeekableInputStream* stream, const std::function<void(int)>& before_page = nullptr,
                    bool with_page_sizes = true) {
        PageReader reader(stream, 0, _chunk.size(), kPages * 10, _opts, tparquet::CompressionCodec::UNCOMPRESSED);
        if (with_page_sizes) {
            reader.set_page_locations(&_locations);
        }
        for (int i = 0; i < kPages; ++i) {
            if (before_page) {
                before_page(i);
            }
            ASSERT_OK(reader.next_header());
            auto data = reader.read_and_decompress_page_data();
            ASSERT_OK(data.status());
            ASSERT_EQ(std::string(kBodySize, static_cast<char>('a' + i)),
                      std::string(data.value().data, data.value().size))
                    << "page " << i;
            if (i + 1 < kPages) {
                ASSERT_OK(reader.next_page());
            }
        }
    }

    std::string key_of(int page) const {
        std::string key = PageReader::make_page_cache_key(
                ParquetUtils::get_file_cache_key(CacheType::PAGE, kFileName, kModificationTime, _chunk.size()));
        PageReader::set_page_cache_key_offset(&key, _locations[page].offset);
        return key;
    }

    // [begin, end) of the page in the file.
    std::pair<int64_t, int64_t> page_range(int page) const {
        return {_spans[page].offset, _spans[page].offset + _spans[page].size};
    }

    // The first read: it fills the page cache with every page, with one request for the chunk.
    void fill_page_cache() {
        Scan scan;
        ASSERT_NO_FATAL_FAILURE(open_scan(&scan, false));
        ASSERT_NO_FATAL_FAILURE(read_pages(scan.stream.get()));
        ASSERT_EQ(1u, scan.remote->ranges().size());
        ASSERT_EQ(0, _stats.page_cache_read_counter);
        ASSERT_EQ(kPages, _stats.page_cache_write_counter);
        _stats = HdfsScannerStats();
    }

    // The page cache loses the pages.
    void lose_pages(const std::vector<int>& pages) {
        for (int page : pages) {
            ASSERT_OK(_engine->remove(key_of(page)));
        }
    }

    std::shared_ptr<LRUCacheEngine> _engine;
    std::shared_ptr<StoragePageCache> _page_cache;
    std::shared_ptr<StoragePageCache> _prev_page_cache;
    std::string _chunk;
    std::vector<tparquet::PageLocation> _locations;
    std::vector<PageSpan> _spans;
    std::unique_ptr<RandomAccessFile> _file;
    HdfsScannerStats _stats;
    FileMetaData _file_meta;
    DataCacheOptions _datacache_options;
    ColumnReaderOptions _opts;
    int32_t _saved_distance = 0;
    bool _saved_enable = true;
};

// The page cache holds B, C and E when the chunk is read again. The pages A, D and F are loaded with one request each,
// and the cached pages are not downloaded. The bytes of every page are the same as in the first read.
TEST_F(ParquetPageCacheIoTest, SecondReadLoadsOnlyThePagesThatThePageCacheMisses) {
    ASSERT_NO_FATAL_FAILURE(fill_page_cache());
    ASSERT_NO_FATAL_FAILURE(lose_pages({0, 3, 5}));

    Scan scan;
    ASSERT_NO_FATAL_FAILURE(open_scan(&scan, true));
    ASSERT_NO_FATAL_FAILURE(read_pages(scan.stream.get()));
    using Ranges = std::vector<std::pair<int64_t, int64_t>>;
    ASSERT_EQ((Ranges{page_range(0), page_range(3), page_range(5)}), scan.remote->ranges());
    ASSERT_EQ(3, _stats.page_cache_read_counter);
    ASSERT_EQ(3, _stats.page_cache_write_counter);
    ASSERT_EQ(3 * static_cast<int64_t>(_spans[0].size), scan.stream->skip_ranges_bytes());
    ASSERT_EQ(1, scan.stream->skip_ranges_split_count());
    ASSERT_EQ(3, scan.stream->shared_io_count());
    ASSERT_EQ(3 * static_cast<int64_t>(_spans[0].size), scan.stream->shared_io_bytes());
    ASSERT_EQ(0, scan.stream->direct_io_count());
}

// With the config off the first miss loads the whole chunk, as before.
TEST_F(ParquetPageCacheIoTest, ConfigOffLoadsTheWholeChunk) {
    ASSERT_NO_FATAL_FAILURE(fill_page_cache());
    ASSERT_NO_FATAL_FAILURE(lose_pages({0, 3, 5}));

    config::parquet_page_cache_aware_io_enable = false;
    Scan scan;
    ASSERT_NO_FATAL_FAILURE(open_scan(&scan, true));
    ASSERT_NO_FATAL_FAILURE(read_pages(scan.stream.get()));
    ASSERT_EQ(1u, scan.remote->ranges().size());
    ASSERT_EQ(0, scan.remote->ranges()[0].first);
    ASSERT_EQ(static_cast<int64_t>(_chunk.size()), scan.remote->ranges()[0].second);
    ASSERT_EQ(3, _stats.page_cache_read_counter);
    ASSERT_EQ(0, scan.stream->skip_ranges_bytes());
    ASSERT_EQ(0, scan.stream->skip_ranges_split_count());
}

// A column that does not know the pages of its chunk is read as before.
TEST_F(ParquetPageCacheIoTest, ChunkWithoutPagesLoadsTheWholeChunk) {
    ASSERT_NO_FATAL_FAILURE(fill_page_cache());
    ASSERT_NO_FATAL_FAILURE(lose_pages({0, 3, 5}));

    Scan scan;
    ASSERT_NO_FATAL_FAILURE(open_scan(&scan, true, false));
    ASSERT_NO_FATAL_FAILURE(read_pages(scan.stream.get()));
    ASSERT_EQ(1u, scan.remote->ranges().size());
    ASSERT_EQ(3, _stats.page_cache_read_counter);
    ASSERT_EQ(0, scan.stream->skip_ranges_split_count());
}

// The page cache can lose a page after the planner looked at it. The page is not in a buffer, but it lies in a cut
// span, so the reader gets it with one request for the rest of the span, and the bytes are right.
TEST_F(ParquetPageCacheIoTest, PageThatTheCacheLostAfterThePlanIsReadWithItsSpan) {
    ASSERT_NO_FATAL_FAILURE(fill_page_cache());
    ASSERT_NO_FATAL_FAILURE(lose_pages({0, 3, 5}));

    Scan scan;
    ASSERT_NO_FATAL_FAILURE(open_scan(&scan, true));
    // The first read of the chunk plans its buffer, which cuts the pages B, C and E out. Then the cache loses C.
    ASSERT_NO_FATAL_FAILURE(read_pages(scan.stream.get(), [&](int page) {
        if (page == 2) {
            ASSERT_OK(_engine->remove(key_of(2)));
        }
    }));
    // C missed the page cache, so it was read from the file with one request, and B and E were not
    ASSERT_EQ(2, _stats.page_cache_read_counter);
    ASSERT_EQ(0, scan.stream->direct_io_count());
    ASSERT_EQ(1, scan.stream->skip_ranges_recover_count());
    ASSERT_EQ(page_range(2).second - page_range(2).first, scan.stream->skip_ranges_recover_bytes());
    ASSERT_TRUE(scan.remote->touched(page_range(2).first, page_range(2).second));
    ASSERT_FALSE(scan.remote->touched(page_range(1).first, page_range(1).second));
    ASSERT_FALSE(scan.remote->touched(page_range(4).first, page_range(4).second));
    ASSERT_TRUE(scan.remote->touched(page_range(0).first, page_range(0).second));
    ASSERT_TRUE(scan.remote->touched(page_range(3).first, page_range(3).second));
    ASSERT_TRUE(scan.remote->touched(page_range(5).first, page_range(5).second));
}

// The page cache loses B and C, which the planner cut out together. The header and the body of B and of C cost four
// reads without the recovery, and cost one request for the two pages with it.
TEST_F(ParquetPageCacheIoTest, PagesOfOneSpanThatTheCacheLostAreReadWithOneRequest) {
    ASSERT_NO_FATAL_FAILURE(fill_page_cache());
    ASSERT_NO_FATAL_FAILURE(lose_pages({0, 3, 5}));

    Scan scan;
    ASSERT_NO_FATAL_FAILURE(open_scan(&scan, true));
    ASSERT_NO_FATAL_FAILURE(read_pages(scan.stream.get(), [&](int page) {
        if (page == 1) {
            ASSERT_OK(_engine->remove(key_of(1)));
            ASSERT_OK(_engine->remove(key_of(2)));
        }
    }));
    // only E came from the page cache
    ASSERT_EQ(1, _stats.page_cache_read_counter);
    ASSERT_EQ(0, scan.stream->direct_io_count());
    ASSERT_EQ(1, scan.stream->skip_ranges_recover_count());
    ASSERT_EQ(page_range(2).second - page_range(1).first, scan.stream->skip_ranges_recover_bytes());
    // the runs A, D and F, and the span of B and C
    ASSERT_EQ(4, scan.stream->shared_io_count());
    ASSERT_FALSE(scan.remote->touched(page_range(4).first, page_range(4).second));
}

// A reader that does not know the sizes of the pages reads the header of a page with the default size, which is larger
// than a page here, so the read reaches into the pages that the stream cut out. Such a read is direct, and every page
// still has the right bytes. This is why the column gives the sizes of the pages to its page reader.
TEST_F(ParquetPageCacheIoTest, ReaderWithoutPageSizesStillReadsRightBytes) {
    ASSERT_NO_FATAL_FAILURE(fill_page_cache());
    ASSERT_NO_FATAL_FAILURE(lose_pages({0, 3, 5}));

    Scan scan;
    ASSERT_NO_FATAL_FAILURE(open_scan(&scan, true));
    ASSERT_NO_FATAL_FAILURE(read_pages(scan.stream.get(), nullptr, false));
    ASSERT_EQ(3, _stats.page_cache_read_counter);
    ASSERT_GE(scan.stream->direct_io_count(), 1);
}

// A page cache that holds every page: no page is read from the file at all.
TEST_F(ParquetPageCacheIoTest, PageCacheThatHoldsEveryPageReadsNothing) {
    ASSERT_NO_FATAL_FAILURE(fill_page_cache());

    Scan scan;
    ASSERT_NO_FATAL_FAILURE(open_scan(&scan, true));
    ASSERT_NO_FATAL_FAILURE(read_pages(scan.stream.get()));
    ASSERT_TRUE(scan.remote->ranges().empty());
    ASSERT_EQ(kPages, _stats.page_cache_read_counter);
    ASSERT_EQ(0, scan.stream->shared_io_count());
}

} // namespace starrocks::parquet
