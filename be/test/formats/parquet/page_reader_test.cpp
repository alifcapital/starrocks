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

#include <iostream>
#include <limits>
#include <tuple>
#include <utility>
#include <vector>

#include "column/column_helper.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/column_chunk_reader.h"
#include "formats/parquet/column_reader.h"
#include "formats/parquet/metadata.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/stored_column_reader.h"
#include "fs/fs_memory.h"
#include "gen_cpp/parquet_types.h"
#include "io/shared_buffered_input_stream.h"
#include "io/string_input_stream.h"
#include "runtime/types.h"
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

} // namespace starrocks::parquet
