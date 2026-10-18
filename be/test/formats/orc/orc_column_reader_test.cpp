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

#include <gtest/gtest.h>

#include <cmath>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "column/nullable_column.h"
#include "formats/orc/column_reader.h"
#include "formats/orc/memory_stream/MemoryInputStream.hh"
#include "formats/orc/memory_stream/MemoryOutputStream.hh"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_mapping.h"

namespace starrocks {

// 100mb buffer
const static size_t bufferSize = 100 * 1024 * 1024;

TEST(OrcColumnReaderTest, TestDateColumn) {
    const static size_t batchSize = 3;

    MemoryOutputStream buffer(bufferSize);
    ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:date>"));
    const orc::Type* orcType = schema->getSubtype(0);

    // prepare data.
    {
        orc::WriterOptions writerOptions;
        ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);

        for (size_t i = 0; i < batchSize; i++) {
            c0->data[i] = i;
        }
        c0->notNull[0] = 1;
        c0->notNull[1] = 0;
        c0->notNull[2] = 1;

        c0->numElements = batchSize;
        root->numElements = batchSize;
        writer->add(*batch);
        writer->close();
    }

    // read
    {
        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);

        orc::RowReaderOptions options;
        std::list<std::string> columns = {"c0"};
        options.include(columns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        // Set for OrcMapping and OrcChunkReader, just used it to pass arguments in function, actually it's not used
        const OrcMappingPtr orcMapping = nullptr;
        OrcChunkReader orcChunkReader(batchSize, {});
        orcChunkReader.disable_broker_load_mode();

        TypeDescriptor c0Type = TypeDescriptor::from_logical_type(LogicalType::TYPE_DATE);

        std::unique_ptr<ORCColumnReader> orcColumnReader =
                ORCColumnReader::create(c0Type, orcType, true, orcMapping, &orcChunkReader).value();

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(batchSize);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::LongVectorBatch*>(root->fields[0]);
        orc::RowReader::ReadPosition pos;
        EXPECT_TRUE(rr->next(*batch, &pos));
        MutableColumnPtr column = ColumnHelper::create_column(c0Type, true);
        EXPECT_TRUE(orcColumnReader->get_next(c0, column, 0, batchSize).ok());
        EXPECT_EQ(batchSize, column->size());

        EXPECT_EQ("1970-01-01", column->debug_item(0));
        EXPECT_EQ("NULL", column->debug_item(1));
        EXPECT_EQ("1970-01-02", column->debug_item(2));
    }
}

// A nullable array, map or struct with NULL rows reads its string children range by range, once for every run of rows
// that are not NULL, and all the ranges fill one column. We fear a reader that reserves the exact size of the bytes
// for every range: it reallocates and copies all the bytes of the column in every call, and a column of wide strings
// costs O(ranges^2). So we read a column in ranges of 4 rows and check that the number of reallocations of its bytes
// grows with the logarithm of the number of ranges.
TEST(OrcColumnReaderTest, TestStringColumnReadInSmallRangesDoesNotReallocateTheBytesEveryTime) {
    constexpr size_t kNumRows = 4096;
    constexpr size_t kRowsPerRange = 4;
    constexpr size_t kWidth = 200;
    // The bytes of a column are reallocated when, and only when, their capacity changes. A buffer that grows by at
    // least a factor of two on each reallocation is reallocated about log2(ranges) times in all the ranges.
    const size_t max_reallocations =
            static_cast<size_t>(std::ceil(std::log2(static_cast<double>(kNumRows / kRowsPerRange)))) + 4;

    struct Case {
        const char* name;
        // The reader and the column are nullable.
        bool nullable;
        // The ORC batch has NULL rows. Row 1 of every 5 rows is NULL.
        bool has_nulls;
    };
    const std::vector<Case> cases = {{"nullable, NULL rows", true, true},
                                     {"nullable, no NULL rows", true, false},
                                     {"not nullable", false, false}};
    for (const Case& c : cases) {
        SCOPED_TRACE(c.name);
        auto is_null = [&](size_t row) { return c.has_nulls && row % 5 == 1; };
        std::vector<std::string> values;
        for (size_t row = 0; row < kNumRows; ++row) {
            values.push_back(std::string(kWidth, static_cast<char>('a' + row % 26)));
        }

        MemoryOutputStream buffer(bufferSize);
        ORC_UNIQUE_PTR<orc::Type> schema(orc::Type::buildTypeFromString("struct<c0:string>"));
        const orc::Type* orcType = schema->getSubtype(0);
        {
            orc::WriterOptions writerOptions;
            writerOptions.setDictionaryKeySizeThreshold(0.0);
            ORC_UNIQUE_PTR<orc::Writer> writer = createWriter(*schema, &buffer, writerOptions);
            ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = writer->createRowBatch(kNumRows);
            auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
            auto* c0 = dynamic_cast<orc::StringVectorBatch*>(root->fields[0]);
            for (size_t row = 0; row < kNumRows; ++row) {
                c0->data[row] = const_cast<char*>(values[row].data());
                c0->length[row] = values[row].size();
                c0->notNull[row] = !is_null(row);
            }
            c0->hasNulls = c.has_nulls;
            c0->numElements = kNumRows;
            root->numElements = kNumRows;
            writer->add(*batch);
            writer->close();
        }

        orc::ReaderOptions readerOptions;
        ORC_UNIQUE_PTR<orc::InputStream> inputStream(new MemoryInputStream(buffer.getData(), buffer.getLength()));
        ORC_UNIQUE_PTR<orc::Reader> reader = createReader(std::move(inputStream), readerOptions);
        orc::RowReaderOptions options;
        std::list<std::string> columns = {"c0"};
        options.include(columns);
        ORC_UNIQUE_PTR<orc::RowReader> rr = reader->createRowReader(options);

        // Set for OrcMapping and OrcChunkReader, just used it to pass arguments in function, actually it's not used
        const OrcMappingPtr orcMapping = nullptr;
        OrcChunkReader orcChunkReader(kNumRows, {});
        orcChunkReader.disable_broker_load_mode();
        TypeDescriptor c0Type = TypeDescriptor::create_varchar_type(TypeDescriptor::MAX_VARCHAR_LENGTH);
        std::unique_ptr<ORCColumnReader> orcColumnReader =
                ORCColumnReader::create(c0Type, orcType, c.nullable, orcMapping, &orcChunkReader).value();

        ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch = rr->createRowBatch(kNumRows);
        auto* root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
        auto* c0 = dynamic_cast<orc::StringVectorBatch*>(root->fields[0]);
        orc::RowReader::ReadPosition pos;
        ASSERT_TRUE(rr->next(*batch, &pos));
        ASSERT_EQ(kNumRows, batch->numElements);

        MutableColumnPtr column = ColumnHelper::create_column(c0Type, c.nullable);
        auto* binary_column = down_cast<BinaryColumn*>(ColumnHelper::get_data_column(column.get()));
        size_t capacity = binary_column->get_bytes().capacity();
        size_t num_reallocations = 0;
        for (size_t from = 0; from < kNumRows; from += kRowsPerRange) {
            ASSERT_TRUE(orcColumnReader->get_next(c0, column.get(), from, kRowsPerRange).ok());
            const size_t new_capacity = binary_column->get_bytes().capacity();
            num_reallocations += new_capacity != capacity;
            capacity = new_capacity;
        }
        ASSERT_LE(num_reallocations, max_reallocations);

        ASSERT_EQ(kNumRows, column->size());
        ASSERT_EQ(binary_column->get_bytes().size(), binary_column->get_offset().back());
        for (size_t row = 0; row < kNumRows; ++row) {
            if (is_null(row)) {
                ASSERT_TRUE(column->is_null(row)) << "row " << row;
                ASSERT_EQ(0U, binary_column->get_slice(row).size) << "row " << row;
            } else {
                ASSERT_FALSE(column->is_null(row)) << "row " << row;
                ASSERT_EQ(values[row], binary_column->get_slice(row).to_string()) << "row " << row;
            }
        }
    }
}

} // namespace starrocks