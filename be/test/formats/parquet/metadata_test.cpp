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

#include "formats/parquet/metadata.h"

#include <gtest/gtest.h>

#include <limits>

#include "formats/parquet/utils.h"

namespace starrocks::parquet {

class ParquetMetaDataTest : public testing::Test {
public:
    ParquetMetaDataTest() = default;
    ~ParquetMetaDataTest() override = default;

private:
    tparquet::SchemaElement _create_root_schema_element();
    tparquet::SchemaElement _create_schema_element(const std::string& name);
    std::vector<tparquet::SchemaElement> _create_schema_elements();
    tparquet::FileMetaData _create_t_file_meta();
};

tparquet::SchemaElement ParquetMetaDataTest::_create_root_schema_element() {
    tparquet::SchemaElement element;

    element.__set_num_children(2);

    return element;
}

tparquet::SchemaElement ParquetMetaDataTest::_create_schema_element(const std::string& name) {
    tparquet::SchemaElement element;

    element.__set_name(name);
    element.__set_type(tparquet::Type::type::INT32);
    element.__set_type_length(4);
    element.__set_num_children(0);

    return element;
}

std::vector<tparquet::SchemaElement> ParquetMetaDataTest::_create_schema_elements() {
    std::vector<tparquet::SchemaElement> elements;

    auto c0 = _create_root_schema_element();
    auto c1 = _create_schema_element("c1");
    auto c2 = _create_schema_element("c2");

    elements.emplace_back(c0);
    elements.emplace_back(c1);
    elements.emplace_back(c2);

    return elements;
}

tparquet::FileMetaData ParquetMetaDataTest::_create_t_file_meta() {
    auto elements = _create_schema_elements();

    tparquet::FileMetaData meta;

    meta.__set_version(0);
    meta.__set_schema(elements);
    meta.__set_num_rows(1024);

    return meta;
}

TEST_F(ParquetMetaDataTest, NumRows) {
    auto t_meta = _create_t_file_meta();

    FileMetaData meta_data;
    Status status = meta_data.init(t_meta, true);
    ASSERT_TRUE(status.ok());

    // check
    ASSERT_EQ(1024, meta_data.num_rows());
}

// The footer is the only source of the width of a FIXED_LEN_BYTE_ARRAY column. A width that is not positive
// fails the init. A large width does not, because the file can hold other columns that the query reads, and
// the column reader checks the width of the column that it reads.
TEST_F(ParquetMetaDataTest, FixedLenByteArrayTypeLength) {
    auto init = [](int32_t type_length) {
        tparquet::SchemaElement root;
        root.__set_num_children(1);

        tparquet::SchemaElement leaf;
        leaf.__set_name("flba_col");
        leaf.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
        leaf.__set_type_length(type_length);
        leaf.__set_num_children(0);

        tparquet::FileMetaData t_meta;
        t_meta.__set_version(0);
        t_meta.__set_schema({root, leaf});
        t_meta.__set_num_rows(1);

        FileMetaData meta_data;
        return meta_data.init(t_meta, true);
    };

    for (int32_t type_length : {1, 16, ParquetUtils::MAX_FLBA_TYPE_LENGTH + 1, std::numeric_limits<int32_t>::max()}) {
        Status status = init(type_length);
        ASSERT_TRUE(status.ok()) << type_length << " " << status.to_string();
    }
    for (int32_t type_length : {0, -1, std::numeric_limits<int32_t>::min()}) {
        Status status = init(type_length);
        ASSERT_TRUE(status.is_corruption()) << type_length << " " << status.to_string();
        ASSERT_NE(std::string::npos, status.message().find("flba_col")) << status.message();
    }
}

// A column with a huge width that the query does not read must not stop the query from reading the other columns.
TEST_F(ParquetMetaDataTest, WideFixedLenByteArrayColumnKeepsOtherColumns) {
    tparquet::SchemaElement root;
    root.__set_num_children(2);

    tparquet::SchemaElement id;
    id.__set_name("id");
    id.__set_type(tparquet::Type::INT32);
    id.__set_num_children(0);

    tparquet::SchemaElement wide;
    wide.__set_name("wide");
    wide.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    wide.__set_type_length(std::numeric_limits<int32_t>::max());
    wide.__set_num_children(0);

    tparquet::FileMetaData t_meta;
    t_meta.__set_version(0);
    t_meta.__set_schema({root, id, wide});
    t_meta.__set_num_rows(1);

    FileMetaData meta_data;
    Status status = meta_data.init(t_meta, true);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(2U, meta_data.schema().get_fields_size());
    ASSERT_EQ(tparquet::Type::INT32, meta_data.schema().get_parquet_fields()[0].physical_type);
    ASSERT_EQ(std::numeric_limits<int32_t>::max(), meta_data.schema().get_parquet_fields()[1].type_length);
}

TEST(ApplicationVersion, Basics) {
    ApplicationVersion version("parquet-mr version 1.7.9");
    ApplicationVersion version1("parquet-mr version 1.8.0");
    ApplicationVersion version2("parquet-cpp version 1.0.0");
    ApplicationVersion version3("");
    ApplicationVersion version4("parquet-mr version 1.5.0ab-cdh5.5.0+cd (build abcd)");
    ApplicationVersion version5("parquet-mr");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(7, version.version.minor);
    ASSERT_EQ(9, version.version.patch);

    ASSERT_EQ("parquet-cpp", version2.application_);
    ASSERT_EQ(1, version2.version.major);
    ASSERT_EQ(0, version2.version.minor);
    ASSERT_EQ(0, version2.version.patch);

    ASSERT_EQ("parquet-mr", version4.application_);
    ASSERT_EQ("abcd", version4.build_);
    ASSERT_EQ(1, version4.version.major);
    ASSERT_EQ(5, version4.version.minor);
    ASSERT_EQ(0, version4.version.patch);
    ASSERT_EQ("ab", version4.version.unknown);
    ASSERT_EQ("cdh5.5.0", version4.version.pre_release);
    ASSERT_EQ("cd", version4.version.build_info);

    ASSERT_EQ("parquet-mr", version5.application_);
    ASSERT_EQ(0, version5.version.major);
    ASSERT_EQ(0, version5.version.minor);
    ASSERT_EQ(0, version5.version.patch);

    ASSERT_EQ(true, version.VersionLt(version1));

    tparquet::ColumnMetaData column_metadata;
    column_metadata.__set_type(tparquet::Type::INT96);
    ASSERT_FALSE(version1.HasCorrectStatistics(column_metadata, SortOrder::UNKNOWN));
    column_metadata.__set_type(tparquet::Type::INT32);
    ASSERT_TRUE(version.HasCorrectStatistics(column_metadata, SortOrder::SIGNED));
    column_metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    ASSERT_FALSE(version.HasCorrectStatistics(column_metadata, SortOrder::SIGNED));
    ASSERT_TRUE(version1.HasCorrectStatistics(column_metadata, SortOrder::SIGNED));
    ASSERT_FALSE(version1.HasCorrectStatistics(column_metadata, SortOrder::UNSIGNED));
    column_metadata.__set_type(tparquet::Type::FIXED_LEN_BYTE_ARRAY);
    ASSERT_TRUE(version3.HasCorrectStatistics(column_metadata, SortOrder::SIGNED));

    // Check that the old stats are correct if min and max are the same
    // regardless of sort order
    tparquet::Statistics statistics;
    column_metadata.__set_type(tparquet::Type::BYTE_ARRAY);
    statistics.__set_min("a");
    statistics.__set_max("b");
    column_metadata.__set_statistics(statistics);
    ASSERT_FALSE(version1.HasCorrectStatistics(column_metadata, SortOrder::UNSIGNED));
    statistics.__set_max("a");
    column_metadata.__set_statistics(statistics);
    ASSERT_TRUE(version1.HasCorrectStatistics(column_metadata, SortOrder::UNSIGNED));

    // Check that the same holds true for ints
    int32_t int_min = 100, int_max = 200;
    statistics.__set_min(std::string(reinterpret_cast<const char*>(&int_min), 4));
    statistics.__set_max(std::string(reinterpret_cast<const char*>(&int_max), 4));
    column_metadata.__set_statistics(statistics);
    ASSERT_FALSE(version1.HasCorrectStatistics(column_metadata, SortOrder::UNSIGNED));
    statistics.__set_max(std::string(reinterpret_cast<const char*>(&int_min), 4));
    column_metadata.__set_statistics(statistics);
    ASSERT_TRUE(version1.HasCorrectStatistics(column_metadata, SortOrder::UNSIGNED));
}

TEST(ApplicationVersion, Empty) {
    ApplicationVersion version("");

    ASSERT_EQ("", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(0, version.version.major);
    ASSERT_EQ(0, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, NoVersion) {
    ApplicationVersion version("parquet-mr (build abcd)");

    ASSERT_EQ("parquet-mr (build abcd)", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(0, version.version.major);
    ASSERT_EQ(0, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionEmpty) {
    ApplicationVersion version("parquet-mr version ");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(0, version.version.major);
    ASSERT_EQ(0, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoMajor) {
    ApplicationVersion version("parquet-mr version .");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(0, version.version.major);
    ASSERT_EQ(0, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionInvalidMajor) {
    ApplicationVersion version("parquet-mr version x1");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(0, version.version.major);
    ASSERT_EQ(0, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionMajorOnly) {
    ApplicationVersion version("parquet-mr version 1");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(0, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoMinor) {
    ApplicationVersion version("parquet-mr version 1.");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(0, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionMajorMinorOnly) {
    ApplicationVersion version("parquet-mr version 1.7");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(7, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionInvalidMinor) {
    ApplicationVersion version("parquet-mr version 1.x7");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(0, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoPatch) {
    ApplicationVersion version("parquet-mr version 1.7.");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(7, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionInvalidPatch) {
    ApplicationVersion version("parquet-mr version 1.7.x9");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(7, version.version.minor);
    ASSERT_EQ(0, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoUnknown) {
    ApplicationVersion version("parquet-mr version 1.7.9-cdh5.5.0+cd");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(7, version.version.minor);
    ASSERT_EQ(9, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("cdh5.5.0", version.version.pre_release);
    ASSERT_EQ("cd", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoPreRelease) {
    ApplicationVersion version("parquet-mr version 1.7.9ab+cd");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(7, version.version.minor);
    ASSERT_EQ(9, version.version.patch);
    ASSERT_EQ("ab", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("cd", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoUnknownNoPreRelease) {
    ApplicationVersion version("parquet-mr version 1.7.9+cd");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(7, version.version.minor);
    ASSERT_EQ(9, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("cd", version.version.build_info);
}

TEST(ApplicationVersion, VersionNoUnknownBuildInfoPreRelease) {
    ApplicationVersion version("parquet-mr version 1.7.9+cd-cdh5.5.0");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(7, version.version.minor);
    ASSERT_EQ(9, version.version.patch);
    ASSERT_EQ("", version.version.unknown);
    ASSERT_EQ("", version.version.pre_release);
    ASSERT_EQ("cd-cdh5.5.0", version.version.build_info);
}

TEST(ApplicationVersion, FullWithSpaces) {
    ApplicationVersion version(" parquet-mr \t version \v 1.5.3ab-cdh5.5.0+cd \r (build \n abcd \f) ");

    ASSERT_EQ("parquet-mr", version.application_);
    ASSERT_EQ("abcd", version.build_);
    ASSERT_EQ(1, version.version.major);
    ASSERT_EQ(5, version.version.minor);
    ASSERT_EQ(3, version.version.patch);
    ASSERT_EQ("ab", version.version.unknown);
    ASSERT_EQ("cdh5.5.0", version.version.pre_release);
    ASSERT_EQ("cd", version.version.build_info);
}

} // namespace starrocks::parquet
