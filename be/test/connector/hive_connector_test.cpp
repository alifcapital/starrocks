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

#include "connector/hive_connector.h"

#include <gtest/gtest.h>

#include <algorithm>

#include "column/chunk.h"
#include "exec/pipeline/fragment_context.h"
#include "exprs/expr_context.h"
#include "fs/fs.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/exprs_test_helper.h"
#include "util/defer_op.h"

namespace starrocks::connector {

class HiveConnectorTest : public ::testing::Test {
public:
    void SetUp() override { _exec_env = ExecEnv::GetInstance(); }

protected:
    void check_global_dict_slot_conjunct(const std::string& file, size_t file_rows, const std::string& value);

    ExecEnv* _exec_env = nullptr;
};

void HiveConnectorTest::check_global_dict_slot_conjunct(const std::string& file, size_t file_rows,
                                                        const std::string& value) {
    TQueryOptions options;
    options.__set_enable_scan_datacache(false);
    options.__set_enable_connector_split_io_tasks(false);
    RuntimeState state(TUniqueId{}, options, TQueryGlobals{}, _exec_env);
    state.init_instance_mem_tracker();
    auto* pool = state.obj_pool();
    auto* fragment = pool->add(new pipeline::FragmentContext());
    fragment->set_pred_tree_params({true, true});
    state.set_fragment_ctx(fragment);

    // Use lexicographically ordered codes, deliberately different from the numeric strings.
    TGlobalDict dict;
    dict.__set_columnId(1);
    for (int i = 0; i < 100; ++i) {
        dict.strings.emplace_back(std::to_string(i));
    }
    std::sort(dict.strings.begin(), dict.strings.end());
    for (int i = 0; i < 100; ++i) {
        dict.ids.emplace_back(i + 1);
    }
    ASSERT_OK(state.init_query_global_dict({dict}));
    const int32_t expected_code = state.get_query_global_dict_map().at(1).first.at(Slice(value));

    TDescriptorTableBuilder descriptors;
    TTupleDescriptorBuilder tuple;
    TSlotDescriptorBuilder slot;
    tuple.add_slot(slot.type(TYPE_INT).column_name("c0").column_pos(0).nullable(true).build());
    tuple.add_slot(slot.type(TYPE_INT).column_name("c2").column_pos(2).nullable(true).build());
    tuple.build(&descriptors);
    DescriptorTbl* desc_tbl = nullptr;
    ASSERT_OK(DescriptorTbl::create(&state, pool, descriptors.desc_tbl(), &desc_tbl, state.chunk_size()));
    TTableDescriptor table;
    table.__set_fileTable(TFileTable{});
    desc_tbl->get_tuple_descriptor(0)->set_table_desc(pool->add(new FileTableDescriptor(table, pool)));
    state.set_desc_tbl(desc_tbl);

    THdfsScanNode node;
    node.__set_tuple_id(0);
    node.__set_hive_column_names({"c0", "c2"});
    HiveDataSourceProvider provider(nullptr, node);
    auto size = FileSystem::Default()->get_file_size(file);
    ASSERT_TRUE(size.ok()) << size.status();
    THdfsScanRange range;
    range.__set_full_path(file);
    range.__set_file_format(THdfsFileFormat::PARQUET);
    range.__set_offset(0);
    range.__set_length(size.value());
    range.__set_file_length(size.value());

    auto read_codes = [&](const std::vector<ExprContext*>& predicates) -> StatusOr<std::vector<int32_t>> {
        HiveDataSource source(&provider, range);
        source.set_runtime_profile(state.runtime_profile());
        source.set_predicates(predicates);
        DeferOp close([&] { source.close(&state); });
        RETURN_IF_ERROR(source.open(&state));
        std::vector<int32_t> codes;
        while (true) {
            ChunkPtr chunk;
            auto status = source.get_next(&state, &chunk);
            if (status.is_end_of_file()) {
                break;
            }
            RETURN_IF_ERROR(status);
            const auto& column = chunk->get_column_by_slot_id(1);
            for (size_t row = 0; row < chunk->num_rows(); ++row) {
                // Code 0 represents NULL and cannot match this equality predicate.
                codes.emplace_back(column->is_null(row) ? 0 : column->get(row).get_int32());
            }
        }
        return codes;
    };

    auto all = read_codes({});
    ASSERT_TRUE(all.ok()) << all.status();
    ASSERT_EQ(file_rows, all->size());
    const auto matches = std::count(all->begin(), all->end(), expected_code);
    ASSERT_GT(matches, 0);
    ASSERT_LT(matches, all->size());

    TExprNode mapping;
    mapping.__set_node_type(TExprNodeType::DICT_EXPR);
    mapping.__set_type(gen_type_desc(TPrimitiveType::BOOLEAN));
    mapping.__set_num_children(2);
    mapping.__set_is_nullable(true);
    mapping.__set_has_nullable_child(true);
    TExprNode placeholder;
    placeholder.__set_node_type(TExprNodeType::PLACEHOLDER_EXPR);
    placeholder.__set_type(gen_type_desc(TPrimitiveType::VARCHAR));
    placeholder.__set_num_children(0);
    placeholder.__set_is_nullable(true);
    TPlaceHolder ref;
    ref.__set_slot_id(1);
    ref.__set_nullable(true);
    placeholder.__set_vslot_ref(ref);
    TExpr predicate;
    predicate.nodes = {mapping, ExprsTestHelper::create_slot_expr_node_t<TYPE_INT>(0, 1, true),
                       ExprsTestHelper::create_binary_pred_node(TPrimitiveType::VARCHAR, TExprOpcode::EQ), placeholder,
                       ExprsTestHelper::create_literal<TYPE_VARCHAR, std::string>(value, false)};
    std::vector<ExprContext*> contexts;
    ASSERT_OK(Expr::create_expr_trees(pool, {predicate}, &contexts, &state));
    DeferOp close_contexts([&] { Expr::close(contexts, &state); });
    ASSERT_OK(Expr::prepare(contexts, &state));
    // Connector scans defer rewriting until the data source decomposes the predicates.
    // Do not call rewrite_conjuncts here: removing the production by_slot loop must fail this test.
    DictOptimizeParser::disable_open_rewrite(&contexts);
    ASSERT_OK(Expr::open(contexts, &state));
    auto filtered = read_codes(contexts);
    ASSERT_TRUE(filtered.ok()) << filtered.status();
    ASSERT_EQ(matches, filtered->size());
    ASSERT_TRUE(std::all_of(filtered->begin(), filtered->end(), [&](int32_t code) { return code == expected_code; }));
}

TEST_F(HiveConnectorTest, global_dict_slot_conjunct_plain_parquet) {
    check_global_dict_slot_conjunct("./be/test/formats/parquet/test_data/low_rows_non_dict.parquet", 100, "7");
}

TEST_F(HiveConnectorTest, global_dict_slot_conjunct_dictionary_parquet) {
    check_global_dict_slot_conjunct("./be/test/formats/parquet/test_data/page_index_small_page.parquet", 20000, "2");
}

// Test HiveConnector type
TEST_F(HiveConnectorTest, test_connector_type) {
    HiveConnector connector;
    EXPECT_EQ(connector.connector_type(), ConnectorType::HIVE);
}

// Test HiveDataSourceProvider creates data source
TEST_F(HiveConnectorTest, test_create_data_source) {
    THdfsScanNode hdfs_scan_node;
    HiveDataSourceProvider provider(nullptr, hdfs_scan_node);

    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(THdfsScanRange());

    auto data_source = provider.create_data_source(scan_range);
    EXPECT_NE(data_source, nullptr);
    EXPECT_EQ(data_source->name(), "HiveDataSource");
}

// Test open with no data (file_length = 0) - covers early return path
TEST_F(HiveConnectorTest, test_open_no_data) {
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__set_tuple_id(0);
    HiveDataSourceProvider provider(nullptr, hdfs_scan_node);

    THdfsScanRange hdfs_scan_range;
    hdfs_scan_range.file_length = 0; // Triggers early return before _check_all_slots_nullable

    auto data_source = std::make_unique<HiveDataSource>(&provider, hdfs_scan_range);

    TUniqueId fragment_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    auto runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, _exec_env);
    TUniqueId id;
    runtime_state->init_mem_trackers(id);

    auto status = data_source->open(runtime_state.get());
    EXPECT_TRUE(status.ok());
}

// Test bucket properties constructor
TEST_F(HiveConnectorTest, test_bucket_properties) {
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__isset.bucket_properties = true;

    HiveDataSourceProvider provider(nullptr, hdfs_scan_node);

    TScanRange scan_range;
    scan_range.__set_hdfs_scan_range(THdfsScanRange());

    auto data_source = provider.create_data_source(scan_range);
    EXPECT_NE(data_source, nullptr);
}

// Test extended column index
TEST_F(HiveConnectorTest, test_extended_column_index) {
    THdfsScanNode hdfs_scan_node;
    hdfs_scan_node.__isset.extended_slot_ids = true;
    hdfs_scan_node.extended_slot_ids = {10, 20, 30};

    HiveDataSourceProvider provider(nullptr, hdfs_scan_node);

    THdfsScanRange hdfs_scan_range;
    auto data_source = std::make_unique<HiveDataSource>(&provider, hdfs_scan_range);

    EXPECT_EQ(data_source->extended_column_index(10), 0);
    EXPECT_EQ(data_source->extended_column_index(20), 1);
    EXPECT_EQ(data_source->extended_column_index(30), 2);
    EXPECT_EQ(data_source->extended_column_index(99), -1); // Not found
}

// Test scan_range_indicate_const_column_index
TEST_F(HiveConnectorTest, test_scan_range_indicate_const_column_index) {
    THdfsScanNode hdfs_scan_node;

    HiveDataSourceProvider provider(nullptr, hdfs_scan_node);

    THdfsScanRange hdfs_scan_range;
    hdfs_scan_range.__isset.identity_partition_slot_ids = true;
    hdfs_scan_range.identity_partition_slot_ids = {5, 10, 15};

    auto data_source = std::make_unique<HiveDataSource>(&provider, hdfs_scan_range);

    EXPECT_EQ(data_source->scan_range_indicate_const_column_index(5), 0);
    EXPECT_EQ(data_source->scan_range_indicate_const_column_index(10), 1);
    EXPECT_EQ(data_source->scan_range_indicate_const_column_index(15), 2);
    EXPECT_EQ(data_source->scan_range_indicate_const_column_index(99), -1); // Not found
}

} // namespace starrocks::connector
