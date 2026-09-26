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

#include <set>

#include "column/column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exprs/column_ref.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "runtime/runtime_state.h"
#include "testutil/exprs_test_helper.h"

namespace starrocks {
namespace {

TRuntimeFilterLayout make_layout(int32_t filter_id) {
    TRuntimeFilterLayout layout;
    layout.__set_filter_id(filter_id);
    layout.__set_local_layout(TRuntimeFilterLayoutMode::SINGLETON);
    layout.__set_global_layout(TRuntimeFilterLayoutMode::GLOBAL_SHUFFLE_1L);
    layout.__set_pipeline_level_multi_partitioned(false);
    layout.__set_num_instances(1);
    layout.__set_num_drivers_per_instance(1);
    return layout;
}

// A singleton (non-multi-partitioned) build descriptor with a consumer, used to drive the merger.
TRuntimeFilterDescription make_merger_desc(int32_t filter_id, bool remote) {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(filter_id);
    desc.__set_expr_order(0);
    desc.__set_has_remote_targets(remote);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::PARTITIONED);
    desc.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    desc.__set_build_expr(ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(2, true));
    desc.__set_layout(make_layout(filter_id));
    desc.__set_plan_node_id_to_target_expr({{11, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(3, true)}});
    return desc;
}

} // namespace

class RuntimeFilterMergerTest : public ::testing::Test {
protected:
    ObjectPool pool;
    RuntimeState runtime_state;

    ColumnPtr column(int distinct, int rows = 1000) {
        auto data = Int32Column::create();
        data->append(0);
        for (int i = 0; i < rows; ++i) data->append(i % distinct);
        return data;
    }

    RuntimeFilterBuildDescriptor* merge(
            const std::vector<ColumnPtr>& columns, bool remote, size_t cap, bool partitioned = false,
            bool disjoint_counts = false,
            int function_version = TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3, bool with_null = false) {
        pipeline::PartialRuntimeFilterMerger merger(&pool, cap, cap, function_version, false);
        auto description = make_merger_desc(7, remote);
        if (partitioned) {
            description.layout.__set_pipeline_level_multi_partitioned(true);
            description.layout.__set_local_layout(TRuntimeFilterLayoutMode::PIPELINE_SHUFFLE);
        }
        if (!disjoint_counts) {
            std::set<int32_t> values;
            for (const auto& input : columns) {
                const auto& data = down_cast<const Int32Column*>(input.get())->get_data();
                values.insert(data.begin() + 1, data.end());
            }
            description.__set_estimated_build_ndv(values.size());
        }
        auto* desc = pool.add(new RuntimeFilterBuildDescriptor());
        CHECK(desc->init(&pool, description, &runtime_state).ok());
        for (size_t i = 0; i < columns.size(); ++i) merger.incr_builder();
        for (size_t i = 0; i < columns.size(); ++i) {
            Columns inputs{columns[i]};
            const auto& data = down_cast<const Int32Column*>(columns[i].get())->get_data();
            size_t ndv =
                    desc->estimate_local_ndv(data.size() - 1, std::set<int32_t>(data.begin() + 1, data.end()).size());
            MutableRuntimeFilterPtr partial;
            if (partitioned) {
                partial.reset(RuntimeFilterHelper::create_runtime_bloom_filter(nullptr, TYPE_INT, desc->join_mode()));
                partial->get_membership_filter()->init(ndv);
                CHECK(RuntimeFilterHelper::fill_runtime_filter(inputs, TYPE_INT, partial.get(), 1, false).ok());
                if (with_null) down_cast<ComposedRuntimeBloomFilter<TYPE_INT>*>(partial.get())->insert_null();
            }
            pipeline::OpTRuntimeBloomFilterBuildParams params;
            params.emplace_back(pipeline::RuntimeMembershipFilterBuildParam(partitioned, false, false, inputs,
                                                                            std::move(partial), TYPE_INT_DESC));
            params.back()->ndv = ndv;
            auto result = merger.add_partial_filters(i, columns[i]->size() - 1, {}, std::move(params), {desc});
            CHECK(result.ok());
            EXPECT_EQ(result.value(), i + 1 == columns.size());
        }
        return desc;
    }
};

TEST_F(RuntimeFilterMergerTest, LowNdvKeepsBloomAndAllBuildValues) {
    auto* desc = merge({column(10)}, true, 20);
    ASSERT_NE(desc->runtime_filter(), nullptr);
    auto* filter = down_cast<ComposedRuntimeBloomFilter<TYPE_INT>*>(desc->runtime_filter());
    EXPECT_EQ(10, filter->membership_filter().size());
    EXPECT_TRUE(filter->membership_filter().can_use_bf());
    for (int i = 0; i < 10; ++i) EXPECT_TRUE(filter->membership_filter().test_data(i));
}

TEST_F(RuntimeFilterMergerTest, GlobalAndLocalCaps) {
    auto* remote = merge({column(30)}, true, 20);
    ASSERT_NE(remote->runtime_filter(), nullptr);
    EXPECT_FALSE(remote->runtime_filter()->get_membership_filter()->can_use_bf());
    EXPECT_EQ(nullptr, merge({column(30)}, false, 20)->runtime_filter());
    EXPECT_NE(nullptr, merge({column(10)}, false, 20)->runtime_filter());
}

TEST_F(RuntimeFilterMergerTest, SingletonUsesFeBoundForOverlappingComponents) {
    auto* desc = merge({column(10), column(10)}, true, 15);
    ASSERT_NE(desc->runtime_filter(), nullptr);
    EXPECT_EQ(10, desc->runtime_filter()->get_membership_filter()->size());
    EXPECT_TRUE(desc->runtime_filter()->get_membership_filter()->can_use_bf());
}

TEST_F(RuntimeFilterMergerTest, PartitionedFilterKeepsSeparateBitArrays) {
    auto* desc = merge({column(10), column(10)}, true, 30, true);
    ASSERT_NE(desc->runtime_filter(), nullptr);
    EXPECT_EQ(2, desc->runtime_filter()->num_hash_partitions());
    EXPECT_EQ(20, desc->runtime_filter()->get_membership_filter()->size());
    EXPECT_TRUE(desc->runtime_filter()->get_membership_filter()->can_use_bf());
    auto input = column(10);
    RuntimeFilter::RunningContext context;
    context.use_merged_selection = false;
    context.selection.assign(input->size(), 1);
    context.hash_values.resize(input->size());
    for (size_t i = 0; i < input->size(); ++i) context.hash_values[i] = i % 2;
    desc->runtime_filter()->evaluate(input.get(), &context);
    for (auto selected : context.selection) EXPECT_EQ(1, selected);
}

TEST_F(RuntimeFilterMergerTest, SingleKeyDisjointCountsUseHashTableEstimates) {
    auto first = column(10);
    auto second = Int32Column::create();
    second->append(0);
    for (int i = 0; i < 1000; ++i) second->append(10 + i % 10);
    for (bool partitioned : {false, true}) {
        auto* desc = merge({first, second}, true, 20, partitioned, true);
        ASSERT_NE(nullptr, desc->runtime_filter());
        EXPECT_EQ(20, desc->runtime_filter()->get_membership_filter()->size());
        EXPECT_TRUE(desc->runtime_filter()->get_membership_filter()->can_use_bf());
        if (!partitioned) {
            auto* filter = down_cast<ComposedRuntimeBloomFilter<TYPE_INT>*>(desc->runtime_filter());
            for (int value = 0; value < 20; ++value) EXPECT_TRUE(filter->membership_filter().test_data(value));
        }
        auto* capped = merge({first, second}, true, 19, partitioned, true);
        ASSERT_NE(nullptr, capped->runtime_filter());
        EXPECT_FALSE(capped->runtime_filter()->get_membership_filter()->can_use_bf());
    }
}

TEST_F(RuntimeFilterMergerTest, PartitionedGlobalCapRetainsMinMaxAcrossWireVersions) {
    for (int version : {TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_2,
                        TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3}) {
        auto* desc = merge({column(10), column(10)}, true, 19, true, false, version, true);
        auto* filter = desc->runtime_filter();
        ASSERT_NE(nullptr, filter);
        EXPECT_FALSE(filter->get_membership_filter()->can_use_bf());
        EXPECT_EQ(0, filter->get_membership_filter()->bf_alloc_size());
        int wire_version =
                version == TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3 ? RF_VERSION_V3 : RF_VERSION_V2;
        std::vector<uint8_t> buffer(RuntimeFilterHelper::max_runtime_filter_serialized_size(wire_version, filter));
        auto size = RuntimeFilterHelper::serialize_runtime_filter(wire_version, filter, buffer.data());
        RuntimeFilter* decoded = nullptr;
        RuntimeFilterHelper::deserialize_runtime_filter(&pool, &decoded, buffer.data(), size);
        ASSERT_NE(nullptr, decoded);
        auto input = NullableColumn::create(Int32Column::create(), NullColumn::create());
        input->append_datum(Datum(0));
        input->append_datum(Datum(9));
        input->append_datum(Datum(100));
        input->append_nulls(1);
        for (auto* candidate : {filter, decoded}) {
            EXPECT_TRUE(candidate->has_null());
            RuntimeFilter::RunningContext context;
            context.use_merged_selection = false;
            context.selection.assign(input->size(), 1);
            context.hash_values.assign(input->size(), 0);
            candidate->evaluate(input.get(), &context);
            EXPECT_EQ((std::vector<uint8_t>{1, 1, 0, 1}),
                      std::vector<uint8_t>(context.selection.begin(), context.selection.end()));
        }
    }
}

TEST_F(RuntimeFilterMergerTest, SizedFilterSurvivesWireRoundTrip) {
    auto* desc = merge({column(10), column(10)}, true, 15);
    for (int version : {RF_VERSION_V2, RF_VERSION_V3}) {
        std::vector<uint8_t> buffer(
                RuntimeFilterHelper::max_runtime_filter_serialized_size(version, desc->runtime_filter()));
        auto size = RuntimeFilterHelper::serialize_runtime_filter(version, desc->runtime_filter(), buffer.data());
        RuntimeFilter* decoded = nullptr;
        RuntimeFilterHelper::deserialize_runtime_filter(&pool, &decoded, buffer.data(), size);
        ASSERT_NE(nullptr, decoded);
        auto* filter = down_cast<ComposedRuntimeBloomFilter<TYPE_INT>*>(decoded);
        for (int key = 0; key < 10; ++key) EXPECT_TRUE(filter->membership_filter().test_data(key));
    }
}

TEST_F(RuntimeFilterMergerTest, MissingPartialDisablesFilter) {
    pipeline::PartialRuntimeFilterMerger merger(&pool, 100, 100,
                                                TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3, false);
    merger.incr_builder();
    merger.incr_builder();
    auto* desc = pool.add(new RuntimeFilterBuildDescriptor());
    ASSERT_TRUE(desc->init(&pool, make_merger_desc(7, false), &runtime_state).ok());
    Columns columns{column(10)};
    pipeline::OpTRuntimeBloomFilterBuildParams present;
    present.emplace_back(
            pipeline::RuntimeMembershipFilterBuildParam(false, false, false, columns, nullptr, TYPE_INT_DESC));
    present.back()->ndv = 10;
    ASSERT_TRUE(merger.add_partial_filters(0, 1000, {}, std::move(present), {desc}).ok());
    pipeline::OpTRuntimeBloomFilterBuildParams missing(1);
    ASSERT_TRUE(merger.add_partial_filters(1, 1000, {}, std::move(missing), {desc}).ok());
    EXPECT_EQ(nullptr, desc->runtime_filter());
}

TEST_F(RuntimeFilterMergerTest, FeComponentEstimateIsBoundedByLocalRows) {
    auto description = make_merger_desc(7, true);
    description.__set_estimated_build_ndv(30);
    RuntimeFilterBuildDescriptor desc;
    ASSERT_TRUE(desc.init(&pool, description, &runtime_state).ok());
    EXPECT_EQ(30, desc.estimate_local_ndv(1000, 900));
    EXPECT_EQ(5, desc.estimate_local_ndv(5, 900));
    // A NULL in another component may exclude tuples while this component still enters the RF.
    EXPECT_EQ(30, desc.estimate_local_ndv(1000, 1));
    description.__isset.estimated_build_ndv = false;
    ASSERT_TRUE(desc.init(&pool, description, &runtime_state).ok());
    EXPECT_EQ(900, desc.estimate_local_ndv(1000, 900));
    description.__set_estimated_build_ndv(-1);
    ASSERT_TRUE(desc.init(&pool, description, &runtime_state).ok());
    EXPECT_EQ(900, desc.estimate_local_ndv(1000, 900));
}

TEST_F(RuntimeFilterMergerTest, EachConjunctUsesItsOwnDistinctCount) {
    pipeline::PartialRuntimeFilterMerger merger(&pool, 10, 10,
                                                TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3, false);
    merger.incr_builder();
    pipeline::RuntimeMembershipFilters descriptors;
    pipeline::OpTRuntimeBloomFilterBuildParams params;
    for (int i = 0; i < 2; ++i) {
        auto description = make_merger_desc(i, false);
        description.__set_expr_order(i);
        auto* desc = pool.add(new RuntimeFilterBuildDescriptor());
        ASSERT_TRUE(desc->init(&pool, description, &runtime_state).ok());
        descriptors.push_back(desc);
        Columns columns{column(i == 0 ? 100 : 2)};
        params.emplace_back(
                pipeline::RuntimeMembershipFilterBuildParam(false, false, false, columns, nullptr, TYPE_INT_DESC));
        params.back()->ndv = i == 0 ? 100 : 2;
    }
    auto* wide = descriptors[0];
    auto* narrow = descriptors[1];
    auto merged = merger.add_partial_filters(0, 1000, {}, std::move(params), std::move(descriptors));
    ASSERT_TRUE(merged.ok());
    ASSERT_TRUE(merged.value());
    EXPECT_EQ(nullptr, wide->runtime_filter());
    ASSERT_NE(nullptr, narrow->runtime_filter());
    EXPECT_EQ(2, narrow->runtime_filter()->get_membership_filter()->size());
}

TEST_F(RuntimeFilterMergerTest, EmptyBuildHasNoDistinctValues) {
    auto empty = Int32Column::create();
    empty->append(0);
    auto* desc = merge({empty}, true, 20);
    ASSERT_NE(nullptr, desc->runtime_filter());
    EXPECT_EQ(0, desc->runtime_filter()->get_membership_filter()->size());
}

TEST_F(RuntimeFilterMergerTest, ExactInStopsAtActualDistinctLimit) {
    auto* ref = pool.add(new ColumnRef(TYPE_INT_DESC, 1));
    VectorizedInConstPredicateBuilder builder(&runtime_state, &pool, ref);
    builder.use_as_join_runtime_filter();
    ASSERT_TRUE(builder.create().ok());
    EXPECT_TRUE(builder.add_values(column(10), 1, 10));
    EXPECT_FALSE(builder.add_values(column(20), 1, 10));
    EXPECT_EQ(11, VectorizedInConstPredicateBuilder::values_count(builder.get_in_const_predicate()->root()));
}

TEST_F(RuntimeFilterMergerTest, ExactInMergeUsesQueryLimitAndActualUnion) {
    for (size_t cap : {size_t{9}, size_t{10}}) {
        pipeline::PartialRuntimeFilterMerger merger(
                &pool, 100, 100, TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3, false, cap);
        merger.incr_builder();
        merger.incr_builder();
        for (size_t i = 0; i < 2; ++i) {
            auto* ref = pool.add(new ColumnRef(TYPE_INT_DESC, 1));
            VectorizedInConstPredicateBuilder builder(&runtime_state, &pool, ref);
            builder.use_as_join_runtime_filter();
            ASSERT_TRUE(builder.create().ok());
            ASSERT_TRUE(builder.add_values(column(10), 1, 10));
            auto result = merger.add_partial_filters(i, 1000, {builder.get_in_const_predicate()}, {}, {});
            ASSERT_TRUE(result.ok());
        }
        EXPECT_EQ(cap == 10 ? 1 : 0, merger.get_total_in_filters().size());
    }
}

TEST_F(RuntimeFilterMergerTest, BloomUnionUsesActualNdvForCap) {
    for (int version : {TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_2,
                        TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3}) {
        for (size_t cap : {size_t{100}, size_t{1}}) {
            pipeline::PartialRuntimeFilterMerger merger(&pool, cap, cap, version, false);
            auto thrift = make_merger_desc(91, true);
            auto* desc = pool.add(new RuntimeFilterBuildDescriptor());
            ASSERT_TRUE(desc->init(&pool, thrift, &runtime_state).ok());
            merger.incr_builder();
            merger.incr_builder();
            for (size_t driver = 0; driver < 2; ++driver) {
                auto data = Int32Column::create();
                data->append(0); // Hash-table sentinel, not a build key.
                for (int value = 1; value <= 50; ++value) data->append(value + driver * 50);
                pipeline::OpTRuntimeBloomFilterBuildParams params;
                params.emplace_back(pipeline::RuntimeMembershipFilterBuildParam(false, false, false, Columns{data},
                                                                                nullptr, TYPE_INT_DESC));
                params.back()->ndv = 50;
                auto result = merger.add_partial_filters(driver, 50, {}, std::move(params), {desc});
                ASSERT_TRUE(result.ok());
                ASSERT_EQ(driver == 1, result.value());
            }
            ASSERT_NE(nullptr, desc->runtime_filter());
            auto* rf = desc->runtime_filter();
            int wire_version = version == TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3 ? RF_VERSION_V3
                                                                                                     : RF_VERSION_V2;
            std::vector<uint8_t> buffer(RuntimeFilterHelper::max_runtime_filter_serialized_size(wire_version, rf));
            auto size = RuntimeFilterHelper::serialize_runtime_filter(wire_version, rf, buffer.data());
            RuntimeFilter* decoded = nullptr;
            RuntimeFilterHelper::deserialize_runtime_filter(&pool, &decoded, buffer.data(), size);
            ASSERT_NE(nullptr, decoded);
            for (auto* candidate : {rf, decoded}) {
                auto data = Int32Column::create();
                for (int value = 0; value <= 101; ++value) data->append(value);
                RuntimeFilter::RunningContext context;
                context.use_merged_selection = false;
                context.selection.assign(102, 1);
                candidate->evaluate(data.get(), &context);
                ASSERT_EQ(100, std::count(context.selection.begin(), context.selection.end(), 1));
                ASSERT_EQ(0, context.selection.front());
                ASSERT_EQ(0, context.selection.back());
                ASSERT_EQ(cap >= 100, candidate->get_membership_filter()->can_use_bf());
            }
        }
    }
}

} // namespace starrocks
