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

#include "storage/runtime_filter_predicate.h"

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "exec/partition/bucket_aware_partition.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "exprs/runtime_filter_layout.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "testutil/column_test_helper.h"
#include "testutil/exprs_test_helper.h"
#include "util/defer_op.h"

namespace starrocks {
namespace {

constexpr SlotId kProbeSlotId = 1;
constexpr TPlanNodeId kProbeNodeId = 11;

// Three buckets mapped onto three instances in a deliberately non-identity order, so a
// path that forgets the bucketseq_to_instance indirection cannot accidentally agree.
const std::vector<int32_t> kBucketSeqToInstance = {2, 0, 1};

TRuntimeFilterLayout make_bucket_aware_layout(int32_t filter_id) {
    TBucketProperty bucket_property;
    bucket_property.bucket_func = TBucketFunction::MURMUR3_X86_32;
    bucket_property.bucket_num = kBucketSeqToInstance.size();

    TRuntimeFilterLayout layout;
    layout.__set_filter_id(filter_id);
    layout.__set_local_layout(TRuntimeFilterLayoutMode::PIPELINE_BUCKET_LX);
    layout.__set_global_layout(TRuntimeFilterLayoutMode::GLOBAL_BUCKET_1L);
    layout.__set_pipeline_level_multi_partitioned(false);
    layout.__set_num_instances(kBucketSeqToInstance.size());
    layout.__set_num_drivers_per_instance(1);
    layout.__set_bucketseq_to_instance(kBucketSeqToInstance);
    layout.__set_bucket_properties({bucket_property});
    return layout;
}

// What FE emits for a bucket-shuffle join over an Iceberg table with a bucket transform
// under enable_bucket_aware_execution_on_lake.
TRuntimeFilterDescription make_local_hash_bucket_desc(int32_t filter_id) {
    TRuntimeFilterDescription desc;
    desc.__set_filter_id(filter_id);
    desc.__set_has_remote_targets(true);
    desc.__set_build_plan_node_id(5);
    desc.__set_build_join_mode(TRuntimeFilterBuildJoinMode::LOCAL_HASH_BUCKET);
    desc.__set_filter_type(TRuntimeFilterBuildType::JOIN_FILTER);
    desc.__set_layout(make_bucket_aware_layout(filter_id));
    desc.__set_plan_node_id_to_target_expr(
            {{kProbeNodeId, ExprsTestHelper::create_column_ref_t_expr<TYPE_INT>(kProbeSlotId, true)}});
    return desc;
}

// The sub-filter index the operator-level probe assigns to each row: the murmur3 bucket
// transform, then bucketseq_to_instance.
std::vector<uint32_t> operator_partitions(const RuntimeFilterLayout& layout, const Column* column) {
    std::vector<uint32_t> hash_values;
    std::vector<uint32_t> round_hashes;
    std::vector<uint32_t> bucket_ids;
    std::vector<uint32_t> round_ids;
    BucketAwarePartitionCtx bctx(layout.bucket_properties(), hash_values, round_hashes, bucket_ids, round_ids);
    calc_hash_values_and_bucket_ids({column}, bctx);

    std::vector<uint32_t> partitions;
    partitions.reserve(bucket_ids.size());
    for (uint32_t bucket_id : bucket_ids) {
        EXPECT_LT(bucket_id, kBucketSeqToInstance.size());
        partitions.push_back(static_cast<uint32_t>(kBucketSeqToInstance[bucket_id]));
    }
    return partitions;
}

} // namespace

class RuntimeFilterPredicateTest : public ::testing::Test {
protected:
    static ColumnPtr make_probe_column(size_t num_rows) {
        auto column = Int32Column::create();
        for (size_t i = 0; i < num_rows; ++i) {
            column->append(static_cast<int32_t>(i) * 7 + 3);
        }
        return column;
    }

    RuntimeFilterProbeDescriptor* make_desc(int32_t filter_id) {
        auto* desc = _pool.add(new RuntimeFilterProbeDescriptor());
        CHECK(desc->init(&_pool, make_local_hash_bucket_desc(filter_id), kProbeNodeId, &_runtime_state).ok());
        return desc;
    }

    // A partitioned bloom filter whose sub-filter i holds exactly the values the
    // operator-level probe routes to partition i -- i.e. the filter a correct build side
    // would have produced.
    TRuntimeBloomFilter<TYPE_INT>* build_partitioned_filter(const RuntimeFilterLayout& layout, const Column* column) {
        const auto partitions = operator_partitions(layout, column);
        const auto& data = down_cast<const Int32Column*>(column)->get_data();

        auto* filter = _pool.add(new TRuntimeBloomFilter<TYPE_INT>());
        filter->set_global();
        for (uint32_t p = 0; p < kBucketSeqToInstance.size(); ++p) {
            auto* part = _pool.add(new TRuntimeBloomFilter<TYPE_INT>());
            part->init(1024);
            for (size_t i = 0; i < data.size(); ++i) {
                if (partitions[i] == p) {
                    part->insert(data[i]);
                }
            }
            filter->concat(part);
        }
        // After the concats: RuntimeMembershipFilter::concat() copies the join mode off the
        // filter being absorbed, which would otherwise reset this back to NONE.
        filter->set_join_mode(TRuntimeFilterBuildJoinMode::LOCAL_HASH_BUCKET);
        return filter;
    }

    // Runs RuntimeFilterPredicates over rows that an earlier stage thinned out and checks that only the rows
    // of `expected` stay selected. It covers the sample and the normal stage, in the branchless and in the
    // selection mode.
    //   row    0     1     2     3     4     5     6    7   8..15
    //   value  10    NULL  NULL  100   200   300   NULL 20  1000
    //   input  1     0     1     1     1     1     0    0   1
    //   expected: rows 0 and 2.
    // The filter holds 10 and 20 and accepts NULL, so the selected NULL row 2 stays selected and the dropped NULL
    // rows 1 and 6 stay dropped. Most selected rows are out of the filter range, so the filter is selective
    // enough to be kept after the sample stage.
    void check_null_rows_stay_dropped(const RuntimeFilter* filter, int32_t filter_id) {
        auto* desc = _pool.add(new RuntimeFilterProbeDescriptor());
        ASSERT_OK(desc->init(filter_id, nullptr));
        desc->set_runtime_filter(filter);

        RuntimeFilterPredicate pred(desc, kProbeSlotId);
        ASSERT_TRUE(pred.init(0 /*driver_sequence*/));

        std::vector<int32_t> values = {10, 100, 100, 100, 200, 300, 100, 20};
        std::vector<uint8_t> nulls = {0, 1, 1, 0, 0, 0, 1, 0};
        std::vector<uint8_t> input = {1, 0, 1, 1, 1, 1, 0, 0};
        for (int i = 0; i < 8; ++i) {
            values.push_back(1000);
            nulls.push_back(0);
            input.push_back(1);
        }
        std::vector<uint8_t> expected(input.size(), 0);
        expected[0] = 1;
        expected[2] = 1;

        ColumnPtr column = ColumnTestHelper::build_nullable_column<int32_t>(values, nulls);
        auto chunk = std::make_shared<Chunk>();
        chunk->append_column(column, static_cast<ColumnId>(kProbeSlotId), true);

        const auto old_sample_rows = config::rf_sample_rows;
        const auto old_branchless_ratio = config::rf_branchless_ratio;
        DeferOp restore_config([&] {
            config::rf_sample_rows = old_sample_rows;
            config::rf_branchless_ratio = old_branchless_ratio;
        });
        // One chunk ends the sample stage, so the second evaluation runs the normal stage.
        config::rf_sample_rows = 1;

        // A ratio of 0 makes every chunk take the branchless path, a large ratio makes it take the selection path.
        for (int64_t branchless_ratio : {0, 1000}) {
            config::rf_branchless_ratio = branchless_ratio;
            RuntimeFilterPredicates preds(0 /*driver_sequence*/);
            preds.add_predicate(&pred);

            std::vector<uint8_t> selection = input;
            ASSERT_OK(preds.evaluate(chunk.get(), selection.data(), 0, static_cast<uint16_t>(selection.size())));
            EXPECT_EQ(selection, expected) << "sample stage, branchless_ratio=" << branchless_ratio;

            ASSERT_EQ(preds._state, RuntimeFilterPredicates::NORMAL);
            ASSERT_FALSE(preds._selectivity_map.empty());
            selection = input;
            ASSERT_OK(preds.evaluate(chunk.get(), selection.data(), 0, static_cast<uint16_t>(selection.size())));
            EXPECT_EQ(selection, expected) << "normal stage, branchless_ratio=" << branchless_ratio;
        }
    }

    ObjectPool _pool;
    RuntimeState _runtime_state;
};

// The invariant the whole bucket-aware pushdown rests on: the storage-layer predicate must
// resolve the same sub-filter as the operator-level probe. A bloom filter has no false
// negatives, so if every row was inserted into the sub-filter its own bucket maps to, every
// row must survive. Resolving a different sub-filter tests the row against a filter that
// never saw it, and it is dropped -- silently, with no error anywhere.
TEST_F(RuntimeFilterPredicateTest, BucketAwareFilterKeepsEveryRowItWasBuiltFrom) {
    auto* desc = make_desc(1);
    ASSERT_FALSE(desc->layout().bucket_properties().empty());

    auto column = make_probe_column(256);
    auto* filter = build_partitioned_filter(desc->layout(), column.get());
    ASSERT_EQ(filter->num_hash_partitions(), kBucketSeqToInstance.size());
    desc->set_runtime_filter(filter);

    RuntimeFilterPredicate pred(desc, kProbeSlotId);
    ASSERT_TRUE(pred.init(0 /*driver_sequence*/));

    // Keyed by ColumnId, matching how the connector path builds the probe chunk:
    // ConnectorPredicateParser::column_id() returns SlotDescriptor::id(), so the
    // predicate's ColumnId is the probe slot id.
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(column, static_cast<ColumnId>(kProbeSlotId), true);

    std::vector<uint8_t> selection(column->size(), 1);
    ASSERT_OK(pred.evaluate(chunk.get(), selection.data(), 0, static_cast<uint16_t>(column->size())));

    // The direct form of the invariant (these tests build with -fno-access-control): the
    // indices the predicate resolved are exactly the operator's.
    EXPECT_EQ(pred._running_ctx.hash_values, operator_partitions(desc->layout(), column.get()));

    for (size_t i = 0; i < selection.size(); ++i) {
        EXPECT_EQ(selection[i], 1) << "row " << i << " was dropped: wrong sub-filter selected";
    }

    // Same invariant through the selective (sel/sel_size) entry point.
    std::vector<uint16_t> sel(column->size());
    for (size_t i = 0; i < sel.size(); ++i) {
        sel[i] = static_cast<uint16_t>(i);
    }
    std::vector<uint16_t> target_sel(column->size());
    uint16_t kept = 0;
    ASSIGN_OR_ABORT(kept, pred.evaluate(chunk.get(), sel.data(), static_cast<uint16_t>(sel.size()), target_sel.data()));
    EXPECT_EQ(kept, column->size());
}

// Documents *why* RuntimeFilterPredicate has to route bucket-aware filters through the
// RunningContext overload: the selection-aware overloads have no bucket_properties branch,
// so they hash with CRC32 and index bucketseq_to_instance[hash % size] instead of
// bucketseq_to_instance[bucket_id]. If this ever starts failing, those overloads have
// gained bucket handling and the routing in RuntimeFilterPredicate can be simplified.
TEST_F(RuntimeFilterPredicateTest, SelectionAwareOverloadsIgnoreBucketProperties) {
    auto* desc = make_desc(2);
    auto column = make_probe_column(256);
    auto* filter = build_partitioned_filter(desc->layout(), column.get());

    RuntimeFilter::RunningContext operator_ctx;
    operator_ctx.use_merged_selection = false;
    operator_ctx.compatibility = false;
    filter->compute_partition_index(desc->layout(), {column.get()}, &operator_ctx);
    ASSERT_EQ(operator_ctx.hash_values, operator_partitions(desc->layout(), column.get()));

    RuntimeFilter::RunningContext selection_ctx;
    selection_ctx.use_merged_selection = false;
    std::vector<uint8_t> selection(column->size(), 1);
    std::vector<uint32_t> selection_hash_values(column->size());
    filter->compute_partition_index(desc->layout(), {column.get()}, selection.data(), 0,
                                    static_cast<uint16_t>(column->size()), selection_hash_values, &selection_ctx);

    EXPECT_NE(operator_ctx.hash_values, selection_hash_values);
}

// A null-safe join puts a NULL into the runtime filter, so the filter accepts NULL. The rows that an earlier
// stage (a conjunct, a delete vector, another runtime filter) dropped must stay dropped, NULL rows included.
TEST_F(RuntimeFilterPredicateTest, NullAcceptingFilterKeepsDroppedNullRowsDropped) {
    auto* filter = _pool.add(new ComposedRuntimeBloomFilter<TYPE_INT>());
    filter->membership_filter().init(100);
    filter->insert(10);
    filter->insert(20);
    filter->insert_null();
    ASSERT_NO_FATAL_FAILURE(check_null_rows_stay_dropped(filter, 3));
}

// The branchless mode evaluates the rows that are still selected. A bloom filter built by merging partial filters
// accepts NULL when one of them does, and the selected NULL row has to stay selected. The second filter has NULL
// only in its membership filter, which the min-max filter must not override.
TEST_F(RuntimeFilterPredicateTest, MergedBloomFilterKeepsSelectedNullRows) {
    auto new_bloom_filter = [&]() {
        auto* filter = _pool.add(new ComposedRuntimeBloomFilter<TYPE_INT>());
        filter->membership_filter().init(100);
        return filter;
    };
    auto* with_null = new_bloom_filter();
    with_null->insert(10);
    with_null->insert_null();
    auto* with_values = new_bloom_filter();
    with_values->insert(20);

    auto* merged = new_bloom_filter();
    merged->merge(with_null);
    merged->merge(with_values);
    ASSERT_TRUE(merged->membership_filter().has_null());
    ASSERT_NO_FATAL_FAILURE(check_null_rows_stay_dropped(merged, 4));

    auto* membership_only = new_bloom_filter();
    membership_only->insert(10);
    membership_only->insert(20);
    membership_only->membership_filter().insert_null();
    ASSERT_FALSE(membership_only->min_max_filter().has_null());
    ASSERT_NO_FATAL_FAILURE(check_null_rows_stay_dropped(membership_only, 5));
}

} // namespace starrocks
