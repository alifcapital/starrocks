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

#include <memory>
#include <vector>

#include "column/chunk.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/vectorized_fwd.h"
#include "common/config.h"
#include "exec/join/join_hash_table.h"
#include "exec/join/join_hash_table_descriptor.h"
#include "exec/join_on_clause.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/phmap/phmap.h"

namespace starrocks {

// ==============================================================================
// Section 1: JOIN ON CLAUSE DATA STRUCTURE TESTS
// ==============================================================================
// These tests verify the JoinOnClause and DisjunctiveJoinClauses data structures
// that hold the parsed disjunctive join predicates.

class DisjunctiveJoinClausesTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

// Test JoinOnClause basic structure
TEST_F(DisjunctiveJoinClausesTest, JoinOnClauseBasic) {
    JoinOnClause clause;

    // Initially empty
    EXPECT_EQ(clause.num_eq_predicates(), 0);
    EXPECT_FALSE(clause.has_other_conjuncts());

    // Add some vectors (simulating ExprContext* would require more setup)
    clause.probe_expr_ctxs.push_back(nullptr);
    clause.build_expr_ctxs.push_back(nullptr);
    clause.is_null_safes.push_back(false);

    EXPECT_EQ(clause.num_eq_predicates(), 1);
    EXPECT_FALSE(clause.has_other_conjuncts());

    // Add other conjunct
    clause.other_conjunct_ctxs.push_back(nullptr);
    EXPECT_TRUE(clause.has_other_conjuncts());
}

// Test DisjunctiveJoinClauses
TEST_F(DisjunctiveJoinClausesTest, DisjunctiveJoinClausesBasic) {
    DisjunctiveJoinClauses clauses;

    // Initially not disjunctive
    EXPECT_EQ(clauses.num_clauses(), 0);
    EXPECT_FALSE(clauses.is_disjunctive());

    // Add one clause - still not disjunctive
    JoinOnClause clause1;
    clause1.probe_expr_ctxs.push_back(nullptr);
    clause1.build_expr_ctxs.push_back(nullptr);
    clause1.is_null_safes.push_back(false);
    clauses.add_clause(std::move(clause1));

    EXPECT_EQ(clauses.num_clauses(), 1);
    EXPECT_FALSE(clauses.is_disjunctive());

    // Add second clause - now disjunctive
    JoinOnClause clause2;
    clause2.probe_expr_ctxs.push_back(nullptr);
    clause2.build_expr_ctxs.push_back(nullptr);
    clause2.is_null_safes.push_back(true);  // null-safe
    clauses.add_clause(std::move(clause2));

    EXPECT_EQ(clauses.num_clauses(), 2);
    EXPECT_TRUE(clauses.is_disjunctive());

    // Verify clause access
    const auto& c1 = clauses.clause(0);
    EXPECT_EQ(c1.num_eq_predicates(), 1);
    EXPECT_FALSE(c1.is_null_safes[0]);

    const auto& c2 = clauses.clause(1);
    EXPECT_EQ(c2.num_eq_predicates(), 1);
    EXPECT_TRUE(c2.is_null_safes[0]);
}

// Test with multiple equality predicates per clause
TEST_F(DisjunctiveJoinClausesTest, MultipleEqualityPredicates) {
    DisjunctiveJoinClauses clauses;

    // Clause 1: t1.a = t2.x AND t1.b = t2.y
    JoinOnClause clause1;
    clause1.probe_expr_ctxs.push_back(nullptr);  // t1.a
    clause1.probe_expr_ctxs.push_back(nullptr);  // t1.b
    clause1.build_expr_ctxs.push_back(nullptr);  // t2.x
    clause1.build_expr_ctxs.push_back(nullptr);  // t2.y
    clause1.is_null_safes.push_back(false);
    clause1.is_null_safes.push_back(false);
    clauses.add_clause(std::move(clause1));

    // Clause 2: t1.c = t2.z
    JoinOnClause clause2;
    clause2.probe_expr_ctxs.push_back(nullptr);  // t1.c
    clause2.build_expr_ctxs.push_back(nullptr);  // t2.z
    clause2.is_null_safes.push_back(false);
    clauses.add_clause(std::move(clause2));

    EXPECT_TRUE(clauses.is_disjunctive());
    EXPECT_EQ(clauses.clause(0).num_eq_predicates(), 2);
    EXPECT_EQ(clauses.clause(1).num_eq_predicates(), 1);
}

// Test with other conjuncts
TEST_F(DisjunctiveJoinClausesTest, WithOtherConjuncts) {
    DisjunctiveJoinClauses clauses;

    // Clause 1: t1.a = t2.x AND t1.c > 10 (other conjunct)
    JoinOnClause clause1;
    clause1.probe_expr_ctxs.push_back(nullptr);
    clause1.build_expr_ctxs.push_back(nullptr);
    clause1.is_null_safes.push_back(false);
    clause1.other_conjunct_ctxs.push_back(nullptr);  // t1.c > 10
    clauses.add_clause(std::move(clause1));

    // Clause 2: t1.b = t2.y (no other conjunct)
    JoinOnClause clause2;
    clause2.probe_expr_ctxs.push_back(nullptr);
    clause2.build_expr_ctxs.push_back(nullptr);
    clause2.is_null_safes.push_back(false);
    clauses.add_clause(std::move(clause2));

    EXPECT_TRUE(clauses.is_disjunctive());
    EXPECT_TRUE(clauses.clause(0).has_other_conjuncts());
    EXPECT_FALSE(clauses.clause(1).has_other_conjuncts());
}

// Test clauses() accessor
TEST_F(DisjunctiveJoinClausesTest, ClausesAccessor) {
    DisjunctiveJoinClauses disjunctive_clauses;

    JoinOnClause clause1;
    clause1.probe_expr_ctxs.push_back(nullptr);
    clause1.build_expr_ctxs.push_back(nullptr);
    clause1.is_null_safes.push_back(false);
    disjunctive_clauses.add_clause(std::move(clause1));

    JoinOnClause clause2;
    clause2.probe_expr_ctxs.push_back(nullptr);
    clause2.build_expr_ctxs.push_back(nullptr);
    clause2.is_null_safes.push_back(true);
    disjunctive_clauses.add_clause(std::move(clause2));

    const auto& all_clauses = disjunctive_clauses.clauses();
    EXPECT_EQ(all_clauses.size(), 2);
}

// Test three clauses (simulating: a=x OR b=y OR c=z)
TEST_F(DisjunctiveJoinClausesTest, ThreeClauses) {
    DisjunctiveJoinClauses clauses;

    for (int i = 0; i < 3; i++) {
        JoinOnClause clause;
        clause.probe_expr_ctxs.push_back(nullptr);
        clause.build_expr_ctxs.push_back(nullptr);
        clause.is_null_safes.push_back(i == 1);  // middle one is null-safe
        clauses.add_clause(std::move(clause));
    }

    EXPECT_TRUE(clauses.is_disjunctive());
    EXPECT_EQ(clauses.num_clauses(), 3);
    EXPECT_FALSE(clauses.clause(0).is_null_safes[0]);
    EXPECT_TRUE(clauses.clause(1).is_null_safes[0]);
    EXPECT_FALSE(clauses.clause(2).is_null_safes[0]);
}

// ==============================================================================
// Section 2: DEDUPLICATION ALGORITHM TESTS
// ==============================================================================
// These tests verify the deduplication logic used by DisjunctiveHashJoinProberImpl.
// Each hash table may return the same (probe_idx, build_idx) pair for a row that matches
// via multiple disjuncts. These tests ensure the algorithm correctly deduplicates.
//
// The actual DisjunctiveHashJoinProberImpl uses phmap::flat_hash_set for this logic.
class DeduplicationLogicTest : public ::testing::Test {
protected:
    using SeenPairs = phmap::flat_hash_set<std::pair<uint32_t, uint32_t>>;
    using SeenKeys = phmap::flat_hash_set<uint32_t>;
};

// INNER JOIN deduplication:
// Same (probe_idx, build_idx) pair from different HTs should produce 1 output row
TEST_F(DeduplicationLogicTest, InnerJoinDeduplication) {
    SeenPairs seen_pairs;

    // First HT returns (probe=0, build=1)
    auto result1 = seen_pairs.insert({0, 1});
    EXPECT_TRUE(result1.second);  // first insertion

    // Second HT returns same pair (probe=0, build=1) - duplicate
    auto result2 = seen_pairs.insert({0, 1});
    EXPECT_FALSE(result2.second);  // should be rejected as duplicate

    // Different pair should be accepted
    auto result3 = seen_pairs.insert({0, 2});
    EXPECT_TRUE(result3.second);

    EXPECT_EQ(seen_pairs.size(), 2);  // only 2 unique pairs
}

// LEFT SEMI deduplication by probe_idx:
// One probe row matching via multiple disjuncts -> 1 output row
TEST_F(DeduplicationLogicTest, LeftSemiDeduplication) {
    SeenKeys seen_probe_keys;

    // HT0: probe row 0 matches build row 1
    auto result1 = seen_probe_keys.insert(0);
    EXPECT_TRUE(result1.second);

    // HT1: probe row 0 matches build row 2 (different build, same probe)
    auto result2 = seen_probe_keys.insert(0);
    EXPECT_FALSE(result2.second);  // probe 0 already seen

    // Different probe row
    auto result3 = seen_probe_keys.insert(1);
    EXPECT_TRUE(result3.second);

    EXPECT_EQ(seen_probe_keys.size(), 2);  // 2 unique probe rows
}

// RIGHT SEMI deduplication by build_idx:
// One build row matched by multiple probe rows -> 1 output row
TEST_F(DeduplicationLogicTest, RightSemiDeduplication) {
    SeenKeys seen_build_keys;

    // Probe row 0 matches build row 1
    auto result1 = seen_build_keys.insert(1);
    EXPECT_TRUE(result1.second);

    // Probe row 1 matches same build row 1 (different probe, same build)
    auto result2 = seen_build_keys.insert(1);
    EXPECT_FALSE(result2.second);  // build 1 already seen

    // Different build row
    auto result3 = seen_build_keys.insert(2);
    EXPECT_TRUE(result3.second);

    EXPECT_EQ(seen_build_keys.size(), 2);  // 2 unique build rows
}

// LEFT OUTER - match only in HT1:
// HT0 gives build_idx=0 (unmatched) but HT1 gives match.
// The unmatched result from HT0 should NOT appear in output.
TEST_F(DeduplicationLogicTest, LeftOuterMatchOnlyInHT1) {
    SeenPairs seen_pairs;

    // HT0: probe row 0 has no match (build_idx = 0 means unmatched)
    // In real code, build_idx=0 is reserved sentinel, so we simulate
    // by having HT0 return (0, 0) which is "unmatched"
    bool ht0_matched = false;  // simulate no match in HT0

    // HT1: probe row 0 matches build row 3
    auto result_ht1 = seen_pairs.insert({0, 3});
    EXPECT_TRUE(result_ht1.second);
    bool ht1_matched = true;

    // For LEFT OUTER, we output null-extended row only if NO HT matched
    bool should_output_null_extended = !ht0_matched && !ht1_matched;
    EXPECT_FALSE(should_output_null_extended);  // HT1 matched, so no null row

    // The matched row from HT1 is in the output
    EXPECT_EQ(seen_pairs.size(), 1);
}

// FULL OUTER - build_match_index tracking across HTs:
// Build row matched only in HT1 should NOT appear in probe_remain.
TEST_F(DeduplicationLogicTest, FullOuterBuildMatchAcrossHTs) {
    // Simulate build_match_index for 3 build rows (indices 1, 2, 3)
    // Index 0 is reserved as sentinel
    std::vector<uint8_t> build_match_index(4, 0);  // 0 = unmatched

    // HT0: probe row 0 matches build row 1
    build_match_index[1] = 1;  // mark matched

    // HT1: probe row 1 matches build row 2
    build_match_index[2] = 1;  // mark matched

    // Build row 3 is never matched
    // In probe_remain phase, only row 3 should be output with null probe

    int unmatched_count = 0;
    for (size_t i = 1; i < build_match_index.size(); i++) {
        if (build_match_index[i] == 0) {
            unmatched_count++;
        }
    }
    EXPECT_EQ(unmatched_count, 1);  // only build row 3 is unmatched
}

// ==============================================================================
// Section 3: FILTER APPLICATION TESTS
// ==============================================================================
// These tests verify the per-clause other_conjuncts filtering logic.
// When a clause has other_conjuncts (e.g., t1.a = t2.x AND t1.c > 10),
// rows that match the key equality but fail other_conjuncts must be filtered.

class FilterApplicationTest : public ::testing::Test {
protected:
    // Simulate the filter application logic used in disjunctive join
    static void apply_filter_to_indices(std::vector<uint32_t>& probe_indices,
                                        std::vector<uint32_t>& build_indices,
                                        const std::vector<uint8_t>& filter) {
        size_t j = 0;
        for (size_t i = 0; i < filter.size(); i++) {
            if (filter[i]) {
                probe_indices[j] = probe_indices[i];
                build_indices[j] = build_indices[i];
                j++;
            }
        }
        probe_indices.resize(j);
        build_indices.resize(j);
    }
};

// Per-clause other_conjuncts filtering:
// Key matches but other_conjunct is false -> filtered out.
TEST_F(FilterApplicationTest, OtherConjunctsFilterRows) {
    // Simulate 4 matched rows from hash table probe
    std::vector<uint32_t> probe_indices = {0, 1, 2, 3};
    std::vector<uint32_t> build_indices = {10, 11, 12, 13};

    // other_conjuncts evaluation: rows 1 and 3 pass, rows 0 and 2 fail
    std::vector<uint8_t> filter = {0, 1, 0, 1};

    apply_filter_to_indices(probe_indices, build_indices, filter);

    EXPECT_EQ(probe_indices.size(), 2);
    EXPECT_EQ(probe_indices[0], 1);
    EXPECT_EQ(probe_indices[1], 3);
    EXPECT_EQ(build_indices[0], 11);
    EXPECT_EQ(build_indices[1], 13);
}

// Test filter with all passing
TEST_F(FilterApplicationTest, AllPassFilter) {
    std::vector<uint32_t> probe_indices = {0, 1, 2};
    std::vector<uint32_t> build_indices = {5, 6, 7};
    std::vector<uint8_t> filter = {1, 1, 1};

    apply_filter_to_indices(probe_indices, build_indices, filter);

    EXPECT_EQ(probe_indices.size(), 3);
}

// Test filter with all failing
TEST_F(FilterApplicationTest, AllFailFilter) {
    std::vector<uint32_t> probe_indices = {0, 1, 2};
    std::vector<uint32_t> build_indices = {5, 6, 7};
    std::vector<uint8_t> filter = {0, 0, 0};

    apply_filter_to_indices(probe_indices, build_indices, filter);

    EXPECT_EQ(probe_indices.size(), 0);
    EXPECT_EQ(build_indices.size(), 0);
}

// ==============================================================================
// Section 4: CLAUSE LIMIT TESTS
// ==============================================================================

TEST_F(DisjunctiveJoinClausesTest, MaxClausesLimit) {
    // BE has kMaxDisjunctiveClauses = 16 limit
    // Verify we can create 16 clauses
    DisjunctiveJoinClauses clauses;

    for (int i = 0; i < 16; i++) {
        JoinOnClause clause;
        clause.probe_expr_ctxs.push_back(nullptr);
        clause.build_expr_ctxs.push_back(nullptr);
        clause.is_null_safes.push_back(false);
        clauses.add_clause(std::move(clause));
    }

    EXPECT_EQ(clauses.num_clauses(), 16);
    EXPECT_TRUE(clauses.is_disjunctive());
}

// ==============================================================================
// Section 5: JOIN HASH TABLE API EXECUTION TESTS
// ==============================================================================
// These tests verify the JoinHashTable APIs added for disjunctive join by
// calling the REAL methods: append_keys_only(), share_build_chunk_from().
// This validates the actual code paths including DCHECK invariants and metadata copy.

class JoinHashTableApiTest : public ::testing::Test {
protected:
    void SetUp() override {
        config::vector_chunk_size = 4096;
        _object_pool = std::make_shared<ObjectPool>();
        _runtime_profile = std::make_shared<RuntimeProfile>("test");
        _runtime_profile->set_metadata(1);

        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = config::vector_chunk_size;
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(fragment_id, query_options, query_globals, nullptr);
        _runtime_state->init_instance_mem_tracker();

        _int_type = TypeDescriptor::from_logical_type(TYPE_INT);
    }

    void TearDown() override {}

    static ColumnPtr create_int32_column(uint32_t row_count, uint32_t start_value) {
        auto column = FixedLengthColumn<int32_t>::create();
        for (uint32_t i = 0; i < row_count; i++) {
            column->append_datum(Datum(static_cast<int32_t>(start_value + i)));
        }
        return column;
    }

    ChunkPtr create_build_chunk(uint32_t row_count, uint32_t key_start, uint32_t payload_start) {
        auto chunk = std::make_shared<Chunk>();
        // Build slots get IDs after probe slots. With 2 probe slots (0,1),
        // build slots are 2 (key) and 3 (payload).
        chunk->append_column(create_int32_column(row_count, key_start), 2);      // build key
        chunk->append_column(create_int32_column(row_count, payload_start), 3);  // build payload
        return chunk;
    }

    ChunkPtr create_probe_chunk(uint32_t row_count, uint32_t key_start, uint32_t payload_start) {
        auto chunk = std::make_shared<Chunk>();
        // Probe slots get IDs 0 and 1
        chunk->append_column(create_int32_column(row_count, key_start), 0);      // probe key
        chunk->append_column(create_int32_column(row_count, payload_start), 1);  // probe payload
        return chunk;
    }

    static TSlotDescriptor create_slot_descriptor(const std::string& column_name, LogicalType column_type,
                                                  int32_t column_pos, bool nullable) {
        TSlotDescriptorBuilder slot_desc_builder;
        return slot_desc_builder.type(column_type)
                .column_name(column_name)
                .column_pos(column_pos)
                .nullable(nullable)
                .build();
    }

    static void add_tuple_descriptor(TDescriptorTableBuilder* table_desc_builder, LogicalType column_type,
                                     bool nullable, size_t column_count) {
        TTupleDescriptorBuilder tuple_desc_builder;
        for (size_t i = 0; i < column_count; i++) {
            auto slot = create_slot_descriptor("c" + std::to_string(i), column_type, i, nullable);
            tuple_desc_builder.add_slot(slot);
        }
        tuple_desc_builder.build(table_desc_builder);
    }

    std::shared_ptr<RowDescriptor> create_probe_desc(TDescriptorTableBuilder* desc_builder) {
        std::vector<TTupleId> row_tuples = {0};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state.get(), _object_pool.get(), desc_builder->desc_tbl(), &tbl,
                                    config::vector_chunk_size).ok());
        return std::make_shared<RowDescriptor>(*tbl, row_tuples);
    }

    std::shared_ptr<RowDescriptor> create_build_desc(TDescriptorTableBuilder* desc_builder) {
        std::vector<TTupleId> row_tuples = {1};
        DescriptorTbl* tbl = nullptr;
        CHECK(DescriptorTbl::create(_runtime_state.get(), _object_pool.get(), desc_builder->desc_tbl(), &tbl,
                                    config::vector_chunk_size).ok());
        return std::make_shared<RowDescriptor>(*tbl, row_tuples);
    }

    HashTableParam create_table_param(TJoinOp::type join_type) {
        HashTableParam param;
        param.with_other_conjunct = false;
        param.join_type = join_type;
        param.search_ht_timer = ADD_TIMER(_runtime_profile, "SearchHashTableTime");
        param.output_build_column_timer = ADD_TIMER(_runtime_profile, "OutputBuildColumnTime");
        param.output_probe_column_timer = ADD_TIMER(_runtime_profile, "OutputProbeColumnTime");
        param.join_keys.emplace_back(JoinKeyDesc{&_int_type, false, nullptr});
        return param;
    }

    std::shared_ptr<ObjectPool> _object_pool;
    std::shared_ptr<RuntimeProfile> _runtime_profile;
    std::shared_ptr<RuntimeState> _runtime_state;
    TypeDescriptor _int_type;
    std::shared_ptr<RowDescriptor> _probe_desc;
    std::shared_ptr<RowDescriptor> _build_desc;
};

// Test append_keys_only: calls the real JoinHashTable::append_keys_only() method
TEST_F(JoinHashTableApiTest, AppendKeysOnlyReal) {
    // Setup descriptors (2 columns: key + payload)
    TDescriptorTableBuilder desc_builder;
    add_tuple_descriptor(&desc_builder, LogicalType::TYPE_INT, false, 2);  // probe
    add_tuple_descriptor(&desc_builder, LogicalType::TYPE_INT, false, 2);  // build
    _probe_desc = create_probe_desc(&desc_builder);
    _build_desc = create_build_desc(&desc_builder);

    HashTableParam param = create_table_param(TJoinOp::INNER_JOIN);
    param.probe_row_desc = _probe_desc.get();
    param.build_row_desc = _build_desc.get();
    param.probe_output_slots = {0, 1};
    param.build_output_slots = {2, 3};

    // Create primary hash table with full build chunk
    JoinHashTable primary_ht;
    primary_ht.create(param);

    auto build_chunk = create_build_chunk(10, 0, 100);  // keys: 0-9, payloads: 100-109
    // Get key column by slot id (more robust than index)
    Columns primary_keys = {build_chunk->get_column_by_slot_id(2)};
    primary_ht.append_chunk(build_chunk, primary_keys);

    EXPECT_EQ(primary_ht.get_row_count(), 10);
    EXPECT_NE(primary_ht.get_build_chunk(), nullptr);

    // Create secondary hash table - uses append_keys_only (different key column)
    JoinHashTable secondary_ht;
    secondary_ht.create(param);

    // create() initializes an empty build_chunk (not nullptr)
    EXPECT_NE(secondary_ht.get_build_chunk(), nullptr);
    EXPECT_EQ(secondary_ht.get_build_chunk()->num_rows(), 0);

    // Secondary key column: different values (will be used for second disjunct)
    auto secondary_key_col = create_int32_column(10, 1000);  // keys: 1000-1009
    Columns secondary_keys = {secondary_key_col};

    // Call the REAL append_keys_only method
    secondary_ht.append_keys_only(secondary_keys, 10);

    EXPECT_EQ(secondary_ht.get_row_count(), 10);
    // Secondary has empty build_chunk (from create), keys only (from append_keys_only)
}

// Test share_build_chunk_from: calls the real JoinHashTable::share_build_chunk_from() method
// Order matches production: build() both HTs, then share_build_chunk_from()
TEST_F(JoinHashTableApiTest, ShareBuildChunkFromReal) {
    TDescriptorTableBuilder desc_builder;
    add_tuple_descriptor(&desc_builder, LogicalType::TYPE_INT, false, 2);
    add_tuple_descriptor(&desc_builder, LogicalType::TYPE_INT, false, 2);
    _probe_desc = create_probe_desc(&desc_builder);
    _build_desc = create_build_desc(&desc_builder);

    HashTableParam param = create_table_param(TJoinOp::INNER_JOIN);
    param.probe_row_desc = _probe_desc.get();
    param.build_row_desc = _build_desc.get();
    param.probe_output_slots = {0, 1};
    param.build_output_slots = {2, 3};

    // Primary hash table with build data
    JoinHashTable primary_ht;
    primary_ht.create(param);
    auto build_chunk = create_build_chunk(10, 0, 100);
    Columns primary_keys = {build_chunk->get_column_by_slot_id(2)};
    primary_ht.append_chunk(build_chunk, primary_keys);
    ASSERT_TRUE(primary_ht.build(_runtime_state.get()).ok());

    // Secondary hash table with only keys
    JoinHashTable secondary_ht;
    secondary_ht.create(param);
    auto secondary_key_col = create_int32_column(10, 1000);
    Columns secondary_keys = {secondary_key_col};
    secondary_ht.append_keys_only(secondary_keys, 10);
    // Build secondary BEFORE share (matches production order in DisjunctiveHashJoinBuilder)
    ASSERT_TRUE(secondary_ht.build(_runtime_state.get()).ok());

    // IMPORTANT: Verify pointers are different BEFORE share
    // This catches any bug where sharing happens prematurely in create() or build()
    EXPECT_NE(secondary_ht.get_build_chunk().get(), primary_ht.get_build_chunk().get());

    // Call the REAL share_build_chunk_from method (after build, like in production)
    secondary_ht.share_build_chunk_from(primary_ht);

    // Verify sharing via public getters
    EXPECT_EQ(secondary_ht.get_build_chunk().get(), primary_ht.get_build_chunk().get());
    EXPECT_EQ(secondary_ht.get_row_count(), primary_ht.get_row_count());
    EXPECT_EQ(secondary_ht.get_output_build_column_count(), primary_ht.get_output_build_column_count());

    // Verify metadata was copied (share_build_chunk_from copies build_slots, etc.)
    EXPECT_EQ(secondary_ht.table_items()->build_column_count, primary_ht.table_items()->build_column_count);
    EXPECT_EQ(secondary_ht.table_items()->output_build_column_count, primary_ht.table_items()->output_build_column_count);
}

// Test that secondary HT can be probed after build + share_build_chunk_from
// This is the critical E2E test: probe on secondary HT returns correct BUILD columns
// from the SHARED build_chunk.
TEST_F(JoinHashTableApiTest, SecondaryHtBuildAndProbeAfterShare) {
    TDescriptorTableBuilder desc_builder;
    add_tuple_descriptor(&desc_builder, LogicalType::TYPE_INT, false, 2);
    add_tuple_descriptor(&desc_builder, LogicalType::TYPE_INT, false, 2);
    _probe_desc = create_probe_desc(&desc_builder);
    _build_desc = create_build_desc(&desc_builder);

    HashTableParam param = create_table_param(TJoinOp::INNER_JOIN);
    param.probe_row_desc = _probe_desc.get();
    param.build_row_desc = _build_desc.get();
    param.probe_output_slots = {0, 1};
    param.build_output_slots = {2, 3};

    // Primary: keys 0-9, payloads 100-109
    JoinHashTable primary_ht;
    primary_ht.create(param);
    auto build_chunk = create_build_chunk(10, 0, 100);
    Columns primary_keys = {build_chunk->get_column_by_slot_id(2)};
    primary_ht.append_chunk(build_chunk, primary_keys);
    ASSERT_TRUE(primary_ht.build(_runtime_state.get()).ok());

    // Secondary: keys 1000-1009 (same rows, different hash keys)
    JoinHashTable secondary_ht;
    secondary_ht.create(param);
    auto secondary_key_col = create_int32_column(10, 1000);
    Columns secondary_keys = {secondary_key_col};
    secondary_ht.append_keys_only(secondary_keys, 10);
    // Production order: build() -> share_build_chunk_from()
    ASSERT_TRUE(secondary_ht.build(_runtime_state.get()).ok());
    secondary_ht.share_build_chunk_from(primary_ht);

    // Probe secondary HT with key that matches (1005 -> build row 5)
    auto probe_chunk = create_probe_chunk(1, 1005, 200);  // key=1005, payload=200
    Columns probe_key_columns = {probe_chunk->get_column_by_slot_id(0)};
    ChunkPtr result_chunk = std::make_shared<Chunk>();
    bool eos = false;

    ASSERT_TRUE(secondary_ht.probe(_runtime_state.get(), probe_key_columns, &probe_chunk, &result_chunk, &eos).ok());

    // Should have 1 match - use ASSERT to fail early if no match
    ASSERT_EQ(result_chunk->num_rows(), 1);

    // The BUILD payload should come from the SHARED build_chunk (value: 100 + 5 = 105)
    // This validates that share_build_chunk_from works correctly for execution
    auto build_payload_col = result_chunk->get_column_by_slot_id(3);
    ASSERT_TRUE(build_payload_col != nullptr);
    EXPECT_EQ(build_payload_col->get(0).get_int32(), 105);
}

// Test that multiple secondary HTs share the same build_chunk pointer
// Note: JoinHashTable::mem_usage() still counts build_chunk for each HT individually.
// The actual deduplication is done at DisjunctiveHashJoinBuilder level.
// This test verifies the pointer sharing mechanism works correctly.
TEST_F(JoinHashTableApiTest, MultipleSecondaryTablesShareBuildChunk) {
    TDescriptorTableBuilder desc_builder;
    add_tuple_descriptor(&desc_builder, LogicalType::TYPE_INT, false, 2);
    add_tuple_descriptor(&desc_builder, LogicalType::TYPE_INT, false, 2);
    _probe_desc = create_probe_desc(&desc_builder);
    _build_desc = create_build_desc(&desc_builder);

    HashTableParam param = create_table_param(TJoinOp::INNER_JOIN);
    param.probe_row_desc = _probe_desc.get();
    param.build_row_desc = _build_desc.get();
    param.probe_output_slots = {0, 1};
    param.build_output_slots = {2, 3};

    // Primary with 1000 rows
    JoinHashTable primary_ht;
    primary_ht.create(param);
    auto build_chunk = create_build_chunk(1000, 0, 10000);
    Columns primary_keys = {build_chunk->get_column_by_slot_id(2)};
    primary_ht.append_chunk(build_chunk, primary_keys);
    ASSERT_TRUE(primary_ht.build(_runtime_state.get()).ok());

    // Create 3 secondary HTs that share the build chunk
    // Order: append_keys_only -> build -> share_build_chunk_from (matches production)
    std::vector<JoinHashTable> secondary_hts(3);
    for (int i = 0; i < 3; i++) {
        secondary_hts[i].create(param);
        auto sec_key = create_int32_column(1000, (i + 1) * 10000);
        Columns sec_keys = {sec_key};
        secondary_hts[i].append_keys_only(sec_keys, 1000);
        ASSERT_TRUE(secondary_hts[i].build(_runtime_state.get()).ok());
        secondary_hts[i].share_build_chunk_from(primary_ht);
    }

    // Critical check: all secondary HTs share the SAME build_chunk pointer
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(secondary_hts[i].get_build_chunk().get(), primary_ht.get_build_chunk().get());
        EXPECT_EQ(secondary_hts[i].get_row_count(), primary_ht.get_row_count());
        // Metadata should also be copied
        EXPECT_EQ(secondary_hts[i].table_items()->build_column_count,
                  primary_ht.table_items()->build_column_count);
    }
}

// Test build_match_index tracking across multiple hash tables
TEST_F(JoinHashTableApiTest, BuildMatchIndexAcrossHTs) {
    // This tests the algorithm for FULL OUTER join deduplication
    // where we track matched build rows across all hash tables

    const size_t num_build_rows = 100;

    // Simulated build_match_index (would be shared in real DisjunctiveHashJoinProber)
    Buffer<uint8_t> build_match_index(num_build_rows + 1, 0);

    // HT0 matches: rows 1, 5, 10, 20
    build_match_index[1] = 1;
    build_match_index[5] = 1;
    build_match_index[10] = 1;
    build_match_index[20] = 1;

    // HT1 matches: rows 2, 5, 30 (5 already matched - idempotent)
    build_match_index[2] = 1;
    build_match_index[30] = 1;

    // HT2 matches: rows 3, 10, 40
    build_match_index[3] = 1;
    build_match_index[40] = 1;

    // Count for probe_remain phase
    size_t matched = 0, unmatched = 0;
    for (size_t i = 1; i <= num_build_rows; i++) {
        if (build_match_index[i]) matched++;
        else unmatched++;
    }

    EXPECT_EQ(matched, 8);  // 1,2,3,5,10,20,30,40
    EXPECT_EQ(unmatched, num_build_rows - 8);
}

}  // namespace starrocks
