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

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/hash_join_components.h"
#include "exec/join/join_hash_table.h"
#include "exec/join_on_clause.h"
#include "runtime/runtime_state.h"
#include "util/phmap/phmap.h"

namespace starrocks {

class HashJoiner;
struct HashJoinProbeMetrics;

/**
 * DisjunctiveHashJoinBuilder builds N hash tables for hash join with OR in ON clause.
 *
 * For example: ON t1.x = t2.a OR t1.y = t2.b
 *   - Hash table 0: keyed by t2.a
 *   - Hash table 1: keyed by t2.b
 *
 * Each build row is inserted into ALL hash tables (with different keys per disjunct).
 * This is different from partitioned hash join where each row goes to exactly one partition.
 */
class DisjunctiveHashJoinBuilder final : public HashJoinBuilder {
public:
    DisjunctiveHashJoinBuilder(HashJoiner& hash_joiner, const DisjunctiveJoinClauses& clauses);
    ~DisjunctiveHashJoinBuilder() override = default;

    void create(const HashTableParam& param) override;

    void close() override;

    void reset(const HashTableParam& param) override;

    Status do_append_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    Status build(RuntimeState* state) override;

    bool anti_join_key_column_has_null() const override;

    int64_t ht_mem_usage() const override;

    size_t get_output_probe_column_count() const override;
    size_t get_output_build_column_count() const override;

    void get_build_info(size_t* bucket_size, float* avg_keys_per_bucket, std::string* hash_map_type) override;

    void visitHt(const std::function<void(JoinHashTable*)>& visitor) override;

    // For spilling, only visit the first hash table since all hash tables have the same data.
    // The key columns differ per hash table, but the build chunk data is identical.
    void visitHtForSpill(const std::function<void(JoinHashTable*)>& visitor) override;

    std::unique_ptr<HashJoinProberImpl> create_prober() override;

    void clone_readable(HashJoinBuilder* builder) override;

    ChunkPtr convert_to_spill_schema(const ChunkPtr& chunk) const override;

    const DisjunctiveJoinClauses* get_disjunctive_clauses() const override { return _clauses; }

    // Number of logical clauses (OR branches). This is the loop bound for the prober.
    size_t num_hash_tables() const { return _clauses->num_clauses(); }

    size_t num_unique_hash_tables() const { return _unique_hash_tables.size(); }

    // Hash table used by a given clause. Multiple clauses may map to the same HT.
    JoinHashTable& hash_table(size_t clause_idx) { return _unique_hash_tables[_clause_to_ht[clause_idx]]; }
    const JoinHashTable& hash_table(size_t clause_idx) const { return _unique_hash_tables[_clause_to_ht[clause_idx]]; }

    JoinHashTable& unique_hash_table(size_t ht_idx) { return _unique_hash_tables[ht_idx]; }
    const JoinHashTable& unique_hash_table(size_t ht_idx) const { return _unique_hash_tables[ht_idx]; }

    JoinHashTable& primary_hash_table() { return _unique_hash_tables[0]; }
    const JoinHashTable& primary_hash_table() const { return _unique_hash_tables[0]; }

    const DisjunctiveJoinClauses& clauses() const { return *_clauses; }

private:
    Status _prepare_build_key_columns(size_t clause_idx, Columns* key_columns, const ChunkPtr& chunk);

    // Stored as pointer to allow proper clone semantics
    const DisjunctiveJoinClauses* _clauses;

    // One hash table per unique build-key signature.
    std::vector<JoinHashTable> _unique_hash_tables;

    // clause_idx -> ht_idx mapping (size = num_clauses)
    std::vector<uint32_t> _clause_to_ht;

    // ht_idx -> representative clause index for build-key evaluation / param init
    std::vector<uint32_t> _ht_rep_clause_idx;

    // Per-HT key columns cache (evaluated on build input chunks)
    std::vector<Columns> _key_columns_per_ht;
};

/**
 * DisjunctiveHashJoinProberImpl probes multiple hash tables and deduplicates results.
 *
 * For each probe row:
 * 1. Probe hash table 0 with key from clause 0
 * 2. Probe hash table 1 with key from clause 1
 * ... and so on
 *
 * Deduplication is done using a set of (build_row_ptr) to avoid outputting
 * the same build row multiple times if it matches through different disjuncts.
 */
class DisjunctiveHashJoinProberImpl final : public HashJoinProberImpl {
public:
    DisjunctiveHashJoinProberImpl(HashJoiner& hash_joiner, DisjunctiveHashJoinBuilder* builder);
    ~DisjunctiveHashJoinProberImpl() override = default;

    bool probe_chunk_empty() const override { return _probe_chunk == nullptr; }
    Status on_input_finished(RuntimeState* state) override { return Status::OK(); }
    Status push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) override;
    StatusOr<ChunkPtr> probe_chunk(RuntimeState* state) override;
    StatusOr<ChunkPtr> probe_remain(RuntimeState* state, bool* has_remain) override;
    void reset(RuntimeState* runtime_state) override;

private:
    Status _prepare_probe_key_columns(size_t clause_idx, Columns* key_columns);

    // Evaluate clause-specific other_conjuncts and filter the chunk.
    // Also updates match tracking via ht.remove_duplicate_index().
    // out_keep_filter: selection filter applied to the original rows (size = original num_rows).
    // out_nullify_filter: marks rows that should be treated as "unmatched" (build_idx=0 semantics) after
    // clause predicates, but were kept by join semantics (LEFT/FULL OUTER). Size = original num_rows.
    Status _eval_and_filter_by_clause_conjuncts(ChunkPtr& chunk, const JoinOnClause& clause,
                                                JoinHashTable& ht, Filter* out_keep_filter,
                                                Filter* out_nullify_filter);

    // Apply filter to probe/build index vectors, keeping only passing entries
    static void _apply_filter_to_indices(std::vector<uint32_t>& probe_indices,
                                          std::vector<uint32_t>& build_indices,
                                          const Filter& filter);

    // Apply filter to a byte filter-vector itself, keeping only passing entries.
    static void _apply_filter_to_values(Filter& values, const Filter& filter);

    // Append rows from additional_chunk to result_chunk, filtering out duplicate (probe_idx, build_idx) pairs
    void _append_deduplicated_rows(ChunkPtr& result_chunk, ChunkPtr& additional_chunk,
                                   std::vector<uint32_t>& probe_indices,
                                   std::vector<uint32_t>& build_indices,
                                   phmap::flat_hash_set<std::pair<uint32_t, uint32_t>>& seen_pairs);

    DisjunctiveHashJoinBuilder* _builder = nullptr;
    ChunkPtr _probe_chunk;
    // Per-clause key columns for current probe chunk
    std::vector<Columns> _key_columns_per_clause;
    // Current probe row index (for multi-batch processing)
    size_t _current_probe_row_idx = 0;
    bool _current_probe_has_remain = false;
    // Current hash table being probed (for multi-hash-table iteration)
    size_t _current_ht_idx = 0;

    // For post-probe (RIGHT/FULL OUTER JOIN)
    size_t _remain_ht_idx = 0;
    bool _remain_has_more = false;

    // Track seen (probe_idx, build_idx) pairs for deduplication across multiple probe_chunk calls
    // when has_remain is true. Cleared on push_probe_chunk.
    phmap::flat_hash_set<std::pair<uint32_t, uint32_t>> _seen_pairs;

    // For SEMI join: track seen probe_idx (LEFT SEMI) or build_idx (RIGHT SEMI)
    // to ensure each probe/build row is output at most once across all HTs and probe_chunk calls.
    // Cleared on push_probe_chunk.
    phmap::flat_hash_set<uint32_t> _seen_semi_keys;

    // Two-level cache for probe expression evaluation results.
    // Cleared on each push_probe_chunk to avoid cross-chunk state.
    // This optimizes "common probe key" case where all disjuncts use the same probe expr.
    //
    // Level 1: Expr* pointer (zero allocations, works if FE reuses Expr objects)
    // Level 2: debug_string() fallback (handles structurally identical but different Expr objects)
    phmap::flat_hash_map<const Expr*, ColumnPtr> _probe_expr_cache_by_ptr;
    phmap::flat_hash_map<std::string, ColumnPtr> _probe_expr_cache_by_str;
};

} // namespace starrocks
