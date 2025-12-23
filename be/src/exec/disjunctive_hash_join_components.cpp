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

#include "exec/disjunctive_hash_join_components.h"

#include "column/column_helper.h"
#include "column/const_column.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "common/logging.h"
#include "exec/hash_joiner.h"
#include "exprs/expr_context.h"
#include "gutil/casts.h"

namespace starrocks {

namespace {

struct BuildKeySigKey {
    std::string expr_fp;
    std::string type_fp;
    bool is_null_safe = false;

    bool operator==(const BuildKeySigKey& rhs) const {
        return expr_fp == rhs.expr_fp && type_fp == rhs.type_fp && is_null_safe == rhs.is_null_safe;
    }
};

struct BuildKeySig {
    std::vector<BuildKeySigKey> keys;

    bool operator==(const BuildKeySig& rhs) const { return keys == rhs.keys; }
};

struct BuildKeySigHash {
    size_t operator()(const BuildKeySig& s) const {
        size_t h = 0;
        auto mix = [&](size_t x) { h ^= x + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2); };
        mix(s.keys.size());
        for (const auto& k : s.keys) {
            mix(std::hash<std::string>{}(k.expr_fp));
            mix(std::hash<std::string>{}(k.type_fp));
            mix(std::hash<bool>{}(k.is_null_safe));
        }
        return h;
    }
};

} // namespace

// ============================================================================
// DisjunctiveHashJoinBuilder
// ============================================================================

DisjunctiveHashJoinBuilder::DisjunctiveHashJoinBuilder(HashJoiner& hash_joiner, const DisjunctiveJoinClauses& clauses)
        : HashJoinBuilder(hash_joiner), _clauses(&clauses) {
    DCHECK(_clauses->is_disjunctive()) << "DisjunctiveHashJoinBuilder requires multiple clauses";
}

void DisjunctiveHashJoinBuilder::create(const HashTableParam& param) {
    size_t num_clauses = _clauses->num_clauses();
    DCHECK_GE(num_clauses, 2);

    _unique_hash_tables.clear();
    _clause_to_ht.clear();
    _ht_rep_clause_idx.clear();
    _key_columns_per_ht.clear();

    // Group clauses by identical build key signature to avoid building duplicate hash maps.
    // This is common for patterns like:
    //   t.user_id = ah.user_id OR t.user2_id = ah.user_id
    // where build side key (ah.user_id) is identical across disjuncts.
    phmap::flat_hash_map<BuildKeySig, uint32_t, BuildKeySigHash> sig_to_ht;
    std::vector<bool> ht_has_other_conjuncts;

    _clause_to_ht.resize(num_clauses);
    for (size_t clause_idx = 0; clause_idx < num_clauses; ++clause_idx) {
        const auto& clause = _clauses->clause(clause_idx);

        BuildKeySig sig;
        sig.keys.reserve(clause.build_expr_ctxs.size());
        for (size_t k = 0; k < clause.build_expr_ctxs.size(); ++k) {
            const Expr* expr = clause.build_expr_ctxs[k]->root();
            BuildKeySigKey key;
            key.expr_fp = expr->debug_string();
            key.type_fp = expr->type().debug_string();
            key.is_null_safe = clause.is_null_safes[k];
            sig.keys.emplace_back(std::move(key));
        }

        auto it = sig_to_ht.find(sig);
        if (it == sig_to_ht.end()) {
            uint32_t ht_idx = _unique_hash_tables.size();
            sig_to_ht.emplace(std::move(sig), ht_idx);
            _unique_hash_tables.emplace_back();
            _ht_rep_clause_idx.emplace_back(static_cast<uint32_t>(clause_idx));
            ht_has_other_conjuncts.emplace_back(clause.has_other_conjuncts());
            _clause_to_ht[clause_idx] = ht_idx;
        } else {
            uint32_t ht_idx = it->second;
            _clause_to_ht[clause_idx] = ht_idx;
            ht_has_other_conjuncts[ht_idx] = ht_has_other_conjuncts[ht_idx] || clause.has_other_conjuncts();
        }
    }

    _key_columns_per_ht.resize(_unique_hash_tables.size());

    // Create one hash table per unique build-key signature.
    for (size_t ht_idx = 0; ht_idx < _unique_hash_tables.size(); ++ht_idx) {
        const uint32_t rep_clause_idx = _ht_rep_clause_idx[ht_idx];
        const auto& clause = _clauses->clause(rep_clause_idx);

        HashTableParam clause_param = param;
        clause_param.join_keys.clear();
        clause_param.join_keys.reserve(clause.build_expr_ctxs.size());
        for (size_t j = 0; j < clause.build_expr_ctxs.size(); ++j) {
            JoinKeyDesc key_desc;
            key_desc.type = &clause.build_expr_ctxs[j]->root()->type();
            key_desc.is_null_safe_equal = clause.is_null_safes[j];
            key_desc.col_ref = nullptr;
            clause_param.join_keys.push_back(key_desc);
        }

        clause_param.with_other_conjunct = param.with_other_conjunct || ht_has_other_conjuncts[ht_idx];
        _unique_hash_tables[ht_idx].create(clause_param);
    }
}

void DisjunctiveHashJoinBuilder::close() {
    for (auto& ht : _unique_hash_tables) {
        ht.close();
    }
    _unique_hash_tables.clear();
    _clause_to_ht.clear();
    _ht_rep_clause_idx.clear();
    _key_columns_per_ht.clear();
}

void DisjunctiveHashJoinBuilder::reset(const HashTableParam& param) {
    close();
    create(param);
}

Status DisjunctiveHashJoinBuilder::_prepare_build_key_columns(size_t clause_idx, Columns* key_columns,
                                                               const ChunkPtr& chunk) {
    const auto& clause = _clauses->clause(clause_idx);
    key_columns->resize(0);

    for (auto* expr_ctx : clause.build_expr_ctxs) {
        ASSIGN_OR_RETURN(auto column_ptr, expr_ctx->evaluate(chunk.get()));
        if (column_ptr->only_null()) {
            MutableColumnPtr column = ColumnHelper::create_column(expr_ctx->root()->type(), true);
            column->append_nulls(chunk->num_rows());
            key_columns->emplace_back(std::move(column));
        } else if (column_ptr->is_constant()) {
            auto* const_column = ColumnHelper::as_raw_column<ConstColumn>(column_ptr);
            const_column->data_column()->as_mutable_raw_ptr()->assign(chunk->num_rows(), 0);
            key_columns->emplace_back(const_column->data_column());
        } else {
            key_columns->emplace_back(std::move(column_ptr));
        }
    }
    return Status::OK();
}

Status DisjunctiveHashJoinBuilder::do_append_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    DCHECK(!_unique_hash_tables.empty());
    if (UNLIKELY(_unique_hash_tables[0].get_row_count() + chunk->num_rows() >= max_hash_table_element_size)) {
        return Status::NotSupported(strings::Substitute("row count of right table in hash join > $0", UINT32_MAX));
    }

    // OPTIMIZATION: Only the first hash table stores build chunk data.
    // Secondary hash tables only store key columns (different per clause).
    // This reduces memory from O(N * build_data) to O(build_data + N * key_data).

    // First hash table: append build chunk + key columns
    const uint32_t primary_clause_idx = _ht_rep_clause_idx[0];
    RETURN_IF_ERROR(_prepare_build_key_columns(primary_clause_idx, &_key_columns_per_ht[0], chunk));
    TRY_CATCH_BAD_ALLOC(_unique_hash_tables[0].append_chunk(chunk, _key_columns_per_ht[0]));

    // Secondary unique hash tables: only append key columns (no build chunk duplication)
    for (size_t ht_idx = 1; ht_idx < _unique_hash_tables.size(); ++ht_idx) {
        const uint32_t rep_clause_idx = _ht_rep_clause_idx[ht_idx];
        RETURN_IF_ERROR(_prepare_build_key_columns(rep_clause_idx, &_key_columns_per_ht[ht_idx], chunk));
        TRY_CATCH_BAD_ALLOC(_unique_hash_tables[ht_idx].append_keys_only(_key_columns_per_ht[ht_idx], chunk->num_rows()));
    }

    return Status::OK();
}

Status DisjunctiveHashJoinBuilder::build(RuntimeState* state) {
    SCOPED_TIMER(_hash_joiner.build_metrics().build_ht_timer);

    // Build all hash tables
    for (auto& ht : _unique_hash_tables) {
        TRY_CATCH_BAD_ALLOC(RETURN_IF_ERROR(ht.build(state)));
    }

    // OPTIMIZATION: Share build chunk from first hash table to secondary ones.
    // This allows secondary hash tables to output build columns during probe
    // while avoiding N-fold memory duplication during append phase.
    for (size_t i = 1; i < _unique_hash_tables.size(); ++i) {
        _unique_hash_tables[i].share_build_chunk_from(_unique_hash_tables[0]);
    }

    _ready = true;
    return Status::OK();
}

bool DisjunctiveHashJoinBuilder::anti_join_key_column_has_null() const {
    // For disjunctive join, check all hash tables and all their key columns
    // ANTI join needs to check if any key column in any disjunct has NULL
    for (const auto& ht : _unique_hash_tables) {
        const auto& key_columns = ht.get_key_columns();
        for (const auto& column : key_columns) {
            if (column->is_nullable()) {
                const auto& null_column = ColumnHelper::as_raw_column<NullableColumn>(column)->null_column();
                if (null_column->size() > 0 && null_column->contain_value(1, null_column->size(), 1)) {
                    return true;
                }
            }
        }
    }
    return false;
}

int64_t DisjunctiveHashJoinBuilder::ht_mem_usage() const {
    if (_unique_hash_tables.empty()) {
        return 0;
    }

    // First hash table has the actual build chunk data
    int64_t total = _unique_hash_tables[0].mem_usage();

    // Secondary hash tables share build chunk with first, so only count their
    // hash map overhead (first, next, key_columns, etc.), not build_chunk.
    // We approximate this by subtracting the shared build_chunk memory.
    for (size_t i = 1; i < _unique_hash_tables.size(); ++i) {
        int64_t ht_usage = _unique_hash_tables[i].mem_usage();
        // After share_build_chunk_from, secondary HTs point to same build_chunk,
        // so mem_usage() double-counts it. Subtract to avoid over-counting.
        const auto& build_chunk = _unique_hash_tables[i].get_build_chunk();
        if (build_chunk != nullptr) {
            ht_usage -= build_chunk->memory_usage();
        }
        total += std::max<int64_t>(0, ht_usage);
    }
    return total;
}

size_t DisjunctiveHashJoinBuilder::get_output_probe_column_count() const {
    return _unique_hash_tables[0].get_output_probe_column_count();
}

size_t DisjunctiveHashJoinBuilder::get_output_build_column_count() const {
    return _unique_hash_tables[0].get_output_build_column_count();
}

void DisjunctiveHashJoinBuilder::get_build_info(size_t* bucket_size, float* avg_keys_per_bucket,
                                                 std::string* hash_map_type) {
    size_t total_bucket_size = 0;
    float total_keys_per_bucket = 0;

    for (const auto& ht : _unique_hash_tables) {
        total_bucket_size += ht.get_bucket_size();
        total_keys_per_bucket += ht.get_keys_per_bucket();
    }

    *bucket_size = total_bucket_size;
    *avg_keys_per_bucket = total_keys_per_bucket / _unique_hash_tables.size();
    *hash_map_type = "DisjunctiveHashMap[clauses=" + std::to_string(_clauses->num_clauses()) +
                     ", unique_ht=" + std::to_string(_unique_hash_tables.size()) + "]";
}

void DisjunctiveHashJoinBuilder::visitHt(const std::function<void(JoinHashTable*)>& visitor) {
    for (auto& ht : _unique_hash_tables) {
        visitor(&ht);
    }
}

void DisjunctiveHashJoinBuilder::visitHtForSpill(const std::function<void(JoinHashTable*)>& visitor) {
    // For disjunctive join, all unique hash tables share the same build chunk data (rows).
    // They differ only in the key columns used for hashing. Spill only once to avoid duplicates.
    if (!_unique_hash_tables.empty()) {
        visitor(&_unique_hash_tables[0]);
    }
}

std::unique_ptr<HashJoinProberImpl> DisjunctiveHashJoinBuilder::create_prober() {
    return std::make_unique<DisjunctiveHashJoinProberImpl>(_hash_joiner, this);
}

void DisjunctiveHashJoinBuilder::clone_readable(HashJoinBuilder* builder) {
    auto* other = down_cast<DisjunctiveHashJoinBuilder*>(builder);

    // Copy the clauses pointer - all clones share the same clauses
    other->_clauses = _clauses;

    other->_unique_hash_tables.clear();
    other->_unique_hash_tables.reserve(_unique_hash_tables.size());
    for (auto& ht : _unique_hash_tables) {
        other->_unique_hash_tables.push_back(ht.clone_readable_table());
    }

    other->_clause_to_ht = _clause_to_ht;
    other->_ht_rep_clause_idx = _ht_rep_clause_idx;

    // Key columns cache structure (populated during build append, not needed in readable clone)
    other->_key_columns_per_ht.resize(_key_columns_per_ht.size());

    other->_ready = _ready;
}

ChunkPtr DisjunctiveHashJoinBuilder::convert_to_spill_schema(const ChunkPtr& chunk) const {
    // For disjunctive join, all hash tables share the same build row descriptor and thus
    // the same spill schema. The first hash table's schema is used.
    // When restoring, do_append_chunk() correctly inserts the data into all hash tables
    // with the appropriate key columns computed for each clause.
    return _unique_hash_tables[0].convert_to_spill_schema(chunk);
}

// ============================================================================
// DisjunctiveHashJoinProberImpl
// ============================================================================

DisjunctiveHashJoinProberImpl::DisjunctiveHashJoinProberImpl(HashJoiner& hash_joiner,
                                                              DisjunctiveHashJoinBuilder* builder)
        : HashJoinProberImpl(hash_joiner), _builder(builder) {
    _key_columns_per_clause.resize(_builder->num_hash_tables());
}

// Normalize evaluation result to a regular column of the expected size.
// Handles only_null and constant columns by creating new columns to avoid
// mutating shared state and to ensure correct size.
static ColumnPtr normalize_eval_result(const ColumnPtr& col, const TypeDescriptor& type, size_t num_rows) {
    if (col->only_null()) {
        // Create a new nullable column filled with nulls
        MutableColumnPtr out = ColumnHelper::create_column(type, true);
        out->append_nulls(num_rows);
        return out;
    }
    if (col->is_constant()) {
        // Avoid mutating const column's shared data_column.
        // Create a new column and fill with repeated values.
        // Use col->is_nullable() (not data_column's) for correct nullable semantics,
        // since ConstColumn's nullable state is on the wrapper, not the data part.
        const auto* cc = ColumnHelper::as_raw_column<ConstColumn>(col);
        MutableColumnPtr out = ColumnHelper::create_column(type, col->is_nullable());
        out->append_value_multiple_times(*cc->data_column(), 0, num_rows);
        return out;
    }
    return col;
}

Status DisjunctiveHashJoinProberImpl::_prepare_probe_key_columns(size_t clause_idx, Columns* key_columns) {
    const auto& clause = _builder->clauses().clause(clause_idx);
    key_columns->clear();
    key_columns->reserve(clause.probe_expr_ctxs.size());

    const size_t num_rows = _probe_chunk->num_rows();

    for (auto* expr_ctx : clause.probe_expr_ctxs) {
        const Expr* expr_ptr = expr_ctx->root();

        // Two-level cache lookup to optimize "common probe key" case.
        // Level 1: Try Expr* pointer (zero allocations, O(1))
        auto it_ptr = _probe_expr_cache_by_ptr.find(expr_ptr);
        if (it_ptr != _probe_expr_cache_by_ptr.end()) {
            key_columns->emplace_back(it_ptr->second);
            continue;
        }

        // Level 2: Try debug_string() for structurally identical but different Expr objects
        std::string fingerprint = expr_ptr->debug_string();
        auto it_str = _probe_expr_cache_by_str.find(fingerprint);
        if (it_str != _probe_expr_cache_by_str.end()) {
            // Cache hit by string - also cache by pointer for future lookups
            _probe_expr_cache_by_ptr.emplace(expr_ptr, it_str->second);
            key_columns->emplace_back(it_str->second);
            continue;
        }

        // Cache miss: evaluate, normalize, and cache in both levels
        ASSIGN_OR_RETURN(auto column_ptr, expr_ctx->evaluate(_probe_chunk.get()));
        column_ptr = normalize_eval_result(column_ptr, expr_ptr->type(), num_rows);

        _probe_expr_cache_by_ptr.emplace(expr_ptr, column_ptr);
        _probe_expr_cache_by_str.emplace(std::move(fingerprint), column_ptr);

        key_columns->emplace_back(column_ptr);
    }
    return Status::OK();
}

Status DisjunctiveHashJoinProberImpl::push_probe_chunk(RuntimeState* state, ChunkPtr&& chunk) {
    DCHECK(!_probe_chunk);
    _probe_chunk = std::move(chunk);
    _current_probe_has_remain = true;
    _current_probe_row_idx = 0;
    _current_ht_idx = 0;

    // Clear per-chunk state
    _seen_pairs.clear();
    _seen_semi_keys.clear();
    _probe_expr_cache_by_ptr.clear();
    _probe_expr_cache_by_str.clear();

    // Prepare probe key columns for all clauses.
    // The expression cache automatically optimizes "common probe key" case
    // where all disjuncts use the same probe expression.
    for (size_t i = 0; i < _builder->num_hash_tables(); ++i) {
        RETURN_IF_ERROR(_prepare_probe_key_columns(i, &_key_columns_per_clause[i]));
    }

    return Status::OK();
}

Status DisjunctiveHashJoinProberImpl::_eval_and_filter_by_clause_conjuncts(
        ChunkPtr& chunk, const JoinOnClause& clause, JoinHashTable& ht, Filter* out_keep_filter,
        Filter* out_nullify_filter) {
    if (!clause.has_other_conjuncts() || chunk == nullptr || chunk->is_empty()) {
        // No filtering needed - output empty filter to indicate "keep all"
        out_keep_filter->clear();
        out_nullify_filter->clear();
        return Status::OK();
    }

    size_t num_rows = chunk->num_rows();
    out_keep_filter->assign(num_rows, 1);

    // Evaluate each conjunct and merge into filter
    for (auto* ctx : clause.other_conjunct_ctxs) {
        ASSIGN_OR_RETURN(auto result_col, ctx->evaluate(chunk.get()));

        // Merge the boolean result into filter
        // result_col should be a boolean column (possibly nullable)
        if (result_col->only_null()) {
            // All nulls - filter out all rows
            out_keep_filter->assign(num_rows, 0);
            break;
        }

        const Column* data_col = result_col.get();
        const uint8_t* null_data = nullptr;

        if (result_col->is_nullable()) {
            const auto* nullable = down_cast<const NullableColumn*>(result_col.get());
            null_data = nullable->null_column()->raw_data();
            data_col = nullable->data_column().get();
        }

        // Data column should be BooleanColumn
        if (data_col->is_constant()) {
            const auto* const_col = down_cast<const ConstColumn*>(data_col);
            bool value = const_col->data_column()->get(0).get_int8();
            if (!value) {
                out_keep_filter->assign(num_rows, 0);
                break;
            }
            // value is true - continue to next conjunct
        } else {
            const auto* bool_col = down_cast<const BooleanColumn*>(data_col);
            const auto& bool_data = bool_col->get_data();

            for (size_t i = 0; i < num_rows; ++i) {
                if ((*out_keep_filter)[i] == 0) continue;  // Already filtered out

                bool is_null = (null_data != nullptr) && null_data[i];
                bool value = bool_data[i];

                if (is_null || !value) {
                    (*out_keep_filter)[i] = 0;
                }
            }
        }
    }

    // Preserve predicate result before JoinHashTable adjusts the filter for join semantics.
    // For LEFT/FULL OUTER JOIN, rows that do not satisfy other conjuncts should remain, but
    // the build-side columns must be set to NULL.
    Filter predicate_filter = *out_keep_filter;

    // Update match tracking (for OUTER/SEMI/ANTI join semantics).
    // This modifies out_keep_filter in-place based on join type.
    ht.remove_duplicate_index(out_keep_filter);

    // Mark rows that should be treated as "unmatched" after clause predicates, but were kept by join semantics.
    out_nullify_filter->assign(num_rows, 0);
    for (size_t i = 0; i < num_rows; ++i) {
        if (predicate_filter[i] == 0 && (*out_keep_filter)[i] == 1) {
            (*out_nullify_filter)[i] = 1;
        }
    }

    // For LEFT/FULL OUTER JOIN variants, null out build columns for rows that failed the clause conjuncts,
    // but were kept by remove_duplicate_index().
    switch (_hash_joiner.join_type()) {
    case TJoinOp::LEFT_OUTER_JOIN:
    case TJoinOp::ASOF_LEFT_OUTER_JOIN:
    case TJoinOp::FULL_OUTER_JOIN: {
        const size_t start_column = ht.get_output_probe_column_count();
        const size_t column_count = ht.get_output_build_column_count();
        if (column_count > 0) {
            auto& columns = chunk->columns();
            for (size_t col = start_column; col < start_column + column_count; ++col) {
                // Ensure nullable so we can mark NULLs.
                columns[col] = ColumnHelper::cast_to_nullable_column(std::move(columns[col]));
                auto* null_column =
                        ColumnHelper::as_raw_column<NullableColumn>(columns[col]->as_mutable_raw_ptr());
                auto& null_data = null_column->null_column_raw_ptr()->get_data();
                for (size_t i = 0; i < num_rows; ++i) {
                    if ((*out_nullify_filter)[i] == 1) {
                        null_data[i] = 1;
                        null_column->set_has_null(true);
                    }
                }
            }
        }
        break;
    }
    default:
        break;
    }

    // Apply filter to chunk
    chunk->filter(*out_keep_filter);

    return Status::OK();
}

void DisjunctiveHashJoinProberImpl::_apply_filter_to_indices(
        std::vector<uint32_t>& probe_indices,
        std::vector<uint32_t>& build_indices,
        const Filter& filter) {
    DCHECK_EQ(probe_indices.size(), build_indices.size());

    if (filter.empty()) {
        // Empty filter means "keep all" - no filtering needed
        return;
    }

    DCHECK_EQ(probe_indices.size(), filter.size());

    size_t j = 0;
    for (size_t i = 0; i < filter.size(); ++i) {
        if (filter[i]) {
            probe_indices[j] = probe_indices[i];
            build_indices[j] = build_indices[i];
            ++j;
        }
    }
    probe_indices.resize(j);
    build_indices.resize(j);
}

void DisjunctiveHashJoinProberImpl::_apply_filter_to_values(Filter& values, const Filter& filter) {
    if (filter.empty()) {
        return;
    }
    DCHECK_EQ(values.size(), filter.size());
    size_t j = 0;
    for (size_t i = 0; i < filter.size(); ++i) {
        if (filter[i]) {
            values[j] = values[i];
            ++j;
        }
    }
    values.resize(j);
}

void DisjunctiveHashJoinProberImpl::_append_deduplicated_rows(
        ChunkPtr& result_chunk, ChunkPtr& additional_chunk,
        std::vector<uint32_t>& probe_indices,
        std::vector<uint32_t>& build_indices,
        phmap::flat_hash_set<std::pair<uint32_t, uint32_t>>& seen_pairs) {
    if (additional_chunk == nullptr || additional_chunk->is_empty()) {
        probe_indices.clear();
        build_indices.clear();
        return;
    }

    DCHECK_EQ(additional_chunk->num_rows(), probe_indices.size());

    // Build a filter for rows that are not duplicates
    Filter filter(probe_indices.size(), 1);
    size_t num_filtered = 0;

    for (size_t i = 0; i < probe_indices.size(); ++i) {
        auto pair = std::make_pair(probe_indices[i], build_indices[i]);

        if (seen_pairs.count(pair) > 0) {
            filter[i] = 0;
            num_filtered++;
        } else {
            seen_pairs.insert(pair);
        }
    }

    if (num_filtered == probe_indices.size()) {
        probe_indices.clear();
        build_indices.clear();
        return;
    }

    if (num_filtered > 0) {
        additional_chunk->filter(filter);
        _apply_filter_to_indices(probe_indices, build_indices, filter);
    }

    if (result_chunk == nullptr || result_chunk->is_empty()) {
        result_chunk = std::move(additional_chunk);
    } else {
        result_chunk->append(*additional_chunk);
    }
}

StatusOr<ChunkPtr> DisjunctiveHashJoinProberImpl::probe_chunk(RuntimeState* state) {
    ChunkPtr result_chunk = std::make_shared<Chunk>();
    TRY_CATCH_ALLOC_SCOPE_START()

    DCHECK(_current_probe_has_remain && _probe_chunk);

    size_t num_hash_tables = _builder->num_hash_tables();
    bool has_remain = false;
    auto& first_ht = _builder->primary_hash_table();
    std::vector<uint32_t> result_probe_indices;
    std::vector<uint32_t> result_build_indices;

    // Probe all hash tables and merge results with deduplication
    // Use _seen_pairs (member) to track across multiple probe_chunk calls when has_remain is true
    // Start from _current_ht_idx to support resuming after chunk size limit
    const size_t max_chunk_size = state->chunk_size();

    // For OUTER joins, we must process all HTs before returning to ensure correct
    // deduplication of null-extended rows. Only limit chunk size for non-OUTER joins.
    TJoinOp::type join_type = _hash_joiner.join_type();
    bool is_outer_join = (join_type == TJoinOp::LEFT_OUTER_JOIN ||
                          join_type == TJoinOp::RIGHT_OUTER_JOIN ||
                          join_type == TJoinOp::FULL_OUTER_JOIN ||
                          join_type == TJoinOp::ASOF_LEFT_OUTER_JOIN);

    for (size_t ht_idx = _current_ht_idx; ht_idx < num_hash_tables; ++ht_idx) {
        // Check BEFORE processing next HT if we already have enough rows
        if (!is_outer_join && result_chunk && result_chunk->num_rows() >= max_chunk_size) {
            _current_ht_idx = ht_idx;  // Resume from this HT on next call
            has_remain = true;
            break;
        }

        auto& ht = _builder->hash_table(ht_idx);
        const auto& clause = _builder->clauses().clause(ht_idx);
        ChunkPtr ht_result_chunk = std::make_shared<Chunk>();
        bool ht_has_remain = false;

        RETURN_IF_ERROR(ht.probe(state, _key_columns_per_clause[ht_idx], &_probe_chunk,
                                 &ht_result_chunk, &ht_has_remain));

        // Copy indices before filtering (probe may overwrite on next call)
        uint32_t count = ht.get_last_probe_count();
        const auto& probe_idx_buf = ht.get_last_probe_indices();
        const auto& build_idx_buf = ht.get_last_build_indices();
        std::vector<uint32_t> probe_indices(probe_idx_buf.data(),
                                            probe_idx_buf.data() + count);
        std::vector<uint32_t> build_indices(build_idx_buf.data(),
                                            build_idx_buf.data() + count);

        // Apply clause-specific other_conjuncts BEFORE merge.
        // This ensures only rows matching (eq_keys AND other_conjuncts) pass for this clause.
        Filter keep_filter;
        Filter nullify_filter;
        if (clause.has_other_conjuncts() && ht_result_chunk && !ht_result_chunk->is_empty()) {
            RETURN_IF_ERROR(_eval_and_filter_by_clause_conjuncts(
                    ht_result_chunk, clause, ht, &keep_filter, &nullify_filter));
            _apply_filter_to_indices(probe_indices, build_indices, keep_filter);
            _apply_filter_to_values(nullify_filter, keep_filter);
            DCHECK_EQ(build_indices.size(), nullify_filter.size());
            for (size_t i = 0; i < build_indices.size(); ++i) {
                if (nullify_filter[i]) {
                    build_indices[i] = 0;
                }
            }
        }

        // For SEMI joins, deduplicate by probe_idx (LEFT) or build_idx (RIGHT) directly during merge.
        // This is more efficient than using _seen_pairs and then filtering again.
        TJoinOp::type join_type = _hash_joiner.join_type();
        bool is_left_semi = (join_type == TJoinOp::LEFT_SEMI_JOIN);
        bool is_right_semi = (join_type == TJoinOp::RIGHT_SEMI_JOIN);

        if (ht_idx == 0) {
            // First hash table - initialize result and update seen keys
            if (is_left_semi) {
                // Deduplicate by probe_idx for LEFT SEMI
                Filter semi_filter(probe_indices.size(), 1);
                for (size_t i = 0; i < probe_indices.size(); ++i) {
                    if (_seen_semi_keys.count(probe_indices[i])) {
                        semi_filter[i] = 0;
                    } else {
                        _seen_semi_keys.insert(probe_indices[i]);
                    }
                }
                ht_result_chunk->filter(semi_filter);
                _apply_filter_to_indices(probe_indices, build_indices, semi_filter);
            } else if (is_right_semi) {
                // Deduplicate by build_idx for RIGHT SEMI
                Filter semi_filter(build_indices.size(), 1);
                for (size_t i = 0; i < build_indices.size(); ++i) {
                    if (build_indices[i] == 0 || _seen_semi_keys.count(build_indices[i])) {
                        semi_filter[i] = 0;
                    } else {
                        _seen_semi_keys.insert(build_indices[i]);
                    }
                }
                ht_result_chunk->filter(semi_filter);
                _apply_filter_to_indices(probe_indices, build_indices, semi_filter);
            } else {
                // For non-SEMI joins, use pair-based deduplication
                for (size_t i = 0; i < probe_indices.size(); ++i) {
                    _seen_pairs.insert({probe_indices[i], build_indices[i]});
                }
            }
            result_chunk = std::move(ht_result_chunk);
            result_probe_indices = probe_indices;
            result_build_indices = build_indices;
        } else {
            if (is_left_semi) {
                // Deduplicate by probe_idx for LEFT SEMI
                Filter semi_filter(probe_indices.size(), 1);
                size_t num_filtered = 0;
                for (size_t i = 0; i < probe_indices.size(); ++i) {
                    if (_seen_semi_keys.count(probe_indices[i])) {
                        semi_filter[i] = 0;
                        num_filtered++;
                    } else {
                        _seen_semi_keys.insert(probe_indices[i]);
                    }
                }
                if (num_filtered < probe_indices.size()) {
                    ht_result_chunk->filter(semi_filter);
                    _apply_filter_to_indices(probe_indices, build_indices, semi_filter);
                    if (result_chunk == nullptr || result_chunk->is_empty()) {
                        result_chunk = std::move(ht_result_chunk);
                    } else {
                        result_chunk->append(*ht_result_chunk);
                    }
                    result_probe_indices.insert(result_probe_indices.end(), probe_indices.begin(), probe_indices.end());
                    result_build_indices.insert(result_build_indices.end(), build_indices.begin(), build_indices.end());
                }
            } else if (is_right_semi) {
                // Deduplicate by build_idx for RIGHT SEMI
                Filter semi_filter(build_indices.size(), 1);
                size_t num_filtered = 0;
                for (size_t i = 0; i < build_indices.size(); ++i) {
                    if (build_indices[i] == 0 || _seen_semi_keys.count(build_indices[i])) {
                        semi_filter[i] = 0;
                        num_filtered++;
                    } else {
                        _seen_semi_keys.insert(build_indices[i]);
                    }
                }
                if (num_filtered < build_indices.size()) {
                    ht_result_chunk->filter(semi_filter);
                    _apply_filter_to_indices(probe_indices, build_indices, semi_filter);
                    if (result_chunk == nullptr || result_chunk->is_empty()) {
                        result_chunk = std::move(ht_result_chunk);
                    } else {
                        result_chunk->append(*ht_result_chunk);
                    }
                    result_probe_indices.insert(result_probe_indices.end(), probe_indices.begin(), probe_indices.end());
                    result_build_indices.insert(result_build_indices.end(), build_indices.begin(), build_indices.end());
                }
            } else {
                // For non-SEMI joins, use pair-based deduplication
                _append_deduplicated_rows(result_chunk, ht_result_chunk,
                                          probe_indices, build_indices, _seen_pairs);
                result_probe_indices.insert(result_probe_indices.end(), probe_indices.begin(), probe_indices.end());
                result_build_indices.insert(result_build_indices.end(), build_indices.begin(), build_indices.end());

                // Sync match state to first hash table for RIGHT/FULL OUTER JOIN
                // Only mark build rows as matched if they passed clause's other_conjuncts
                auto& first_build_match = first_ht.get_build_match_index();
                for (uint32_t build_idx : build_indices) {
                    if (build_idx < first_build_match.size()) {
                        first_build_match[build_idx] = 1;  // Mark as matched
                    }
                }
            }
        }

        has_remain = has_remain || ht_has_remain;
    }

    // Reset _current_ht_idx if we processed all hash tables
    if (!has_remain || _current_ht_idx >= num_hash_tables) {
        _current_ht_idx = 0;
    }

    // For LEFT/FULL OUTER joins across multiple hash tables: suppress null-extended rows from a hash table
    // when another hash table produced a real match for the same probe row.
    if (_hash_joiner.join_type() == TJoinOp::LEFT_OUTER_JOIN || _hash_joiner.join_type() == TJoinOp::ASOF_LEFT_OUTER_JOIN ||
        _hash_joiner.join_type() == TJoinOp::FULL_OUTER_JOIN) {
        DCHECK_EQ(result_probe_indices.size(), result_build_indices.size());
        if (result_chunk && !result_chunk->is_empty() && result_probe_indices.size() == result_chunk->num_rows()) {
            phmap::flat_hash_set<uint32_t> probes_with_match;
            probes_with_match.reserve(result_probe_indices.size());
            for (size_t i = 0; i < result_probe_indices.size(); ++i) {
                if (result_build_indices[i] != 0) {
                    probes_with_match.insert(result_probe_indices[i]);
                }
            }

            Filter outer_filter(result_probe_indices.size(), 1);
            for (size_t i = 0; i < result_probe_indices.size(); ++i) {
                if (result_build_indices[i] == 0 &&
                    probes_with_match.find(result_probe_indices[i]) != probes_with_match.end()) {
                    outer_filter[i] = 0;
                }
            }
            result_chunk->filter(outer_filter);
        }
    }

    // Apply global filters (WHERE clause predicates) and lazy output
    // Note: We already applied per-clause other_conjuncts above, so we skip
    // _hash_joiner.filter_probe_output_chunk which would apply global other_join_conjunct_ctxs.
    // For disjunctive join, other_conjuncts are per-clause, not global.
    if (result_chunk && !result_chunk->is_empty()) {
        // Apply WHERE clause predicates (conjunct_ctxs) if any
        RETURN_IF_ERROR(_hash_joiner.filter_post_probe_output_chunk(result_chunk));
        RETURN_IF_ERROR(_hash_joiner.lazy_output_chunk<false>(state, &_probe_chunk, &result_chunk, first_ht));
    }

    if (!has_remain) {
        _probe_chunk = nullptr;
        _current_probe_has_remain = false;
    }

    TRY_CATCH_ALLOC_SCOPE_END()
    return result_chunk;
}

StatusOr<ChunkPtr> DisjunctiveHashJoinProberImpl::probe_remain(RuntimeState* state, bool* has_remain) {
    ChunkPtr result_chunk = std::make_shared<Chunk>();
    TRY_CATCH_ALLOC_SCOPE_START()

    // For RIGHT/FULL OUTER JOIN, we need to output unmatched build rows.
    // Since all hash tables contain the same build rows (just with different keys),
    // we only use the first hash table's probe_remain.
    //
    // During probe_chunk, we sync matched rows from all hash tables to the first
    // hash table's build_match_index, so probe_remain correctly identifies unmatched rows.

    if (_remain_ht_idx > 0) {
        // Already processed probe_remain from first hash table
        *has_remain = false;
        return result_chunk;
    }

    auto& ht = _builder->primary_hash_table();
    RETURN_IF_ERROR(ht.probe_remain(state, &result_chunk, &_remain_has_more));

    if (!_remain_has_more) {
        _remain_ht_idx++;
    }

    if (!result_chunk->is_empty()) {
        *has_remain = true;
        RETURN_IF_ERROR(_hash_joiner.filter_post_probe_output_chunk(result_chunk));
        RETURN_IF_ERROR(_hash_joiner.lazy_output_chunk<true>(state, nullptr, &result_chunk, ht));
        return result_chunk;
    }

    *has_remain = _remain_has_more;
    TRY_CATCH_ALLOC_SCOPE_END()
    return result_chunk;
}

void DisjunctiveHashJoinProberImpl::reset(RuntimeState* runtime_state) {
    _probe_chunk.reset();
    _current_probe_has_remain = false;
    _current_probe_row_idx = 0;
    _current_ht_idx = 0;
    _remain_ht_idx = 0;
    _remain_has_more = false;
    _seen_pairs.clear();
    _seen_semi_keys.clear();
    _probe_expr_cache_by_ptr.clear();
    _probe_expr_cache_by_str.clear();

    for (size_t i = 0; i < _builder->num_unique_hash_tables(); ++i) {
        _builder->unique_hash_table(i).reset_probe_state(runtime_state);
    }
}

} // namespace starrocks
