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

#include "exprs/conditional_two_phase.h"

#include <algorithm>

#include "column/chunk.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/const_column.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {

// Phase 3 (shared): read, for each row, its routed source column at pos[r] and append in row order.
// Sequential write (append-only, required for variable-length types); random read at pos[r] is fine.
StatusOr<ColumnPtr> assemble(const TypeDescriptor& result_type, size_t num_rows, const std::vector<uint32_t>& branch_of,
                             const std::vector<uint32_t>& pos, std::vector<ColumnPtr> sources) {
    bool nullable = false;
    std::vector<uint8_t> constant(sources.size(), 0);
    for (size_t i = 0; i < sources.size(); ++i) {
        auto& source = sources[i];
        if (source == nullptr) continue;
        nullable |= source->is_nullable() || source->only_null();
        if (source->is_constant()) {
            constant[i] = 1;
            source = down_cast<const ConstColumn*>(source.get())->data_column();
        }
    }
    MutableColumnPtr result = ColumnHelper::create_column(result_type, nullable);
    result->reserve(num_rows);
    for (size_t begin = 0; begin < num_rows;) {
        const uint32_t branch = branch_of[begin];
        const Column& source = *sources[branch];
        const uint32_t source_pos = constant[branch] ? 0 : pos[begin];
        size_t end = begin + 1;
        while (end < num_rows && branch_of[end] == branch &&
               (constant[branch] || pos[end] == source_pos + end - begin)) {
            ++end;
        }
        const size_t count = end - begin;
        if (constant[branch]) {
            if (source.is_null(0)) {
                (void)result->append_nulls(count);
            } else {
                result->append_value_multiple_times(source, 0, count);
            }
        } else {
            result->append(source, source_pos, count);
        }
        begin = end;
    }
    return result;
}

} // namespace

StatusOr<ColumnPtr> two_phase_eval_predicate_routed(ExprContext* context, Chunk* chunk,
                                                    const TypeDescriptor& result_type, int num_branches,
                                                    const std::function<StatusOr<ColumnPtr>(int)>& guard_fn,
                                                    const std::vector<Expr*>& then_exprs,
                                                    const std::vector<uint8_t>& then_two_phase, Expr* else_expr,
                                                    bool else_two_phase, bool enable_first_all_true_shortcut) {
    const size_t num_rows = chunk->num_rows();
    const uint32_t kElseIdx = static_cast<uint32_t>(num_branches);

    std::vector<uint32_t> branch_of(num_rows, kElseIdx); // default: ELSE / no-match
    std::vector<uint32_t> pos(num_rows);                 // identity; overridden for DENSE-routed rows
    for (size_t r = 0; r < num_rows; ++r) {
        pos[r] = static_cast<uint32_t>(r);
    }

    // Phase 1 -- routing. Evaluate guards lazily, first-match-wins. Mirror eager's only early-out (the first
    // surviving branch being all-true => direct return) and its skip (all-false/null guard); no other break.
    std::vector<uint8_t> matched_so_far(num_rows, 0);
    bool saw_surviving = false;
    for (int i = 0; i < num_branches; ++i) {
        ASSIGN_OR_RETURN(ColumnPtr guard, guard_fn(i));
        ColumnViewer<TYPE_BOOLEAN> guard_viewer(guard);
        size_t trues = 0;
        size_t newly = 0;
        for (size_t r = 0; r < num_rows; ++r) {
            const bool valid = !guard_viewer.is_null(r) && guard_viewer.value(r);
            trues += valid;
            if (valid && !matched_so_far[r]) {
                branch_of[r] = static_cast<uint32_t>(i);
                matched_so_far[r] = 1;
                ++newly;
            }
        }
        if (trues != 0 && enable_first_all_true_shortcut && !saw_surviving && trues == num_rows) {
            return then_exprs[i]->evaluate_checked(context, chunk);
        }
        if (newly > 0) {
            saw_surviving = true;
        }
    }

    // Phase 2 -- evaluate each routed branch (incl. ELSE) by cost tier; record sources and DENSE positions.
    std::vector<ColumnPtr> sources(num_branches + 1);
    std::vector<std::vector<uint32_t>> rows_of(num_branches + 1);
    for (size_t r = 0; r < num_rows; ++r) {
        rows_of[branch_of[r]].push_back(static_cast<uint32_t>(r));
    }

    auto eval_branch = [&](int idx, Expr* value_expr, bool two_phase) -> Status {
        const std::vector<uint32_t>& rows = rows_of[idx];
        if (rows.empty()) {
            return Status::OK(); // no row routes here (T3 k==0)
        }
        if (value_expr == nullptr) {
            // No ELSE clause: unmatched rows are SQL NULL.
            sources[idx] = ColumnHelper::create_const_null_column(num_rows);
            return Status::OK();
        }
        if (!two_phase || rows.size() == num_rows) {
            ASSIGN_OR_RETURN(sources[idx], value_expr->evaluate_checked(context, chunk)); // FULL; pos stays r
            return Status::OK();
        }
        ASSIGN_OR_RETURN(sources[idx], value_expr->evaluate_selected(context, chunk, rows));
        for (size_t j = 0; j < rows.size(); ++j) {
            pos[rows[j]] = static_cast<uint32_t>(j);
        }
        return Status::OK();
    };

    for (int i = 0; i < num_branches; ++i) {
        RETURN_IF_ERROR(eval_branch(i, then_exprs[i], then_two_phase[i] != 0));
    }
    RETURN_IF_ERROR(eval_branch(static_cast<int>(kElseIdx), else_expr, else_two_phase));

    return assemble(result_type, num_rows, branch_of, pos, sources);
}

StatusOr<ColumnPtr> two_phase_eval_null_routed(ExprContext* context, Chunk* chunk, const TypeDescriptor& result_type,
                                               const std::vector<Expr*>& arg_exprs,
                                               const std::vector<uint8_t>& arg_two_phase) {
    const size_t num_rows = chunk->num_rows();
    const int num_args = static_cast<int>(arg_exprs.size());
    const uint32_t kElseIdx = static_cast<uint32_t>(num_args);

    std::vector<uint32_t> branch_of(num_rows, kElseIdx); // default: all-args-null => SQL NULL
    std::vector<uint32_t> pos(num_rows);
    for (size_t r = 0; r < num_rows; ++r) {
        pos[r] = static_cast<uint32_t>(r);
    }

    std::vector<ColumnPtr> sources(num_args + 1);
    std::vector<uint8_t> still_null(num_rows, 1); // rows not yet resolved to a non-null arg
    size_t remaining = num_rows;

    for (int i = 0; i < num_args && remaining > 0; ++i) {
        // arg0 always full (its null-ness gates routing); cheap arg full; full when nothing resolved yet.
        const bool full = (arg_two_phase[i] == 0) || i == 0 || remaining == num_rows;
        if (full) {
            ASSIGN_OR_RETURN(sources[i], arg_exprs[i]->evaluate_checked(context, chunk));
            const Column& col = *sources[i];
            for (size_t r = 0; r < num_rows; ++r) {
                if (still_null[r] && !col.is_null(r)) {
                    branch_of[r] = static_cast<uint32_t>(i);
                    pos[r] = static_cast<uint32_t>(r);
                    still_null[r] = 0;
                    --remaining;
                }
            }
        } else {
            std::vector<uint32_t> idx;
            idx.reserve(remaining);
            for (size_t r = 0; r < num_rows; ++r) {
                if (still_null[r]) {
                    idx.push_back(static_cast<uint32_t>(r));
                }
            }
            ASSIGN_OR_RETURN(sources[i], arg_exprs[i]->evaluate_selected(context, chunk, idx));
            const Column& dense = *sources[i];
            for (size_t j = 0; j < idx.size(); ++j) {
                if (!dense.is_null(j)) {
                    branch_of[idx[j]] = static_cast<uint32_t>(i);
                    pos[idx[j]] = static_cast<uint32_t>(j);
                    still_null[idx[j]] = 0;
                    --remaining;
                }
            }
        }
    }

    if (remaining > 0) {
        sources[kElseIdx] = ColumnHelper::create_const_null_column(num_rows);
    }
    return assemble(result_type, num_rows, branch_of, pos, sources);
}

} // namespace starrocks
