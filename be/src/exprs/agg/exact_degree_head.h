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
#include <queue>

#include "column/column_helper.h"
#include "exprs/agg/exact_degree_state.h"
#include "runtime/runtime_state.h"

namespace starrocks {
// Each input row describes one DISJOINT original-key range across all sources.
// Keep only its best candidates. No unbounded union of source distributions.
struct ExactDegreeHeadState {
    using Entry = std::pair<double, uint64_t>;
    struct Better {
        bool operator()(const Entry& l, const Entry& r) const {
            return l.first > r.first || (l.first == r.first && l.second < r.second);
        }
    };
    std::priority_queue<Entry, std::vector<Entry>, Better> best;
    uint64_t budget = 0;
    void set_budget(uint64_t value) {
        if (value < 1 || value > 16384 || (budget && budget != value))
            throw std::runtime_error("Invalid exact degree head budget");
        budget = value;
    }
    void offer(Entry value) {
        if (best.size() < budget)
            best.push(value);
        else if (Better()(value, best.top())) {
            best.pop();
            best.push(value);
        }
    }
    void update(FunctionContext* ctx, const Column** columns, size_t row) {
        set_budget(down_cast<const Int32Column*>(ColumnHelper::get_data_column(columns[4]))
                           ->immutable_data()[columns[4]->is_constant() ? 0 : row]);
        std::array<ExactDegreeState, 4> sources;
        for (int i = 0; i < 4; ++i) {
            auto bytes = ColumnHelper::get_binary_column(columns[i])->get_slice(columns[i]->is_constant() ? 0 : row);
            sources[i] = ExactDegreeState::deserialize(bytes);
        }
        BitmapValue candidates;
        for (int i = 0; i < 4; ++i)
            for (int j = 0; j < i; ++j) {
                BitmapValue common = sources[i].keys;
                common &= sources[j].keys;
                candidates |= common;
            }
        int active = 0;
        bool unit = true;
        for (const auto& source : sources) {
            active += source.rows != 0;
            unit &= source.extra.empty();
        }
        if (active == 2 && unit) {
            // All contributions equal one. Key order alone decides; once a key
            // cannot enter the heap, no later key from this range can enter it.
            BitmapValueIter it;
            it.reset(candidates);
            std::array<uint64_t, 256> keys;
            uint64_t n;
            while ((n = it.next_batch(keys.data(), keys.size())) != 0) {
                for (uint64_t i = 0; i < n; ++i) {
                    Entry candidate(1, keys[i]);
                    if (best.size() == budget && !Better()(candidate, best.top())) {
                        return;
                    }
                    offer(candidate);
                }
            }
            return;
        }
        uint64_t visits = 0;
        ExactDegreeState::each(candidates, [&](uint64_t key) {
            if ((++visits & 4095) == 0 && ctx->state() && ctx->state()->is_cancelled())
                throw std::runtime_error("Exact degree head cancelled");
            std::array<double, 4> n;
            for (int i = 0; i < 4; ++i) n[i] = sources[i].frequency(key);
            double score = 0;
            for (int i = 0; i < 4; ++i)
                for (int j = 0; j < i; ++j) score += n[i] * n[j];
            offer({score, key});
        });
    }
    std::string serialize() const {
        std::string out;
        ExactDegreeState::put64(out, budget);
        auto copy = best;
        while (!copy.empty()) {
            ExactDegreeState::put64(out, std::bit_cast<uint64_t>(copy.top().first));
            ExactDegreeState::put64(out, copy.top().second);
            copy.pop();
        }
        return out;
    }
    void merge(Slice in) {
        auto b = ExactDegreeState::get64(in);
        if (!b && in.empty()) return;
        set_budget(b);
        if (in.size % 16 || in.size / 16 > budget) throw std::runtime_error("Invalid degree head payload");
        while (!in.empty()) {
            double score = std::bit_cast<double>(ExactDegreeState::get64(in));
            uint64_t key = ExactDegreeState::get64(in);
            offer({score, key});
        }
    }
    std::string finish() const {
        ExactDegreeState keys;
        auto copy = best;
        while (!copy.empty()) {
            keys.update(ExactDegreeState::decode(copy.top().second));
            copy.pop();
        }
        return keys.serialize();
    }
};
class ExactDegreeHeadAggregateFunction final
        : public AggregateFunctionBatchHelper<ExactDegreeHeadState, ExactDegreeHeadAggregateFunction> {
public:
    bool is_exception_safe() const override { return false; }
    void reset(FunctionContext*, const Columns&, AggDataPtr state) const override {
        this->data(state) = ExactDegreeHeadState();
    }
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row) const override {
        this->data(state).update(ctx, columns, row);
    }
    void merge(FunctionContext*, const Column* column, AggDataPtr state, size_t row) const override {
        this->data(state).merge(down_cast<const BinaryColumn*>(column)->get_slice(row));
    }
    void serialize_to_column(FunctionContext*, ConstAggDataPtr state, Column* out) const override {
        auto bytes = this->data(state).serialize();
        down_cast<BinaryColumn*>(out)->append(Slice(bytes));
    }
    void finalize_to_column(FunctionContext*, ConstAggDataPtr state, Column* out) const override {
        auto bytes = this->data(state).finish();
        down_cast<BinaryColumn*>(out)->append(Slice(bytes));
    }
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t n,
                                     MutableColumnPtr& out) const override {
        const Column* columns[5];
        for (int i = 0; i < 5; ++i) columns[i] = src[i].get();
        for (size_t row = 0; row < n; ++row) {
            ExactDegreeHeadState state;
            state.update(ctx, columns, row);
            auto bytes = state.serialize();
            down_cast<BinaryColumn*>(out.get())->append(Slice(bytes));
        }
    }
    std::string get_name() const override { return "stats_degree_head_agg"; }
};
} // namespace starrocks
