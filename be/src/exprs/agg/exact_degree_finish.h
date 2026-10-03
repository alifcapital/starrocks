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
#include <charconv>
#include <cmath>
#include <iomanip>
#include <sstream>

#include "column/column_helper.h"
#include "exprs/agg/exact_degree_state.h"
#include "runtime/runtime_state.h"
#include "util/hash_util.hpp"

namespace starrocks {
// Inputs must be finalized disjoint key ranges from stats_degree_state. The collector
// partitions by the original integer key, never by a possibly colliding hash.
struct ExactDegreeFinishState {
    uint64_t rows = 0, ndv = 0, maximum = 0;
    std::array<double, 10> excess{};
    std::array<std::array<double, 768>, 8> tail{};
    phmap::flat_hash_map<uint64_t, uint64_t> heads;
    BitmapValue head_keys;
    bool has_head = false;
    static constexpr std::array<int, 8> orders{0, 2, 3, 4, 6, 8, 9, 12};
    static std::array<int, 3> buckets(uint64_t key) {
        char value[32];
        auto end = std::to_chars(value, value + 32, ExactDegreeState::decode(key));
        auto seed = HashUtil::xx_hash3_64(value, end.ptr - value, HashUtil::XXHASH3_64_SEED);
        std::array<int, 3> result;
        for (int i = 0; i < 3; ++i) {
            char salt[] = "join-statistics-0";
            salt[16] = '0' + i;
            result[i] = (HashUtil::xx_hash3_64(salt, 17, seed) & 255) + 256 * i;
        }
        return result;
    }
    void update(FunctionContext* ctx, const ExactDegreeState& part, Slice head) {
        if (!has_head) {
            head_keys = ExactDegreeState::deserialize(head).keys;
            has_head = true;
        }
        ExactDegreeState::checked_add(rows, part.rows);
        ExactDegreeState::checked_add(ndv, part.keys.cardinality());
        if (part.rows) maximum = std::max(maximum, uint64_t(1));
        uint64_t visits = 0;
        ExactDegreeState::each(part.keys, [&](uint64_t key) {
            if ((++visits & 4095) == 0 && ctx->state() && ctx->state()->is_cancelled()) {
                throw std::runtime_error("Exact degree collection cancelled");
            }
            if (head_keys.contains(key)) {
                ExactDegreeState::checked_add(heads[key], 1);
            } else {
                for (int b : buckets(key)) tail[0][b] += 1;
            }
        });
        for (auto [key, e] : part.extra) {
            uint64_t d = e + 1;
            maximum = std::max(maximum, d);
            double power = 1;
            int slot = 1;
            bool is_head = head_keys.contains(key);
            if (is_head) ExactDegreeState::checked_add(heads[key], e);
            auto b = is_head ? std::array<int, 3>{} : buckets(key);
            for (int p = 1; p <= 12; ++p) {
                power *= d;
                if (p <= 10) excess[p - 1] += power - 1;
                if (slot < 8 && p == orders[slot]) {
                    if (!is_head)
                        for (int bucket : b) tail[slot][bucket] += power - 1;
                    ++slot;
                }
            }
        }
    }
    std::string serialize() const {
        std::string out;
        out.reserve(49272 + heads.size() * 16);
        auto put = [&](uint64_t value) { ExactDegreeState::put64(out, value); };
        put(rows);
        put(ndv);
        put(maximum);
        put(heads.size());
        for (double x : excess) put(std::bit_cast<uint64_t>(x));
        for (const auto& m : tail)
            for (double x : m) put(std::bit_cast<uint64_t>(x));
        for (auto [key, count] : heads) {
            put(key);
            put(count);
        }
        return out;
    }
    void merge(Slice in) {
        auto get = [&]() { return ExactDegreeState::get64(in); };
        ExactDegreeState::checked_add(rows, get());
        ExactDegreeState::checked_add(ndv, get());
        maximum = std::max(maximum, get());
        uint64_t count = get();
        for (double& x : excess) x += std::bit_cast<double>(get());
        for (auto& m : tail)
            for (double& x : m) x += std::bit_cast<double>(get());
        if (count > in.size / 16 || count * 16 != in.size) throw std::runtime_error("Invalid degree summary");
        for (uint64_t i = 0; i < count; ++i) {
            auto key = get();
            auto value = get();
            ExactDegreeState::checked_add(heads[key], value);
        }
    }
    std::string json() const {
        std::ostringstream out;
        out << std::setprecision(17);
        out << "{\"rows\":" << rows << ",\"ndv\":" << ndv << ",\"max\":" << maximum << ",\"moments\":[";
        for (int p = 0; p < 10; ++p) {
            if (p) out << ',';
            out << ndv + excess[p];
        }
        out << "],\"head\":[";
        bool comma = false;
        for (auto [key, count] : heads) {
            if (comma) out << ',';
            comma = true;
            out << '[' << ExactDegreeState::decode(key) << ',' << count << ']';
        }
        out << "],\"tail\":[";
        for (int p = 0; p < 8; ++p) {
            if (p) out << ',';
            out << '[';
            for (int b = 0; b < 768; ++b) {
                if (b) out << ',';
                out << (tail[p][b] + (p ? tail[0][b] : 0));
            }
            out << ']';
        }
        out << "]}";
        return out.str();
    }
};
class ExactDegreeFinishAggregateFunction final
        : public AggregateFunctionBatchHelper<ExactDegreeFinishState, ExactDegreeFinishAggregateFunction> {
public:
    bool is_exception_safe() const override { return false; }
    void reset(FunctionContext*, const Columns&, AggDataPtr state) const override {
        this->data(state) = ExactDegreeFinishState();
    }
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row) const override {
        auto part = ColumnHelper::get_binary_column(columns[0])->get_slice(columns[0]->is_constant() ? 0 : row);
        auto head = ColumnHelper::get_binary_column(columns[1])->get_slice(columns[1]->is_constant() ? 0 : row);
        this->data(state).update(ctx, ExactDegreeState::deserialize(part), head);
    }
    void merge(FunctionContext*, const Column* col, AggDataPtr state, size_t row) const override {
        this->data(state).merge(down_cast<const BinaryColumn*>(col)->get_slice(row));
    }
    void serialize_to_column(FunctionContext*, ConstAggDataPtr state, Column* to) const override {
        auto bytes = this->data(state).serialize();
        down_cast<BinaryColumn*>(to)->append(Slice(bytes));
    }
    void finalize_to_column(FunctionContext*, ConstAggDataPtr state, Column* to) const override {
        auto bytes = this->data(state).json();
        down_cast<BinaryColumn*>(to)->append(Slice(bytes));
    }
    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t n,
                                     MutableColumnPtr& dst) const override {
        for (size_t row = 0; row < n; ++row) {
            ExactDegreeFinishState state;
            auto part = ColumnHelper::get_binary_column(src[0].get())->get_slice(src[0]->is_constant() ? 0 : row);
            auto head = ColumnHelper::get_binary_column(src[1].get())->get_slice(src[1]->is_constant() ? 0 : row);
            state.update(ctx, ExactDegreeState::deserialize(part), head);
            auto bytes = state.serialize();
            down_cast<BinaryColumn*>(dst.get())->append(Slice(bytes));
        }
    }
    std::string get_name() const override { return "stats_degree_finish"; }
};
} // namespace starrocks
