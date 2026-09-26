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

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/type_traits.h"
#include "exprs/agg/aggregate.h"
#include "types/bitmap_value.h"

namespace starrocks {

// Exact multiset. The common frequency 1 costs no per-key counter. Signed ordering is
// preserved in the bitmap, including INT64_MIN/MAX; no hashing or key truncation.
struct ExactDegreeState {
    BitmapValue keys;
    phmap::flat_hash_map<uint64_t, uint64_t> extra;
    uint64_t rows = 0;

    static uint64_t encode(int64_t value) { return std::bit_cast<uint64_t>(value) ^ (uint64_t(1) << 63); }
    static int64_t decode(uint64_t value) { return std::bit_cast<int64_t>(value ^ (uint64_t(1) << 63)); }

    static void checked_add(uint64_t& target, uint64_t value) {
        if (value > std::numeric_limits<uint64_t>::max() - target) {
            throw std::overflow_error("Exact degree count overflow");
        }
        target += value;
    }

    bool update(int64_t value) {
        uint64_t key = encode(value);
        // Repeated keys usually dominate skewed distributions. Once a repeat counter
        // exists, the support bitmap need not be probed again for every source row.
        auto repeated = extra.find(key);
        bool changed = repeated == extra.end();
        if (repeated != extra.end()) {
            checked_add(repeated->second, 1);
        } else if (keys.contains(key)) {
            extra.emplace(key, 1);
        } else {
            keys.add(key);
        }
        checked_add(rows, 1);
        return changed;
    }

    template <class F>
    static void each(const BitmapValue& bitmap, F&& f) {
        BitmapValueIter it;
        it.reset(bitmap);
        std::array<uint64_t, 4096> batch;
        uint64_t n;
        while ((n = it.next_batch(batch.data(), batch.size())) != 0) {
            for (uint64_t i = 0; i < n; ++i) f(batch[i]);
        }
    }

    uint64_t frequency(uint64_t key) const {
        if (!keys.contains(key)) return 0;
        auto it = extra.find(key);
        return it == extra.end() ? 1 : it->second + 1;
    }

    void merge(const ExactDegreeState& rhs) {
        if (rhs.rows == 0) return;
        if (rows == 0) {
            *this = rhs;
            return;
        }
        BitmapValue overlap = rhs.keys;
        overlap &= keys;
        each(overlap, [&](uint64_t key) { checked_add(extra[key], 1); });
        for (const auto& [key, count] : rhs.extra) checked_add(extra[key], count);
        keys |= rhs.keys;
        checked_add(rows, rhs.rows);
    }

    static void put64(std::string& out, uint64_t value) {
        for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>(value >> (i * 8)));
    }
    static uint64_t get64(Slice& in) {
        if (in.size < 8) throw std::runtime_error("Truncated exact degree state");
        uint64_t value = 0;
        for (int i = 0; i < 8; ++i) value |= uint64_t(static_cast<unsigned char>(in.data[i])) << (i * 8);
        in.remove_prefix(8);
        return value;
    }
    static void put_varint(std::string& out, uint64_t value) {
        while (value >= 128) {
            out.push_back(static_cast<char>((value & 127) | 128));
            value >>= 7;
        }
        out.push_back(static_cast<char>(value));
    }
    static uint64_t get_varint(Slice& in) {
        uint64_t value = 0;
        for (int shift = 0; shift <= 63; shift += 7) {
            if (in.empty()) throw std::runtime_error("Truncated degree counter");
            uint8_t byte = static_cast<uint8_t>(in.data[0]);
            in.remove_prefix(1);
            if (shift == 63 && byte > 1) throw std::runtime_error("Degree counter overflow");
            value |= uint64_t(byte & 127) << shift;
            if (!(byte & 128)) return value;
        }
        throw std::runtime_error("Invalid degree counter");
    }
    std::string serialize() const {
        size_t size = keys.get_size_in_bytes();
        std::string out;
        out.reserve(32 + size + extra.size() * 4);
        put64(out, 0x324752444553); // SEDRG2, temporary internal format.
        put64(out, rows);
        put64(out, size);
        put64(out, extra.size());
        out.resize(32 + size);
        keys.write(out.data() + 32);
        // Sorted deltas and variable-width counts avoid sixteen bytes per repeated
        // key. This changes only scratch/spill encoding, never key identities.
        std::vector<std::pair<uint64_t, uint64_t>> counters(extra.begin(), extra.end());
        std::sort(counters.begin(), counters.end());
        uint64_t previous = 0;
        for (const auto& [key, count] : counters) {
            put_varint(out, key - previous);
            put_varint(out, count);
            previous = key;
        }
        return out;
    }
    static ExactDegreeState deserialize(Slice in) {
        ExactDegreeState result;
        if (in.empty()) return result;
        if (get64(in) != 0x324752444553) throw std::runtime_error("Invalid exact degree state version");
        result.rows = get64(in);
        uint64_t size = get64(in), count = get64(in);
        if (size > in.size || count > (in.size - size) / 2 ||
            !result.keys.valid_and_deserialize(in.data, size)) {
            throw std::runtime_error("Invalid exact degree state payload");
        }
        in.remove_prefix(size);
        if (count > result.keys.cardinality()) throw std::runtime_error("Invalid degree counter count");
        result.extra.reserve(count);
        uint64_t rows = result.keys.cardinality();
        uint64_t key = 0;
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t delta = get_varint(in), value = get_varint(in);
            checked_add(key, delta);
            if ((i && delta == 0) || value == 0 || !result.keys.contains(key) ||
                !result.extra.emplace(key, value).second) {
                throw std::runtime_error("Invalid exact degree multiplicity");
            }
            checked_add(rows, value);
        }
        if (!in.empty() || rows != result.rows) throw std::runtime_error("Inconsistent exact degree row count");
        return result;
    }
    std::array<double, 4> pair(const ExactDegreeState& rhs) const {
        BitmapValue common = keys;
        common &= rhs.keys;
        long double n = common.cardinality(), ff = n, pf = n, fp = n;
        if (n == 0) return {};
        if (n <= extra.size() + rhs.extra.size()) {
            // Sparse intersections (e.g. a rare predicate slice) should only visit
            // matching keys, not every repeated key on both sides.
            ff = pf = fp = 0;
            each(common, [&](uint64_t key) {
                auto l = extra.find(key), r = rhs.extra.find(key);
                long double a = l == extra.end() ? 1 : l->second + 1;
                long double b = r == rhs.extra.end() ? 1 : r->second + 1;
                ff += a * b;
                pf += b;
                fp += a;
            });
            return {double(ff), double(pf), double(fp), double(n)};
        }
        for (const auto& [key, e] : extra) {
            if (!rhs.keys.contains(key)) continue;
            fp += e;
            ff += e;
            auto it = rhs.extra.find(key);
            if (it != rhs.extra.end()) ff += static_cast<long double>(e) * it->second;
        }
        for (const auto& [key, e] : rhs.extra) {
            if (keys.contains(key)) {
                pf += e;
                ff += e;
            }
        }
        return {double(ff), double(pf), double(fp), double(n)};
    }
};

// A dense counter array is an in-memory representation of a sufficiently populated
// 32768-key range, not another collection algorithm. Sparse/unique ranges retain the
// bitmap representation. Both serialize to the same exact multiset format.
struct ExactDegreeAggregateState {
    static constexpr size_t WIDTH = 32768;
    ExactDegreeState value;
    std::unique_ptr<std::array<uint64_t, WIDTH>> dense;
    uint64_t base = 0, dense_rows = 0;
    int64_t charged = 0;
    uint32_t mutations = 0;
    bool dense_disabled = false, needs_account = false;

    ExactDegreeState materialize() const {
        ExactDegreeState out;
        out.rows = dense_rows;
        for (size_t i = 0; i < WIDTH; ++i) {
            uint64_t n = (*dense)[i];
            if (!n) continue;
            out.keys.add(base + i);
            if (n > 1) out.extra.emplace(base + i, n - 1);
        }
        return out;
    }
    bool update(int64_t key) {
        uint64_t encoded = ExactDegreeState::encode(key);
        if (dense) {
            if ((encoded & ~(uint64_t(WIDTH) - 1)) == base) {
                ExactDegreeState::checked_add((*dense)[encoded - base], 1);
                ExactDegreeState::checked_add(dense_rows, 1);
                return false;
            }
            value = materialize();
            dense.reset();
            dense_disabled = true;
            needs_account = true;
        }
        bool changed = value.update(key);
        if (!dense_disabled && value.extra.size() >= 4096) {
            auto low = *value.keys.min(), high = *value.keys.max();
            base = low & ~(uint64_t(WIDTH) - 1);
            if ((high & ~(uint64_t(WIDTH) - 1)) == base) {
                dense = std::make_unique<std::array<uint64_t, WIDTH>>();
                ExactDegreeState::each(value.keys, [&](uint64_t k) { (*dense)[k - base] = 1; });
                for (auto [k, e] : value.extra) (*dense)[k - base] += e;
                dense_rows = value.rows;
                value = ExactDegreeState();
                needs_account = true;
                changed = true;
            } else
                dense_disabled = true;
        }
        return changed || needs_account;
    }
    void merge(const ExactDegreeState& part) {
        if (dense) {
            value = materialize();
            dense.reset();
        }
        value.merge(part);
    }
    std::string serialize() const { return dense ? materialize().serialize() : value.serialize(); }
    void account(FunctionContext* ctx) {
        int64_t bytes = dense ? sizeof(*dense) : 512 + 2 * value.keys.get_size_in_bytes() + value.extra.capacity() * 17;
        ctx->add_mem_usage(bytes - charged);
        charged = bytes;
        needs_account = false;
    }
};

template <bool MergeInput>
class ExactDegreeAggregateFunction final
        : public AggregateFunctionBatchHelper<ExactDegreeAggregateState, ExactDegreeAggregateFunction<MergeInput>> {
public:
    bool is_exception_safe() const override { return false; }
    void reset(FunctionContext* ctx, const Columns&, AggDataPtr state) const override {
        ctx->add_mem_usage(-this->data(state).charged);
        this->data(state) = ExactDegreeAggregateState();
    }
    void destroy(FunctionContext* ctx, AggDataPtr state) const override {
        ctx->add_mem_usage(-this->data(state).charged);
        this->data(state).~ExactDegreeAggregateState();
    }
    void update(FunctionContext* ctx, const Column** columns, AggDataPtr state, size_t row) const override {
        if constexpr (MergeInput) {
            merge(ctx, columns[0], state, row);
        } else {
            auto& value = this->data(state);
            if (value.update(down_cast<const Int64Column*>(columns[0])->immutable_data()[row]) &&
                (((++value.mutations & 63) == 1) || value.needs_account)) {
                value.account(ctx);
            }
        }
    }
    void merge(FunctionContext* ctx, const Column* column, AggDataPtr state, size_t row) const override {
        auto& value = this->data(state);
        auto charged = value.charged;
        value.merge(ExactDegreeState::deserialize(down_cast<const BinaryColumn*>(column)->get_slice(row)));
        value.charged = charged;
        value.account(ctx);
    }
    void serialize_to_column(FunctionContext*, ConstAggDataPtr state, Column* to) const override {
        std::string value = this->data(state).serialize();
        down_cast<BinaryColumn*>(to)->append(Slice(value));
    }
    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr state, Column* to) const override {
        serialize_to_column(ctx, state, to);
    }
    void convert_to_serialize_format(FunctionContext*, const Columns& src, size_t n,
                                     MutableColumnPtr& dst) const override {
        auto* out = down_cast<BinaryColumn*>(dst.get());
        for (size_t row = 0; row < n; ++row) {
            if constexpr (MergeInput) {
                out->append(down_cast<const BinaryColumn*>(src[0].get())->get_slice(row));
            } else {
                ExactDegreeState state;
                state.update(down_cast<const Int64Column*>(src[0].get())->immutable_data()[row]);
                auto bytes = state.serialize();
                out->append(Slice(bytes));
            }
        }
    }
    std::string get_name() const override { return MergeInput ? "stats_degree_merge" : "stats_degree_state"; }
};

} // namespace starrocks
