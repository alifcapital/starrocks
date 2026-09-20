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
#include <cstring>
#include <string>
#include <vector>

#include "column/binary_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/stats_sketch_common.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "simdjson.h"
#include "types/constexpr.h"
#include "types/hll.h"
#include "util/phmap/phmap.h"

namespace starrocks {

template <LogicalType LT>
struct HistogramByBoundsState {
    using Item = RunTimeCppType<LT>;

    struct Bucket {
        int64_t count = 0;
        Item lo{};
        Item hi{};
        // Rows whose value equals hi.
        int64_t upper_repeats = 0;
        HyperLogLog hll;
    };

    bool initialized = false;
    // Owns the bytes of string MCV values parsed from the specification.
    MemPool mem_pool;
    std::vector<Item> mcv_values;
    std::vector<int64_t> mcv_counts;
    phmap::flat_hash_map<Item, uint32_t, SketchItemHash<Item>> mcv_index;
    // Sorted, distinct.
    std::vector<Item> bounds;
    std::vector<Bucket> buckets;
    int64_t reported_mem = 0;
};

/**
 * histogram_by_bounds(col, mcv_json, bounds_json) -> VARCHAR
 *
 * Second pass of statistics collection: given the most common values and the bucket boundaries
 * found by a first pass (ds_frequent_items, ds_kll_quantiles), counts exactly how many rows carry
 * each MCV and fills the buckets between the boundaries with the remaining rows. Both
 * specifications are constant JSON arrays of value strings in the column's literal form.
 *
 * With m boundaries b0 < b1 < ... < b(m-1) there are m-1 buckets: [b0, b1], (b1, b2], ...,
 * (b(m-2), b(m-1)]; rows below b0 fall into the first bucket and rows above b(m-1) into the
 * last. MCV rows are excluded from the buckets, so the MCV counts and the bucket counts
 * partition the rows. Each bucket reports its actual min and max, its row count, how many rows
 * equal the max, and an HLL estimate of its distinct values. String columns take an empty
 * bounds_json and only get MCV counts.
 *
 * Result: {"mcv":[["value","count"],...],"buckets":[["lo","hi","count","upper_repeats","ndv"],...]}
 * MCV entries keep the specification order; empty buckets are left out.
 *
 * The state is O(|mcv| + |bounds|) and merges across fragments.
 *
 * RETURN_TYPE: TYPE_VARCHAR
 * ARGS_TYPE: fixed-length or string column, VARCHAR, VARCHAR
 * SERIALIZED_TYPE: TYPE_VARBINARY
 */
template <LogicalType LT>
class HistogramByBoundsAggregateFunction final
        : public AggregateFunctionBatchHelper<HistogramByBoundsState<LT>, HistogramByBoundsAggregateFunction<LT>> {
public:
    using ColumnType = RunTimeColumnType<LT>;
    using State = HistogramByBoundsState<LT>;
    using Item = typename State::Item;
    using Bucket = typename State::Bucket;

    static constexpr const char* NAME = "histogram_by_bounds";

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        auto& s = this->data(state);
        ctx->add_mem_usage(-s.reported_mem);
        s.reported_mem = 0;
        s.initialized = false;
        s.mcv_values.clear();
        s.mcv_counts.clear();
        s.mcv_index.clear();
        s.bounds.clear();
        s.buckets.clear();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto& s = this->data(state);
        if (UNLIKELY(!s.initialized)) {
            _init(ctx, s);
        }
        const auto* column = down_cast<const ColumnType*>(columns[0]);
        Item v = _value(column, row_num);
        auto it = s.mcv_index.find(v);
        if (it != s.mcv_index.end()) {
            s.mcv_counts[it->second]++;
            return;
        }
        if constexpr (!lt_is_string<LT>) {
            if (s.buckets.empty()) {
                return;
            }
            Bucket& b = s.buckets[_bucket_index(s.bounds, v)];
            if (b.count == 0) {
                b.lo = v;
                b.hi = v;
                b.upper_repeats = 1;
            } else {
                if (v < b.lo) {
                    b.lo = v;
                }
                if (b.hi < v) {
                    b.hi = v;
                    b.upper_repeats = 1;
                } else if (v == b.hi) {
                    b.upper_repeats++;
                }
            }
            b.count++;
            b.hll.update(HashUtil::murmur_hash64A(&v, sizeof(v), HashUtil::MURMUR_SEED));
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice bytes = down_cast<const BinaryColumn*>(column)->get_slice(row_num);
        if (bytes.size == 0) {
            // A fragment that saw no rows serializes an empty state.
            return;
        }
        auto& s = this->data(state);
        if (!s.initialized) {
            _init(ctx, s);
        }
        Reader in(bytes);
        uint32_t num_mcv = in.template read<uint32_t>();
        uint32_t num_buckets = in.read_at(sizeof(uint32_t) + num_mcv * sizeof(int64_t));
        if (num_mcv != s.mcv_counts.size() || num_buckets != s.buckets.size()) {
            throw std::runtime_error(std::string(NAME) + ": merging states built from different specifications");
        }
        for (uint32_t i = 0; i < num_mcv; ++i) {
            s.mcv_counts[i] += in.template read<int64_t>();
        }
        in.skip(sizeof(uint32_t));
        for (uint32_t i = 0; i < num_buckets; ++i) {
            int64_t count = in.template read<int64_t>();
            if (count == 0) {
                continue;
            }
            Item lo = in.template read<Item>();
            Item hi = in.template read<Item>();
            int64_t upper_repeats = in.template read<int64_t>();
            uint32_t hll_size = in.template read<uint32_t>();
            HyperLogLog hll(in.read_bytes(hll_size));
            _merge_bucket(s.buckets[i], count, lo, hi, upper_repeats, hll);
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        const auto& s = this->data(state);
        if (!s.initialized) {
            column->append_default();
            return;
        }
        std::string bytes;
        _serialize(s, &bytes);
        column->append(Slice(bytes));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        auto* result = down_cast<BinaryColumn*>(dst.get());
        std::vector<const Column*> columns;
        columns.reserve(src.size());
        for (const auto& c : src) {
            columns.push_back(c.get());
        }
        State s;
        _init(ctx, s);
        std::string bytes;
        for (size_t i = 0; i < chunk_size; ++i) {
            std::fill(s.mcv_counts.begin(), s.mcv_counts.end(), 0);
            for (Bucket& b : s.buckets) {
                b = Bucket();
            }
            update(ctx, columns.data(), reinterpret_cast<AggDataPtr>(&s), i);
            bytes.clear();
            _serialize(s, &bytes);
            result->append(Slice(bytes));
        }
        ctx->add_mem_usage(-s.reported_mem);
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        auto& s = const_cast<State&>(this->data(state));
        if (!s.initialized) {
            // No input rows: still report every MCV with a zero count.
            _init(ctx, s);
        }
        StatsValueCodec<LT> codec(ctx->get_arg_type(0));
        std::string json = "{\"mcv\":[";
        for (size_t i = 0; i < s.mcv_values.size(); ++i) {
            if (i > 0) {
                json.push_back(',');
            }
            json.push_back('[');
            append_json_string(json, _text(codec, s.mcv_values[i]));
            json.push_back(',');
            append_json_string(json, std::to_string(s.mcv_counts[i]));
            json.push_back(']');
        }
        json += "],\"buckets\":[";
        bool first = true;
        for (const Bucket& b : s.buckets) {
            if (b.count == 0) {
                continue;
            }
            if (!first) {
                json.push_back(',');
            }
            first = false;
            int64_t ndv = std::max<int64_t>(1, b.hll.estimate_cardinality());
            json.push_back('[');
            append_json_string(json, _text(codec, b.lo));
            json.push_back(',');
            append_json_string(json, _text(codec, b.hi));
            json.push_back(',');
            append_json_string(json, std::to_string(b.count));
            json.push_back(',');
            append_json_string(json, std::to_string(b.upper_repeats));
            json.push_back(',');
            append_json_string(json, std::to_string(ndv));
            json.push_back(']');
        }
        json += "]}";
        to->append_datum(Slice(json));
    }

    std::string get_name() const override { return NAME; }

private:
    // Sequential reader over a serialized state; every read is bounds-checked.
    class Reader {
    public:
        explicit Reader(Slice bytes) : _data(bytes.data), _size(bytes.size) {}

        template <typename T>
        T read() {
            T v;
            memcpy(&v, read_bytes(sizeof(T)).data, sizeof(T));
            return v;
        }

        // Reads a uint32_t at an absolute offset without moving the cursor.
        uint32_t read_at(size_t offset) const {
            if (offset + sizeof(uint32_t) > _size) {
                _throw();
            }
            uint32_t v;
            memcpy(&v, _data + offset, sizeof(v));
            return v;
        }

        void skip(size_t n) { read_bytes(n); }

        Slice read_bytes(size_t n) {
            if (_pos + n > _size) {
                _throw();
            }
            Slice s(_data + _pos, n);
            _pos += n;
            return s;
        }

    private:
        [[noreturn]] static void _throw() {
            throw std::runtime_error(std::string(NAME) + ": truncated serialized state");
        }

        const char* _data;
        size_t _size;
        size_t _pos = 0;
    };

    static Item _value(const ColumnType* column, size_t row_num) {
        if constexpr (lt_is_string<LT>) {
            return column->get_slice(row_num);
        } else {
            return column->immutable_data()[row_num];
        }
    }

    static std::string _text(const StatsValueCodec<LT>& codec, const Item& v) {
        if constexpr (lt_is_string<LT>) {
            return v.to_string();
        } else {
            return codec.to_string(v);
        }
    }

    // Number of interior boundaries strictly below v: bucket 0 is [b0, b1], bucket i is (bi, bi+1].
    static size_t _bucket_index(const std::vector<Item>& bounds, const Item& v) {
        if (bounds.size() <= 2) {
            return 0;
        }
        auto begin = bounds.begin() + 1;
        auto end = bounds.end() - 1;
        return std::lower_bound(begin, end, v) - begin;
    }

    static void _merge_bucket(Bucket& b, int64_t count, const Item& lo, const Item& hi, int64_t upper_repeats,
                              const HyperLogLog& hll) {
        if (b.count == 0) {
            b.lo = lo;
            b.hi = hi;
            b.upper_repeats = upper_repeats;
        } else {
            if (lo < b.lo) {
                b.lo = lo;
            }
            if (b.hi < hi) {
                b.hi = hi;
                b.upper_repeats = upper_repeats;
            } else if (hi == b.hi) {
                b.upper_repeats += upper_repeats;
            }
        }
        b.count += count;
        b.hll.merge(hll);
    }

    template <typename T>
    static void _put(std::string* out, const T& v) {
        out->append(reinterpret_cast<const char*>(&v), sizeof(T));
    }

    // Layout: u32 num_mcv, i64 count[num_mcv], u32 num_buckets, then per bucket i64 count and,
    // when the count is non-zero, Item lo, Item hi, i64 upper_repeats, u32 hll_size, hll bytes.
    static void _serialize(const State& s, std::string* out) {
        _put(out, static_cast<uint32_t>(s.mcv_counts.size()));
        for (int64_t c : s.mcv_counts) {
            _put(out, c);
        }
        _put(out, static_cast<uint32_t>(s.buckets.size()));
        for (const Bucket& b : s.buckets) {
            _put(out, b.count);
            if (b.count == 0) {
                continue;
            }
            _put(out, b.lo);
            _put(out, b.hi);
            _put(out, b.upper_repeats);
            size_t offset = out->size();
            out->resize(offset + sizeof(uint32_t) + b.hll.max_serialized_size());
            auto* hll_dst = reinterpret_cast<uint8_t*>(out->data() + offset + sizeof(uint32_t));
            uint32_t hll_size = b.hll.serialize(hll_dst);
            memcpy(out->data() + offset, &hll_size, sizeof(hll_size));
            out->resize(offset + sizeof(uint32_t) + hll_size);
        }
    }

    static void _parse_values(FunctionContext* ctx, int arg, const StatsValueCodec<LT>& codec, MemPool* mem_pool,
                              std::vector<Item>* out) {
        std::string text = stats_const_string_arg(ctx, NAME, arg);
        try {
            simdjson::padded_string padded(text);
            simdjson::ondemand::parser parser;
            simdjson::ondemand::document doc;
            parser.iterate(padded).get(doc);
            simdjson::ondemand::array array = doc.get_array();
            for (auto element : array) {
                std::string_view value_view;
                if (element.get_string().get(value_view) != simdjson::SUCCESS) {
                    throw std::runtime_error(std::string(NAME) + ": argument " + std::to_string(arg + 1) +
                                             " must be a JSON array of strings");
                }
                std::string value_text{value_view};
                Item v;
                Status st = codec.from_string(value_text, &v, mem_pool);
                if (!st.ok()) {
                    throw std::runtime_error(std::string(NAME) + ": argument " + std::to_string(arg + 1) +
                                             " holds a value that is not valid for the column type: " + value_text);
                }
                out->push_back(v);
            }
        } catch (const simdjson::simdjson_error& e) {
            throw std::runtime_error(std::string(NAME) + ": argument " + std::to_string(arg + 1) +
                                     " must be a JSON array of strings");
        }
    }

    static void _init(FunctionContext* ctx, State& s) {
        StatsValueCodec<LT> codec(ctx->get_arg_type(0));
        _parse_values(ctx, 1, codec, &s.mem_pool, &s.mcv_values);
        _parse_values(ctx, 2, codec, &s.mem_pool, &s.bounds);
        if constexpr (lt_is_string<LT>) {
            if (!s.bounds.empty()) {
                throw std::runtime_error(std::string(NAME) + ": buckets are not supported for string columns");
            }
        } else {
            std::sort(s.bounds.begin(), s.bounds.end());
            s.bounds.erase(std::unique(s.bounds.begin(), s.bounds.end()), s.bounds.end());
        }
        s.mcv_counts.assign(s.mcv_values.size(), 0);
        s.mcv_index.reserve(s.mcv_values.size());
        for (uint32_t i = 0; i < s.mcv_values.size(); ++i) {
            // A value repeated in the specification counts under its first position.
            s.mcv_index.try_emplace(s.mcv_values[i], i);
        }
        s.buckets.clear();
        s.buckets.resize(s.bounds.empty() ? 0 : std::max<size_t>(1, s.bounds.size() - 1));
        s.initialized = true;

        // Each bucket's HLL grows to a full register set at most; charge that bound once.
        int64_t bound = int64_t(s.mcv_values.size()) * (sizeof(Item) + 3 * sizeof(int64_t)) +
                        int64_t(s.buckets.size()) * (sizeof(Bucket) + HLL_REGISTERS_COUNT);
        ctx->add_mem_usage(bound - s.reported_mem);
        s.reported_mem = bound;
    }
};

} // namespace starrocks
