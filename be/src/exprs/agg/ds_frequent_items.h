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
#include <memory>
#include <string>

#include "column/binary_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "datasketches/frequent_items_sketch.hpp"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/stats_sketch_common.h"
#include "gutil/casts.h"

namespace starrocks {

template <LogicalType LT>
struct FrequentItemsState {
    using Item = SketchItemType<LT>;
    using Sketch = datasketches::frequent_items_sketch<Item, uint64_t, SketchItemHash<Item>>;

    std::unique_ptr<Sketch> sketch;
    // Reused for string items so that updating an item the sketch already holds does not allocate.
    std::string key_buffer;
    int64_t reported_mem = 0;
};

/**
 * ds_frequent_items(col, k [, lg_max_map_size]) -> VARCHAR
 *
 * JSON array of the k most frequent values of col with their estimated counts, most frequent
 * first: [["value","estimate"], ...]. Memory is bounded by 2^lg_max_map_size entries no matter
 * how many distinct values the column has (Misra-Gries / Space-Saving as implemented by
 * DataSketches frequent_items_sketch). Every value whose share of the rows exceeds
 * 3.5 / 2^lg_max_map_size is guaranteed to be present, and each estimate is off by at most that
 * share of the rows seen. The sketch merges across fragments.
 *
 * RETURN_TYPE: TYPE_VARCHAR
 * ARGS_TYPE: fixed-length or string column, INT, [INT]
 * SERIALIZED_TYPE: TYPE_VARBINARY
 */
template <LogicalType LT>
class FrequentItemsAggregateFunction final
        : public AggregateFunctionBatchHelper<FrequentItemsState<LT>, FrequentItemsAggregateFunction<LT>> {
public:
    using ColumnType = RunTimeColumnType<LT>;
    using State = FrequentItemsState<LT>;
    using Item = typename State::Item;
    using Sketch = typename State::Sketch;

    static constexpr int32_t DEFAULT_LG_MAX_MAP_SIZE = 14;
    static constexpr int32_t MIN_LG_MAX_MAP_SIZE = Sketch::LG_MIN_MAP_SIZE;
    static constexpr int32_t MAX_LG_MAX_MAP_SIZE = 20;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        auto& s = this->data(state);
        ctx->add_mem_usage(-s.reported_mem);
        s.reported_mem = 0;
        s.sketch.reset();
    }

    void update(FunctionContext* ctx, const Column** columns, AggDataPtr __restrict state,
                size_t row_num) const override {
        auto& s = this->data(state);
        if (UNLIKELY(s.sketch == nullptr)) {
            _create_sketch(ctx, s);
        }
        const auto* column = down_cast<const ColumnType*>(columns[0]);
        if constexpr (lt_is_string<LT>) {
            Slice v = column->get_slice(row_num);
            s.key_buffer.assign(v.data, v.size);
            s.sketch->update(s.key_buffer);
        } else {
            s.sketch->update(column->immutable_data()[row_num]);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        Slice bytes = down_cast<const BinaryColumn*>(column)->get_slice(row_num);
        if (bytes.size == 0) {
            // A fragment that saw no rows serializes an empty state.
            return;
        }
        Sketch other = Sketch::deserialize(bytes.data, bytes.size, SketchItemSerde<Item>());
        auto& s = this->data(state);
        if (s.sketch == nullptr) {
            s.sketch = std::make_unique<Sketch>(std::move(other));
            _report_mem(ctx, s, _lg_max_map_size(ctx));
        } else {
            s.sketch->merge(std::move(other));
        }
    }

    void serialize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        DCHECK(to->is_binary());
        auto* column = down_cast<BinaryColumn*>(to);
        const auto& s = this->data(state);
        if (s.sketch == nullptr) {
            column->append_default();
            return;
        }
        auto bytes = s.sketch->serialize(0, SketchItemSerde<Item>());
        column->append(Slice(bytes.data(), bytes.size()));
    }

    void convert_to_serialize_format(FunctionContext* ctx, const Columns& src, size_t chunk_size,
                                     MutableColumnPtr& dst) const override {
        const auto* column = down_cast<const ColumnType*>(src[0].get());
        auto* result = down_cast<BinaryColumn*>(dst.get());
        const int32_t lg_max_map_size = _lg_max_map_size(ctx);
        for (size_t i = 0; i < chunk_size; ++i) {
            Sketch sketch(lg_max_map_size);
            if constexpr (lt_is_string<LT>) {
                Slice v = column->get_slice(i);
                sketch.update(std::string(v.data, v.size));
            } else {
                sketch.update(column->immutable_data()[i]);
            }
            auto bytes = sketch.serialize(0, SketchItemSerde<Item>());
            result->append(Slice(bytes.data(), bytes.size()));
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const auto& s = this->data(state);
        std::string json = "[";
        if (s.sketch != nullptr && !s.sketch->is_empty()) {
            const int32_t k = _top_k(ctx);
            StatsValueCodec<LT> codec(ctx->get_arg_type(0));
            // Sorted by estimate, most frequent first.
            auto rows = s.sketch->get_frequent_items(datasketches::NO_FALSE_NEGATIVES);
            const size_t n = std::min<size_t>(rows.size(), k);
            for (size_t i = 0; i < n; ++i) {
                if (i > 0) {
                    json.push_back(',');
                }
                json.push_back('[');
                if constexpr (lt_is_string<LT>) {
                    append_json_string(json, rows[i].get_item());
                } else {
                    append_json_string(json, codec.to_string(rows[i].get_item()));
                }
                json.push_back(',');
                append_json_string(json, std::to_string(rows[i].get_estimate()));
                json.push_back(']');
            }
        }
        json.push_back(']');
        to->append_datum(Slice(json));
    }

    std::string get_name() const override { return "ds_frequent_items"; }

private:
    static int32_t _top_k(FunctionContext* ctx) {
        int32_t k = stats_const_int_arg(ctx, "ds_frequent_items", 1, 0);
        if (k <= 0) {
            throw std::runtime_error("ds_frequent_items: k must be a positive integer");
        }
        return k;
    }

    static int32_t _lg_max_map_size(FunctionContext* ctx) {
        int32_t lg = stats_const_int_arg(ctx, "ds_frequent_items", 2, DEFAULT_LG_MAX_MAP_SIZE);
        if (lg < MIN_LG_MAX_MAP_SIZE || lg > MAX_LG_MAX_MAP_SIZE) {
            throw std::runtime_error("ds_frequent_items: lg_max_map_size must be between " +
                                     std::to_string(MIN_LG_MAX_MAP_SIZE) + " and " +
                                     std::to_string(MAX_LG_MAX_MAP_SIZE));
        }
        return lg;
    }

    void _create_sketch(FunctionContext* ctx, State& s) const {
        const int32_t lg = _lg_max_map_size(ctx);
        s.sketch = std::make_unique<Sketch>(lg);
        _report_mem(ctx, s, lg);
    }

    // The hash map grows up to 2^lg entries; charge that bound once instead of per row.
    static void _report_mem(FunctionContext* ctx, State& s, int32_t lg) {
        constexpr int64_t item_bytes = sizeof(Item) + sizeof(uint64_t) + (lt_is_string<LT> ? 32 : 0);
        int64_t bound = (int64_t(1) << lg) * item_bytes;
        ctx->add_mem_usage(bound - s.reported_mem);
        s.reported_mem = bound;
    }
};

} // namespace starrocks
