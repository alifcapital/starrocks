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

#include "column/binary_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "datasketches/kll_sketch.hpp"
#include "exprs/agg/aggregate.h"
#include "exprs/agg/stats_sketch_common.h"
#include "gutil/casts.h"

namespace starrocks {

template <LogicalType LT>
struct KllQuantilesState {
    using Item = std::conditional_t<lt_is_string<LT>, StatsString, RunTimeCppType<LT>>;
    using Sketch = datasketches::kll_sketch<Item, std::less<Item>, AggregateStateAllocator<Item>>;

    std::unique_ptr<Sketch> sketch;
};

/**
 * ds_kll_quantiles(col, num_buckets [, k]) -> VARCHAR
 *
 * JSON array of num_buckets + 1 quantile boundaries of col at ranks 0, 1/num_buckets, ...,
 * 1, formatted as strings: ["b0","b1",...]. Boundaries that repeat (a value heavier than one
 * bucket) are collapsed, so the array holds distinct ascending values; the first and last are
 * the exact min and max. The KLL sketch (DataSketches) keeps O(k) items and bounds the rank
 * error of each boundary (about 1.3% at the default k of 200), with no sort of the input.
 * The sketch merges across fragments.
 *
 * RETURN_TYPE: TYPE_VARCHAR
 * ARGS_TYPE: fixed-length or string column, INT, [INT]
 * SERIALIZED_TYPE: TYPE_VARBINARY
 */
template <LogicalType LT>
class KllQuantilesAggregateFunction final
        : public AggregateFunctionBatchHelper<KllQuantilesState<LT>, KllQuantilesAggregateFunction<LT>> {
public:
    using ColumnType = RunTimeColumnType<LT>;
    using State = KllQuantilesState<LT>;
    using Item = typename State::Item;
    using Sketch = typename State::Sketch;

    static constexpr int32_t MAX_NUM_BUCKETS = 10000; // Keep in sync with FE FunctionSet.DS_KLL_MAX_BUCKETS.
    static constexpr int32_t DEFAULT_K = datasketches::kll_constants::DEFAULT_K;

    void reset(FunctionContext* ctx, const Columns& args, AggDataPtr state) const override {
        auto& s = this->data(state);
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
            Slice value = column->get_slice(row_num);
            s.sketch->update(Item(value.data, value.size));
        } else {
            s.sketch->update(column->immutable_data()[row_num]);
        }
    }

    void merge(FunctionContext* ctx, const Column* column, AggDataPtr __restrict state, size_t row_num) const override {
        DCHECK(column->is_binary());
        if (this->data(state).sketch == nullptr) {
            _num_buckets(ctx);
            _k(ctx);
        }
        Slice bytes = down_cast<const BinaryColumn*>(column)->get_slice(row_num);
        if (bytes.size == 0) {
            // A fragment that saw no rows serializes an empty state.
            return;
        }
        Sketch other = Sketch::deserialize(bytes.data, bytes.size, SketchItemSerde<Item>());
        auto& s = this->data(state);
        if (s.sketch == nullptr) {
            s.sketch = std::make_unique<Sketch>(std::move(other));
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
        _num_buckets(ctx);
        const uint16_t k = _k(ctx);
        for (size_t i = 0; i < chunk_size; ++i) {
            Sketch sketch(k);
            if constexpr (lt_is_string<LT>) {
                Slice value = column->get_slice(i);
                sketch.update(Item(value.data, value.size));
            } else {
                sketch.update(column->immutable_data()[i]);
            }
            auto bytes = sketch.serialize(0, SketchItemSerde<Item>());
            result->append(Slice(bytes.data(), bytes.size()));
        }
    }

    void finalize_to_column(FunctionContext* ctx, ConstAggDataPtr __restrict state, Column* to) const override {
        const int32_t num_buckets = _num_buckets(ctx);
        _k(ctx);
        const auto& s = this->data(state);
        std::string json = "[";
        if (s.sketch != nullptr && !s.sketch->is_empty()) {
            std::vector<double> ranks(static_cast<size_t>(num_buckets) + 1);
            for (size_t i = 0; i < ranks.size(); ++i) {
                ranks[i] = double(i) / num_buckets;
            }
            auto quantiles = s.sketch->get_quantiles(ranks.data(), ranks.size(), true);
            // The sketch keeps the smallest and the largest item exactly; the quantiles at rank 0 and
            // 1 come from its sample.
            quantiles.front() = s.sketch->get_min_item();
            quantiles.back() = s.sketch->get_max_item();
            StatsValueCodec<LT> codec(ctx->get_arg_type(0));
            bool first = true;
            for (size_t i = 0; i < quantiles.size(); ++i) {
                if (i > 0 && quantiles[i] == quantiles[i - 1]) {
                    continue;
                }
                if (!first) {
                    json.push_back(',');
                }
                first = false;
                if constexpr (lt_is_string<LT>) {
                    append_json_string(json, std::string_view(quantiles[i].data(), quantiles[i].size()));
                } else {
                    append_json_string(json, codec.to_string(quantiles[i]));
                }
            }
        }
        json.push_back(']');
        to->append_datum(Slice(json));
    }

    std::string get_name() const override { return "ds_kll_quantiles"; }

private:
    static int32_t _num_buckets(FunctionContext* ctx) {
        int32_t n = stats_const_int_arg(ctx, "ds_kll_quantiles", 1, 0);
        if (n <= 0 || n > MAX_NUM_BUCKETS) {
            throw std::runtime_error("ds_kll_quantiles: num_buckets must be between 1 and " +
                                     std::to_string(MAX_NUM_BUCKETS));
        }
        return n;
    }

    static uint16_t _k(FunctionContext* ctx) {
        int32_t k = stats_const_int_arg(ctx, "ds_kll_quantiles", 2, DEFAULT_K);
        if (k < Sketch::MIN_K || k > Sketch::MAX_K) {
            throw std::runtime_error("ds_kll_quantiles: k must be between " + std::to_string(Sketch::MIN_K) + " and " +
                                     std::to_string(Sketch::MAX_K));
        }
        return static_cast<uint16_t>(k);
    }

    void _create_sketch(FunctionContext* ctx, State& s) const {
        _num_buckets(ctx);
        const uint16_t k = _k(ctx);
        s.sketch = std::make_unique<Sketch>(k);
    }
};

} // namespace starrocks
