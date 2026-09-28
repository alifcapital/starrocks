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

#include <utility>

#include "column/column.h"
#include "exec/pipeline/source_operator.h"
#include "runtime/types.h"

namespace starrocks::pipeline {

// RawValuesSourceOperator is optimized for large constant lists.
// Instead of evaluating expressions like UnionConstSourceOperator,
// it outputs slices of one column that the factory builds from the typed raw data.
class RawValuesSourceOperator final : public SourceOperator {
public:
    RawValuesSourceOperator(OperatorFactory* factory, int32_t id, int32_t plan_node_id, int32_t driver_sequence,
                            SlotId slot_id, ColumnPtr values, size_t start_index, size_t rows_count)
            : SourceOperator(factory, id, "raw_values_source", plan_node_id, false, driver_sequence),
              _slot_id(slot_id),
              _values(std::move(values)),
              _start_index(start_index),
              _rows_total(rows_count) {}

    bool has_output() const override { return !is_finished(); }

    bool is_finished() const override { return _is_finished || _next_processed_row_index >= _rows_total; }

    Status set_finished(RuntimeState* state) override {
        _is_finished = true;
        return Status::OK();
    }

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

private:
    const SlotId _slot_id;
    const ColumnPtr _values;

    const size_t _start_index;
    const size_t _rows_total;
    size_t _next_processed_row_index{0};
    bool _is_finished{false};
};

class RawValuesSourceOperatorFactory final : public SourceOperatorFactory {
public:
    // FE sends integer values as long_values, and the values of the other types as string_values, each the text of
    // a literal of the type.
    RawValuesSourceOperatorFactory(int32_t id, int32_t plan_node_id, const std::vector<SlotDescriptor*>& dst_slots,
                                   std::vector<int64_t>&& long_values, std::vector<std::string>&& string_values)
            : SourceOperatorFactory(id, "raw_values_source", plan_node_id),
              _dst_slots(dst_slots),
              _long_values(std::move(long_values)),
              _string_values(std::move(string_values)) {
        DCHECK(_dst_slots.size() == 1);
        DCHECK((_long_values.empty()) ^ (_string_values.empty()));
        _total_rows = _long_values.empty() ? _string_values.size() : _long_values.size();
    }

    bool support_event_scheduler() const override { return true; }

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override {
        // Divide the values into *degree_of_parallelism* parts
        size_t num_rows_per_driver = (_total_rows + degree_of_parallelism - 1) / degree_of_parallelism;
        size_t start_index = num_rows_per_driver * driver_sequence;
        DCHECK(_total_rows > start_index);
        size_t rows_count = std::min(num_rows_per_driver, _total_rows - start_index);
        // prepare builds the column before the drivers are created
        DCHECK(_values != nullptr);

        return std::make_shared<RawValuesSourceOperator>(this, _id, _plan_node_id, driver_sequence, _dst_slots[0]->id(),
                                                         _values, start_index, rows_count);
    }

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override { SourceOperatorFactory::close(state); }

    SourceOperatorFactory::AdaptiveState adaptive_initial_state() const override { return AdaptiveState::ACTIVE; }

    // Builds a column of the type from the values: long_values for an integer type, string_values parsed as
    // literals for a string, decimal, DATE or DATETIME type.
    static StatusOr<ColumnPtr> build_column(const TypeDescriptor& type, bool nullable,
                                            const std::vector<int64_t>& long_values,
                                            const std::vector<std::string>& string_values);

private:
    const std::vector<SlotDescriptor*>& _dst_slots;

    std::vector<int64_t> _long_values;
    std::vector<std::string> _string_values;

    size_t _total_rows;
    ColumnPtr _values;
};

} // namespace starrocks::pipeline
