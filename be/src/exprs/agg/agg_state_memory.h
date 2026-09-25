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

#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"

namespace starrocks {

// Track the bytes actually reported by this state. Nested batch/row guards can
// synchronize independently without charging an allocation twice.
struct AggStateMemoryAccount {
    void sync_memory_usage(FunctionContext* ctx, int64_t bytes) const {
        ctx->add_mem_usage(bytes - _reported_bytes);
        _reported_bytes = bytes;
    }
    void release_memory_usage(FunctionContext* ctx) const { sync_memory_usage(ctx, 0); }

private:
    mutable int64_t _reported_bytes = 0;
};

template <typename State>
class ScopedAggStateMemoryUsage {
public:
    ScopedAggStateMemoryUsage(FunctionContext* ctx, const State& state) : _ctx(ctx), _state(state) {}
    ~ScopedAggStateMemoryUsage() { _state.sync_memory_usage(_ctx, _state.mem_usage()); }
    ScopedAggStateMemoryUsage(const ScopedAggStateMemoryUsage&) = delete;
    ScopedAggStateMemoryUsage& operator=(const ScopedAggStateMemoryUsage&) = delete;

private:
    FunctionContext* _ctx;
    const State& _state;
};

template <typename State, typename Derived>
class MemoryTrackedAggregateFunctionBatchHelper : public AggregateFunctionBatchHelper<State, Derived> {
public:
    void create(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        AggregateFunctionBatchHelper<State, Derived>::create(ctx, ptr);
        this->data(ptr).sync_memory_usage(ctx, this->data(ptr).mem_usage());
    }
    void destroy(FunctionContext* ctx, AggDataPtr __restrict ptr) const override {
        this->data(ptr).release_memory_usage(ctx);
        AggregateFunctionBatchHelper<State, Derived>::destroy(ctx, ptr);
    }
};

} // namespace starrocks
