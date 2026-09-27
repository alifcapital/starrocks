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

#include <gtest/gtest.h>

#include <thread>

#include "column/binary_column.h"
#include "exec/pipeline/adaptive/adaptive_dop_param.h"
#include "exec/pipeline/adaptive/collect_stats_context.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"

namespace starrocks::pipeline {
namespace {
ChunkPtr make_chunk(size_t rows, size_t width) {
    auto column = BinaryColumn::create();
    std::string value(width, 'x');
    for (size_t i = 0; i < rows; ++i) column->append(Slice(value));
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(column), 0);
    return chunk;
}

void add_sinkers(CollectStatsContext& ctx, size_t dop) {
    for (size_t i = 0; i < dop; ++i) ctx.incr_sinker();
}
} // namespace

TEST(AdaptiveDopByteLimitTest, WideChunkKeepsDopAndIsPreserved) {
    RuntimeState state;
    auto chunk = make_chunk(16, 1024);
    AdaptiveDopParam param;
    param.max_block_rows_per_driver_seq = 16384;
    param.max_block_bytes_per_driver_seq = chunk->memory_usage() / 2;
    CollectStatsContext ctx(&state, 2, param);
    add_sinkers(ctx, 2);
    ASSERT_OK(ctx.push_chunk(0, chunk));
    ASSERT_TRUE(ctx.is_downstream_ready());
    ASSERT_EQ("Passthrough", ctx.readable_state());
    ASSERT_EQ(2, ctx.downstream_dop());
    auto result = ctx.pull_chunk(0);
    ASSERT_TRUE(result.ok());
    ASSERT_EQ(chunk.get(), result.value().get());
    ASSERT_OK(ctx.set_finishing(0));
    ASSERT_OK(ctx.set_finishing(1));
    ASSERT_EQ(2, ctx.downstream_dop());
}

TEST(AdaptiveDopByteLimitTest, SmallInputStillReducesDop) {
    RuntimeState state;
    AdaptiveDopParam param;
    param.max_block_rows_per_driver_seq = 16384;
    param.max_block_bytes_per_driver_seq = 16 * 1024 * 1024;
    CollectStatsContext ctx(&state, 2, param);
    add_sinkers(ctx, 2);
    ASSERT_OK(ctx.push_chunk(0, make_chunk(16, 8)));
    ASSERT_FALSE(ctx.is_downstream_ready());
    ASSERT_OK(ctx.set_finishing(0));
    ASSERT_FALSE(ctx.is_downstream_ready());
    ASSERT_OK(ctx.set_finishing(1));
    ASSERT_EQ("RoundRobin", ctx.readable_state());
    ASSERT_EQ(1, ctx.downstream_dop());
}

TEST(AdaptiveDopByteLimitTest, ByteBudgetScalesWithDop) {
    RuntimeState state;
    auto chunk = make_chunk(16, 1024);
    AdaptiveDopParam param;
    param.max_block_rows_per_driver_seq = 16384;
    param.max_block_bytes_per_driver_seq = chunk->memory_usage();
    CollectStatsContext ctx(&state, 2, param);
    add_sinkers(ctx, 2);
    ASSERT_OK(ctx.push_chunk(0, chunk));
    ASSERT_FALSE(ctx.is_downstream_ready());
    ASSERT_OK(ctx.push_chunk(1, make_chunk(16, 1024)));
    ASSERT_EQ("Passthrough", ctx.readable_state());
    ASSERT_EQ(2, ctx.downstream_dop());
}

TEST(AdaptiveDopByteLimitTest, DisabledByteBudgetPreservesRowThreshold) {
    RuntimeState state;
    AdaptiveDopParam param;
    param.max_block_rows_per_driver_seq = 16;
    // Default zero also represents an older FE omitting the optional byte budget.
    CollectStatsContext ctx(&state, 2, param);
    add_sinkers(ctx, 2);
    ASSERT_OK(ctx.push_chunk(0, make_chunk(16, 1024)));
    ASSERT_FALSE(ctx.is_downstream_ready());
    ASSERT_OK(ctx.push_chunk(1, make_chunk(16, 1024)));
    ASSERT_EQ("Passthrough", ctx.readable_state());
    ASSERT_EQ(2, ctx.downstream_dop());
}

TEST(AdaptiveDopByteLimitTest, ConcurrentThresholdsKeepAllChunks) {
    RuntimeState state;
    AdaptiveDopParam param;
    param.max_block_rows_per_driver_seq = 16;
    param.max_block_bytes_per_driver_seq = 1024;
    CollectStatsContext ctx(&state, 2, param);
    add_sinkers(ctx, 2);
    std::thread first([&] { ASSERT_OK(ctx.push_chunk(0, make_chunk(32, 128))); });
    std::thread second([&] { ASSERT_OK(ctx.push_chunk(1, make_chunk(32, 128))); });
    first.join();
    second.join();
    ASSERT_OK(ctx.set_finishing(0));
    ASSERT_OK(ctx.set_finishing(1));
    ASSERT_EQ("Passthrough", ctx.readable_state());
    ASSERT_EQ(2, ctx.downstream_dop());
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(ctx.has_output(i));
        auto result = ctx.pull_chunk(i);
        ASSERT_TRUE(result.ok());
        ASSERT_EQ(32, result.value()->num_rows());
        ASSERT_FALSE(ctx.has_output(i));
    }
}
} // namespace starrocks::pipeline
