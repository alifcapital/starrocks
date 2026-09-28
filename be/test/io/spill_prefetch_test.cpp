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

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/config.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/input_stream.h"
#include "exec/spill/log_block_manager.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "exec/spill/spiller_factory.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "util/defer_op.h"
#include "util/failpoint/fail_point.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace starrocks::spill {

#ifdef FIU_ENABLE
namespace {
ChunkPtr make_int_chunk(int32_t base, size_t rows) {
    auto col = Int32Column::create();
    for (size_t i = 0; i < rows; ++i) {
        col->append(base + i);
    }
    auto chunk = std::make_shared<Chunk>();
    chunk->append_column(std::move(col), 0);
    return chunk;
}
} // namespace

// A prefetch that finds another prefetch reading leaves the refill to it. When a consumer takes the chunk of the
// reading prefetch before that prefetch releases, the reading prefetch must fill the buffer again: the consumer
// triggers no prefetch while the stream is not ready, so nothing else would fill it.
TEST(SpillPrefetchTest, refills_chunk_taken_before_release) {
    auto path = config::storage_root_path + "/spill_prefetch_test/" + print_id(generate_uuid());
    ASSERT_OK(FileSystem::Default()->create_dir_recursive(path));
    DirManager dir_mgr;
    ASSERT_OK(dir_mgr.init(path));
    LogBlockManager block_mgr(generate_uuid(), &dir_mgr);
    RuntimeState state;
    state.set_chunk_size(config::vector_chunk_size);
    RuntimeProfile profile("spill_prefetch_test");
    std::atomic_int64_t spill_bytes{0};
    SpillProcessMetrics metrics(&profile, &spill_bytes);

    SpilledOptions options;
    options.mem_table_pool_size = 2;
    options.spill_mem_table_bytes_size = 1024 * 1024;
    options.spill_type = SpillFormaterType::SPILL_BY_COLUMN;
    options.block_manager = &block_mgr;
    auto spiller = make_spilled_factory()->create(options);
    spiller->set_metrics(metrics);
    ASSERT_OK(spiller->prepare(&state));
    for (int i = 0; i < 16; ++i) {
        ASSERT_OK(spiller->spill<SyncTaskExecutor>(&state, make_int_chunk(i * 1000, 1000), EmptyMemGuard{}));
    }
    ASSERT_OK(spiller->flush<SyncTaskExecutor>(&state, EmptyMemGuard{}));
    ASSERT_OK(spiller->_acquire_input_stream(&state));

    std::vector<SpillInputStream*> io_streams;
    spiller->_reader->_stream->get_io_stream(&io_streams);
    ASSERT_EQ(1, io_streams.size());
    SpillInputStream* buffered = io_streams[0];
    ASSERT_FALSE(buffered->is_ready());

    PFailPointTriggerMode trigger_mode;
    trigger_mode.set_mode(FailPointTriggerModeType::ENABLE);
    auto* fp = failpoint::FailPointRegistry::GetInstance()->get("spill_prefetch_after_put");
    ASSERT_TRUE(fp != nullptr);
    fp->setMode(trigger_mode);
    DeferOp disable_fp([&]() {
        trigger_mode.set_mode(FailPointTriggerModeType::DISABLE);
        fp->setMode(trigger_mode);
    });

    // The reading prefetch stops after its first put, while it still holds the prefetch.
    std::thread reader([buffered]() {
        workgroup::YieldContext yield_ctx;
        yield_ctx.task_context_data = std::make_shared<SpillIOTaskContext>();
        SerdeContext serde_ctx;
        EXPECT_OK(buffered->prefetch(yield_ctx, serde_ctx));
    });
    while (!buffered->is_ready()) {
        std::this_thread::yield();
    }

    workgroup::YieldContext yield_ctx;
    yield_ctx.task_context_data = std::make_shared<SpillIOTaskContext>();
    SerdeContext serde_ctx;
    // The consumer takes the chunk, and the prefetch it triggers finds the reading prefetch in progress.
    ASSERT_OK(buffered->get_next(yield_ctx, serde_ctx).status());
    ASSERT_FALSE(buffered->is_ready());
    ASSERT_OK(buffered->prefetch(yield_ctx, serde_ctx));
    ASSERT_FALSE(buffered->is_ready());

    spill_prefetch_after_put_barrier().arrive_B();
    reader.join();
    ASSERT_TRUE(buffered->is_ready());
}
#endif

} // namespace starrocks::spill
