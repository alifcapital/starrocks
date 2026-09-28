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
#include <vector>

#include "column/chunk.h"
#include "column/fixed_length_column.h"
#include "common/config.h"
#include "exec/spill/dir_manager.h"
#include "exec/spill/executor.h"
#include "exec/spill/log_block_manager.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "exec/spill/spiller_factory.h"
#include "fs/fs.h"
#include "runtime/runtime_state.h"
#include "testutil/assert.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace starrocks::spill {
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

// Keeps submitted tasks until run_all(), so a test can change state while a task is queued.
struct QueuedTaskExecutor {
    static std::vector<workgroup::ScanTask>& tasks() {
        static std::vector<workgroup::ScanTask> queued;
        return queued;
    }
    static Status submit(workgroup::ScanTask task) {
        tasks().emplace_back(std::move(task));
        return Status::OK();
    }
    static void force_submit(workgroup::ScanTask task) { (void)submit(std::move(task)); }
    static void run_all() {
        while (!tasks().empty()) {
            auto task = std::move(tasks().front());
            tasks().erase(tasks().begin());
            do {
                task.run();
            } while (!task.is_finished());
        }
    }
};
} // namespace

// The spillable hash join probe owns its partition readers and drops them when the query is cancelled, while a
// restore task for such a reader can still be queued. The task must complete its IO when it runs, or the spiller
// reports running IO forever and the probe driver never leaves PENDING_FINISH.
TEST(SpillRestoreReaderTest, queued_restore_completes_after_reader_is_dropped) {
    auto path = config::storage_root_path + "/spill_restore_reader_test/" + print_id(generate_uuid());
    ASSERT_OK(FileSystem::Default()->create_dir_recursive(path));
    DirManager dir_mgr;
    ASSERT_OK(dir_mgr.init(path));
    LogBlockManager block_mgr(generate_uuid(), &dir_mgr);
    RuntimeState state;
    state.set_chunk_size(config::vector_chunk_size);
    RuntimeProfile profile("spill_restore_reader_test");
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
    ASSERT_FALSE(spiller->has_running_io_tasks());

    std::shared_ptr<SpillInputStream> stream;
    ASSERT_OK(spiller->_writer->acquire_stream(&stream));
    auto reader = std::make_shared<SpillerReader>(spiller.get());
    reader->set_stream(std::move(stream));
    // The probe watches the reader in the guard of its restore tasks.
    auto guard = ResourceMemTrackerGuard(nullptr, spiller->weak_from_this(), std::weak_ptr<SpillerReader>(reader));
    ASSERT_OK(reader->trigger_restore<QueuedTaskExecutor>(&state, guard));
    ASSERT_EQ(1, QueuedTaskExecutor::tasks().size());
    ASSERT_TRUE(spiller->has_running_io_tasks());

    reader.reset();
    QueuedTaskExecutor::run_all();
    ASSERT_FALSE(spiller->has_running_io_tasks());
}

} // namespace starrocks::spill
