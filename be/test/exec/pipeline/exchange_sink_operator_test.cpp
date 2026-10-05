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

#include "exec/pipeline/exchange/exchange_sink_operator.h"

#include <brpc/server.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/config.h"
#include "exec/pipeline/exchange/sink_buffer.h"
#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/query_context.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/Partitions_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/data.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "testutil/assert.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/internal_service_recoverable_stub.h"

namespace starrocks::pipeline {

class ExchangeSinkOperatorTest : public ::testing::Test {
public:
    void SetUp() override {
        BackendOptions::set_localhost("0.0.0.0");

        _exec_env = ExecEnv::GetInstance();

        _query_context = std::make_shared<QueryContext>();
        _query_context->set_exec_env(_exec_env);
        _query_context->init_mem_tracker(-1, GlobalEnv::GetInstance()->process_mem_tracker());

        TQueryOptions query_options;
        // Use a large query timeout so an in-flight RPC does not complete on its own during the
        // cancellation test; the test relies on cancel_one_sinker() to abort it promptly instead.
        query_options.__set_query_timeout(300);
        TQueryGlobals query_globals;
        _runtime_state = std::make_shared<RuntimeState>(_fragment_id, query_options, query_globals, _exec_env);
        _runtime_state->set_query_ctx(_query_context.get());
        _runtime_state->init_instance_mem_tracker();

        _fragment_context = std::make_shared<FragmentContext>();
        _fragment_context->set_fragment_instance_id(_fragment_id);
        _fragment_context->set_runtime_state(std::shared_ptr<RuntimeState>{_runtime_state});
        _runtime_state->set_fragment_ctx(_fragment_context.get());

        TNetworkAddress address;
        address.__set_hostname(BackendOptions::get_local_ip());
        address.__set_port(config::brpc_port);
        // Set lo=-1 so Channel::init skips brpc stub creation (no brpc infra in test env).
        TUniqueId dest_fragment_id;
        dest_fragment_id.__set_lo(-1);
        dest_fragment_id.__set_hi(0);
        _destination.__set_fragment_instance_id(dest_fragment_id);
        _destination.__set_brpc_server(address);

        _destinations = {_destination};
        _sink_buffer = std::make_shared<SinkBuffer>(_fragment_context.get(), _destinations, /*is_dest_merge*/ false);

        _factory = std::make_shared<ExchangeSinkOperatorFactory>(
                0, 0, _sink_buffer, TPartitionType::UNPARTITIONED, _destinations,
                /*is_pipeline_level_shuffle*/ false, /*num_shuffles_per_channel*/ 1,
                /*sender_id*/ 0, /*dest_node_id*/ 0, /*partition_exprs*/ std::vector<ExprContext*>(),
                /*enable_exchange_pass_through*/ false, /*enable_exchange_perf*/ false, _fragment_context.get(),
                /*output_columns*/ std::vector<int32_t>(),
                /*bucket_properties*/ std::vector<TBucketProperty>());
        _factory->set_runtime_state(_runtime_state.get());
    }

    void TearDown() override {}

protected:
    TUniqueId _fragment_id;
    ExecEnv* _exec_env = nullptr;
    std::shared_ptr<QueryContext> _query_context;
    std::shared_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<FragmentContext> _fragment_context;
    std::vector<TPlanFragmentDestination> _destinations;
    std::shared_ptr<SinkBuffer> _sink_buffer;
    std::shared_ptr<ExchangeSinkOperatorFactory> _factory;
    TPlanFragmentDestination _destination;
};

// A brpc PInternalService whose transmit_chunk blocks until explicitly released, so the client-side
// RPC stays in-flight. This lets us assert that cancel_one_sinker() aborts outstanding RPCs actively
// rather than waiting for them to drain (which would otherwise take until the RPC timeout).
class HangingInternalService : public starrocks::PInternalService {
public:
    using Latch = CountDownLatch;

    void transmit_chunk(google::protobuf::RpcController* /*controller*/,
                        const starrocks::PTransmitChunkParams* /*request*/, starrocks::PTransmitChunkResult* response,
                        google::protobuf::Closure* done) override {
        received.count_down();
        // Block the handler until the test tears down, keeping the client RPC pending.
        release.wait();
        if (response != nullptr) {
            response->mutable_status()->set_status_code(0);
        }
        done->Run();
    }

    Latch received{1};
    Latch release{1};
};

class SinkBufferCancelTest : public ExchangeSinkOperatorTest {
protected:
    // Build a SinkBuffer with a single real remote destination pointing at the given brpc port.
    std::shared_ptr<SinkBuffer> make_remote_sink_buffer(int port, const TUniqueId& dest_id) {
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(port);

        TPlanFragmentDestination dest;
        dest.__set_fragment_instance_id(dest_id);
        dest.__set_brpc_server(addr);

        std::vector<TPlanFragmentDestination> destinations{dest};
        return std::make_shared<SinkBuffer>(_fragment_context.get(), destinations, /*is_dest_merge*/ false);
    }

    static TUniqueId make_dest_id(int64_t lo) {
        TUniqueId id;
        id.__set_hi(0);
        id.__set_lo(lo);
        return id;
    }

    static TransmitChunkInfo make_request(const TUniqueId& dest_id, int port,
                                          std::shared_ptr<PInternalService_RecoverableStub> stub) {
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(port);

        auto params = std::make_shared<PTransmitChunkParams>();
        params->set_eos(false);
        return TransmitChunkInfo{dest_id, std::move(stub), std::move(params), nullptr, /*request_byte_size*/ 0, addr};
    }
};

// cancel_one_sinker() must actively cancel in-flight RPCs. We launch an RPC against a server that
// never responds, then cancel and assert the buffer reaches the finished state quickly (i.e. the
// failure callback fired with ECANCELED) rather than blocking until the RPC timeout.
TEST_F(SinkBufferCancelTest, cancel_aborts_inflight_rpc) {
    brpc::Server server;
    HangingInternalService service;
    brpc::ServerOptions options;
    options.num_threads = 2;
    ASSERT_EQ(server.AddService(&service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
    ASSERT_EQ(server.Start(0, &options), 0);
    const int port = server.listen_address().port;
    DeferOp stop_server([&] {
        // Release the blocked handler and shut down the server.
        service.release.count_down();
        server.Stop(0);
        server.Join();
    });

    auto dest_id = make_dest_id(/*lo*/ 987654321);
    auto buffer = make_remote_sink_buffer(port, dest_id);
    buffer->incr_sinker(_runtime_state.get());

    auto stub = std::make_shared<PInternalService_RecoverableStub>(server.listen_address(), "");
    ASSERT_OK(stub->reset_channel());

    auto request = make_request(dest_id, port, stub);
    ASSERT_OK(buffer->add_request(request));

    // Wait until the server has actually received the RPC, guaranteeing it is in-flight.
    ASSERT_TRUE(service.received.wait_for(std::chrono::seconds(10)));
    EXPECT_FALSE(buffer->is_finished());

    const auto cancel_start = std::chrono::steady_clock::now();
    buffer->cancel_one_sinker(_runtime_state.get());

    // The in-flight RPC should be aborted promptly. The query timeout is 300s, so if cancellation
    // did not work this loop would time out here and fail rather than hanging for the full timeout.
    const auto deadline = cancel_start + std::chrono::seconds(30);
    while (!buffer->is_finished() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_TRUE(buffer->is_finished());

    const auto elapsed =
            std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - cancel_start);
    EXPECT_LT(elapsed.count(), 30) << "cancellation did not abort the in-flight RPC promptly";
}

// cancel_one_sinker() must be safe when there are no in-flight RPCs registered (the swap-and-reset
// runs against a freshly initialized, empty bthread_id_list).
TEST_F(SinkBufferCancelTest, cancel_with_no_inflight_rpc_is_safe) {
    auto dest_id = make_dest_id(/*lo*/ 123456789);
    // Any port works; no RPC is ever sent in this test.
    auto buffer = make_remote_sink_buffer(/*port*/ 1, dest_id);
    buffer->incr_sinker(_runtime_state.get());

    buffer->cancel_one_sinker(_runtime_state.get());
    EXPECT_TRUE(buffer->is_finished());
}

// A brpc PInternalService that counts the received transmit_chunk calls and holds every handler until
// release(), so the RPCs stay in flight while the test looks at the trackers.
class HoldingInternalService : public starrocks::PInternalService {
public:
    void transmit_chunk(google::protobuf::RpcController* /*controller*/,
                        const starrocks::PTransmitChunkParams* /*request*/, starrocks::PTransmitChunkResult* response,
                        google::protobuf::Closure* done) override {
        received.fetch_add(1);
        {
            std::unique_lock lock(_mutex);
            _cv.wait(lock, [this] { return _released; });
        }
        if (response != nullptr) {
            response->mutable_status()->set_status_code(0);
        }
        done->Run();
    }

    void release() {
        std::lock_guard lock(_mutex);
        _released = true;
        _cv.notify_all();
    }

    std::atomic<int> received{0};

private:
    std::mutex _mutex;
    std::condition_variable _cv;
    bool _released = false;
};

class SinkBufferAttachmentMemTest : public SinkBufferCancelTest {
public:
    void SetUp() override {
        SinkBufferCancelTest::SetUp();
        // The checks look at the instance tracker, and release_without_root() changes only the trackers below
        // the root, so the instance tracker gets the query tracker as its parent.
        _factory.reset();
        _sink_buffer.reset();
        _runtime_state->init_mem_trackers(_query_context->mem_tracker());
        _saved_brpc_dop = config::pipeline_sink_brpc_dop;
    }

    void TearDown() override { config::pipeline_sink_brpc_dop = _saved_brpc_dop; }

protected:
    // The memory hook can count a few small objects around the checked calls, so the checks allow this much
    // difference. The attachments are much larger, so a double count or a double release still fails.
    static constexpr int64_t kNoise = 256 * 1024;
    static constexpr size_t kPayload = 4 * 1024 * 1024;

    MemTracker* fragment_tracker() { return _runtime_state->instance_mem_tracker(); }

    std::shared_ptr<SinkBuffer> make_sink_buffer(int port, const std::vector<TUniqueId>& dest_ids) {
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(port);
        std::vector<TPlanFragmentDestination> destinations;
        for (const auto& id : dest_ids) {
            TPlanFragmentDestination dest;
            dest.__set_fragment_instance_id(id);
            dest.__set_brpc_server(addr);
            destinations.push_back(dest);
        }
        return std::make_shared<SinkBuffer>(_fragment_context.get(), destinations, /*is_dest_merge*/ false);
    }

    TransmitAttachmentPtr make_attachment() {
        const std::string payload(kPayload, 'x');
        auto attachment = std::make_shared<TransmitAttachment>(fragment_tracker());
        attachment->append(payload);
        EXPECT_EQ(kPayload, attachment->size());
        EXPECT_GE(attachment->charged_bytes(), 0);
        return attachment;
    }

    // The exchange sink operator builds and queues requests on a driver thread under the instance tracker, and
    // the sink buffer pops them under the same tracker. The test does the same, so the request objects
    // themselves do not move bytes between trackers.
    Status add_request(SinkBuffer* buffer, const TUniqueId& dest_id, int port,
                       const std::shared_ptr<PInternalService_RecoverableStub>& stub,
                       const TransmitAttachmentPtr& attachment) {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(fragment_tracker());
        auto request = make_request(dest_id, port, stub);
        request.attachment = attachment;
        request.request_byte_size = request.attachment_size();
        return buffer->add_request(request);
    }

    static bool wait_until(const std::function<bool()>& done) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (!done()) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return true;
    }

    static void start_server(brpc::Server* server, HoldingInternalService* service) {
        brpc::ServerOptions options;
        // Every held handler blocks a worker, so the server needs more workers than held RPCs.
        options.num_threads = 16;
        ASSERT_EQ(server->AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(server->Start(0, &options), 0);
    }

    int64_t _saved_brpc_dop = 0;
};

// The owner tracker counts a shared attachment once while any holder keeps it, and gets the bytes back once when
// the last holder lets it go, also when this happens under another tracker.
TEST_F(SinkBufferAttachmentMemTest, shared_attachment_is_charged_once) {
    MemTracker* fragment = fragment_tracker();
    const int64_t base = fragment->consumption();

    auto attachment = make_attachment();
    const int64_t charged = attachment->charged_bytes();
    EXPECT_NEAR(base + charged, fragment->consumption(), kNoise);

    std::vector<TransmitAttachmentPtr> holders(3, attachment);
    attachment.reset();
    holders.pop_back();
    holders.pop_back();
    EXPECT_NEAR(base + charged, fragment->consumption(), kNoise);

    MemTracker other(-1, "other");
    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(&other);
        holders.clear();
    }
    EXPECT_NEAR(base, fragment->consumption(), kNoise);
    EXPECT_NEAR(0, other.consumption(), kNoise);
}

// A broadcast sends one attachment to several remote destinations. The fragment tracker counts it once while a
// request still waits in the buffer, and returns to its base value after the last request is sent. It never
// goes below the base value by the attachment size.
TEST_F(SinkBufferAttachmentMemTest, broadcast_attachment_is_released_once) {
    brpc::Server server;
    HoldingInternalService service;
    ASSERT_NO_FATAL_FAILURE(start_server(&server, &service));
    const int port = server.listen_address().port;
    DeferOp stop_server([&] {
        service.release();
        server.Stop(0);
        server.Join();
    });

    // One RPC in flight per destination, so a request to a busy destination waits in the buffer.
    config::pipeline_sink_brpc_dop = 1;

    const std::vector<TUniqueId> dest_ids{make_dest_id(1001), make_dest_id(1002), make_dest_id(1003)};
    auto buffer = make_sink_buffer(port, dest_ids);
    buffer->incr_sinker(_runtime_state.get());
    auto stub = std::make_shared<PInternalService_RecoverableStub>(server.listen_address(), "");
    ASSERT_OK(stub->reset_channel());

    MemTracker* fragment = fragment_tracker();
    MemTracker* query = fragment->parent();
    ASSERT_NE(nullptr, query);

    // Keep the third destination busy, so the broadcast request to it waits in the buffer.
    ASSERT_OK(add_request(buffer.get(), dest_ids[2], port, stub, nullptr));
    ASSERT_TRUE(wait_until([&] { return service.received.load() == 1; }));

    const int64_t fragment_base = fragment->consumption();
    const int64_t query_base = query->consumption();

    auto attachment = make_attachment();
    const int64_t charged = attachment->charged_bytes();
    for (const auto& dest_id : dest_ids) {
        ASSERT_OK(add_request(buffer.get(), dest_id, port, stub, attachment));
    }
    attachment.reset();

    // Two broadcast requests are in flight and one waits in the buffer.
    ASSERT_TRUE(wait_until([&] { return service.received.load() == 3; }));
    EXPECT_NEAR(fragment_base + charged, fragment->consumption(), kNoise);
    EXPECT_NEAR(query_base + charged, query->consumption(), kNoise);

    // The response to the busy destination lets the waiting request go out, and the buffer drops the attachment.
    service.release();
    ASSERT_TRUE(wait_until([&] { return service.received.load() == 4; }));
    ASSERT_TRUE(wait_until([&] { return std::abs(fragment->consumption() - fragment_base) <= kNoise; }));
    EXPECT_NEAR(query_base, query->consumption(), kNoise);

    buffer->cancel_one_sinker(_runtime_state.get());
    ASSERT_TRUE(wait_until([&] { return buffer->is_finished(); }));
    EXPECT_NEAR(fragment_base, fragment->consumption(), kNoise);
}

// A shuffle request goes to one destination. The fragment tracker counts the attachment until the request is
// sent, and is back at its base value while the RPC is in flight and after the response.
TEST_F(SinkBufferAttachmentMemTest, shuffle_attachment_is_released_once) {
    brpc::Server server;
    HoldingInternalService service;
    ASSERT_NO_FATAL_FAILURE(start_server(&server, &service));
    const int port = server.listen_address().port;
    DeferOp stop_server([&] {
        service.release();
        server.Stop(0);
        server.Join();
    });

    const auto dest_id = make_dest_id(2001);
    auto buffer = make_sink_buffer(port, {dest_id});
    buffer->incr_sinker(_runtime_state.get());
    auto stub = std::make_shared<PInternalService_RecoverableStub>(server.listen_address(), "");
    ASSERT_OK(stub->reset_channel());

    MemTracker* fragment = fragment_tracker();
    const int64_t base = fragment->consumption();

    auto attachment = make_attachment();
    EXPECT_NEAR(base + attachment->charged_bytes(), fragment->consumption(), kNoise);
    ASSERT_OK(add_request(buffer.get(), dest_id, port, stub, attachment));
    attachment.reset();

    ASSERT_TRUE(wait_until([&] { return service.received.load() == 1; }));
    EXPECT_NEAR(base, fragment->consumption(), kNoise);

    service.release();
    buffer->cancel_one_sinker(_runtime_state.get());
    ASSERT_TRUE(wait_until([&] { return buffer->is_finished(); }));
    EXPECT_NEAR(base, fragment->consumption(), kNoise);
}

// The buffer is cancelled while a request waits, so the request is never sent. The fragment tracker gets the
// attachment back when the buffer drops the request.
TEST_F(SinkBufferAttachmentMemTest, dropped_attachment_is_released_once) {
    brpc::Server server;
    HoldingInternalService service;
    ASSERT_NO_FATAL_FAILURE(start_server(&server, &service));
    const int port = server.listen_address().port;
    DeferOp stop_server([&] {
        service.release();
        server.Stop(0);
        server.Join();
    });

    config::pipeline_sink_brpc_dop = 1;

    const auto dest_id = make_dest_id(3001);
    auto buffer = make_sink_buffer(port, {dest_id});
    buffer->incr_sinker(_runtime_state.get());
    auto stub = std::make_shared<PInternalService_RecoverableStub>(server.listen_address(), "");
    ASSERT_OK(stub->reset_channel());

    ASSERT_OK(add_request(buffer.get(), dest_id, port, stub, nullptr));
    ASSERT_TRUE(wait_until([&] { return service.received.load() == 1; }));

    MemTracker* fragment = fragment_tracker();
    const int64_t base = fragment->consumption();

    auto attachment = make_attachment();
    const int64_t charged = attachment->charged_bytes();
    ASSERT_OK(add_request(buffer.get(), dest_id, port, stub, attachment));
    attachment.reset();
    EXPECT_NEAR(base + charged, fragment->consumption(), kNoise);

    // Cancelling aborts the RPC in flight, and the waiting request is never sent.
    buffer->cancel_one_sinker(_runtime_state.get());
    ASSERT_TRUE(wait_until([&] { return buffer->is_finished(); }));
    {
        SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(fragment);
        buffer.reset();
    }
    EXPECT_EQ(1, service.received.load());
    EXPECT_NEAR(base, fragment->consumption(), kNoise);
}

} // namespace starrocks::pipeline
