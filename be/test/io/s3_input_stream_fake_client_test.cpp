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

#include "io/fake_s3_client.h"
#include "io/s3_input_stream.h"
#include "testutil/assert.h"

namespace starrocks::io {

// These tests read through an S3 client that serves objects from memory, so they need no S3 endpoint.
class S3InputStreamFakeClientTest : public testing::Test {
public:
    static void SetUpTestSuite() { Aws::InitAPI(Aws::SDKOptions()); }
    static void TearDownTestSuite() { Aws::ShutdownAPI(Aws::SDKOptions()); }

    void SetUp() override {
        _client = std::make_shared<FakeS3Client>();
        _client->put("object", "0123456789");
    }
    void TearDown() override { _client.reset(); }

protected:
    std::shared_ptr<FakeS3Client> _client;
};

// A read that is not larger than the read-ahead size fills the read-ahead buffer. We expect the GET of the buffer to
// ask for exactly the bytes that the buffer holds: the Range header names its last byte, not the byte after it.
TEST_F(S3InputStreamFakeClientTest, read_ahead_asks_for_the_bytes_of_the_buffer) {
    S3InputStream stream(_client, "bucket", "object", 4);
    stream.set_size(10);

    char buf[4] = {};
    ASSIGN_OR_ABORT(auto n, stream.read(buf, 2));
    ASSERT_EQ(2, n);
    EXPECT_EQ("01", std::string(buf, 2));
    // The rest of the buffer serves the next read, and a read past the buffer loads the next bytes.
    ASSIGN_OR_ABORT(n, stream.read(buf, 4));
    ASSERT_EQ(4, n);
    EXPECT_EQ("2345", std::string(buf, 4));
    // The end of the object limits the last buffer.
    ASSERT_OK(stream.seek(8));
    ASSIGN_OR_ABORT(n, stream.read(buf, 4));
    ASSERT_EQ(2, n);
    EXPECT_EQ("89", std::string(buf, 2));

    auto gets = _client->gets();
    ASSERT_EQ(3, gets.size());
    EXPECT_EQ(0, gets[0].first);
    EXPECT_EQ(3, gets[0].last);
    EXPECT_EQ(4, gets[1].first);
    EXPECT_EQ(7, gets[1].last);
    EXPECT_EQ(8, gets[2].first);
    EXPECT_EQ(9, gets[2].last);
}

// A read larger than the read-ahead size goes straight to the caller.
TEST_F(S3InputStreamFakeClientTest, large_read_asks_for_its_bytes) {
    S3InputStream stream(_client, "bucket", "object", 4);
    stream.set_size(10);

    char buf[8] = {};
    ASSERT_OK(stream.seek(1));
    ASSIGN_OR_ABORT(auto n, stream.read(buf, 6));
    ASSERT_EQ(6, n);
    EXPECT_EQ("123456", std::string(buf, 6));

    auto gets = _client->gets();
    ASSERT_EQ(1, gets.size());
    EXPECT_EQ(1, gets[0].first);
    EXPECT_EQ(6, gets[0].last);
}

} // namespace starrocks::io
