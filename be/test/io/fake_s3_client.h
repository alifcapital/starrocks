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

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/client/CoreErrors.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>

#include <algorithm>
#include <cstdio>
#include <map>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace starrocks::io {

// An S3 client that serves GetObject from objects held in memory and records the byte range of every GET. It lets a
// test check the requests that a stream sends without a network. The caller has to init the AWS SDK before it creates
// the client.
//
// A request that sets a continue handler gets the body in pieces of `piece_size` bytes, and the handler is asked
// before each piece, as the HTTP client of the SDK asks it before each write of the body. A handler that answers false
// ends the request with USER_CANCELLED.
class FakeS3Client final : public Aws::S3::S3Client {
public:
    struct Get {
        std::string key;
        int64_t first = 0; // the first byte of the range
        int64_t last = 0;  // the last byte of the range, inclusive as in the Range header
    };

    FakeS3Client()
            : Aws::S3::S3Client(Aws::Auth::AWSCredentials("ak", "sk"), Aws::Client::ClientConfiguration(),
                                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, false) {}

    void put(const std::string& key, std::string data) {
        std::lock_guard l(_mutex);
        _objects[key] = std::move(data);
    }

    void set_piece_size(int64_t piece_size) { _piece_size = piece_size; }

    std::vector<Get> gets() const {
        std::lock_guard l(_mutex);
        return _gets;
    }

    void clear_gets() {
        std::lock_guard l(_mutex);
        _gets.clear();
    }

    Aws::S3::Model::GetObjectOutcome GetObject(const Aws::S3::Model::GetObjectRequest& request) const override {
        std::string data;
        Get get;
        get.key = request.GetKey();
        {
            std::lock_guard l(_mutex);
            auto it = _objects.find(request.GetKey());
            if (it == _objects.end()) {
                return Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::NO_SUCH_KEY,
                                                                                 "NoSuchKey", "no such key", false));
            }
            data = it->second;
        }
        const int64_t size = static_cast<int64_t>(data.size());
        get.first = 0;
        get.last = size - 1;
        if (request.RangeHasBeenSet()) {
            long long first = 0;
            long long last = 0;
            if (sscanf(request.GetRange().c_str(), "bytes=%lld-%lld", &first, &last) != 2) {
                return Aws::S3::S3Error(Aws::Client::AWSError<Aws::S3::S3Errors>(
                        Aws::S3::S3Errors::INVALID_PARAMETER_VALUE, "InvalidRange", request.GetRange(), false));
            }
            get.first = first;
            get.last = last;
        }
        {
            std::lock_guard l(_mutex);
            _gets.push_back(get);
        }
        const int64_t first = std::min(get.first, size);
        const int64_t last = std::min(get.last, size - 1);
        const int64_t length = std::max<int64_t>(0, last - first + 1);

        Aws::IOStream* body = request.GetResponseStreamFactory() ? request.GetResponseStreamFactory()()
                                                                 : Aws::New<Aws::StringStream>("FakeS3Client");
        const auto& handler = request.GetContinueRequestHandler();
        for (int64_t written = 0; written < length;) {
            if (handler && !handler(nullptr)) {
                Aws::Delete(body);
                return Aws::S3::S3Error(Aws::Client::AWSError<Aws::Client::CoreErrors>(
                        Aws::Client::CoreErrors::USER_CANCELLED, "UserCancelled", "request cancelled", false));
            }
            const int64_t piece = handler ? std::min(_piece_size, length - written) : length - written;
            body->write(data.data() + first + written, piece);
            written += piece;
        }
        Aws::S3::Model::GetObjectResult result;
        result.ReplaceBody(body);
        result.SetContentLength(length);
        return Aws::S3::Model::GetObjectOutcome(std::move(result));
    }

private:
    mutable std::mutex _mutex;
    std::map<std::string, std::string> _objects;
    mutable std::vector<Get> _gets;
    int64_t _piece_size = 4096;
};

} // namespace starrocks::io
