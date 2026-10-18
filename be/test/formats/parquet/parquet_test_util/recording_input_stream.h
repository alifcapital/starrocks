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
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/ownership.h"
#include "common/status.h"
#include "common/statusor.h"
#include "io/seekable_input_stream.h"
#include "io/string_input_stream.h"

namespace starrocks::parquet {

// An in-memory stream that records the byte ranges that the reader takes from it. A peek counts as a read of the bytes
// that it returns, because the caller then holds them. seek() and skip() move the position and read nothing.
class RecordingInputStream final : public io::SeekableInputStreamWrapper {
public:
    explicit RecordingInputStream(std::string contents)
            : io::SeekableInputStreamWrapper(&_inner, kDontTakeOwnership), _inner(std::move(contents)) {}

    StatusOr<std::string_view> peek(int64_t nbytes) override {
        ASSIGN_OR_RETURN(auto view, io::SeekableInputStreamWrapper::peek(nbytes));
        ASSIGN_OR_RETURN(auto pos, position());
        _ranges.emplace_back(pos, pos + static_cast<int64_t>(view.size()));
        return view;
    }

    Status read_at_fully(int64_t offset, void* out, int64_t count) override {
        _ranges.emplace_back(offset, offset + count);
        return io::SeekableInputStreamWrapper::read_at_fully(offset, out, count);
    }

    StatusOr<int64_t> read(void* data, int64_t count) override {
        ASSIGN_OR_RETURN(auto pos, position());
        auto n = io::SeekableInputStreamWrapper::read(data, count);
        if (n.ok()) {
            _ranges.emplace_back(pos, pos + n.value());
        }
        return n;
    }

    Status read_fully(void* data, int64_t count) override {
        ASSIGN_OR_RETURN(auto pos, position());
        _ranges.emplace_back(pos, pos + count);
        return io::SeekableInputStreamWrapper::read_fully(data, count);
    }

    // The ranges [begin, end) in the order of the reads.
    const std::vector<std::pair<int64_t, int64_t>>& ranges() const { return _ranges; }

    // True if a read took one of the bytes of [begin, end).
    bool touched(int64_t begin, int64_t end) const {
        return std::any_of(_ranges.begin(), _ranges.end(), [&](const std::pair<int64_t, int64_t>& range) {
            return range.first < end && begin < range.second && range.first < range.second;
        });
    }

    void clear() { _ranges.clear(); }

private:
    io::StringInputStream _inner;
    std::vector<std::pair<int64_t, int64_t>> _ranges;
};

} // namespace starrocks::parquet
