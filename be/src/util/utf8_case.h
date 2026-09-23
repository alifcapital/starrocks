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

#include <cstddef>
#include <string>

namespace starrocks {

using UTF8CaseConverter = size_t (*)(const char*, size_t, char*);

// Full default Unicode 17 mappings. Invalid bytes are preserved.
// Source and destination must not overlap. Destination capacity must be at least 3 * length.
UTF8CaseConverter utf8_lower_converter();
UTF8CaseConverter utf8_upper_converter();

// Source must not refer to dst storage.
void utf8_tolower(const char* src, size_t length, std::string& dst);

inline void utf8_tolower(const std::string& src, std::string& dst) {
    utf8_tolower(src.data(), src.size(), dst);
}

} // namespace starrocks
