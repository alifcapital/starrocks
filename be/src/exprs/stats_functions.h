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

#include "exprs/function_helper.h"

namespace starrocks {

// Helpers for statistics collection queries.
class StatsFunctions {
public:
    /**
     * stats_tuple_key(v1, v2, ...) -> VARCHAR
     *
     * Encodes a tuple of values into one string so that sketches and GROUP BY can treat a group
     * of columns as a single value, and the FE can read the values back. Components are joined
     * with '#'; inside a component '\' becomes "\\" and '#' becomes "\#"; a NULL component is the
     * two characters "\N", which escaping makes impossible for a non-NULL value. The result is
     * never NULL.
     *
     * @param columns: [BinaryColumn, ...]
     * @return BinaryColumn
     */
    DEFINE_VECTORIZED_FN(tuple_key);
};

} // namespace starrocks
