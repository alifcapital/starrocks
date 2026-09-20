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

#include "exprs/stats_functions.h"

#include <string>
#include <vector>

#include "column/column_builder.h"
#include "column/column_helper.h"
#include "column/column_viewer.h"

namespace starrocks {

StatusOr<ColumnPtr> StatsFunctions::tuple_key(FunctionContext* context, const Columns& columns) {
    std::vector<ColumnViewer<TYPE_VARCHAR>> viewers;
    viewers.reserve(columns.size());
    for (const auto& column : columns) {
        viewers.emplace_back(column);
    }

    const size_t size = columns[0]->size();
    ColumnBuilder<TYPE_VARCHAR> builder(size);
    std::string key;
    for (size_t row = 0; row < size; ++row) {
        key.clear();
        for (size_t i = 0; i < viewers.size(); ++i) {
            if (i > 0) {
                key.push_back('#');
            }
            if (viewers[i].is_null(row)) {
                key += "\\N";
                continue;
            }
            Slice value = viewers[i].value(row);
            for (size_t j = 0; j < value.size; ++j) {
                char c = value.data[j];
                if (c == '\\' || c == '#') {
                    key.push_back('\\');
                }
                key.push_back(c);
            }
        }
        builder.append(Slice(key));
    }
    return builder.build(ColumnHelper::is_all_const(columns));
}

} // namespace starrocks

#include "gen_cpp/opcode/StatsFunctions.inc"
