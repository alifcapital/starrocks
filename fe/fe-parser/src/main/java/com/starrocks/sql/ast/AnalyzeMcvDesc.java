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

package com.starrocks.sql.ast;

import com.starrocks.sql.parser.NodePosition;

import java.util.List;

/**
 * ANALYZE TABLE ... MCV (columns): the most common value tuples of a column set of an external table
 * with their exact row counts, and the number of distinct tuples.
 */
public class AnalyzeMcvDesc extends AnalyzeTypeDesc {
    public AnalyzeMcvDesc() {
        super(NodePosition.ZERO);
    }

    @Override
    public List<StatisticsType> getStatsTypes() {
        return List.of(StatisticsType.MCV);
    }
}
