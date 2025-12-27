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

package com.starrocks.connector.share.iceberg;

import org.apache.iceberg.expressions.Expression;

import java.io.Serializable;

public class IcebergPredicateInfo implements Serializable {
    private final Expression predicate;
    private final boolean partitionPredicate;

    public IcebergPredicateInfo(Expression predicate, boolean partitionPredicate) {
        this.predicate = predicate;
        this.partitionPredicate = partitionPredicate;
    }

    public Expression predicate() {
        return predicate;
    }

    public boolean partitionPredicate() {
        return partitionPredicate;
    }
}
