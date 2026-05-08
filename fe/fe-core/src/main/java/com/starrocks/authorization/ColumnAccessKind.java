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

package com.starrocks.authorization;

/**
 * Describes how a column is used in the query plan, classified by the
 * visibility of the column's value to the requesting user. Surfaced to
 * Ranger via RangerAccessRequest.requestData so audit consumers can
 * distinguish "user read PII" from "user touched PII without seeing it".
 *
 * Classification is based on the optimized logical plan (after predicate
 * push-down and join rewrite), not the original SQL syntax. A WHERE
 * condition between two tables that the optimizer rewrites into an
 * inner-join shows up as JOIN_KEY, not FILTER.
 */
public enum ColumnAccessKind {
    // Column value flows out of the query as a scalar visible in result rows
    // (SELECT-list, or any expression whose ColumnRef reaches the plan root).
    PROJECTION,
    // Column participates in aggregation but the value is not visible to the user:
    // arguments of aggregate functions (count/sum/avg/...) and GROUP BY keys that
    // are not also projected to the root.
    AGG_ARG,
    // Column appears in a join on-condition or join residual predicate.
    JOIN_KEY,
    // Column appears in WHERE / HAVING / scan-pushed predicate / ORDER BY /
    // window partition or order spec.
    FILTER
}
