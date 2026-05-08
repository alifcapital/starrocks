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

package com.starrocks.authorization.ranger;

import com.starrocks.authorization.ColumnAccessKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;

/**
 * Pins the exact wire format of the JSON envelope put into {@code RangerAccessRequest.requestData}.
 * Audit consumers (Splunk/Clickhouse/etc) parse this string — changing it silently breaks downstream
 * dashboards. Update this test consciously when changing the contract, not by accident.
 */
public class RangerStarRocksAccessRequestTest {

    private static final String QUERY_ID = "abc-123-def";

    @Test
    public void emptyUsageEmitsEmptyArray() {
        String payload = RangerStarRocksAccessRequest.buildRequestData(
                QUERY_ID, EnumSet.noneOf(ColumnAccessKind.class));
        Assertions.assertEquals("{\"queryId\":\"abc-123-def\",\"usage\":[]}", payload);
    }

    @Test
    public void singleRoleIsEmittedAsArrayOfOne() {
        String payload = RangerStarRocksAccessRequest.buildRequestData(
                QUERY_ID, EnumSet.of(ColumnAccessKind.PROJECTION));
        Assertions.assertEquals("{\"queryId\":\"abc-123-def\",\"usage\":[\"PROJECTION\"]}", payload);
    }

    @Test
    public void rolesAreEmittedInEnumDeclarationOrder() {
        // EnumSet iterates in enum-declaration order regardless of insertion order — verify both
        // directions to make this explicit.
        EnumSet<ColumnAccessKind> a = EnumSet.of(ColumnAccessKind.FILTER, ColumnAccessKind.PROJECTION);
        EnumSet<ColumnAccessKind> b = EnumSet.of(ColumnAccessKind.PROJECTION, ColumnAccessKind.FILTER);
        String expected = "{\"queryId\":\"abc-123-def\",\"usage\":[\"PROJECTION\",\"FILTER\"]}";
        Assertions.assertEquals(expected, RangerStarRocksAccessRequest.buildRequestData(QUERY_ID, a));
        Assertions.assertEquals(expected, RangerStarRocksAccessRequest.buildRequestData(QUERY_ID, b));
    }

    @Test
    public void allRolesEmittedInDeclarationOrder() {
        String payload = RangerStarRocksAccessRequest.buildRequestData(
                QUERY_ID, EnumSet.allOf(ColumnAccessKind.class));
        Assertions.assertEquals(
                "{\"queryId\":\"abc-123-def\",\"usage\":[\"PROJECTION\",\"AGG_ARG\",\"JOIN_KEY\",\"FILTER\"]}",
                payload);
    }

    @Test
    public void nullUsageIsTreatedAsEmpty() {
        String payload = RangerStarRocksAccessRequest.buildRequestData(QUERY_ID, null);
        Assertions.assertEquals("{\"queryId\":\"abc-123-def\",\"usage\":[]}", payload);
    }
}
