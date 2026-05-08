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
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class RangerStarRocksAccessRequest extends RangerAccessRequestImpl {
    private static final Logger LOG = LogManager.getLogger(RangerStarRocksAccessRequest.class);

    private RangerStarRocksAccessRequest() {
    }

    public static RangerStarRocksAccessRequest createAccessRequest(RangerAccessResourceImpl resource,
                                                                   UserIdentity user,
                                                                   Set<String> groups,
                                                                   String accessType) {
        return createAccessRequest(resource, user, groups, accessType, EnumSet.noneOf(ColumnAccessKind.class));
    }

    public static RangerStarRocksAccessRequest createAccessRequest(RangerAccessResourceImpl resource,
                                                                   UserIdentity user,
                                                                   Set<String> groups,
                                                                   String accessType,
                                                                   EnumSet<ColumnAccessKind> usage) {
        RangerStarRocksAccessRequest request = new RangerStarRocksAccessRequest();
        request.setUser(user.getUser());
        request.setUserGroups(groups);
        request.setAccessType(accessType);
        request.setResource(resource);
        request.setClientIPAddress(user.getHost());
        request.setClientType("starrocks");
        request.setClusterName("starrocks");
        request.setAccessTime(new Date());

        // Populate Ranger's requestData with a JSON envelope so audit logs can be correlated
        // with fe.audit.log (queryId) and downstream consumers can tell whether the user actually
        // saw the column value (PROJECTION) or merely touched it (AGG_ARG/JOIN_KEY/FILTER).
        // ConnectContext is thread-local and may be absent for background tasks (e.g. checkpoint
        // thread) — leave requestData unset in that case.
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getQueryId() != null) {
            request.setRequestData(buildRequestData(ctx.getQueryId().toString(), usage));
        }

        LOG.debug("RangerStarRocksAccessRequest | " + request);

        return request;
    }

    /**
     * Build the JSON payload placed in {@link RangerAccessRequestImpl#setRequestData(String)}.
     * Roles are emitted in enum-declaration order (PROJECTION, AGG_ARG, JOIN_KEY, FILTER) so the
     * resulting string is deterministic and safe to dedupe/index downstream.
     */
    static String buildRequestData(String queryId, EnumSet<ColumnAccessKind> usage) {
        StringBuilder sb = new StringBuilder(64);
        sb.append("{\"queryId\":\"").append(queryId).append("\",\"usage\":[");
        if (usage != null && !usage.isEmpty()) {
            // EnumSet iteration is in enum-declaration order, which gives us a stable layout
            // without an explicit sort.
            List<String> names = new ArrayList<>(usage.size());
            for (ColumnAccessKind u : usage) {
                names.add("\"" + u.name() + "\"");
            }
            sb.append(String.join(",", names));
        }
        sb.append("]}");
        return sb.toString();
    }
}
