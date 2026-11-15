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

package com.starrocks.authentication;

import com.google.common.base.Strings;
import com.starrocks.authorization.PrivilegeException;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.thrift.TUserRoles;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UserIdentityUtils {
    private static final Logger LOG = LogManager.getLogger(UserIdentityUtils.class);

    public static UserIdentity fromString(String userIdentStr) {
        if (Strings.isNullOrEmpty(userIdentStr)) {
            return null;
        }

        String[] parts = userIdentStr.split("@");
        if (parts.length != 2) {
            return null;
        }

        String user = parts[0];
        if (!user.startsWith("'") || !user.endsWith("'")) {
            return null;
        }

        String host = parts[1];
        if (host.startsWith("['") && host.endsWith("']")) {
            return new UserIdentity(user.substring(1, user.length() - 1),
                    host.substring(2, host.length() - 2), true);
        } else if (host.startsWith("'") && host.endsWith("'")) {
            return new UserIdentity(user.substring(1, user.length() - 1),
                    host.substring(1, host.length() - 1));
        }

        return null;
    }

    public static TUserIdentity toThrift(UserIdentity userIdentity) {
        TUserIdentity tUserIdent = new TUserIdentity();
        tUserIdent.setHost(userIdentity.getHost());
        tUserIdent.setUsername(userIdentity.getUser());
        tUserIdent.setIs_domain(userIdentity.isDomain());
        tUserIdent.setIs_ephemeral(userIdentity.isEphemeral());
        return tUserIdent;
    }

    /**
     * We want a privilege check on the other side of an RPC to see the session as it is here: the same user,
     * the same active roles and the same groups. Ranger policies granted to a group and roles granted to a group
     * only apply when the groups are there, so this carries all three. Use it wherever the identity stands for
     * the authority of the session; use {@link #toThrift(UserIdentity)} where the identity only names a user.
     */
    public static TUserIdentity toThrift(ConnectContext context) {
        TUserIdentity tUserIdent = toThrift(context.getCurrentUserIdentity());
        if (context.getCurrentRoleIds() != null) {
            TUserRoles userRoles = new TUserRoles();
            userRoles.setRole_id_list(new ArrayList<>(context.getCurrentRoleIds()));
            tUserIdent.setCurrent_role_ids(userRoles);
        }
        if (context.getGroups() != null) {
            tUserIdent.setGroups(new ArrayList<>(context.getGroups()));
        }
        return tUserIdent;
    }

    public static UserIdentity fromThrift(TUserIdentity tUserIdent) {
        return new UserIdentity(tUserIdent.getUsername(), tUserIdent.getHost(), tUserIdent.is_domain,
                tUserIdent.is_ephemeral);
    }

    public static void setAuthInfoFromThrift(ConnectContext context, TAuthInfo authInfo) {
        if (authInfo.isSetCurrent_user_ident()) {
            setAuthInfoFromThrift(context, authInfo.getCurrent_user_ident());
        } else {
            UserIdentity userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(authInfo.user, authInfo.user_ip);
            context.setCurrentUserIdentity(userIdentity);
            context.setCurrentRoleIds(userIdentity);
        }
    }

    public static void setAuthInfoFromThrift(ConnectContext context, TUserIdentity tUserIdent) {
        UserIdentity userIdentity = UserIdentityUtils.fromThrift(tUserIdent);
        context.setCurrentUserIdentity(userIdentity);
        // The groups are the groups of the sending session. We cannot check them the way the roles are checked
        // below: the group providers resolve them at login, and asking LDAP again here would make every call
        // wait on it and could give another answer than the session got. The sender is a FE or BE of this
        // cluster, and we already take the user name and the roles of an ephemeral user from it as they are,
        // so we take the groups the same way. When the field is not set the context keeps its own groups.
        if (tUserIdent.isSetGroups()) {
            context.setGroups(new HashSet<>(tUserIdent.getGroups()));
        }
        if (tUserIdent.isSetCurrent_role_ids()) {
            List<Long> roleIdList = tUserIdent.current_role_ids.getRole_id_list();
            Set<Long> requestedRoleIds = roleIdList != null ? new HashSet<>(roleIdList) : new HashSet<>();
            if (userIdentity.isEphemeral()) {
                // An ephemeral user, one authenticated through a security integration, owns no roles to check
                // against: its privileges are the roles its session activated, as in the privilege collection.
                // The roles must not be looked up either, as that reads the session of the current thread, and
                // the RPC threads calling this have none.
                context.setCurrentRoleIds(requestedRoleIds);
                return;
            }
            try {
                Set<Long> actualRoleIds =
                        GlobalStateMgr.getCurrentState().getAuthorizationMgr().getRoleIdsByUser(userIdentity);
                requestedRoleIds.retainAll(actualRoleIds);
            } catch (PrivilegeException e) {
                LOG.warn("Failed to validate role IDs for user {}: {}", userIdentity, e.getMessage());
            }
            context.setCurrentRoleIds(requestedRoleIds);
        } else {
            context.setCurrentRoleIds(userIdentity);
        }
    }

}
