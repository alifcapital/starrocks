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

import com.starrocks.authorization.AccessDeniedException;
import com.starrocks.authorization.ColumnAccessKind;
import com.starrocks.authorization.ExternalAccessController;
import com.starrocks.authorization.PrivilegeType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.TableName;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.parser.SqlParser;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ranger.audit.provider.AuditProviderFactory;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.apache.ranger.plugin.model.RangerServiceDef;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerPolicyEngine;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import static com.starrocks.server.GlobalStateMgr.isCheckpointThread;
import static java.util.Locale.ENGLISH;

public abstract class RangerAccessController extends ExternalAccessController implements AccessTypeConverter {
    private static final Logger LOG = LoggerFactory.getLogger(RangerAccessController.class);
    private static volatile boolean auditInitFailed = false;
    protected final RangerBasePlugin rangerPlugin;

    // Row filters and masks come from Ranger, per user and resource: any table may have one.
    @Override
    public boolean mayHavePolicy(ConnectContext context, TableName tableName) {
        return true;
    }

    public RangerAccessController(String serviceType, String serviceName) {
        RangerPluginConfig rangerPluginContext = buildRangerPluginContext(serviceType, serviceName);
        rangerPlugin = new Plugin(rangerPluginContext);
        if (!isCheckpointThread()) {
            // Audit first, so a broken audit config disables only audit; init() then skips it.
            initAuditIfNeeded(rangerPlugin);
            rangerPlugin.init(); // this will initialize policy engine and policy refresher
        }
        rangerPlugin.setResultProcessor(new RangerDefaultAuditHandler());

        LOG.info("Start Ranger plugin ({} - {}) success",
                rangerPluginContext.getServiceType(), rangerPluginContext.getServiceName());
    }

    /**
     * Initializes Ranger's process-wide audit factory so that a broken audit configuration
     * disables only audit.
     * <p>
     * {@link RangerBasePlugin#init()} initializes the audit factory before building the policy
     * engine, and the factory marks itself initialized before it creates any destination. A
     * destination class that is not on the classpath (Solr on a build without
     * ranger-audit-dest-solr, or log4j, hdfs and kafka, which are not shipped since Ranger 2.8)
     * therefore failed the first catalog access after every FE start, and later accesses
     * silently dropped every audit event. Retrying is pointless: the classpath and the audit
     * file only change with a restart.
     */
    private static synchronized void initAuditIfNeeded(RangerBasePlugin plugin) {
        AuditProviderFactory factory = AuditProviderFactory.getInstance();
        Properties properties = plugin.getConfig().getProperties();
        if (factory.isInitDone() || properties == null) {
            return;
        }
        try {
            factory.init(properties, plugin.getAppId());
        } catch (Exception | LinkageError e) {
            auditInitFailed = true;
            LOG.error("Failed to initialize Ranger audit for service type {}. Authorization keeps working, but this FE "
                    + "writes no Ranger audit event until it is restarted with a fixed audit configuration. Check the "
                    + "enabled xasecure.audit.* destinations in ranger-{}-audit.xml: this FE ships only the 'file' and "
                    + "'solr' Ranger audit destinations.", plugin.getServiceType(), plugin.getServiceType(), e);
        }
    }

    public static boolean isAuditInitFailed() {
        return auditInitFailed;
    }

    protected RangerPluginConfig buildRangerPluginContext(String serviceType, String serviceName) {
        LOG.info("Interacting with Ranger Admin Server using SIMPLE authentication");
        return new RangerPluginConfig(serviceType, serviceName, serviceType,
                null, null, null);
    }

    public RangerBasePlugin getRangerPlugin() {
        return rangerPlugin;
    }

    // RangerBasePlugin keeps its policy engine protected. We need it to read the tag policies.
    private static class Plugin extends RangerBasePlugin {
        Plugin(RangerPluginConfig config) {
            super(config);
        }

        RangerPolicyEngine currentPolicyEngine() {
            return getPolicyEngine();
        }
    }

    // The answer of tagPolicyMayAllow for one policy engine and policy version.
    private record TagAllowCheck(WeakReference<RangerPolicyEngine> engine, long policyVersion, boolean mayAllow) {
    }

    private volatile TagAllowCheck tagAllowCheck;

    /**
     * Whether a tag policy of this service can allow an access check: an enabled access policy with
     * an allow item. Without one, tag policies can only deny, mask or filter rows.
     * <p>
     * We want to read the tag policies once per policy version, not per check. Ranger Admin raises
     * the policy version of the service when a tag policy changes, and a refresh either builds a new
     * engine or updates the version of the old one. So the answer is kept for one engine and version.
     */
    protected boolean tagPolicyMayAllow() {
        try {
            RangerPolicyEngine engine =
                    rangerPlugin instanceof Plugin plugin ? plugin.currentPolicyEngine() : null;
            if (engine == null) {
                return true;
            }
            long policyVersion = engine.getPolicyVersion();
            TagAllowCheck check = tagAllowCheck;
            if (check != null && check.engine().get() == engine && check.policyVersion() == policyVersion) {
                return check.mayAllow();
            }
            boolean mayAllow = false;
            for (RangerPolicy policy : engine.getTagPolicies()) {
                // The engine keeps only the access types of this service in a tag policy, so an item
                // that allows only another service is already gone.
                boolean isAccessPolicy = policy.getPolicyType() == null
                        || policy.getPolicyType() == RangerPolicy.POLICY_TYPE_ACCESS;
                if (!Boolean.FALSE.equals(policy.getIsEnabled()) && isAccessPolicy
                        && CollectionUtils.isNotEmpty(policy.getPolicyItems())) {
                    mayAllow = true;
                    break;
                }
            }
            tagAllowCheck = new TagAllowCheck(new WeakReference<>(engine), policyVersion, mayAllow);
            return mayAllow;
        } catch (RuntimeException e) {
            // We cannot tell, so we answer as if a tag policy could allow.
            return true;
        }
    }

    // The transformer of a mask, before the column name and type are put into it. It can be null.
    private record Mask(String transformer) {
    }

    public Expr getColumnMaskingExpression(RangerAccessResourceImpl resource, Column column, ConnectContext context) {
        UserIdentity user = context.getCurrentUserIdentity();
        Set<String> groups = context.getGroups();
        String accessType = PrivilegeType.SELECT.name().toLowerCase(ENGLISH);
        // The analyzer changes an Expr in place, so we keep the transformer text and parse it on every call.
        Optional<Mask> mask = decide(context, RangerDecisionCache.Kind.MASK, resource, user, groups, accessType,
                null, () -> evalMask(resource, user, groups, accessType));
        if (mask.isEmpty()) {
            return null;
        }
        String transformer = mask.get().transformer();
        if (StringUtils.isNotEmpty(transformer)) {
            transformer = transformer.replace("{col}", column.getName())
                    .replace("{type}", column.getType().toSql());
        }

        return SqlParser.parseSqlToExpr(transformer, context.getSessionVariable().getSqlMode());
    }

    private Optional<Mask> evalMask(RangerAccessResourceImpl resource, UserIdentity user, Set<String> groups,
                                    String accessType) {
        RangerStarRocksAccessRequest request =
                RangerStarRocksAccessRequest.createAccessRequest(resource, user, groups, accessType);

        RangerAccessResult result = rangerPlugin.evalDataMaskPolicies(request, null);
        if (result != null && result.isMaskEnabled()) {
            String maskType = result.getMaskType();
            RangerServiceDef.RangerDataMaskTypeDef maskTypeDef = result.getMaskTypeDef();
            String transformer = null;

            if (maskTypeDef != null) {
                transformer = maskTypeDef.getTransformer();
            }

            if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_NULL)) {
                transformer = "NULL";
            } else if (StringUtils.equalsIgnoreCase(maskType, RangerPolicy.MASK_TYPE_CUSTOM)) {
                String maskedValue = result.getMaskedValue();
                transformer = Objects.requireNonNullElse(maskedValue, "NULL");
            }

            return Optional.of(new Mask(transformer));
        }

        return Optional.empty();
    }

    protected Expr getRowAccessExpression(RangerAccessResourceImpl resource, ConnectContext context) {
        UserIdentity user = context.getCurrentUserIdentity();
        Set<String> groups = context.getGroups();
        String accessType = PrivilegeType.SELECT.name().toLowerCase(ENGLISH);
        // The analyzer changes an Expr in place, so we keep the filter text and parse it on every call.
        Optional<String> filter = decide(context, RangerDecisionCache.Kind.ROW_FILTER, resource, user, groups,
                accessType, null, () -> {
                    RangerStarRocksAccessRequest request =
                            RangerStarRocksAccessRequest.createAccessRequest(resource, user, groups, accessType);
                    RangerAccessResult result = rangerPlugin.evalRowFilterPolicies(request, null);
                    if (result != null && result.isRowFilterEnabled()) {
                        return Optional.of(result.getFilterExpr());
                    } else {
                        return Optional.empty();
                    }
                });
        return filter.map(expr -> SqlParser.parseSqlToExpr(expr, context.getSessionVariable().getSqlMode()))
                .orElse(null);
    }

    protected void hasPermission(RangerAccessResourceImpl resource, ConnectContext context,
                                 PrivilegeType privilegeType)
            throws AccessDeniedException {
        hasPermission(resource, context, privilegeType, EnumSet.noneOf(ColumnAccessKind.class));
    }

    protected void hasPermission(RangerAccessResourceImpl resource, ConnectContext context,
                                 PrivilegeType privilegeType, EnumSet<ColumnAccessKind> usage)
            throws AccessDeniedException {
        if (!decideAccess(resource, context, privilegeType, usage).allowed()) {
            throw new AccessDeniedException();
        }
    }

    /**
     * The answer of Ranger to one access check. decidedByPolicy is false when no policy set the
     * answer: the check is denied only because nothing allows it.
     */
    protected record AccessDecision(boolean allowed, boolean decidedByPolicy) {
    }

    protected AccessDecision decideAccess(RangerAccessResourceImpl resource, ConnectContext context,
                                          PrivilegeType privilegeType, EnumSet<ColumnAccessKind> usage) {
        UserIdentity user = context.getCurrentUserIdentity();
        // root user bypasses Ranger authorization entirely
        if (UserIdentity.ROOT.equals(user)) {
            return new AccessDecision(true, false);
        }
        String accessType;
        if (privilegeType.equals(PrivilegeType.ANY)) {
            accessType = RangerPolicyEngine.ANY_ACCESS;
        } else {
            accessType = convertToAccessType(privilegeType);
        }

        Set<String> groups = context.getGroups();
        return decide(context, RangerDecisionCache.Kind.ACCESS, resource, user, groups, accessType, usage, () -> {
            RangerStarRocksAccessRequest request = RangerStarRocksAccessRequest.createAccessRequest(
                    resource, user, groups, accessType, usage);
            RangerAccessResult result = rangerPlugin.isAccessAllowed(request);
            if (result == null) {
                return new AccessDecision(false, false);
            }
            // A result keeps policyId -1 unless a policy set the answer.
            return new AccessDecision(result.getIsAllowed(), result.getPolicyId() != -1);
        });
    }

    // Every Ranger decision of this controller goes through here, so the statement asks Ranger
    // each question once. The key is built from the same values as the request.
    private <T> T decide(ConnectContext context, RangerDecisionCache.Kind kind, RangerAccessResourceImpl resource,
                         UserIdentity user, Set<String> groups, String accessType, EnumSet<ColumnAccessKind> usage,
                         Supplier<T> evaluate) {
        RangerDecisionCache.Key key =
                RangerDecisionCache.keyOf(this, kind, resource, user, groups, accessType, usage);
        return context.getRangerDecisionCache().get(key, evaluate);
    }
}
