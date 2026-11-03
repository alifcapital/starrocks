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

import org.apache.ranger.audit.destination.AuditDestination;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.audit.provider.MiscUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.LBHttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Sends Ranger audit events to a standalone Solr, with an optional basic auth login.
 * <p>
 * Ranger's own SolrAuditDestination builds its client with solrj's Krb5HttpClientBuilder even
 * without Kerberos, and that class links jetty-client, which the FE does not ship (jetty-client
 * and jetty-http are banned for their CVEs). It also has no login settings, so it cannot write
 * to a Solr that rejects anonymous requests. This destination uses only the Apache HttpClient
 * path of solrj and puts the login on every update request.
 * <p>
 * Enabled in ranger-starrocks-audit.xml:
 * <pre>
 * xasecure.audit.destination.solr           = true
 * xasecure.audit.destination.solr.classname = com.starrocks.authorization.ranger.RangerSolrAuditDestination
 * xasecure.audit.destination.solr.urls      = http://host:6083/solr/ranger_audits  (comma separated)
 * xasecure.audit.destination.solr.user      = optional basic auth user
 * xasecure.audit.destination.solr.password  = its password
 * </pre>
 */
public class RangerSolrAuditDestination extends AuditDestination {
    private static final Logger LOG = LoggerFactory.getLogger(RangerSolrAuditDestination.class);

    public static final String PROP_URLS = "urls";
    public static final String PROP_USER = "user";
    public static final String PROP_PASSWORD = "password";

    // An audit send runs on Ranger's audit queue thread, not on a query. We still do not want a dead Solr to hold
    // that thread for minutes, so both timeouts are short; the queue keeps the events and retries.
    private static final int CONNECT_TIMEOUT_MS = 1000;
    private static final int SOCKET_TIMEOUT_MS = 30000;

    private volatile SolrClient solrClient;
    private String user;
    private String password;

    @Override
    public void init(Properties props, String propPrefix) {
        super.init(props, propPrefix);
        List<String> urls = MiscUtil.toArray(MiscUtil.getStringProperty(props, propPrefix + "." + PROP_URLS), ",")
                .stream().map(String::trim).filter(url -> !url.isEmpty()).collect(Collectors.toList());
        user = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_USER);
        password = MiscUtil.getStringProperty(props, propPrefix + "." + PROP_PASSWORD);
        if (urls.isEmpty()) {
            LOG.error("{}.{} is not set, Ranger audit is not sent to Solr", propPrefix, PROP_URLS);
            return;
        }
        if (user == null || user.isEmpty() || password == null) {
            LOG.warn("{}.{} or {}.{} is not set, Ranger audit is sent to Solr without a login",
                    propPrefix, PROP_USER, propPrefix, PROP_PASSWORD);
            user = null;
        }
        // Building the client does not connect, so a Solr that is down now only fails the sends.
        solrClient = new LBHttpSolrClient.Builder()
                .withBaseSolrUrls(urls.toArray(new String[0]))
                .withConnectionTimeout(CONNECT_TIMEOUT_MS)
                .withSocketTimeout(SOCKET_TIMEOUT_MS)
                .build();
        LOG.info("Ranger audit goes to Solr {} {}", urls, user == null ? "without a login" : "as " + user);
    }

    @Override
    public boolean log(Collection<AuditEventBase> events) {
        logStatusIfRequired();
        addTotalCount(events.size());
        SolrClient client = solrClient;
        if (client == null) {
            addDeferredCount(events.size());
            return false;
        }
        try {
            List<SolrInputDocument> docs = new ArrayList<>(events.size());
            for (AuditEventBase event : events) {
                docs.add(toSolrDoc((AuthzAuditEvent) event));
            }
            UpdateRequest request = new UpdateRequest();
            request.add(docs);
            if (user != null) {
                request.setBasicAuthCredentials(user, password);
            }
            UpdateResponse response = request.process(client);
            if (response.getStatus() != 0) {
                addFailedCount(events.size());
                logFailedEvent(events, response.toString());
                return false;
            }
            addSuccessCount(events.size());
            return true;
        } catch (SolrException e) {
            // Solr answered with an error, for example 401 for a wrong login: sending again does not help.
            addFailedCount(events.size());
            logFailedEvent(events, e);
            return false;
        } catch (Exception e) {
            // Solr did not answer: the queue keeps the events for another try.
            addDeferredCount(events.size());
            logError("Error sending Ranger audit to Solr", e);
            return false;
        }
    }

    @Override
    public void stop() {
        logStatus();
        SolrClient client = solrClient;
        solrClient = null;
        if (client != null) {
            try {
                client.close();
            } catch (IOException e) {
                LOG.warn("Failed to close the Solr client of Ranger audit", e);
            }
        }
    }

    // Ranger Admin reads these field names from the ranger_audits core for Audit > Access, so they are the same as in
    // Ranger's SolrAuditDestination.
    static SolrInputDocument toSolrDoc(AuthzAuditEvent event) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", event.getEventId());
        doc.addField("access", event.getAccessType());
        doc.addField("enforcer", event.getAclEnforcer());
        doc.addField("agent", event.getAgentId());
        doc.addField("repo", event.getRepositoryName());
        doc.addField("sess", event.getSessionId());
        doc.addField("reqUser", event.getUser());
        doc.addField("reqData", event.getRequestData());
        doc.addField("resource", event.getResourcePath());
        doc.addField("cliIP", event.getClientIP());
        doc.addField("logType", event.getLogType());
        doc.addField("result", event.getAccessResult());
        doc.addField("policy", event.getPolicyId());
        doc.addField("repoType", event.getRepositoryType());
        doc.addField("resType", event.getResourceType());
        doc.addField("reason", event.getResultReason());
        doc.addField("action", event.getAction());
        doc.addField("evtTime", event.getEventTime());
        doc.addField("seq_num", event.getSeqNum());
        doc.setField("event_count", event.getEventCount());
        doc.setField("event_dur_ms", event.getEventDurationMS());
        doc.setField("tags", event.getTags());
        doc.setField("cluster", event.getClusterName());
        doc.setField("zoneName", event.getZoneName());
        doc.setField("agentHost", event.getAgentHostname());
        doc.setField("policyVersion", event.getPolicyVersion());
        return doc;
    }
}
