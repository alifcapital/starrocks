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

import com.sun.net.httpserver.HttpServer;
import org.apache.ranger.audit.model.AuditEventBase;
import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.solr.common.util.JavaBinCodec;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * Sends audit events to a fake Solr over HTTP: the destination must reach /update of the core with the login on
 * the request, and keep the events for another try when Solr does not answer.
 */
public class RangerSolrAuditDestinationTest {
    private static final String PREFIX = "xasecure.audit.destination.solr";
    private static final String USER = "ranger_audit";
    private static final String PASSWORD = "secret1";

    private HttpServer solr;
    private final List<String> paths = new ArrayList<>();
    private final List<String> authHeaders = new ArrayList<>();
    private final List<byte[]> bodies = new ArrayList<>();

    @BeforeEach
    public void startSolr() throws IOException {
        solr = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String expected = "Basic " + Base64.getEncoder().encodeToString(
                (USER + ":" + PASSWORD).getBytes(StandardCharsets.UTF_8));
        solr.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().getPath());
            String auth = exchange.getRequestHeaders().getFirst("Authorization");
            authHeaders.add(auth);
            bodies.add(exchange.getRequestBody().readAllBytes());
            if (!expected.equals(auth)) {
                exchange.sendResponseHeaders(401, -1);
                exchange.close();
                return;
            }
            byte[] response = okResponse();
            exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(response);
            }
        });
        solr.start();
    }

    @AfterEach
    public void stopSolr() {
        solr.stop(0);
    }

    private static byte[] okResponse() throws IOException {
        NamedList<Object> header = new NamedList<>();
        header.add("status", 0);
        header.add("QTime", 1);
        NamedList<Object> response = new NamedList<>();
        response.add("responseHeader", header);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        new JavaBinCodec().marshal(response, out);
        return out.toByteArray();
    }

    private String coreUrl() {
        return "http://127.0.0.1:" + solr.getAddress().getPort() + "/solr/ranger_audits";
    }

    private static RangerSolrAuditDestination destination(String urls, String user, String password) {
        Properties props = new Properties();
        props.setProperty(PREFIX + ".urls", urls);
        if (user != null) {
            props.setProperty(PREFIX + ".user", user);
            props.setProperty(PREFIX + ".password", password);
        }
        RangerSolrAuditDestination destination = new RangerSolrAuditDestination();
        destination.init(props, PREFIX);
        return destination;
    }

    private static List<AuditEventBase> events(String eventId) {
        AuthzAuditEvent event = new AuthzAuditEvent();
        event.setEventId(eventId);
        event.setUser("eshishkin");
        event.setAccessType("select");
        event.setRepositoryName("starrocks");
        event.setEventTime(new Date());
        return List.of(event);
    }

    private static boolean contains(byte[] body, String text) {
        return new String(body, StandardCharsets.ISO_8859_1).contains(text);
    }

    @Test
    public void testSendsEventsWithLogin() {
        RangerSolrAuditDestination destination = destination(" " + coreUrl() + " ", USER, PASSWORD);
        try {
            Assertions.assertTrue(destination.log(events("event-1")));
            Assertions.assertEquals(List.of("/solr/ranger_audits/update"), paths);
            Assertions.assertTrue(contains(bodies.get(0), "event-1"));
            Assertions.assertEquals(1, destination.getTotalSuccessCount());
        } finally {
            destination.stop();
        }
    }

    @Test
    public void testWrongLoginFailsEvents() {
        RangerSolrAuditDestination destination = destination(coreUrl(), USER, "wrong1");
        try {
            Assertions.assertFalse(destination.log(events("event-2")));
            Assertions.assertEquals(1, destination.getTotalFailedCount());
            Assertions.assertEquals(0, destination.getTotalDeferredCount());
        } finally {
            destination.stop();
        }
    }

    @Test
    public void testNoLoginSendsWithoutAuthorization() {
        RangerSolrAuditDestination destination = destination(coreUrl(), null, null);
        try {
            Assertions.assertFalse(destination.log(events("event-3")));
            Assertions.assertEquals(1, authHeaders.size());
            Assertions.assertNull(authHeaders.get(0));
        } finally {
            destination.stop();
        }
    }

    @Test
    public void testSolrDownDefersEvents() throws IOException {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        RangerSolrAuditDestination destination =
                destination("http://127.0.0.1:" + port + "/solr/ranger_audits", USER, PASSWORD);
        try {
            Assertions.assertFalse(destination.log(events("event-4")));
            Assertions.assertEquals(1, destination.getTotalDeferredCount());
            Assertions.assertEquals(0, destination.getTotalFailedCount());
        } finally {
            destination.stop();
        }
    }

    @Test
    public void testNoUrlsDefersEvents() {
        RangerSolrAuditDestination destination = destination("", USER, PASSWORD);
        Assertions.assertFalse(destination.log(events("event-5")));
        Assertions.assertEquals(1, destination.getTotalDeferredCount());
        destination.stop();
    }
}
