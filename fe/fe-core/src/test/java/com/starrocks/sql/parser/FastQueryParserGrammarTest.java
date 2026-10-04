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

package com.starrocks.sql.parser;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The fast query parser repeats the query part of the ANTLR grammar by hand. A new StarRocks version changes
 * the grammar, and the fast parser does not see it: it would keep building the old AST for a changed rule.
 * This test keeps a fingerprint of every rule that the fast parser owns. When it fails, the listed rules
 * changed: port the change to the fast parser, add fixtures for it to fast-query-parser-fixtures.jsonl, and
 * write the new fingerprints with -Dfast.parser.write.grammar=path.
 */
class FastQueryParserGrammarTest {
    private static final String SNAPSHOT = "sql/parser/fast-query-parser-grammar.txt";

    @Test
    void ownedRulesMatchSnapshot() throws IOException {
        Map<String, String> actual = QueryGrammar.fingerprints();
        String output = System.getProperty("fast.parser.write.grammar");
        if (output != null) {
            StringBuilder text = new StringBuilder();
            actual.forEach((rule, fingerprint) -> text.append(rule).append(' ').append(fingerprint).append('\n'));
            Files.writeString(Path.of(output), text.toString(), StandardCharsets.UTF_8);
        }
        Map<String, String> expected = new TreeMap<>();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(SNAPSHOT)) {
            String text = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            for (String line : text.split("\n")) {
                if (!line.isBlank()) {
                    String[] parts = line.trim().split(" ");
                    expected.put(parts[0], parts[1]);
                }
            }
        }
        List<String> changed = new ArrayList<>();
        for (String rule : actual.keySet()) {
            if (!expected.containsKey(rule)) {
                changed.add("new rule " + rule);
            } else if (!expected.get(rule).equals(actual.get(rule))) {
                changed.add("changed rule " + rule);
            }
        }
        for (String rule : expected.keySet()) {
            if (!actual.containsKey(rule)) {
                changed.add("rule no longer reachable " + rule);
            }
        }
        assertEquals(List.of(), changed, "grammar rules owned by the fast query parser changed");
    }
}
