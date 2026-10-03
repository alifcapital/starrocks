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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PrepareStmt;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UpdateStmt;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The fast query parser must build the same AST as ANTLR for every statement it accepts, and it must accept
 * every valid query and DML statement. ANTLR only gets invalid SQL, to report the usual error, and statements
 * the fast parser does not own.
 *
 * <p>fixturesMatchAntlr() checks this on the fixture files in the SQL modes that change parsing:
 * - an accepted statement has the same AST, including positions, as with ANTLR;
 * - a statement that ANTLR parses as a query or DML is accepted, unless its fixture names the known gap;
 * - the accepted statements use every element of the grammar rules the fast parser owns (see QueryGrammar).
 * When a new grammar element appears, add a fixture that uses it and make the fast parser accept it.
 *
 * <p>Run with -Dfast.parser.corpus=file to compare the parsers on other SQL, for example queries from a
 * production log. The file contains one JSON object with a "sql" field per line, or the binary format
 * (int count, then count pairs of id and SQL, each as int length and UTF-8 bytes). The report lists the
 * fallback reasons; they show what syntax the fast parser still lacks.
 */
class FastQueryParserParityTest {
    private static final List<String> FIXTURES = List.of(
            "sql/parser/fast-query-parser-fixtures.jsonl",
            "sql/parser/fast-query-parser-fixtures-window.jsonl",
            "sql/parser/fast-query-parser-fixtures-expression.jsonl",
            "sql/parser/fast-query-parser-fixtures-statement.jsonl");
    // -Dfast.parser.verbose=true prints why ANTLR rejects a statement.
    private static final boolean VERBOSE = Boolean.getBoolean("fast.parser.verbose");
    private static final long[] MODE_BITS = {
            SqlModeHelper.MODE_PIPES_AS_CONCAT,
            SqlModeHelper.MODE_DOUBLE_LITERAL,
            SqlModeHelper.MODE_SORT_NULLS_LAST,
            SqlModeHelper.MODE_GROUP_CONCAT_LEGACY,
    };

    private ConnectContext previousContext;

    @BeforeEach
    void installParserContext() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
    }

    @AfterEach
    void restoreParserContext() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    // gap: why the fast parser does not accept this valid statement; null when it must accept it.
    private record Fixture(String id, String sql, String gap) {
    }

    // Grammar elements that no accepted fixture needs to use, each with the reason.
    private static final Set<String> NOT_REQUIRED_ELEMENTS = Set.of(
            // AstBuilder rejects array slices.
            "primaryExpression alternative ArraySliceContext",
            "primaryExpression token ':'",
            "primaryExpression token INTEGER_VALUE",
            // AstBuilder rejects a parameter as LIMIT or OFFSET.
            "limitConstExpr token '?'",
            // AstBuilder rejects EXPLAIN, TRACE and INTO OUTFILE in the query that INSERT embeds.
            "queryStatement calls explainDesc",
            "queryStatement calls optimizerTrace",
            "queryStatement calls outfile",
            // AstBuilder rejects a nested field path in a STRUCT type declaration.
            "subfieldDesc calls nestedFieldName",
            "nestedFieldName calls subfieldName",
            "nestedFieldName token '.'",
            "nestedFieldName token DOT_IDENTIFIER",
            "subfieldName calls identifier",
            "subfieldName token '[*]'",
            // ANTLR parses a parenthesized relation as relationPrimary, which is the first alternative.
            "relation token '('",
            "relation token ')'");

    private static final class Report {
        int statements;
        int parsedByFast;
        final List<String> mismatches = new ArrayList<>();
        // One entry per fixture: the reason of its first gap and the SQL.
        final Map<String, String> gaps = new LinkedHashMap<>();
        final Map<String, Integer> fallbackReasons = new TreeMap<>();
        final Set<String> coveredElements = new TreeSet<>();

        void print(PrintStream out) {
            out.printf("statements %d, parsed by fast parser %d, fallbacks %d, mismatches %d, gaps %d%n",
                    statements, parsedByFast, statements - parsedByFast, mismatches.size(), gaps.size());
            fallbackReasons.entrySet().stream()
                    .sorted((a, b) -> b.getValue() - a.getValue())
                    .limit(50)
                    .forEach(e -> out.printf("%8d  %s%n", e.getValue(), e.getKey()));
            mismatches.stream().limit(50).forEach(m -> out.println("MISMATCH " + m));
            gaps.forEach((id, gap) -> out.println("GAP " + id + " " + gap));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.MINUTES)
    void fixturesMatchAntlr() throws IOException {
        List<Fixture> fixtures = new ArrayList<>();
        for (String file : FIXTURES) {
            try (InputStream input = getClass().getClassLoader().getResourceAsStream(file)) {
                for (Fixture fixture : readJsonLines(input)) {
                    String name = file.substring(file.lastIndexOf('/') + 1);
                    fixtures.add(new Fixture(name + fixture.id(), fixture.sql(), fixture.gap()));
                }
            }
        }
        fixtures.addAll(keywordIdentifierFixtures());
        Report report = new Report();
        List<String> staleGaps = new ArrayList<>();
        for (Fixture fixture : fixtures) {
            boolean acceptedInEveryMode = true;
            for (int combination = 0; combination < 1 << MODE_BITS.length; combination++) {
                long mode = 0;
                for (int bit = 0; bit < MODE_BITS.length; bit++) {
                    if ((combination & (1 << bit)) != 0) {
                        mode |= MODE_BITS[bit];
                    }
                }
                acceptedInEveryMode &= compare(fixture, mode, report);
            }
            if (fixture.gap() != null && acceptedInEveryMode) {
                staleGaps.add(fixture.sql());
            }
        }
        Set<String> uncovered = new TreeSet<>(QueryGrammar.elements());
        uncovered.removeAll(report.coveredElements);
        uncovered.removeAll(NOT_REQUIRED_ELEMENTS);
        report.print(System.out);
        uncovered.forEach(e -> System.out.println("UNCOVERED " + e));
        assertTrue(report.mismatches.isEmpty(), () -> "AST differs from ANTLR:\n" + String.join("\n", report.mismatches));
        assertEquals(Map.of(), report.gaps, "valid statements that go to ANTLR");
        assertEquals(List.of(), staleGaps, "the fast parser accepts these statements; remove their gap");
        assertEquals(Set.of(), uncovered, "grammar elements without an accepted fixture");
    }

    // Every key word that the grammar allows as an identifier, used as a column name.
    private static List<Fixture> keywordIdentifierFixtures() {
        List<Fixture> fixtures = new ArrayList<>();
        for (String word : QueryGrammar.nonReservedWords()) {
            fixtures.add(new Fixture("identifier " + word, "SELECT " + word + ", t." + word + " FROM t", null));
        }
        return fixtures;
    }

    @Test
    @Timeout(value = 4, unit = TimeUnit.HOURS)
    void corpusMatchesAntlr() throws IOException {
        String corpus = System.getProperty("fast.parser.corpus");
        Assumptions.assumeTrue(corpus != null, "set -Dfast.parser.corpus to compare the parsers on a corpus");
        long mode = Long.parseLong(System.getProperty("fast.parser.sql_mode", "0"));
        Path path = Path.of(corpus);
        List<Fixture> fixtures;
        try (InputStream input = Files.newInputStream(path)) {
            fixtures = corpus.endsWith(".bin") ? readBinary(input) : readJsonLines(input);
        }
        Report report = new Report();
        for (Fixture fixture : fixtures) {
            compare(fixture, mode, report);
        }
        report.print(System.out);
        assertTrue(report.mismatches.isEmpty(), () -> "AST differs from ANTLR:\n" + String.join("\n", report.mismatches));
    }

    /**
     * Run with -Dfast.parser.timing=file to compare parse CPU time of ANTLR and of the public parser entry,
     * which tries the fast parser first. Surefire runs tests with the C1 compiler only, so for numbers close
     * to production run main() of this class in a JVM with default flags.
     */
    @Test
    @Timeout(value = 4, unit = TimeUnit.HOURS)
    void corpusTiming() throws IOException {
        String corpus = System.getProperty("fast.parser.timing");
        Assumptions.assumeTrue(corpus != null, "set -Dfast.parser.timing to time the parsers on a corpus");
        timeCorpus(corpus, Integer.parseInt(System.getProperty("fast.parser.rounds", "5")), false);
    }

    /**
     * Arguments: corpus file, number of rounds and optionally "fast-only", which skips ANTLR so that a profiler
     * attached to this JVM sees only the public parser entry.
     */
    public static void main(String[] args) throws IOException {
        ConnectContext context = new ConnectContext();
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
        timeCorpus(args[0], Integer.parseInt(args[1]), args.length > 2 && args[2].equals("fast-only"));
        System.exit(0);
    }

    // Every round parses every valid statement once with each parser. The order alternates between rounds
    // so that neither parser always runs second on a warmer JIT and a fuller heap.
    private static void timeCorpus(String corpus, int rounds, boolean fastOnly) throws IOException {
        List<Fixture> fixtures;
        try (InputStream input = Files.newInputStream(Path.of(corpus))) {
            fixtures = corpus.endsWith(".bin") ? readBinary(input) : readJsonLines(input);
        }
        SessionVariable session = new SessionVariable();
        List<String> valid = new ArrayList<>();
        for (Fixture fixture : fixtures) {
            try {
                SqlParser.parseWithAntlr(fixture.sql(), session);
                valid.add(fixture.sql());
            } catch (RuntimeException e) {
                // Invalid statements are timed by neither parser.
            }
        }
        com.sun.management.ThreadMXBean threads =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        for (int round = 0; round < rounds; round++) {
            long[] cpu = new long[2];
            long[] max = new long[2];
            long[] bytes = new long[2];
            for (String sql : valid) {
                for (int step = 0; step < 2; step++) {
                    int arm = round % 2 == 0 ? step : 1 - step;
                    if (fastOnly && arm == 0) {
                        continue;
                    }
                    long startCpu = threads.getCurrentThreadCpuTime();
                    long startBytes = threads.getCurrentThreadAllocatedBytes();
                    if (arm == 0) {
                        SqlParser.parseWithAntlr(sql, session);
                    } else {
                        SqlParser.parse(sql, session);
                    }
                    long spent = threads.getCurrentThreadCpuTime() - startCpu;
                    bytes[arm] += threads.getCurrentThreadAllocatedBytes() - startBytes;
                    cpu[arm] += spent;
                    max[arm] = Math.max(max[arm], spent);
                }
            }
            System.out.printf("round %d: %d statements; ANTLR cpu %.1f ms, max %.2f ms, allocated %.1f MiB; "
                            + "public entry cpu %.1f ms, max %.2f ms, allocated %.1f MiB%n", round, valid.size(),
                    cpu[0] / 1e6, max[0] / 1e6, bytes[0] / 1048576.0,
                    cpu[1] / 1e6, max[1] / 1e6, bytes[1] / 1048576.0);
        }
    }

    /** Returns whether the fast parser accepted the statement. */
    private static boolean compare(Fixture fixture, long mode, Report report) {
        SessionVariable session = new SessionVariable();
        session.setSqlMode(mode);
        report.statements++;
        FastQueryParser.Attempt attempt = FastQueryParser.attempt(fixture.sql(), session);
        List<StatementBase> reference;
        try {
            reference = SqlParser.parseWithAntlr(fixture.sql(), session);
        } catch (RuntimeException e) {
            reference = null;
            if (VERBOSE) {
                System.out.println("ANTLR rejects (" + e.getMessage() + "): " + abbreviate(fixture.sql()));
            }
            if (attempt.parsed()) {
                report.mismatches.add(fixture.id() + " mode " + mode + ": ANTLR rejects (" + e.getMessage() + "): "
                        + abbreviate(fixture.sql()));
            }
        }
        if (!attempt.parsed()) {
            report.fallbackReasons.merge(normalize(attempt.fallbackReason()), 1, Integer::sum);
            if (fixture.gap() == null && reference != null && ownedByFastParser(reference)) {
                report.gaps.putIfAbsent(fixture.id(), "mode " + mode + ": " + normalize(attempt.fallbackReason()) + ": "
                        + abbreviate(fixture.sql()));
            }
            return false;
        }
        report.parsedByFast++;
        if (reference == null) {
            return true;
        }
        // Without a 'seed' property a SAMPLE clause draws a new random seed in every parse.
        boolean randomSampleSeed = !fixture.sql().toLowerCase(Locale.ROOT).contains("seed");
        String difference = AstDigest.difference(reference, attempt.statements(), randomSampleSeed);
        if (difference != null) {
            report.mismatches.add(fixture.id() + " mode " + mode + ": " + difference + ": " + abbreviate(fixture.sql()));
        } else {
            Set<String> elements = new TreeSet<>();
            QueryGrammar.collect(fixture.sql(), mode, elements);
            if (VERBOSE) {
                System.out.println("ELEMENTS " + elements + ": " + abbreviate(fixture.sql()));
            }
            report.coveredElements.addAll(elements);
        }
        return true;
    }

    private static boolean ownedByFastParser(List<StatementBase> statements) {
        for (StatementBase statement : statements) {
            StatementBase inner = statement instanceof PrepareStmt prepare ? prepare.getInnerStmt() : statement;
            if (!(inner instanceof QueryStatement || inner instanceof InsertStmt || inner instanceof UpdateStmt
                    || inner instanceof DeleteStmt)) {
                return false;
            }
        }
        return true;
    }

    // Reasons carry token positions; we drop them to count reasons over many statements.
    private static String normalize(String reason) {
        return reason.replaceAll(" at raw=\\d+", "").replaceAll("\\d+", "N");
    }

    private static String abbreviate(String sql) {
        String line = sql.replaceAll("\\s+", " ");
        return line.length() <= 300 ? line : line.substring(0, 300) + "...";
    }

    private static List<Fixture> readJsonLines(InputStream input) throws IOException {
        List<Fixture> fixtures = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isBlank()) {
                continue;
            }
            JsonObject object = JsonParser.parseString(line).getAsJsonObject();
            String gap = object.has("gap") ? object.get("gap").getAsString() : null;
            fixtures.add(new Fixture("#" + (fixtures.size() + 1), object.get("sql").getAsString(), gap));
        }
        return fixtures;
    }

    private static List<Fixture> readBinary(InputStream input) throws IOException {
        DataInputStream data = new DataInputStream(new BufferedInputStream(input));
        int count = data.readInt();
        List<Fixture> fixtures = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String id = readString(data);
            fixtures.add(new Fixture(id, readString(data), null));
        }
        return fixtures;
    }

    private static String readString(DataInputStream data) throws IOException {
        byte[] bytes = new byte[data.readInt()];
        data.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
