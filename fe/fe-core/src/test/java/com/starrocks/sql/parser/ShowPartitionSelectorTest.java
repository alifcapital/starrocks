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

import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.AdminShowReplicaDistributionStmt;
import com.starrocks.sql.ast.AdminShowReplicaStatusStmt;
import com.starrocks.sql.ast.AdminShowTabletStatusStmt;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.ShowDataDistributionStmt;
import com.starrocks.sql.ast.StatementBase;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.junit.jupiter.api.Test;

import java.util.IdentityHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ShowPartitionSelectorTest {
    private static final List<String> PREFIXES = List.of("ADMIN SHOW REPLICA STATUS", "ADMIN SHOW TABLET STATUS",
            "ADMIN SHOW REPLICA DISTRIBUTION", "SHOW DATA DISTRIBUTION");
    private static final PredictionMode[] MODES = {PredictionMode.SLL, PredictionMode.LL};

    private static StatementBase parse(String sql, PredictionMode mode) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(SqlTextStream.create(sql)));
        lexer.setSqlMode(SqlModeHelper.MODE_DEFAULT);
        lexer.removeErrorListeners();
        lexer.addErrorListener(new ErrorHandler());
        StarRocksParser parser = new StarRocksParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.addErrorListener(new ErrorHandler());
        parser.setErrorHandler(new StarRocksDefaultErrorStrategy());
        parser.getInterpreter().setPredictionMode(mode);
        PostProcessListener listener = new PostProcessListener(100_000, 100_000);
        parser.addParseListener(listener);
        var tree = parser.sqlStatements();
        listener.validateTupleContexts();
        var context = tree.singleStatement(0);
        AstBuilder builder = new AstBuilder(SqlModeHelper.MODE_DEFAULT, false, new IdentityHashMap<>());
        builder.initializeParameterContext(LexicalParameterContext.forRule(parser, context));
        return (StatementBase) builder.visitSingleStatement(context);
    }

    private static PartitionRef selector(StatementBase stmt) {
        if (stmt instanceof AdminShowReplicaStatusStmt s) {
            return s.getPartitionRef();
        }
        if (stmt instanceof AdminShowTabletStatusStmt s) {
            return s.getPartitionRef();
        }
        if (stmt instanceof AdminShowReplicaDistributionStmt s) {
            return s.getPartitionRef();
        }
        return ((ShowDataDistributionStmt) stmt).getPartitionDef();
    }

    @Test
    void rejectUnsupportedKeySelectorsBeforeImplicitPrepareWrapping() {
        for (String prefix : PREFIXES) {
            assertThrows(ParsingException.class,
                    () -> SqlParser.parse(prefix + " FROM db.t PARTITION(k=?)", new SessionVariable()));
        }
        for (PredictionMode mode : MODES) {
            for (String prefix : PREFIXES) {
                for (String value : List.of("1", "NULL", "?", "INTERVAL IF(true,1,2) DAY")) {
                    ParsingException error = assertThrows(ParsingException.class,
                            () -> parse(prefix + " FROM db.t PARTITION(k=" + value + ")", mode));
                    assertTrue(error.getMessage().contains("Key partition selectors are not supported for " + prefix));
                }
            }
        }
    }

    @Test
    void preserveAbsentNamedAndTemporarySelectors() {
        for (PredictionMode mode : MODES) {
            for (String prefix : PREFIXES) {
                assertNull(selector(parse(prefix + " FROM db.t", mode)));
                PartitionRef single = selector(parse(prefix + " FROM db.t PARTITION(p1)", mode));
                assertEquals(List.of("p1"), single.getPartitionNames());
                assertFalse(single.isTemp());
                PartitionRef multiple = selector(parse(prefix + " FROM db.t PARTITIONS(p1,p2)", mode));
                assertEquals(List.of("p1", "p2"), multiple.getPartitionNames());
                PartitionRef temporary = selector(parse(prefix + " FROM db.t TEMPORARY PARTITION(p1)", mode));
                assertEquals(List.of("p1"), temporary.getPartitionNames());
                assertTrue(temporary.isTemp());
            }
        }
    }

    @Test
    void malformedSuffixPrecedesUnsupportedSelectorDiagnostic() {
        for (PredictionMode mode : MODES) {
            for (String prefix : PREFIXES) {
                for (String value : List.of("1", "DATE 'bad'", "INTERVAL IF(true,1,2) DAY")) {
                    ParsingException error = assertThrows(ParsingException.class,
                            () -> parse(prefix + " FROM db.t PARTITION(k=" + value + "),", mode));
                    assertFalse(error.getMessage().contains("Key partition selectors are not supported"));
                    assertFalse(error.getMessage().contains("Invalid date literal"));
                }
            }
        }
    }
}
