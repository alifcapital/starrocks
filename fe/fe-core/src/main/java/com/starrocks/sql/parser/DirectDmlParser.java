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

import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ColumnAssignment;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRef;
import com.starrocks.sql.ast.UpdateStmt;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.ast.expression.Expr;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Token;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

import static com.starrocks.sql.parser.ErrorMsgProxy.PARSER_ERROR_MSG;
import static com.starrocks.sql.parser.FrozenTokenCatalog.AS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BACKQUOTED_IDENTIFIER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BINARY_DOUBLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BINARY_SINGLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BLACKHOLE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.BY;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DATE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DATETIME;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DECIMAL_VALUE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DELETE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DOT_IDENTIFIER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DOUBLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DOUBLE_VALUE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.DUAL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FALSE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FILES;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FOR;
import static com.starrocks.sql.parser.FrozenTokenCatalog.FROM;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INSERT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INTEGER_VALUE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INTERVAL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.INTO;
import static com.starrocks.sql.parser.FrozenTokenCatalog.LABEL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.NAME;
import static com.starrocks.sql.parser.FrozenTokenCatalog.NULL;
import static com.starrocks.sql.parser.FrozenTokenCatalog.OF;
import static com.starrocks.sql.parser.FrozenTokenCatalog.OPEN;
import static com.starrocks.sql.parser.FrozenTokenCatalog.OVERWRITE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PARAMETER;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PARTITION;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PARTITIONS;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PIVOT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.PROPERTIES;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SET;
import static com.starrocks.sql.parser.FrozenTokenCatalog.SINGLE_QUOTED_TEXT;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TEMPORARY;
import static com.starrocks.sql.parser.FrozenTokenCatalog.TRUE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.UPDATE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.USING;
import static com.starrocks.sql.parser.FrozenTokenCatalog.VALUES;
import static com.starrocks.sql.parser.FrozenTokenCatalog.VERSION;
import static com.starrocks.sql.parser.FrozenTokenCatalog.WHERE;
import static com.starrocks.sql.parser.FrozenTokenCatalog.WITH;

/** DML descriptors; all query/relation/expression grammar belongs to DirectQueryParser. */
final class DirectDmlParser {
    private final DirectQueryParser owner;
    private final CommonTokenStream tokens;
    private final boolean insensitive;
    private static final boolean[] IDENTIFIERS = QueryIdentifiers.flags();
    private static final int OPEN = FrozenTokenCatalog.T_2;
    private static final int CLOSE = FrozenTokenCatalog.T_3;
    private static final int COMMA = FrozenTokenCatalog.T_0;
    private static final int DOT = FrozenTokenCatalog.T_1;
    private static final int EQUAL = FrozenTokenCatalog.EQ;

    DirectDmlParser(DirectQueryParser owner, CommonTokenStream tokens, boolean insensitive) {
        this.owner = owner;
        this.tokens = tokens;
        this.insensitive = insensitive;
    }

    private int type() {
        return tokens.LA(1);
    }

    private Token take() {
        Token token = tokens.LT(1);
        if (type() == Token.EOF) {
            throw unsupported("EOF");
        }
        tokens.consume();
        return token;
    }

    private boolean eat(int type) {
        if (type() != type) {
            return false;
        }
        take();
        return true;
    }

    private void expect(int type) {
        if (!eat(type)) {
            throw unsupported("expected " + FrozenTokenNames.lexerDisplayName(type));
        }
    }

    private DirectQueryParser.UnsupportedQuery unsupported(String reason) {
        return new DirectQueryParser.UnsupportedQuery("DML " + reason + " at " + tokens.index());
    }

    private NodePosition pos(int start) {
        return new NodePosition(tokens.get(start), tokens.LT(-1));
    }

    private String identifier() {
        int type = type();
        if (type < 0 || type >= IDENTIFIERS.length || !IDENTIFIERS[type]) {
            throw unsupported("identifier");
        }
        String raw = take().getText();
        return type == BACKQUOTED_IDENTIFIER ? QueryIdentifiers.decodeBackQuoted(raw) : raw;
    }

    private QualifiedName name() {
        int start = tokens.index();
        List<String> parts = new ArrayList<>();
        parts.add(identifier());
        while (type() == DOT || type() == DOT_IDENTIFIER) {
            if (eat(DOT)) {
                parts.add(identifier());
            } else {
                parts.add(take().getText().substring(1));
            }
        }
        if (insensitive) {
            parts.replaceAll(x -> x.toLowerCase(Locale.ROOT));
        }
        return QualifiedName.of(parts, pos(start));
    }

    private boolean partitions() {
        return type() == PARTITION || type() == PARTITIONS || type() == TEMPORARY;
    }

    StatementBase root(int start, List<CTERelation> ctes, StatementBase.ExplainLevel explain) {
        int keyword = tokens.index();
        boolean update = eat(UPDATE);
        if (!update) {
            expect(DELETE);
            expect(FROM);
        }
        QualifiedName name = name();
        PartitionRef partition = !update && partitions() ? owner.showPartitionNames() : null;
        StatementBase statement;
        if (update) {
            expect(SET);
            List<ColumnAssignment> assignments = new ArrayList<>();
            do {
                int assignmentStart = tokens.index();
                String column = identifier();
                expect(EQUAL);
                Expr value = owner.dmlDefault();
                assignments.add(new ColumnAssignment(column, value, pos(assignmentStart)));
            } while (eat(COMMA));
            List<Relation> from = null;
            if (eat(FROM)) {
                int fromStart = tokens.LT(-1).getTokenIndex();
                if (eat(DUAL)) {
                    from = new ArrayList<>(List.of(ValuesRelation.newDualRelation(pos(fromStart))));
                } else {
                    from = owner.dmlRelations();
                    // AstBuilder rejects PIVOT here, so ANTLR reports the error.
                    if (type() == PIVOT) {
                        throw unsupported("UPDATE FROM PIVOT");
                    }
                }
            }
            Expr where = eat(WHERE) ? owner.expression() : null;
            NodePosition position = pos(start);
            statement =
                    new UpdateStmt(
                            new TableRef(name, null, position),
                            assignments,
                            from,
                            where,
                            ctes,
                            position);
        } else {
            List<Relation> using = eat(USING) ? owner.dmlRelations() : null;
            Expr where = eat(WHERE) ? owner.expression() : null;
            NodePosition position = pos(start);
            statement =
                    new DeleteStmt(
                            new TableRef(name, partition, position),
                            partition,
                            using,
                            where,
                            ctes,
                            position);
        }
        owner.dmlHints(statement, keyword, true);
        return statement;
    }

    static void applyRootExplain(StatementBase statement, StatementBase.ExplainLevel explain) {
        if (explain != null) {
            statement.setIsExplain(true, explain);
            if (explain == StatementBase.ExplainLevel.ANALYZE) {
                throw new ParsingException(PARSER_ERROR_MSG.unsupportedOp("analyze"));
            }
        }
    }

    private record Span(int begin, int end) {}

    private boolean stringType() {
        return type() == SINGLE_QUOTED_TEXT || type() == DOUBLE_QUOTED_TEXT;
    }

    private Span propertySpan() {
        int begin = tokens.index();
        expect(OPEN);
        do {
            if (!stringType()) {
                throw unsupported("property key");
            }
            take();
            expect(EQUAL);
            if (!stringType()) {
                throw unsupported("property value");
            }
            take();
        } while (eat(COMMA));
        expect(CLOSE);
        return new Span(begin, tokens.index());
    }

    private Span partitionSpan() {
        int begin = tokens.index();
        boolean temporary = eat(TEMPORARY);
        boolean singular = eat(PARTITION);
        if (!singular) {
            expect(PARTITIONS);
        }
        boolean wrapped = eat(OPEN);
        if (singular && !temporary && wrapped && tokens.LA(2) == EQUAL) {
            do {
                identifier();
                expect(EQUAL);
                keyLiteralSpan();
            } while (eat(COMMA));
            expect(CLOSE);
            return new Span(begin, tokens.index());
        }
        do {
            if (stringType()) {
                take();
            } else {
                identifier();
            }
        } while (eat(COMMA));
        if (wrapped) {
            expect(CLOSE);
        }
        return new Span(begin, tokens.index());
    }

    /** literalExpression syntax only: materialize after source query, never generalLiteralExpression. */
    private void keyLiteralSpan() {
        int token = type();
        if (token == INTERVAL) {
            // The value is an expression, so only the expression parser can find where it ends.
            owner.skipIntervalLiteral();
            return;
        }
        if (token == DATE || token == DATETIME) {
            take();
            if (!stringType()) {
                throw unsupported("key DATE/DATETIME string");
            }
            take();
            return;
        }
        if (token == NULL
                || token == TRUE
                || token == FALSE
                || token == INTEGER_VALUE
                || token == DECIMAL_VALUE
                || token == DOUBLE_VALUE
                || token == SINGLE_QUOTED_TEXT
                || token == DOUBLE_QUOTED_TEXT
                || token == BINARY_SINGLE_QUOTED_TEXT
                || token == BINARY_DOUBLE_QUOTED_TEXT
                || token == PARAMETER) {
            take();
            return;
        }
        throw unsupported("key literal syntax");
    }

    private record Descriptor(
            String kind, String label, List<String> columns, NodePosition position) {}

    StatementBase insert(int start, StatementBase.ExplainLevel explain) {
        expect(INSERT);
        boolean overwrite = eat(OVERWRITE);
        if (!overwrite) {
            expect(INTO);
        }
        QualifiedName target = null;
        Span files = null;
        boolean blackhole = false;
        if (eat(FILES)) {
            files = propertySpan();
        } else if (eat(BLACKHOLE)) {
            expect(OPEN);
            expect(CLOSE);
            blackhole = true;
        } else {
            target = name();
        }
        String branch = null;
        if (target != null && (type() == FOR || type() == VERSION)) {
            eat(FOR);
            expect(VERSION);
            expect(AS);
            expect(OF);
            branch = identifier();
        }
        // Target partition expressions are visited after the source query in AstBuilder.
        Span partitionSpan = target != null && partitions() ? partitionSpan() : null;
        List<Descriptor> descriptors = new ArrayList<>();
        while ((type() == WITH && tokens.LA(2) == LABEL)
                || (type() == OPEN && !owner.dmlQueryInParentheses())
                || type() == BY) {
            int clause = tokens.index();
            if (eat(WITH)) {
                expect(LABEL);
                String label = identifier();
                descriptors.add(new Descriptor("label", label, null, pos(clause)));
            } else if (eat(BY)) {
                expect(NAME);
                descriptors.add(new Descriptor("name", null, null, pos(clause)));
            } else {
                expect(OPEN);
                List<String> columns = new ArrayList<>();
                columns.add(identifier().toLowerCase(Locale.ROOT));
                while (eat(COMMA)) {
                    columns.add(identifier().toLowerCase(Locale.ROOT));
                }
                expect(CLOSE);
                descriptors.add(new Descriptor("columns", null, columns, pos(clause)));
            }
        }
        // Properties are held until the source has constructed its descendants.
        Span properties = null;
        if (eat(PROPERTIES)) {
            properties = propertySpan();
        }
        QueryStatement query;
        if (eat(VALUES)) {
            int valuesStart = tokens.LT(-1).getTokenIndex();
            List<List<Expr>> rows = new ArrayList<>();
            do {
                expect(OPEN);
                List<Expr> row = new ArrayList<>();
                row.add(owner.dmlDefault());
                while (eat(COMMA)) {
                    row.add(owner.dmlDefault());
                }
                expect(CLOSE);
                rows.add(row);
            } while (eat(COMMA));
            List<String> names = new ArrayList<>();
            for (int i = 0; i < rows.get(0).size(); i++) {
                names.add("column_" + i);
            }
            query = new QueryStatement(new ValuesRelation(rows, names, pos(valuesStart)));
        } else {
            query = owner.insertSourceQuery();
        }
        if (explain != null) {
            query.setIsExplain(true, explain);
        }
        // AstBuilder rejects these clauses for FILES() and BLACKHOLE(), so ANTLR reports the error.
        if ((files != null || blackhole) && (overwrite || properties != null
                || descriptors.stream().anyMatch(descriptor -> !descriptor.kind().equals("label")))) {
            throw unsupported("FILES() or BLACKHOLE() clause");
        }
        if (blackhole) {
            InsertStmt result = new InsertStmt(query, pos(start));
            owner.dmlHints(result, start, false);
            return result;
        }
        if (files != null) {
            InsertStmt result =
                    new InsertStmt(
                            owner.dmlReplayProperties(files.begin(), files.end(), true),
                            query,
                            pos(start));
            owner.dmlHints(result, start, true);
            return result;
        }
        PartitionRef partition =
                partitionSpan == null
                        ? null
                        : owner.dmlReplayPartitions(partitionSpan.begin(), partitionSpan.end());
        String label = null;
        List<String> columns = null;
        boolean byName = false;
        for (Descriptor descriptor : descriptors) {
            switch (descriptor.kind()) {
                case "label" -> {
                    if (label != null) {
                        throw new ParsingException(
                                PARSER_ERROR_MSG.duplicatedClause("WITH LABEL", "insert"),
                                descriptor.position());
                    }
                    label = descriptor.label();
                }
                case "columns" -> {
                    if (columns != null) {
                        throw new ParsingException(
                                PARSER_ERROR_MSG.duplicatedClause("COLUMN LIST", "insert"),
                                descriptor.position());
                    }
                    if (byName) {
                        throw new ParsingException(
                                "Cannot use COLUMN LIST and BY NAME clause together in insert");
                    }
                    columns = descriptor.columns();
                }
                case "name" -> {
                    if (byName) {
                        throw new ParsingException(
                                PARSER_ERROR_MSG.duplicatedClause("BY NAME", "insert"),
                                descriptor.position());
                    }
                    if (columns != null) {
                        throw new ParsingException(
                                "Cannot use COLUMN LIST and BY NAME clause together in insert");
                    }
                    byName = true;
                }
                default -> throw new AssertionError();
            }
        }
        NodePosition position = pos(start);
        InsertStmt result =
                new InsertStmt(
                        new TableRef(target, partition, position),
                        partition,
                        label,
                        columns,
                        query,
                        overwrite,
                        properties == null
                                ? new HashMap<>()
                                : owner.dmlReplayProperties(
                                        properties.begin(), properties.end(), false),
                        position);
        result.setTargetBranch(branch);
        result.setColumnMatchPolicy(
                byName ? InsertStmt.ColumnMatchPolicy.NAME : InsertStmt.ColumnMatchPolicy.POSITION);
        owner.dmlHints(result, start, true);
        return result;
    }
}
