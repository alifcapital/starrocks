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

package com.starrocks.sql.optimizer.statistics;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.GroupingFunctionCallExpr;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.SubfieldExpr;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ReplaceColumnRefRewriter;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.StatisticUtils;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * An expression that statistics are collected on, analyzed against the columns of a table: the canonical expression
 * over placeholder columns, its SQL text, its result type and the columns it reads. The text identifies the statistic,
 * the placeholders are replaced by the columns of a scan when the planner uses the statistic.
 */
public final class ExpressionTemplate {
    private static final Logger LOG = LogManager.getLogger(ExpressionTemplate.class);
    /** The version of the text format; a stored statistic of another version is not used. */
    public static final int FORMAT_VERSION = 1;
    // An element of an MCV group is named by its text: the column name, or this prefix and the canonical text.
    // The prefix keeps the digest of a group with an expression apart from a group of a column with that name.
    private static final String ELEMENT_PREFIX = "expr:";
    private static final String CURRENT_ELEMENT_PREFIX = ELEMENT_PREFIX + FORMAT_VERSION + ":";
    // The keys of an expression element in the column_names JSON of the statistics table.
    public static final String JSON_EXPR = "expr";
    public static final String JSON_FORMAT = "format";
    public static final String JSON_TYPE = "type";
    public static final String JSON_COLUMNS = "columns";
    public static final String JSON_NAME = "name";
    public static final String JSON_FIELD_ID = "fieldId";

    public static String elementText(String canonicalText) {
        return CURRENT_ELEMENT_PREFIX + canonicalText;
    }

    public static boolean isExpressionElement(String element) {
        return element.startsWith(ELEMENT_PREFIX);
    }

    /** Whether a column name reads as an expression element in any case: the key of a column name is lower case. */
    public static boolean looksLikeExpressionElement(String name) {
        return name.toLowerCase(Locale.ROOT).startsWith(ELEMENT_PREFIX);
    }

    /**
     * Refuses MCV statistics on a column whose name looks like an expression element: everywhere such a name is
     * taken for an expression, and its group would clash with the group of that expression.
     */
    public static void checkColumnName(String name) {
        if (looksLikeExpressionElement(name)) {
            throw new SemanticException("Can't collect MCV statistics on column %s: a name that starts with %s "
                    + "names statistics on an expression", name, ELEMENT_PREFIX);
        }
    }

    /** The canonical text of an expression element of the current format, null for anything else. */
    public static String canonicalTextOf(String element) {
        return element.startsWith(CURRENT_ELEMENT_PREFIX) ? element.substring(CURRENT_ELEMENT_PREFIX.length()) : null;
    }

    /** What SHOW prints for an element: the column name or the canonical text of the expression. */
    public static String displayText(String element) {
        String text = canonicalTextOf(element);
        return text == null ? element : text;
    }

    /**
     * What SHOW prints for the elements of a group. The text of an expression holds commas, so a group with an
     * expression is printed as SHOW MCV STATS META prints it, a bracketed list; a group of columns as before.
     */
    public static String displayElements(java.util.List<String> elements) {
        if (elements.stream().noneMatch(ExpressionTemplate::isExpressionElement)) {
            return String.join(",", elements);
        }
        return elements.stream().map(ExpressionTemplate::displayText).toList().toString();
    }

    /**
     * The key form of an element: column names are case-insensitive, expression texts are not, because '%Y' and
     * '%y' are different expressions.
     */
    public static String keyText(String element) {
        return isExpressionElement(element) ? element : element.toLowerCase(Locale.ROOT);
    }

    /** A column the expression reads, with the type and the Iceberg field id it had when the text was analyzed. */
    public static final class Source {
        private final String name;
        private final Type type;
        private final Integer fieldId;

        public Source(String name, Type type, Integer fieldId) {
            this.name = name;
            this.type = type;
            this.fieldId = fieldId;
        }

        public String getName() {
            return name;
        }

        public Type getType() {
            return type;
        }

        public Integer getFieldId() {
            return fieldId;
        }
    }

    private final String text;
    private final ScalarOperator canonical;
    private final Map<String, ColumnRefOperator> placeholders;
    private final List<Source> sources;

    private ExpressionTemplate(String text, ScalarOperator canonical, Map<String, ColumnRefOperator> placeholders,
                               List<Source> sources) {
        this.text = text;
        this.canonical = canonical;
        this.placeholders = placeholders;
        this.sources = List.copyOf(sources);
    }

    public String getText() {
        return text;
    }

    public Type getType() {
        return canonical.getType();
    }

    public List<Source> getSources() {
        return sources;
    }

    /** The canonical expression over placeholder columns, one per source, named by the source. */
    public ScalarOperator getCanonical() {
        return canonical;
    }

    /**
     * The canonical expression over the given columns, found by the lower-case name of each source; null when a
     * source has no column.
     */
    public ScalarOperator instantiate(Map<String, ColumnRefOperator> columnsByLowerCaseName) {
        Map<ColumnRefOperator, ScalarOperator> replacement = new HashMap<>();
        for (Map.Entry<String, ColumnRefOperator> placeholder : placeholders.entrySet()) {
            ColumnRefOperator column = columnsByLowerCaseName.get(placeholder.getKey());
            if (column == null) {
                return null;
            }
            replacement.put(placeholder.getValue(), column);
        }
        return new ReplaceColumnRefRewriter(replacement).rewrite(canonical);
    }

    /** Parses and analyzes a stored text against the current columns of the table. */
    public static ExpressionTemplate parse(String text, Table table, ConnectContext session) {
        Expr expr = SqlParser.parseSqlToExpr(text, SqlModeHelper.MODE_DEFAULT);
        return analyze(expr, table, session);
    }

    /**
     * Analyzes an expression against the columns of the table in the session of the caller, so the caller needs
     * the privileges a query with the expression needs. Rejects an expression without columns, with an aggregate, a
     * window, a subquery or a lambda, with a non-deterministic function, or with a result type the statistics do not
     * store.
     */
    public static ExpressionTemplate analyze(Expr expr, Table table, ConnectContext session) {
        checkStructure(expr);
        // Columns resolve by name; the table name also allows a column qualified by it.
        TableName tableName = new TableName(null, null, table.getName());
        List<Field> fields = new ArrayList<>();
        for (Column column : table.getBaseSchema()) {
            fields.add(new Field(column.getName(), column.getType(), tableName, null));
        }
        ExpressionAnalyzer.analyzeExpression(expr, new AnalyzeState(),
                new Scope(RelationId.anonymous(), new RelationFields(fields)), session);
        checkFunctions(expr);

        Map<String, ColumnRefOperator> placeholders = new LinkedHashMap<>();
        List<Source> sources = new ArrayList<>();
        ScalarOperator translated = SqlToScalarOperatorTranslator.translateWithSlotRef(expr, slot -> {
            Column column = table.getColumn(slot.getColumnName());
            if (column == null) {
                return null;
            }
            String key = column.getName().toLowerCase(Locale.ROOT);
            return placeholders.computeIfAbsent(key, ignored -> {
                sources.add(new Source(column.getName(), column.getType(), fieldId(table, column.getName())));
                return new ColumnRefOperator(placeholders.size() + 1, column.getType(), column.getName(),
                        column.isAllowNull());
            });
        });
        if (placeholders.isEmpty()) {
            throw new SemanticException("MCV expression must read at least one column: %s",
                    ExpressionStatisticsSqlPrinter.printOrNull(translated));
        }
        // A query expression gets the scalar rewrite of the translation, which writes 5 < c as c > 5 and BETWEEN as
        // two comparisons, and casts constants to the type of the column; the expression of the statistics gets it
        // too, so both reach the same canonical form.
        translated = new ScalarOperatorRewriter().rewrite(translated, ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        ScalarOperator canonical = ExpressionStatisticsCanonicalizer.canonicalize(translated);
        if (canonical instanceof ColumnRefOperator) {
            // The same values as the column: they would duplicate MCV(c) under another name and bypass the choice of
            // the source of the conditions on c.
            throw new SemanticException("MCV expression %s is the column %s, use MCV(%s)",
                    ExpressionStatisticsSqlPrinter.printOrNull(translated), ((ColumnRefOperator) canonical).getName(),
                    ((ColumnRefOperator) canonical).getName());
        }
        if (!isMcvType(canonical.getType())) {
            throw new SemanticException("Can't collect MCV statistics on expression type %s",
                    canonical.getType().toSql());
        }
        if (ExpressionStatisticsCanonicalizer.keyOf(canonical) == null) {
            // The planner matches a condition with the statistics by this key: without it they never apply.
            throw new SemanticException("MCV expression %s is not supported: its form cannot be matched with a query",
                    ExpressionStatisticsSqlPrinter.printOrNull(canonical));
        }
        String text = ExpressionStatisticsSqlPrinter.print(canonical);
        return new ExpressionTemplate(text, canonical, placeholders, sources);
    }

    /**
     * Analyzes an expression that statistics are collected on. The stored text must load again: its parse and
     * analysis must give the same text, and the same canonical structure, which the planner matches. A text can
     * analyze with other types: cast(s AS BIGINT) % 7 for a SMALLINT s prints as `s` % 7.
     */
    public static ExpressionTemplate analyzeStable(Expr expr, Table table, ConnectContext session) {
        // The rewrite of the template reads session variables from the context of the thread. The collection and
        // the cache load analyze the text with the global values, so ANALYZE does too: a text built with the
        // values of one session would analyze to another text there. The session of the caller checks the
        // privileges.
        ConnectContext previous = ConnectContext.get();
        ConnectContext statistics = StatisticUtils.buildConnectContext();
        statistics.setThreadLocalInfo();
        try {
            ExpressionTemplate template = analyze(expr, table, session);
            ExpressionTemplate reloaded = parse(template.getText(), table, statistics);
            if (!reloaded.getText().equals(template.getText())) {
                throw new SemanticException("MCV expression %s has no stable text: it loads as %s",
                        template.getText(), reloaded.getText());
            }
            if (!java.util.Objects.equals(template.comparisonKey(), reloaded.comparisonKey())) {
                throw new SemanticException("MCV expression %s has no stable canonical form: its text analyzes "
                        + "with other types", template.getText());
            }
            return template;
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            } else {
                ConnectContext.remove();
            }
        }
    }

    // The key of the expression over columns that are the same object for the same column name.
    private ExpressionStatisticsCanonicalizer.Key comparisonKey() {
        Map<String, ColumnRefOperator> columns = new HashMap<>();
        List<Source> sorted = new ArrayList<>(sources);
        sorted.sort(java.util.Comparator.comparing(source -> source.getName().toLowerCase(Locale.ROOT)));
        for (int i = 0; i < sorted.size(); i++) {
            Source source = sorted.get(i);
            columns.put(source.getName().toLowerCase(Locale.ROOT),
                    new ColumnRefOperator(i + 1, source.getType(), source.getName(), true));
        }
        ScalarOperator instantiated = instantiate(columns);
        return instantiated == null ? null : ExpressionStatisticsCanonicalizer.keyOf(instantiated);
    }

    /** Whether the statistics can store values of the type, the same rule as for an MCV column. */
    public static boolean isMcvType(Type type) {
        return type.canStatistic() && !type.isComplexType() && !type.isJsonType() && !type.isOnlyMetricType();
    }

    /** The Iceberg field id of a column, null for other tables or when the schema cannot be read. */
    public static Integer fieldId(Table table, String columnName) {
        if (!(table instanceof IcebergTable)) {
            return null;
        }
        try {
            // The names of the table columns can differ in case from the Iceberg schema.
            org.apache.iceberg.types.Types.NestedField field =
                    ((IcebergTable) table).getNativeTable().schema().caseInsensitiveFindField(columnName);
            return field == null ? null : field.fieldId();
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Whether a column of a table still has the type and the field id the source had. */
    public static boolean sameSource(Source source, Table table, Column column) {
        return column != null && column.getType().toSql().equals(source.getType().toSql())
                && fieldIdMatches(table, column.getName(), source.getFieldId());
    }

    /** The Iceberg schema of a table could not be read, so a recorded field id cannot be compared now. */
    public static final class FieldIdUnavailableException extends RuntimeException {
        public FieldIdUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    // A failed read of the schema repeats for every scan and source; we warn once a minute and log the rest at
    // DEBUG, so the log is not flooded.
    private static final long WARN_INTERVAL_MS = 60_000;
    private static final java.util.concurrent.atomic.AtomicLong LAST_WARN = new java.util.concurrent.atomic.AtomicLong();

    /**
     * Whether a column still has the recorded Iceberg field id. Without a recorded field id only the type is
     * compared. With one, a field id that the table cannot give now cannot show that the column is the same, so
     * the statistics are not used.
     */
    public static boolean fieldIdMatches(Table table, String columnName, Integer recorded) {
        try {
            return fieldIdMatchesOrThrow(table, columnName, recorded);
        } catch (FieldIdUnavailableException e) {
            long now = System.currentTimeMillis();
            long last = LAST_WARN.get();
            if (now - last >= WARN_INTERVAL_MS && LAST_WARN.compareAndSet(last, now)) {
                LOG.warn("{}, the MCV statistics on it are not used", e.getMessage(), e);
            } else {
                LOG.debug("{}, the MCV statistics on it are not used", e.getMessage(), e);
            }
            return false;
        }
    }

    /**
     * Whether a column still has the recorded Iceberg field id. A failed read of the schema throws
     * {@link FieldIdUnavailableException}, so the cache load fails and is retried instead of keeping the statistics
     * out until the entry expires.
     */
    public static boolean fieldIdMatchesOrThrow(Table table, String columnName, Integer recorded) {
        if (recorded == null) {
            return true;
        }
        if (!(table instanceof IcebergTable)) {
            return false;
        }
        org.apache.iceberg.types.Types.NestedField field;
        try {
            field = ((IcebergTable) table).getNativeTable().schema().caseInsensitiveFindField(columnName);
        } catch (RuntimeException e) {
            throw new FieldIdUnavailableException("Cannot read the field id of column " + columnName + " of table "
                    + table.getName(), e);
        }
        return field != null && recorded.equals(field.fieldId());
    }

    private static void checkStructure(Expr expr) {
        if (expr instanceof Subquery) {
            throw new SemanticException("MCV expression must not contain a subquery");
        }
        if (expr instanceof AnalyticExpr) {
            throw new SemanticException("MCV expression must not contain a window function");
        }
        if (expr instanceof LambdaFunctionExpr) {
            throw new SemanticException("MCV expression must not contain a lambda function");
        }
        if (expr instanceof SubfieldExpr) {
            throw new SemanticException("MCV expression must read top-level columns");
        }
        for (Expr child : expr.getChildren()) {
            checkStructure(child);
        }
    }

    private static void checkFunctions(Expr expr) {
        if (expr instanceof GroupingFunctionCallExpr
                || expr instanceof FunctionCallExpr && ((FunctionCallExpr) expr).isAggregateFunction()) {
            throw new SemanticException("MCV expression must not contain an aggregate function");
        }
        if (expr instanceof FunctionCallExpr) {
            String name = ((FunctionCallExpr) expr).getFunctionName();
            String lowerName = name == null ? "" : name.toLowerCase(Locale.ROOT);
            // A time function such as now() changes from run to run; with a column argument it is deterministic.
            if (FunctionSet.nonDeterministicFunctions.contains(lowerName)
                    || FunctionSet.nonDeterministicTimeFunctions.contains(lowerName) && !readsColumn(expr)) {
                throw new SemanticException("MCV expression must be deterministic: %s", name);
            }
        }
        if (expr instanceof SlotRef && expr.getType() != null && !expr.getType().isScalarType()) {
            throw new SemanticException("MCV expression must read scalar columns");
        }
        for (Expr child : expr.getChildren()) {
            checkFunctions(child);
        }
    }

    private static boolean readsColumn(Expr expr) {
        if (expr instanceof SlotRef) {
            return true;
        }
        for (Expr child : expr.getChildren()) {
            if (readsColumn(child)) {
                return true;
            }
        }
        return false;
    }
}
