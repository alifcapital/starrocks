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

package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.qe.SessionVariable;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.DictMappingOperator;
import com.starrocks.sql.optimizer.operator.scalar.LambdaFunctionOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorUtil;
import com.starrocks.sql.optimizer.rewrite.BaseScalarOperatorShuttle;
import com.starrocks.sql.optimizer.task.TaskContext;
import com.starrocks.type.JsonType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Rewrites the extraction of constant paths from JSON documents stored as text, so that the documents of one
 * place of an operator are parsed once.
 *
 * <p>An extraction is a consumer call with a constant, simple path over a source that is a VARCHAR or CHAR
 * expression v, parse_json(v) or CAST(v AS JSON). The extractions of one place (the projection, the predicate
 * or the join ON predicate of an operator) that are evaluated on every row are grouped by v. An extraction that is
 * evaluated on some rows only (a branch of CASE, IF, IFNULL or COALESCE, an operand of AND or OR after the first)
 * joins the group of its text when the text has one, and otherwise stays as it is. The extractions over a text
 * whose parse_json(v) or CAST(v AS JSON) the place also uses as a JSON value, or that an OLAP scan reads in a
 * heavy expression it computes itself, stay as they are. In a conjunct that an OLAP scan evaluates in the storage
 * layer every extraction forms a group of its own. A group with one path keeps a get_json_* call as
 * get_json_*(v, p) and turns json_query, json_exists and json_length into C(json_extract_paths(v, p), '$.0'); a
 * group with more paths turns every call into C(json_extract_paths(v, p1, ..., pn), '$.i'). BE computes every form
 * with the result of the consumer over parse_json(v), so the rewrite does not change any value.
 *
 * <p>We run before ScalarOperatorsReuseRule: projections and predicates are merged and pushed down, and parse_json
 * is not yet moved into a common slot. The common sub-expression elimination then computes one
 * json_extract_paths() per projection or FILTER/JOIN predicate, and BE shares it between the conjuncts of a scan
 * that it evaluates over the chunk.
 */
public class JsonPathExtractRule implements TreeRewriteRule {
    private static final Logger LOG = LogManager.getLogger(JsonPathExtractRule.class);

    private static final Set<String> TEXT_CONSUMERS = Set.of(FunctionSet.GET_JSON_STRING, FunctionSet.GET_JSON_OBJECT,
            FunctionSet.GET_JSON_INT, FunctionSet.GET_JSON_DOUBLE, FunctionSet.GET_JSON_BOOL);
    private static final Set<String> JSON_CONSUMERS =
            Set.of(FunctionSet.JSON_QUERY, FunctionSet.JSON_EXISTS, FunctionSet.JSON_LENGTH);
    private static final Set<String> LAZY_FUNCTIONS = Set.of(FunctionSet.IF, FunctionSet.IFNULL, FunctionSet.COALESCE);

    // One piece of a path between dots, as JsonPathPiece::parse() in BE matches it: a key and array selectors.
    private static final Pattern PATH_PIECE = Pattern.compile("([^\"\\[\\]]*)((?:\\[[0-9:*]+\\])*)");
    private static final Pattern ARRAY_SELECTOR = Pattern.compile("\\[([0-9:*]+)\\]");

    // Tests set it to make the rewrite of every operator fail.
    @VisibleForTesting
    public static volatile boolean failForTest = false;

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
        if (!sessionVariable.isEnableJsonPathRewrite()) {
            return root;
        }
        boolean strict = SqlModeHelper.check(sessionVariable.getSqlMode(), SqlModeHelper.MODE_ALLOW_THROW_EXCEPTION);
        boolean columnExprPredicates = sessionVariable.isEnableColumnExprPredicate();
        try {
            visit(root, strict, sessionVariable.isPushDownHeavyExprs(), columnExprPredicates, taskContext);
        } catch (Exception e) {
            LOG.warn("Stop the JSON path rewrite in query {}", taskContext.getOptimizerContext().getQueryId(), e);
        }
        return root;
    }

    private void visit(OptExpression optExpression, boolean strict, boolean pushDownHeavyExprs,
                       boolean columnExprPredicates, TaskContext taskContext) {
        try {
            rewriteOperator(optExpression.getOp(), strict, pushDownHeavyExprs, columnExprPredicates);
        } catch (Exception e) {
            // The rewrite only saves work, so a failure must not fail the query. The operator keeps its
            // expressions: the new ones are set only after all of them are built.
            LOG.warn("Skip the JSON path rewrite of {} in query {}", optExpression.getOp().getOpType(),
                    taskContext.getOptimizerContext().getQueryId(), e);
        }
        for (OptExpression input : optExpression.getInputs()) {
            visit(input, strict, pushDownHeavyExprs, columnExprPredicates, taskContext);
        }
    }

    private void rewriteOperator(Operator operator, boolean strict, boolean pushDownHeavyExprs,
                                 boolean columnExprPredicates) {
        if (failForTest) {
            throw new IllegalStateException("JSON path rewrite failure injected by a test");
        }
        Projection projection = operator.getProjection();
        Projection newProjection = null;
        if (projection != null && !projection.getColumnRefMap().isEmpty()) {
            List<ColumnRefOperator> columns = new ArrayList<>(projection.getColumnRefMap().keySet());
            List<ScalarOperator> expressions = new ArrayList<>(projection.getColumnRefMap().values());
            Set<ScalarOperator> excluded = operator instanceof PhysicalOlapScanOperator && pushDownHeavyExprs
                    ? textsOfHeavyExpressions(expressions, strict) : Set.of();
            List<ScalarOperator> rewritten = rewritePlace(expressions, strict, false, excluded, false);
            if (rewritten != null) {
                newProjection = projection.deepClone();
                for (int i = 0; i < columns.size(); i++) {
                    newProjection.getColumnRefMap().put(columns.get(i), rewritten.get(i));
                }
            }
        }
        // The conjuncts of a predicate are evaluated one after another, and BE stops at a conjunct that rejects
        // every row of the chunk. The scan conjuncts that BE evaluates over the chunk share json_extract_paths()
        // through its conjunct cache, which computes it only when a conjunct runs, so they can be grouped. An OLAP
        // scan evaluates some conjuncts in the storage layer instead, see rewriteOlapScanPredicate(). The predicates
        // of FILTER and JOIN get common sub-expressions that are computed before the first conjunct, so there every
        // conjunct but the first one counts as evaluated on some rows only.
        boolean eagerConjuncts = operator.getOpType() != OperatorType.PHYSICAL_FILTER
                && operator.getOpType() != OperatorType.PHYSICAL_HASH_JOIN
                && operator.getOpType() != OperatorType.PHYSICAL_NESTLOOP_JOIN;
        ScalarOperator newPredicate = operator instanceof PhysicalOlapScanOperator && columnExprPredicates
                ? rewriteOlapScanPredicate(operator.getPredicate(), strict)
                : rewriteOne(operator.getPredicate(), strict, eagerConjuncts);
        ScalarOperator newOnPredicate = null;
        if (operator instanceof PhysicalJoinOperator join) {
            newOnPredicate = rewriteOne(join.getOnPredicate(), strict, false);
        }

        if (newProjection != null) {
            operator.setProjection(newProjection);
        }
        if (newPredicate != null) {
            operator.setPredicate(newPredicate);
        }
        if (newOnPredicate != null) {
            ((PhysicalJoinOperator) operator).setOnPredicate(newOnPredicate);
        }
    }

    // Returns the rewritten expression, or null when nothing changes.
    private static ScalarOperator rewriteOne(ScalarOperator expression, boolean strict, boolean eagerConjuncts) {
        if (expression == null) {
            return null;
        }
        List<ScalarOperator> rewritten = rewritePlace(List.of(expression), strict, eagerConjuncts, Set.of(), false);
        return rewritten == null ? null : rewritten.get(0);
    }

    // With enable_column_expr_predicate, OlapScanConjunctsManager in BE moves a conjunct over one column of some
    // types, and an AND or OR whose operands it moves too, into the storage layer as column expression predicates. Each of them has
    // its own ExprContext, has no common sub-expressions and runs outside the conjunct cache, so a
    // json_extract_paths() of several paths there is computed once per call and costs more than the extractions it
    // stands for. We expect these conjuncts in the storage layer and rewrite every extraction in them alone, which
    // gives one cheaper parse for one parse. Only the other conjuncts of the scan are grouped. Returns the
    // rewritten predicate, or null when nothing changes.
    private static ScalarOperator rewriteOlapScanPredicate(ScalarOperator predicate, boolean strict) {
        if (predicate == null) {
            return null;
        }
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        List<ScalarOperator> result = new ArrayList<>(conjuncts);
        List<Integer> chunkConjuncts = new ArrayList<>();
        boolean changed = false;
        for (int i = 0; i < conjuncts.size(); i++) {
            if (!evaluatedInStorage(conjuncts.get(i), strict)) {
                chunkConjuncts.add(i);
                continue;
            }
            List<ScalarOperator> rewritten = rewritePlace(List.of(conjuncts.get(i)), strict, false, Set.of(), true);
            if (rewritten != null) {
                result.set(i, rewritten.get(0));
                changed = true;
            }
        }
        if (chunkConjuncts.size() == conjuncts.size()) {
            return rewriteOne(predicate, strict, true);
        }
        if (!chunkConjuncts.isEmpty()) {
            List<ScalarOperator> rewritten = rewritePlace(chunkConjuncts.stream().map(conjuncts::get).toList(),
                    strict, true, Set.of(), false);
            if (rewritten != null) {
                for (int j = 0; j < chunkConjuncts.size(); j++) {
                    result.set(chunkConjuncts.get(j), rewritten.get(j));
                }
                changed = true;
            }
        }
        return changed ? Utils.compoundAnd(result) : null;
    }

    // Whether BE may evaluate the conjunct as column expression predicates. BE checks more conditions than this
    // (the number of OR operands, enable_pushdown_or_predicate); a conjunct it keeps on the chunk then only misses
    // the grouping, which adds no parse. A conjunct over one column without extractions can also become an olap
    // filter, so only the type of a column that holds a document decides.
    private static boolean evaluatedInStorage(ScalarOperator conjunct, boolean strict) {
        if (conjunct.getUsedColumns().cardinality() <= 1) {
            List<ColumnRefOperator> columns = Utils.extractColumnRef(conjunct);
            boolean extracts = ScalarOperatorUtil.getStream(conjunct)
                    .anyMatch(e -> e instanceof CallOperator call && Extraction.of(call, strict) != null);
            return columns.isEmpty() || !extracts || hasColumnExprPredicates(columns.get(0).getType(), strict);
        }
        return conjunct instanceof CompoundPredicateOperator compound && !compound.isNot()
                && compound.getChildren().stream().allMatch(child -> evaluatedInStorage(child, strict));
    }

    // The column types of support_column_expr_predicate() in BE. OlapScanConjunctsManager skips a string column when
    // an expression may throw.
    private static boolean hasColumnExprPredicates(Type type, boolean strict) {
        if (type.isStructType()) {
            return true;
        }
        if (!type.isScalarType()) {
            return false;
        }
        return switch (type.getPrimitiveType()) {
            case BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT, DATE, DATETIME, TIME, BINARY, VARBINARY,
                 DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128, DECIMAL256 -> true;
            case CHAR, VARCHAR -> !strict;
            default -> false;
        };
    }

    // The texts read by the projection expressions over an OLAP scan that push_down_heavy_exprs moves into the scan
    // (PlanFragmentBuilder.containsHeavyExpr: a call whose name contains "regexp"). A moved expression inlines the
    // common sub-expressions it reads, so a json_extract_paths() it shares with the project would be computed in
    // both places, and twice where the moved expression reads it twice. We leave these texts as they are.
    private static Set<ScalarOperator> textsOfHeavyExpressions(List<ScalarOperator> expressions, boolean strict) {
        Set<ScalarOperator> texts = new HashSet<>();
        for (ScalarOperator expression : expressions) {
            if (ScalarOperatorUtil.getStream(expression).anyMatch(e -> e instanceof CallOperator call
                    && !call.getChildren().isEmpty() && call.getFnName().toLowerCase().contains("regexp"))) {
                ScalarOperatorUtil.getStream(expression).forEach(e -> {
                    Extraction extraction = e instanceof CallOperator call ? Extraction.of(call, strict) : null;
                    if (extraction != null) {
                        texts.add(extraction.source);
                    }
                    ScalarOperator parsed = parsedText(e);
                    if (parsed != null) {
                        texts.add(parsed);
                    }
                });
            }
        }
        return texts;
    }

    // The expressions of one place, rewritten; null when nothing changes.
    @VisibleForTesting
    public static List<ScalarOperator> rewritePlace(List<ScalarOperator> expressions, boolean strict) {
        return rewritePlace(expressions, strict, false, Set.of(), false);
    }

    // eagerConjuncts: the top-level conjuncts of the expressions are evaluated on every row. excluded: texts whose
    // extractions stay as they are. alone: every extraction, on every row or on some rows, forms a group of its own.
    private static List<ScalarOperator> rewritePlace(List<ScalarOperator> expressions, boolean strict,
                                                     boolean eagerConjuncts, Set<ScalarOperator> excluded,
                                                     boolean alone) {
        Map<ScalarOperator, Extraction> eager = new LinkedHashMap<>();
        Map<ScalarOperator, Extraction> lazy = new LinkedHashMap<>();
        Set<ScalarOperator> jsonValueSources = new HashSet<>(excluded);
        Set<ScalarOperator> conjunctions = Collections.newSetFromMap(new IdentityHashMap<>());
        for (ScalarOperator expression : expressions) {
            if (expression.getDepth() <= Config.max_scalar_operator_optimize_depth) {
                if (eagerConjuncts) {
                    addConjunctions(expression, conjunctions);
                }
                collect(expression, strict, false, conjunctions, eager, lazy);
                collectJsonValues(expression, null, strict, jsonValueSources);
            }
        }
        // When parse_json(v) is also used as a JSON value in the place, the common sub-expression elimination
        // already shares one parse of v between that value and its extractions, and json_extract_paths() would
        // add a second parse of every row. So the extractions over such a text stay as they are.
        eager.values().removeIf(extraction -> jsonValueSources.contains(extraction.source));
        if (alone) {
            lazy.values().removeIf(extraction -> jsonValueSources.contains(extraction.source));
            Map<ScalarOperator, Extraction> plans = new HashMap<>();
            for (Map<ScalarOperator, Extraction> extractions : List.of(eager, lazy)) {
                extractions.forEach((call, extraction) -> {
                    extraction.paths = List.of(extraction.path);
                    if (extraction.isRewritten()) {
                        plans.put(call, extraction);
                    }
                });
            }
            return rewrite(expressions, plans);
        }
        if (eager.isEmpty()) {
            return null;
        }

        // A text with an extraction evaluated on every row is parsed on every row anyway, so the extractions of
        // the same text on some rows only join its group and read the shared result: that adds no parse. The
        // extractions of the other texts on some rows stay as they are.
        Map<ScalarOperator, List<String>> pathsBySource = new HashMap<>();
        Set<ScalarOperator> sharedSources = new HashSet<>();
        for (Extraction extraction : eager.values()) {
            addPath(pathsBySource, extraction);
        }
        Map<ScalarOperator, Extraction> members = new LinkedHashMap<>(eager);
        for (Map.Entry<ScalarOperator, Extraction> entry : lazy.entrySet()) {
            Extraction extraction = entry.getValue();
            if (pathsBySource.containsKey(extraction.source) && !members.containsKey(entry.getKey())) {
                addPath(pathsBySource, extraction);
                sharedSources.add(extraction.source);
                members.put(entry.getKey(), extraction);
            }
        }
        Map<ScalarOperator, Extraction> plans = new HashMap<>();
        for (Map.Entry<ScalarOperator, Extraction> entry : members.entrySet()) {
            Extraction extraction = entry.getValue();
            extraction.paths = pathsBySource.get(extraction.source);
            extraction.shared = sharedSources.contains(extraction.source);
            if (extraction.isRewritten()) {
                plans.put(entry.getKey(), extraction);
            }
        }
        return rewrite(expressions, plans);
    }

    // The expressions with the planned extractions rewritten; null when nothing changes.
    private static List<ScalarOperator> rewrite(List<ScalarOperator> expressions, Map<ScalarOperator, Extraction> plans) {
        if (plans.isEmpty()) {
            return null;
        }

        BaseScalarOperatorShuttle rewriter = new BaseScalarOperatorShuttle() {
            @Override
            public ScalarOperator visitLambdaFunctionOperator(LambdaFunctionOperator operator, Void context) {
                return operator;
            }

            @Override
            public ScalarOperator visitDictMappingOperator(DictMappingOperator operator, Void context) {
                return operator;
            }

            @Override
            public ScalarOperator visitCall(CallOperator call, Void context) {
                Extraction extraction = plans.get(call);
                if (extraction == null) {
                    return super.visitCall(call, context);
                }
                return extraction.build(extraction.source.accept(this, context));
            }
        };
        List<ScalarOperator> result = new ArrayList<>(expressions.size());
        boolean changed = false;
        for (ScalarOperator expression : expressions) {
            ScalarOperator rewritten = expression.getDepth() <= Config.max_scalar_operator_optimize_depth
                    ? expression.accept(rewriter, null) : expression;
            changed |= rewritten != expression;
            result.add(rewritten);
        }
        return changed ? result : null;
    }

    private static void addPath(Map<ScalarOperator, List<String>> pathsBySource, Extraction extraction) {
        List<String> paths = pathsBySource.computeIfAbsent(extraction.source, k -> new ArrayList<>());
        if (!paths.contains(extraction.path)) {
            paths.add(extraction.path);
        }
    }

    // The AND operators that join the top-level conjuncts of the expression.
    private static void addConjunctions(ScalarOperator expression, Set<ScalarOperator> conjunctions) {
        if (expression instanceof CompoundPredicateOperator compound && compound.isAnd()) {
            conjunctions.add(compound);
            for (ScalarOperator child : compound.getChildren()) {
                addConjunctions(child, conjunctions);
            }
        }
    }

    // BE evaluates the operands of these operators after the first one only on some rows. With conditional
    // two-phase evaluation a branch of CASE, IF, IFNULL and COALESCE sees only the rows that reach it, and the
    // right operand of AND or OR is skipped for a chunk that the left one decides. We do not want such an operand
    // to share json_extract_paths() with a text that is not parsed on every row anyway: the common sub-expression
    // elimination would compute it on every row, and in strict mode it could fail on a row the operand never sees.
    // An AND in `conjunctions` joins conjuncts that are evaluated on every row.
    private static List<ScalarOperator> lazyOperands(ScalarOperator operator, Set<ScalarOperator> conjunctions) {
        boolean lazy = operator instanceof CaseWhenOperator
                || (operator instanceof CallOperator call && LAZY_FUNCTIONS.contains(call.getFnName().toLowerCase()))
                || (operator instanceof CompoundPredicateOperator compound && !compound.isNot()
                && !conjunctions.contains(operator));
        List<ScalarOperator> children = operator.getChildren();
        return lazy && children.size() > 1 ? children.subList(1, children.size()) : List.of();
    }

    private static void collect(ScalarOperator expression, boolean strict, boolean lazy,
                                Set<ScalarOperator> conjunctions, Map<ScalarOperator, Extraction> eager,
                                Map<ScalarOperator, Extraction> lazyExtractions) {
        if (expression instanceof LambdaFunctionOperator || expression instanceof DictMappingOperator) {
            return;
        }
        if (expression instanceof CallOperator call) {
            Map<ScalarOperator, Extraction> target = lazy ? lazyExtractions : eager;
            if (!target.containsKey(call)) {
                Extraction extraction = Extraction.of(call, strict);
                if (extraction != null) {
                    target.put(call, extraction);
                }
            }
        }
        List<ScalarOperator> lazyOperands = lazyOperands(expression, conjunctions);
        for (ScalarOperator child : expression.getChildren()) {
            boolean childLazy = lazy || lazyOperands.stream().anyMatch(operand -> operand == child);
            collect(child, strict, childLazy, conjunctions, eager, lazyExtractions);
        }
    }

    // The texts v whose parse_json(v) or CAST(v AS JSON) is used in the place other than as the document of an
    // extraction.
    private static void collectJsonValues(ScalarOperator expression, ScalarOperator parent, boolean strict,
                                          Set<ScalarOperator> sources) {
        if (expression instanceof LambdaFunctionOperator || expression instanceof DictMappingOperator) {
            return;
        }
        ScalarOperator text = parsedText(expression);
        if (text != null && !(parent instanceof CallOperator call && call.getChildren().size() == 2
                && call.getChild(0) == expression && Extraction.of(call, strict) != null)) {
            sources.add(text);
        }
        for (ScalarOperator child : expression.getChildren()) {
            collectJsonValues(child, expression, strict, sources);
        }
    }

    // v when the expression is parse_json(v) or CAST(v AS JSON) over a VARCHAR or CHAR v.
    private static ScalarOperator parsedText(ScalarOperator expression) {
        if (expression instanceof CallOperator parse && FunctionSet.PARSE_JSON.equalsIgnoreCase(parse.getFnName())
                && parse.getChildren().size() == 1 && isText(parse.getChild(0).getType())) {
            return parse.getChild(0);
        }
        if (expression instanceof CastOperator cast && cast.getType().isJsonType()
                && isText(cast.getChild(0).getType())) {
            return cast.getChild(0);
        }
        return null;
    }

    // A consumer call over a document v with a constant simple path, and how it is rewritten.
    private static final class Extraction {
        private final CallOperator call;
        // v
        private final ScalarOperator source;
        private final String path;
        // The consumer reads v itself, a get_json_* over VARCHAR.
        private final boolean overText;
        // The consumer over JSON with the name and the result type of the call; null when there is none.
        private final Function jsonConsumer;
        // All paths over v in the place, in the order of appearance.
        private List<String> paths;
        // Extractions on some rows only read the group, so every member reads json_extract_paths(): with one path
        // a get_json_* over the text would be another parse of the same rows.
        private boolean shared;

        private Extraction(CallOperator call, ScalarOperator source, String path, boolean overText,
                           Function jsonConsumer) {
            this.call = call;
            this.source = source;
            this.path = path;
            this.overText = overText;
            this.jsonConsumer = jsonConsumer;
        }

        static Extraction of(CallOperator call, boolean strict) {
            String name = call.getFnName().toLowerCase();
            boolean textConsumer = TEXT_CONSUMERS.contains(name);
            if ((!textConsumer && !JSON_CONSUMERS.contains(name)) || call.getChildren().size() != 2
                    || call.getFunction() == null) {
                return null;
            }
            if (!(call.getChild(1) instanceof ConstantOperator constant) || constant.isNull()
                    || !isText(constant.getType())) {
                return null;
            }
            String path = constant.getVarchar();
            if (!isSimplePath(path)) {
                return null;
            }
            ScalarOperator argument = call.getChild(0);
            ScalarOperator source;
            boolean overText = false;
            if (textConsumer && isText(argument.getType())) {
                source = argument;
                overText = true;
            } else if (argument instanceof CallOperator parse && FunctionSet.PARSE_JSON.equalsIgnoreCase(parse.getFnName())
                    && parse.getChildren().size() == 1 && isText(parse.getChild(0).getType())) {
                source = parse.getChild(0);
            } else if (argument instanceof CastOperator cast && cast.getType().isJsonType()
                    && isText(cast.getChild(0).getType())) {
                // In strict mode a malformed text fails with another message in CAST than in parse_json().
                if (strict) {
                    return null;
                }
                source = cast.getChild(0);
            } else {
                return null;
            }
            if (source.isConstant() || Utils.hasNonDeterministicFunc(source)) {
                return null;
            }
            Function jsonConsumer = overText ? sameResult(call, new Type[] {JsonType.JSON, VarcharType.VARCHAR})
                    : call.getFunction();
            if (jsonConsumer == null) {
                return null;
            }
            return new Extraction(call, source, path, overText, jsonConsumer);
        }

        boolean isRewritten() {
            return paths.size() > 1 || !overText || shared;
        }

        ScalarOperator build(ScalarOperator rewrittenSource) {
            ScalarOperator text = rewrittenSource.getType().getPrimitiveType() == PrimitiveType.CHAR
                    ? new CastOperator(VarcharType.VARCHAR, rewrittenSource, true) : rewrittenSource;
            if (paths.size() == 1 && !shared && TEXT_CONSUMERS.contains(call.getFnName().toLowerCase())) {
                Function textConsumer = sameResult(call, new Type[] {VarcharType.VARCHAR, VarcharType.VARCHAR});
                if (textConsumer == null) {
                    return call;
                }
                return new CallOperator(textConsumer.functionName(), call.getType(),
                        List.of(text, call.getChild(1)), textConsumer);
            }
            List<ScalarOperator> arguments = new ArrayList<>();
            arguments.add(text);
            for (String p : paths) {
                arguments.add(ConstantOperator.createVarchar(p));
            }
            Function extract = ExprUtils.getBuiltinFunction(FunctionSet.JSON_EXTRACT_PATHS,
                    arguments.stream().map(argument -> (Type) VarcharType.VARCHAR).toArray(Type[]::new),
                    Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            if (extract == null) {
                return call;
            }
            CallOperator extracted = new CallOperator(extract.functionName(), JsonType.JSON, arguments, extract);
            ConstantOperator member = ConstantOperator.createVarchar("$." + paths.indexOf(path));
            return new CallOperator(jsonConsumer.functionName(), call.getType(), List.of(extracted, member),
                    jsonConsumer);
        }

        // The builtin with the name of the call, the given arguments and the result type of the call.
        private static Function sameResult(CallOperator call, Type[] argumentTypes) {
            Function fn = ExprUtils.getBuiltinFunction(call.getFnName(), argumentTypes,
                    Function.CompareMode.IS_IDENTICAL);
            if (fn == null || fn.getReturnType().getPrimitiveType()
                    != call.getFunction().getReturnType().getPrimitiveType()) {
                return null;
            }
            return fn;
        }
    }

    private static boolean isText(Type type) {
        return type != null && (type.getPrimitiveType() == PrimitiveType.VARCHAR
                || type.getPrimitiveType() == PrimitiveType.CHAR);
    }

    /**
     * Whether BE splits the path into keys and single array indexes only, with "$" only in the first piece.
     * We repeat JsonPathPiece::parse(): pieces are split by dots outside double quotes, the quotes are removed,
     * and each piece is a key followed by array selectors. A backslash escapes characters in BE, which we do not
     * repeat, so a path with one is not simple.
     */
    @VisibleForTesting
    public static boolean isSimplePath(String path) {
        if (path == null || path.isEmpty() || path.indexOf('\\') >= 0) {
            return false;
        }
        List<String> pieces = new ArrayList<>();
        StringBuilder piece = new StringBuilder();
        boolean quoted = false;
        for (int i = 0; i < path.length(); i++) {
            char c = path.charAt(i);
            if (c == '"') {
                quoted = !quoted;
            } else if (c == '.' && !quoted) {
                pieces.add(piece.toString());
                piece.setLength(0);
            } else {
                piece.append(c);
            }
        }
        if (quoted) {
            return false;
        }
        pieces.add(piece.toString());

        for (int i = 0; i < pieces.size(); i++) {
            String current = pieces.get(i);
            if (i == 0 && current.equals("$")) {
                continue;
            }
            Matcher matcher = PATH_PIECE.matcher(current);
            if (!matcher.matches()) {
                return false;
            }
            if (i > 0 && matcher.group(1).equals("$")) {
                return false;
            }
            Matcher selector = ARRAY_SELECTOR.matcher(matcher.group(2));
            while (selector.find()) {
                String index = selector.group(1);
                if (!index.chars().allMatch(Character::isDigit) || index.length() > 10
                        || Long.parseLong(index) > Integer.MAX_VALUE) {
                    return false;
                }
            }
        }
        return true;
    }
}
