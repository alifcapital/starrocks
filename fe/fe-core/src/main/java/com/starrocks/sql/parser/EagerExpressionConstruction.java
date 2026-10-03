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

import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.Config;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.qe.SqlModeHelper;
import com.starrocks.sql.ast.HintNode;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.ast.expression.AnalyticExpr;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.AnalyticWindowBoundary;
import com.starrocks.sql.ast.expression.ArithmeticExpr;
import com.starrocks.sql.ast.expression.ArrayExpr;
import com.starrocks.sql.ast.expression.ArrowExpr;
import com.starrocks.sql.ast.expression.BetweenPredicate;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CaseExpr;
import com.starrocks.sql.ast.expression.CaseWhenClause;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.CollectionElementExpr;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.DictQueryExpr;
import com.starrocks.sql.ast.expression.DictionaryGetExpr;
import com.starrocks.sql.ast.expression.ExistsPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.FunctionParams;
import com.starrocks.sql.ast.expression.GroupingFunctionCallExpr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.InformationFunction;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.IntervalLiteral;
import com.starrocks.sql.ast.expression.IsNullPredicate;
import com.starrocks.sql.ast.expression.LambdaArgument;
import com.starrocks.sql.ast.expression.LambdaFunctionExpr;
import com.starrocks.sql.ast.expression.LargeIntLiteral;
import com.starrocks.sql.ast.expression.LikePredicate;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.MapExpr;
import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.ast.expression.MultiInPredicate;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.OdbcScalarFunctionCall;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.SubfieldExpr;
import com.starrocks.sql.ast.expression.Subquery;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.ast.expression.UserVariableExpr;
import com.starrocks.sql.ast.expression.VarBinaryLiteral;
import com.starrocks.sql.ast.expression.VariableExpr;
import com.starrocks.sql.parser.rewriter.CompoundPredicateExprRewriter;
import com.starrocks.type.AnyMapType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.BitmapType;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.FloatType;
import com.starrocks.type.HLLType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.MapType;
import com.starrocks.type.PercentileType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.StringType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VariantType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static com.starrocks.sql.parser.StarRocksParser.BACKQUOTED_IDENTIFIER;
import static com.starrocks.sql.parser.StarRocksParser.BITNOT;
import static com.starrocks.sql.parser.StarRocksParser.DOUBLE_VALUE;
import static com.starrocks.sql.parser.StarRocksParser.INTEGER_VALUE;
import static com.starrocks.sql.parser.StarRocksParser.LOGICAL_NOT;
import static com.starrocks.sql.parser.StarRocksParser.MINUS_SYMBOL;
import static com.starrocks.sql.parser.StarRocksParser.TRANSLATE;

// AST construction follows AstBuilder.
// No generated parser, parse tree, SQL rewrite, fallback, or AST reuse is used here.
/** Concrete eager construction; returns original AST nodes directly, no descriptor/wrapper graph. */
final class EagerExpressionConstruction
        implements ExpressionConstruction<
                Expr,
                QueryRelation,
                Type,
                StructField,
                OrderByElement,
                AnalyticWindow,
                AnalyticWindowBoundary,
                CaseWhenClause> {
    private final long mode;
    private final CompoundPredicateExprRewriter rewriter = new CompoundPredicateExprRewriter();
    private Errors errors;

    EagerExpressionConstruction(long mode) {
        this.mode = mode;
    }

    public void bind(Errors errors) {
        this.errors = errors;
    }

    public ExpressionConstruction<
                    Expr,
                    QueryRelation,
                    Type,
                    StructField,
                    OrderByElement,
                    AnalyticWindow,
                    AnalyticWindowBoundary,
                    CaseWhenClause>
            fork() {
        return new EagerExpressionConstruction(mode);
    }

    private DirectExpressionParser.UnsupportedExpression unsupported(String why) {
        return errors.unsupported(why);
    }

    private static final Set<String> DATE_FUNCTION_NAMES =
            Set.of("date_add", "adddate", "date_sub", "subdate", "days_sub");
    private static final Set<String> SESSION_INFO_NAMES =
            Set.of("connection_id", "session_user", "session_id");
    private static final Set<String> INFO_NAMES =
            Set.of(
                    "catalog",
                    "database",
                    "schema",
                    "user",
                    "current_user",
                    "current_role",
                    "current_group",
                    "current_warehouse");
    private static final Set<String> REJECT_FUNCTIONS =
            Set.of(
                    "grouping",
                    "grouping_id",
                    "time_slice",
                    "date_slice",
                    "timestampadd",
                    "timestampdiff",
                    "dict_mapping",
                    "map");
    private static final Set<String> OVER_FALLBACK_FUNCTIONS =
            Set.of(
                    "grouping",
                    "grouping_id",
                    "timestampadd",
                    "timestampdiff");
    private static final Set<String> GENERIC_DECIMAL_TYPES = Set.of("NUMERIC", "NUMBER");
    private static final Set<String> V3_DECIMAL_TYPES =
            Set.of("DECIMAL32", "DECIMAL64", "DECIMAL128", "DECIMAL256");

    public Expr number(int kind, long small, String s, NodePosition p) {
        try {
            if (small >= 0) {
                return new IntLiteral(small, p);
            }
            if (kind == INTEGER_VALUE) {
                if (s.length() <= 18) {
                    return new IntLiteral(Long.parseLong(s), p);
                }
                BigInteger n = new BigInteger(s);
                if (n.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0) {
                    return new IntLiteral(n.longValue(), p);
                }
                if (n.compareTo(BigInteger.ONE.shiftLeft(127)) <= 0) {
                    return new LargeIntLiteral(s, p);
                }
                if (n.compareTo(BigInteger.ONE.shiftLeft(255)) <= 0) {
                    return decimalLiteral(s, p);
                }
                throw unsupported("integer overflow");
            }
            if (SqlModeHelper.check(mode, SqlModeHelper.MODE_DOUBLE_LITERAL)) {
                return new FloatLiteral(s, p);
            }
            BigDecimal decimal = new BigDecimal(s);
            if (kind == DOUBLE_VALUE
                    && DecimalLiteral.getRealPrecision(decimal)
                                    - DecimalLiteral.getRealScale(decimal)
                            > 38) {
                return new FloatLiteral(s, p);
            }
            return decimalLiteral(decimal, p);
        } catch (ParsingException | NumberFormatException | ArithmeticException e) {
            throw unsupported("numeric literal constructor requires original path");
        }
    }

    private DecimalLiteral decimalLiteral(String text, NodePosition pos) {

        try {
            return new DecimalLiteral(text, pos);
        } catch (InternalError e) {
            throw unsupported("legacy decimal constructor requires original path");
        }
    }

    private DecimalLiteral decimalLiteral(BigDecimal value, NodePosition pos) {

        try {
            return new DecimalLiteral(value, pos);
        } catch (InternalError e) {
            throw unsupported("legacy decimal constructor requires original path");
        }
    }

    private Expr intervalIntArgument(Expr value) {

        if (!(value instanceof IntLiteral integer)) {
            return value;
        }
        try {
            return new IntLiteral(integer.getValue(), IntegerType.INT);
        } catch (ArithmeticException e) {
            throw unsupported("interval INT conversion requires original error");
        }
    }

    private List<Expr> timeSliceArguments(Expr time, Expr amount, String unit, String boundary) {

        List<Expr> args = new ArrayList<>(4);
        args.add(time);
        args.add(intervalIntArgument(amount));
        args.add(new StringLiteral(unit));
        args.add(new StringLiteral(boundary));
        return args;
    }

    private String timeSliceBoundary(Expr argument, String functionName) {

        if (argument instanceof SlotRef slot
                && slot.getTblNameWithoutAnalyzed() == null
                && slot.getColumnName() != null) {
            String boundary = slot.getColumnName().toLowerCase(Locale.ROOT);
            if (boundary.equals("floor") || boundary.equals("ceil")) {
                return boundary;
            }
        }
        throw new ParsingException(
                ErrorMsgProxy.PARSER_ERROR_MSG.wrongTypeOfArgs(functionName), argument.getPos());
    }

    public void validateDistinctArguments(
            boolean distinct, boolean aggregate, String name, List<Expr> args) {
        if ((distinct || (aggregate && name.equals("array_agg_distinct"))) && args.isEmpty()) {
            throw unsupported("empty DISTINCT arguments");
        }
    }

    public Expr finishFunction(
            String full,
            String name,
            List<String> parts,
            int head,
            boolean identifierHead,
            boolean aggregate,
            boolean star,
            boolean distinct,
            boolean quantified,
            List<String> hints,
            List<Expr> args,
            List<OrderByElement> aggregateOrder,
            boolean separator,
            Expr filterPredicate,
            boolean analytic,
            OverParts<Expr, OrderByElement, AnalyticWindow> over,
            NodePosition p) {

        if (aggregate) {
            boolean groupConcat = name.equals("group_concat");
            if (name.equals("array_agg_distinct")) {
                name = "array_agg";
                distinct = true;
            }
            if (groupConcat
                    && !args.isEmpty()
                    && !separator
                    && (!SqlModeHelper.check(mode, SqlModeHelper.MODE_GROUP_CONCAT_LEGACY)
                            || args.size() == 1)) {
                args.add(
                        new StringLiteral(
                                SqlModeHelper.check(mode, SqlModeHelper.MODE_GROUP_CONCAT_LEGACY)
                                        ? ", "
                                        : ",",
                                p));
            }
            if (!aggregateOrder.isEmpty()) {
                int outputSize = args.size() - (groupConcat ? 1 : 0);
                for (OrderByElement order : aggregateOrder) {
                    if (order.getExpr() instanceof IntLiteral integer) {
                        long ordinal = integer.getLongValue();
                        if (ordinal < 1 || ordinal > outputSize) {
                            throw new ParsingException(
                                    String.format(
                                            "ORDER BY position %s is not in %s output list",
                                            ordinal, name),
                                    p);
                        }
                        order.setExpr(args.get((int) ordinal - 1));
                    }
                }
                aggregateOrder =
                        aggregateOrder.stream()
                                .filter(order -> !order.getExpr().isConstant())
                                .collect(java.util.stream.Collectors.toList());
            }
            for (OrderByElement order : aggregateOrder) {
                args.add(order.getExpr());
            }
            FunctionCallExpr call;
            if (filterPredicate == null) {
                call =
                        new FunctionCallExpr(
                                name,
                                star
                                        ? FunctionParams.createStarParam()
                                        : new FunctionParams(distinct, args, aggregateOrder),
                                p);
            } else {
                boolean count = name.equalsIgnoreCase(FunctionSet.COUNT);
                name += FunctionSet.AGG_STATE_IF_SUFFIX;
                args.add(filterPredicate);
                if (count && star) {
                    // The original COUNT(*) FILTER constructor intentionally has default position.
                    call =
                            new FunctionCallExpr(
                                    name, new FunctionParams(false, args, null, distinct, null));
                } else if (name.startsWith(FunctionSet.ARRAY_AGG) && distinct) {
                    name = FunctionSet.ARRAY_AGG_DISTINCT + FunctionSet.AGG_STATE_IF_SUFFIX;
                    call =
                            new FunctionCallExpr(
                                    name, new FunctionParams(false, args, aggregateOrder), p);
                } else {
                    call =
                            new FunctionCallExpr(
                                    name,
                                    star
                                            ? FunctionParams.createStarParam()
                                            : new FunctionParams(distinct, args, aggregateOrder),
                                    p);
                }
            }
            call = SyntaxSugars.parse(call);
            call.setHints(hints == null ? new ArrayList<>() : hints);
            return analytic ? buildOver(call, over, p) : call;
        }
        // AstBuilder returns the rewritten node of these functions before it reads OVER, so the
        // window is parsed by the caller and dropped here.
        if (name.equals(FunctionSet.ARRAY_GENERATE)
                && args.size() == 3
                && args.get(2) instanceof IntervalLiteral interval) {
            List<Expr> generated = new ArrayList<>(4);
            generated.add(args.get(0));
            generated.add(args.get(1));
            generated.add(intervalIntArgument(interval.getValue()));
            generated.add(
                    new StringLiteral(
                            interval.getUnitIdentifier()
                                    .getDescription()
                                    .toLowerCase(Locale.ROOT)));
            return new FunctionCallExpr(full, generated, p);
        }
        if (name.equals(FunctionSet.TIME_SLICE) || name.equals(FunctionSet.DATE_SLICE)) {
            if (args.size() == 2 || args.size() == 3) {
                Expr amount = args.get(1);
                String unit = "day";
                if (amount instanceof IntervalLiteral interval) {
                    amount = interval.getValue();
                    unit = interval.getUnitIdentifier().getDescription().toLowerCase(Locale.ROOT);
                }
                String boundary = args.size() == 2 ? "floor" : timeSliceBoundary(args.get(2), name);
                return new FunctionCallExpr(
                        full, timeSliceArguments(args.get(0), amount, unit, boundary), p);
            }
            if (args.size() == 4) {
                if (!(args.get(2) instanceof StringLiteral unit)) {
                    throw new ParsingException(
                            ErrorMsgProxy.PARSER_ERROR_MSG.wrongTypeOfArgs(name),
                            args.get(2).getPos());
                }
                if (!(args.get(3) instanceof StringLiteral boundary)) {
                    throw new ParsingException(
                            ErrorMsgProxy.PARSER_ERROR_MSG.wrongTypeOfArgs(name),
                            args.get(3).getPos());
                }
                // The pinned four-argument constructor intentionally uses default position.
                return new FunctionCallExpr(
                        full,
                        timeSliceArguments(
                                args.get(0), args.get(1), unit.getValue(), boundary.getValue()));
            }
            throw unsupported("slice argument count requires original arity error");
        }
        if (name.equals(FunctionSet.MAP)) {
            if ((args.size() & 1) != 0) {
                throw unsupported("odd MAP arguments require original arity error");
            }
            return new MapExpr(AnyMapType.ANY_MAP, args, p);
        }
        if (name.equals(FunctionSet.DICT_MAPPING)) {
            return new DictQueryExpr(args);
        }
        if (REJECT_FUNCTIONS.contains(name)) {
            throw unsupported("special AST function " + name);
        }
        if (name.equals("isnull") || name.equals("isnotnull")) {
            if (args.size() != 1) {
                throw unsupported("null function arity");
            }
            return new IsNullPredicate(args.get(0), name.equals("isnotnull"), p);
        }
        if (ArithmeticExpr.isArithmeticExpr(name)) {
            if (args.isEmpty()) {
                throw unsupported("arithmetic function arity");
            }
            return new ArithmeticExpr(
                    ArithmeticExpr.getArithmeticOperator(name),
                    args.get(0),
                    args.size() > 1 ? args.get(1) : null,
                    p);
        }
        if (DATE_FUNCTION_NAMES.contains(name)) {
            if (args.size() != 2) {
                throw unsupported("date function arity");
            }
            Expr amount = args.get(1);
            String unit = "DAY";
            if (amount instanceof IntervalLiteral interval) {
                amount = interval.getValue();
                unit = interval.getUnitIdentifier().getDescription();
            }
            return new TimestampArithmeticExpr(name, args.get(0), amount, unit, p);
        }
        if (name.equals("element_at")) {
            if (args.size() != 2) {
                throw unsupported("element_at arity");
            }
            return new CollectionElementExpr(args.get(0), args.get(1), false);
        }
        if (SESSION_INFO_NAMES.contains(name)) {
            return new InformationFunction(name.toUpperCase(Locale.ROOT));
        }
        if (INFO_NAMES.contains(name) && parts.size() == 1 && head != BACKQUOTED_IDENTIFIER) {
            // Only the zero-argument information alternative lowers here. Generic USER/CATALOG
            // arguments retain their FunctionCallExpr, so ODBC can validate before mapping.
            // Modifier/star/ORDER syntax was validated above, before any argument-count shortcut.
            if (args.isEmpty() && !star && !quantified && aggregateOrder.isEmpty()) {
                return new InformationFunction(name.toUpperCase(Locale.ROOT), p);
            }
            if (!identifierHead) {
                throw unsupported("information function arguments");
            }
        }
        if (name.equals("str_to_map")) {
            if (args.size() < 1 || args.size() > 3) {
                throw unsupported("str_to_map arity");
            }
            if (args.size() == 1) {
                args.add(new StringLiteral(",", p));
            }
            if (args.size() == 2) {
                args.add(new StringLiteral(":", p));
            }
            return new FunctionCallExpr(name, args, p);
        }
        boolean overDropped = false;
        if (name.equals("substr") || name.equals("substring")) {
            overDropped = true;
            if (args.size() != 2 && args.size() != 3) {
                args.clear();
            }
            for (int i = 1; i < args.size(); i++) {
                if (args.get(i) instanceof IntLiteral n) {
                    args.set(i, new IntLiteral(n.getValue(), IntegerType.INT));
                }
            }
        }
        if ((name.equals("lpad") || name.equals("rpad")) && args.size() == 2) {
            overDropped = true;
            args.add(new StringLiteral(" "));
        }
        FunctionCallExpr call = new FunctionCallExpr(full, new FunctionParams(false, args), p);
        if (overDropped && analytic) {
            return call;
        }
        return analytic ? buildOver(call, over, p) : SyntaxSugars.parse(call);
    }

    public void validateFilterOrder(
            String name, List<Expr> args, List<OrderByElement> aggregateOrder, boolean separator) {
        // Original normalizes ORDER BY before constructing the condition. Delegate invalid
        // ordinals now so a condition constructor cannot change the original error priority.
        boolean groupConcat = name.equals(FunctionSet.GROUP_CONCAT);
        boolean legacy = SqlModeHelper.check(mode, SqlModeHelper.MODE_GROUP_CONCAT_LEGACY);
        int outputSize =
                args.size() - (groupConcat && (separator || (legacy && args.size() > 1)) ? 1 : 0);
        for (OrderByElement order : aggregateOrder) {
            if (order.getExpr() instanceof IntLiteral integer
                    && (integer.getLongValue() < 1 || integer.getLongValue() > outputSize)) {
                throw unsupported("FILTER aggregate ORDER BY ordinal");
            }
        }
    }

    public void validateAnalyticRewrite(
            boolean analytic, boolean aggregate, String name, List<Expr> args, int head) {
        // We fear ANTLR reads these calls as another grammar alternative once OVER follows, so we
        // fall back for them.
        if (analytic
                && !aggregate
                && (OVER_FALLBACK_FUNCTIONS.contains(name)
                        || INFO_NAMES.contains(name)
                        || head == TRANSLATE)) {
            throw unsupported("function rewrite before OVER");
        }
    }

    public boolean overIgnored(String name, List<Expr> args) {
        return name.equals(FunctionSet.TIME_SLICE)
                || name.equals(FunctionSet.DATE_SLICE)
                || name.equals(FunctionSet.MAP)
                || name.equals(FunctionSet.DICT_MAPPING)
                || name.equals("isnull")
                || name.equals("isnotnull")
                || ArithmeticExpr.isArithmeticExpr(name)
                || DATE_FUNCTION_NAMES.contains(name)
                || name.equals("element_at")
                || SESSION_INFO_NAMES.contains(name)
                || name.equals("str_to_map")
                || name.equals("substr")
                || name.equals("substring")
                || ((name.equals("lpad") || name.equals("rpad")) && args.size() == 2)
                || (name.equals(FunctionSet.ARRAY_GENERATE)
                        && args.size() == 3
                        && args.get(2) instanceof IntervalLiteral);
    }

    public boolean isFunctionCall(Expr value) {
        return value instanceof FunctionCallExpr;
    }

    public Expr password(String plain, NodePosition p) {
        return new StringLiteral(new String(MysqlPassword.makeScrambledPassword(plain)), p);
    }

    public Expr negativeGeneralLiteral(Expr node) {
        Expr result;
        // AstBuilder.visitGeneralLiteralExpression uses the NUMBER position,
        // excluding the minus, and creates a new literal rather than swapSign().
        if (node instanceof IntLiteral n) {
            result = new IntLiteral(-n.getLongValue(), node.getPos());
        } else if (node instanceof LargeIntLiteral n) {
            BigInteger value = n.getValue().negate();
            result =
                    value.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) >= 0
                                    && value.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) <= 0
                            ? new IntLiteral(value.longValue(), node.getPos())
                            : new LargeIntLiteral(value.toString(), node.getPos());
        } else if (node instanceof DecimalLiteral n) {
            result = decimalLiteral(n.getValue().negate(), node.getPos());
        } else if (node instanceof FloatLiteral n) {
            result = new FloatLiteral(-n.getDoubleValue(), node.getPos());
        } else {
            throw unsupported("skew negative literal numeric class");
        }
        return result;
    }

    public Expr arithmetic(ArithmeticExpr.Operator op, Expr left, Expr right, NodePosition p) {
        if (left instanceof IntervalLiteral interval) {
            left =
                    new TimestampArithmeticExpr(
                            op,
                            right,
                            interval.getValue(),
                            interval.getUnitIdentifier().getDescription(),
                            true,
                            p);
        } else if (right instanceof IntervalLiteral interval) {
            left =
                    new TimestampArithmeticExpr(
                            op,
                            left,
                            interval.getValue(),
                            interval.getUnitIdentifier().getDescription(),
                            false,
                            p);
        } else {
            left = new ArithmeticExpr(op, left, right, p);
        }
        return left;
    }

    public Expr unary(int t, Expr value, int start) {
        if (t == MINUS_SYMBOL) {
            if (value instanceof LiteralExpr lit && value.getType().isNumericType()) {
                lit.swapSign();
            } else {
                value =
                        new ArithmeticExpr(
                                ArithmeticExpr.Operator.MULTIPLY,
                                new IntLiteral(-1),
                                value,
                                errors.constructionPosition(start));
            }
        } else if (t == BITNOT) {
            value =
                    new ArithmeticExpr(
                            ArithmeticExpr.Operator.BITNOT,
                            value,
                            null,
                            errors.constructionPosition(start));
        } else if (t == LOGICAL_NOT) {
            value =
                    new CompoundPredicate(
                            CompoundPredicate.Operator.NOT,
                            value,
                            null,
                            errors.constructionPosition(start));
        }
        return value;
    }

    public Expr fieldAccess(Expr value, String field, NodePosition p) {
        if (value instanceof SlotRef slot) {
            List<String> parts = new ArrayList<>(slot.getQualifiedName().getParts());
            parts.add(field);
            value = new SlotRef(QualifiedName.of(parts, p));
        } else if (value instanceof SubfieldExpr sub) {
            List<String> fields = new ArrayList<>(sub.getFieldNames());
            fields.add(field);
            value = new SubfieldExpr(sub.getChild(0), fields, p);
        } else {
            value = new SubfieldExpr(value, List.of(field), p);
        }
        return value;
    }

    public Expr odbc(Expr function, NodePosition functionPos, boolean windowHead) {
        // Reserved window grammar is rejected by the mapper; generic scalar OVER is a
        // source-category error.
        if (function instanceof AnalyticExpr && !windowHead) {
            throw new ParsingException("ODBC scalar functions do not support OVER", functionPos);
        }
        return new OdbcScalarFunctionCall(function, functionPos).mappingFunction();
    }

    private Expr buildOver(
            Expr value,
            OverParts<Expr, OrderByElement, AnalyticWindow> over,
            NodePosition position) {

        FunctionCallExpr call = (FunctionCallExpr) value;
        if (over.hints() != null) {
            for (String hint : over.hints()) {
                // AnalyticExpr rejects every other name with an IllegalStateException.
                if (!HintNode.HINT_ANALYTIC_SORT.equalsIgnoreCase(hint)
                        && !HintNode.HINT_ANALYTIC_HASH.equalsIgnoreCase(hint)
                        && !HintNode.HINT_ANALYTIC_SKEW.equalsIgnoreCase(hint)) {
                    throw unsupported("window partition hint name");
                }
            }
        }

        call.setIsAnalyticFnCall(true);
        return new AnalyticExpr(
                call, over.partitions(), over.order(), over.window(), over.hints(), position);
    }

    public Expr windowCall(String name, List<Expr> args, NodePosition p, boolean ignore) {

        FunctionCallExpr call =
                SyntaxSugars.parse(new FunctionCallExpr(name, new FunctionParams(false, args), p));
        call.setIgnoreNulls(ignore);
        return call;
    }

    public Expr over(
            Expr call, OverParts<Expr, OrderByElement, AnalyticWindow> over, NodePosition p) {
        return buildOver(call, over, p);
    }

    public void validateMapKey(Type key) {
        if (!key.isValidMapKeyType()) {
            throw unsupported("map key type requires original TypeParser");
        }
    }

    public Type scalarType(String name, int length, int scale) {
        if (name.startsWith("DECIMAL") || GENERIC_DECIMAL_TYPES.contains(name)) {
            if (name.equals("DECIMALV2")) {
                return length < 0
                        ? DecimalType.DEFAULT_DECIMALV2
                        : scale < 0
                                ? TypeFactory.createDecimalV2Type(length)
                                : TypeFactory.createDecimalV2Type(length, scale);
            }
            if (V3_DECIMAL_TYPES.contains(name)) {
                if (!Config.enable_decimal_v3) {
                    throw unsupported("decimal v3 disabled");
                }
                PrimitiveType t = PrimitiveType.valueOf(name);
                return length < 0
                        ? TypeFactory.createDecimalV3Type(t)
                        : scale < 0
                                ? TypeFactory.createDecimalV3Type(t, length)
                                : TypeFactory.createDecimalV3Type(t, length, scale);
            }
            return length < 0
                    ? TypeFactory.createUnifiedDecimalType(10, 0)
                    : scale < 0
                            ? TypeFactory.createUnifiedDecimalType(length)
                            : TypeFactory.createUnifiedDecimalType(length, scale);
        }
        if (scale >= 0) {
            throw unsupported("nondecimal scale");
        }
        return switch (name) {
            case "BOOLEAN" -> BooleanType.BOOLEAN;
            case "TINYINT" -> IntegerType.TINYINT;
            case "SMALLINT" -> IntegerType.SMALLINT;
            case "INT", "INTEGER" -> IntegerType.INT;
            case "BIGINT" -> IntegerType.BIGINT;
            case "LARGEINT" -> IntegerType.LARGEINT;
            case "FLOAT" -> FloatType.FLOAT;
            case "DOUBLE" -> FloatType.DOUBLE;
            case "DATE" -> DateType.DATE;
            case "DATETIME" -> DateType.DATETIME;
            case "TIME" -> DateType.TIME;
            case "STRING", "TEXT" ->
                    TypeFactory.createVarcharType(StringType.DEFAULT_STRING_LENGTH);
            case "VARCHAR" -> TypeFactory.createVarcharType(length);
            case "CHAR" -> TypeFactory.createCharType(length);
            case "VARBINARY", "BINARY" -> TypeFactory.createVarbinary(length);
            case "HLL" -> HLLType.HLL;
            case "BITMAP" -> BitmapType.BITMAP;
            case "PERCENTILE" -> PercentileType.PERCENTILE;
            case "JSON" -> JsonType.JSON;
            case "VARIANT" -> VariantType.VARIANT;
            default -> throw unsupported("CAST type " + name);
        };
    }

    public int typeParameter(String text) {
        return Integer.parseInt(text);
    }

    public Type signedType(String name) {
        return IntegerType.BIGINT;
    }

    public Expr binaryLiteral(String value, NodePosition p) {
        try {
            return new VarBinaryLiteral(value, p);
        } catch (ParsingException e) {
            throw unsupported("binary literal constructor requires original path");
        }
    }

    public Expr dateLiteral(String value, boolean date) {
        try {
            return new DateLiteral(
                    com.starrocks.common.util.DateUtils.parseStrictDateTime(value),
                    date ? DateType.DATE : DateType.DATETIME);
        } catch (RuntimeException e) {
            throw unsupported("date literal constructor requires original path");
        }
    }

    public Expr slot(List<String> parts, NodePosition p, boolean backQuoted) {
        SlotRef slot = new SlotRef(QualifiedName.of(parts, p));
        if (backQuoted) {
            slot.setBackQuoted(true);
        }
        return slot;
    }

    public Type structType(ArrayList<StructField> fields) {
        try {
            return new StructType(fields);
        } catch (UnsupportedOperationException e) {
            throw unsupported("struct constructor requires original TypeParser");
        }
    }

    public Expr timestamp(
            String name, Expr e3, Expr e2, String unit, NodePosition unitPos, NodePosition p) {
        return new TimestampArithmeticExpr(
                name, e3, e2, new UnitIdentifier(unit, unitPos).getDescription(), p);
    }

    public Expr interval(Expr amount, String unit, NodePosition unitPos, NodePosition p) {
        return new IntervalLiteral(amount, new UnitIdentifier(unit, unitPos), p);
    }

    public Expr subquery(QueryRelation relation) {
        return new Subquery(new QueryStatement(relation));
    }

    public Expr lambdaMap(List<Expr> values) {
        return new MapExpr(AnyMapType.ANY_MAP, values);
    }

    public Expr lambdaArgument(String name) {
        return new LambdaArgument(name);
    }

    public Expr lambda(List<Expr> arguments) {
        return new LambdaFunctionExpr(arguments);
    }

    public Expr logicalNot(Expr value, NodePosition p) {
        return new CompoundPredicate(CompoundPredicate.Operator.NOT, value, null, p);
    }

    public Expr logical(boolean and, Expr left, Expr right, NodePosition p) {
        return new CompoundPredicate(
                and ? CompoundPredicate.Operator.AND : CompoundPredicate.Operator.OR,
                left,
                right,
                p);
    }

    public Expr rewriteLogical(Expr value) {
        return Config.compound_predicate_flatten_threshold > 0 ? rewriter.rewrite(value) : value;
    }

    public Expr isNull(Expr value, boolean negative, NodePosition p) {
        return new IsNullPredicate(value, negative, p);
    }

    public Expr compare(BinaryType operator, Expr left, Expr right, NodePosition p) {
        return new BinaryPredicate(operator, left, right, p);
    }

    public Expr multiIn(List<Expr> values, Expr query, boolean negative, NodePosition p) {
        return new MultiInPredicate(values, (Subquery) query, negative, p);
    }

    public Expr inQuery(Expr value, Expr query, boolean negative, NodePosition p) {
        return new InPredicate(value, (Subquery) query, negative, p);
    }

    public Expr inList(Expr value, List<Expr> values, boolean negative, NodePosition p) {
        return new InPredicate(value, values, negative, p);
    }

    public Expr between(Expr value, Expr lower, Expr upper, boolean negative, NodePosition p) {
        return new BetweenPredicate(value, lower, upper, negative, p);
    }

    public Expr like(boolean like, Expr left, Expr right, NodePosition p) {
        return new LikePredicate(
                like ? LikePredicate.Operator.LIKE : LikePredicate.Operator.REGEXP, left, right, p);
    }

    public Expr parameter(LexicalParameterContext parameters, int offset) {
        return parameters.parameterAt(offset);
    }

    public Expr exists(Expr query, NodePosition p) {
        return new ExistsPredicate((Subquery) query, false, p);
    }

    public Expr variable(String name, SetType scope, NodePosition p) {
        return new VariableExpr(name, scope, p);
    }

    public Expr userVariable(String name, NodePosition p) {
        return new UserVariableExpr(name, p);
    }

    public Expr grouping(List<Expr> args, NodePosition p) {
        return new GroupingFunctionCallExpr("grouping", args, p);
    }

    public Expr nullLiteral(NodePosition p) {
        return new NullLiteral(p);
    }

    public Expr bool(boolean value, NodePosition p) {
        return new BoolLiteral(value, p);
    }

    public Expr string(String value, NodePosition p) {
        return new StringLiteral(value, p);
    }

    public Type arrayType(Type child) {
        return new ArrayType(child);
    }

    public Type mapType(Type key, Type value) {
        return new MapType(key, value);
    }

    public Type anyMapType() {
        return AnyMapType.ANY_MAP;
    }

    public StructField structField(String field, Type type) {
        return new StructField(field, type, null);
    }

    public Expr array(Type type, List<Expr> values, NodePosition p) {
        return new ArrayExpr(type, values, p);
    }

    public Expr map(Type type, List<Expr> values, NodePosition p) {
        return new MapExpr(type, values, p);
    }

    public CaseWhenClause when(Expr condition, Expr result, NodePosition p) {
        return new CaseWhenClause(condition, result, p);
    }

    public Expr caseExpr(Expr base, List<CaseWhenClause> clauses, Expr other, NodePosition p) {
        return new CaseExpr(base, clauses, other, p);
    }

    public Expr cast(Type type, Expr child, NodePosition p) {
        return new CastExpr(new TypeDef(type), child, p);
    }

    public Expr extract(String field, Expr argument, NodePosition p) {
        return new FunctionCallExpr(
                field, new FunctionParams(new ArrayList<>(List.of(argument))), p);
    }

    public Expr precision(String text) {
        return new IntLiteral(Long.parseLong(text), IntegerType.INT);
    }

    public Expr dateTime(String name, List<Expr> args) {
        return new FunctionCallExpr(name, new FunctionParams(false, args));
    }

    public Expr information(String name, NodePosition p) {
        return new InformationFunction(name, p);
    }

    public Expr concat(Expr left, Expr right, NodePosition p) {
        return new FunctionCallExpr(
                "concat", new FunctionParams(new ArrayList<>(List.of(left, right))), p);
    }

    public Expr collection(Expr value, Expr index) {
        return new CollectionElementExpr(value, index, false);
    }

    public Expr arrow(Expr value, Expr key, NodePosition p) {
        return new ArrowExpr(value, (StringLiteral) key, p);
    }

    public Expr match(MatchExpr.MatchOperator operator, Expr left, Expr right, NodePosition p) {
        return new MatchExpr(operator, left, right, p);
    }

    public Expr dictionary(List<Expr> args) {
        return new DictionaryGetExpr(args);
    }

    public Expr specializedFunction(String name, List<Expr> args, NodePosition p) {
        return new FunctionCallExpr(name, args, p);
    }

    public Expr orderAllLiteral() {
        return new IntLiteral(0);
    }

    public OrderByElement order(
            Expr value, boolean ascending, boolean nullsFirst, NodePosition p, boolean all) {
        return all
                ? new OrderByElement(value, ascending, nullsFirst, p, true)
                : new OrderByElement(value, ascending, nullsFirst, p);
    }

    public AnalyticWindow window(
            AnalyticWindow.Type type,
            AnalyticWindowBoundary left,
            AnalyticWindowBoundary right,
            NodePosition p) {
        return new AnalyticWindow(type, left, right, p);
    }

    public AnalyticWindow window(
            AnalyticWindow.Type type, AnalyticWindowBoundary left, NodePosition p) {
        return new AnalyticWindow(type, left, p);
    }

    public AnalyticWindowBoundary windowBoundary(
            AnalyticWindowBoundary.BoundaryType type, Expr amount) {
        return new AnalyticWindowBoundary(type, amount);
    }

    public void validateInList(List<Expr> values) {
        com.starrocks.qe.ConnectContext context = com.starrocks.qe.ConnectContext.get();
        if (context != null
                && context.getSessionVariable().enableLargeInPredicate()
                && values.size() >= context.getSessionVariable().getLargeInPredicateThreshold()) {
            throw unsupported("session LargeInPredicate requires original syntax classification");
        }
    }
}
