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

import com.starrocks.catalog.Function;
import com.starrocks.common.util.ParseUtil;
import com.starrocks.common.util.SqlUtils;
import com.starrocks.sql.ast.ParseNode;
import com.starrocks.sql.ast.expression.BoolLiteral;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.DateLiteral;
import com.starrocks.sql.ast.expression.DecimalLiteral;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.FloatLiteral;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.LiteralExpr;
import com.starrocks.sql.ast.expression.NullLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.formatter.AST2SQLVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.plan.ScalarOperatorToExpr;
import com.starrocks.type.Type;

import java.util.HashMap;
import java.util.Map;

/**
 * Prints a canonical expression as SQL. The text identifies the statistic: it is stored, the collection query runs
 * it, and the planner loads it back. We want parse, analysis, canonicalization and printing of the text to give the
 * same text again, so the printer does not depend on session variables and keeps every type the analysis would
 * otherwise infer differently: columns are backquoted names, string constants are quoted and escaped, and decimal,
 * floating point and date constants are printed as an explicit CAST. Implicit casts are left out because the
 * analysis adds them again.
 */
public final class ExpressionStatisticsSqlPrinter {
    private ExpressionStatisticsSqlPrinter() {
    }

    public static String print(ScalarOperator operator) {
        Map<ColumnRefOperator, Expr> columns = new HashMap<>();
        for (ColumnRefOperator column : Utils.extractColumnRef(operator)) {
            SlotRef slot = new SlotRef(null, column.getName());
            slot.setType(column.getType());
            columns.put(column, slot);
        }
        Expr expr = ScalarOperatorToExpr.buildExecExpression(operator,
                new ScalarOperatorToExpr.FormatterContext(columns));
        return new Printer(false).visit(expr);
    }

    /**
     * The text of an expression as written, before analysis, in the form of the canonical text: what DROP MCV STATS
     * can still find when the expression no longer analyzes, for example after its column or function was dropped.
     * A canonical text that SHOW printed parses back to the same text: constants are printed as written, and the
     * casts of the text keep their types.
     */
    public static String printUnanalyzed(Expr expr) {
        return new Printer(true).visit(expr);
    }

    /** The text for ordering arguments; null when the expression cannot be printed. */
    static String printOrNull(ScalarOperator operator) {
        try {
            return print(operator);
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static final class Printer extends AST2SQLVisitor {
        private final boolean unanalyzed;

        private Printer(boolean unanalyzed) {
            this.unanalyzed = unanalyzed;
            options.setEnableDigest(false);
            options.setAddFunctionDbName(false);
            options.setColumnWithTableName(false);
            options.setEnableNewLine(false);
            options.setHideCredential(false);
        }

        @Override
        public String visitSlot(SlotRef expr, Void context) {
            return ParseUtil.backquote(expr.getColumnName());
        }

        // A user function is printed with its database: the collection and the cache load run without a current
        // database, and a function of the same name in another database must not stand in for it.
        @Override
        public String visitFunctionCall(FunctionCallExpr node, Void context) {
            String text = super.visitFunctionCall(node, context);
            Function function = node.getFn();
            if (function == null) {
                // Not analyzed: the database as written.
                return unanalyzed && node.getDbName() != null
                        ? ParseUtil.backquote(node.getDbName()) + "." + text : text;
            }
            // A global function is found without a database.
            if (function.isUdf() && function.getFunctionName().getDb() != null
                    && !function.getFunctionName().isGlobalFunction()) {
                return ParseUtil.backquote(function.getFunctionName().getDb()) + "." + text;
            }
            return text;
        }

        // An implicit cast is not printed, so its child takes the place of the cast for the parentheses as well.
        @Override
        protected String printWithParentheses(ParseNode node) {
            ParseNode printed = node;
            while (printed instanceof CastExpr && ((CastExpr) printed).isImplicit()) {
                printed = ((CastExpr) printed).getChild(0);
            }
            // A typed constant of the text is a constant under a cast before analysis: it prints as the constant.
            if (unanalyzed && printed instanceof CastExpr && ((CastExpr) printed).getChild(0) instanceof LiteralExpr) {
                return visit(printed);
            }
            return super.printWithParentheses(printed);
        }

        @Override
        public String visitCastExpr(CastExpr node, Void context) {
            if (node.isImplicit()) {
                return visit(node.getChild(0));
            }
            Type target = unanalyzed && node.getTargetTypeDef() != null ? node.getTargetTypeDef().getType()
                    : node.getType();
            return "CAST(" + visit(node.getChild(0)) + " AS " + target.toSql() + ")";
        }

        @Override
        public String visitLiteral(LiteralExpr expr, Void context) {
            if (expr instanceof NullLiteral) {
                return "NULL";
            }
            if (expr instanceof StringLiteral) {
                return "'" + SqlUtils.escapeSqlString(((StringLiteral) expr).getValue()) + "'";
            }
            if (expr instanceof BoolLiteral) {
                return ((BoolLiteral) expr).getValue() ? "TRUE" : "FALSE";
            }
            if (unanalyzed && expr instanceof DecimalLiteral) {
                // A constant of the text stands under the cast that gives its type.
                return ((DecimalLiteral) expr).getValue().toPlainString();
            }
            if (unanalyzed && expr instanceof FloatLiteral) {
                return Double.toString(((FloatLiteral) expr).getValue());
            }
            if (expr instanceof DecimalLiteral) {
                return "CAST(" + ((DecimalLiteral) expr).getValue().toPlainString() + " AS "
                        + expr.getType().toSql() + ")";
            }
            if (expr instanceof FloatLiteral) {
                return "CAST(" + Double.toString(((FloatLiteral) expr).getValue()) + " AS "
                        + expr.getType().toSql() + ")";
            }
            if (expr instanceof DateLiteral) {
                return "CAST('" + expr.getStringValue() + "' AS " + expr.getType().toSql() + ")";
            }
            return expr.getStringValue();
        }
    }
}
