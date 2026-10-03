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

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.starrocks.sql.parser.ErrorMsgProxy.PARSER_ERROR_MSG;

public class PostProcessListener extends com.starrocks.sql.parser.StarRocksBaseListener {

    private final int maxTokensNum;
    private final int maxExprChildCount;

    public PostProcessListener(int maxTokensNum, int maxExprChildCount) {
        this.maxTokensNum = maxTokensNum;
        this.maxExprChildCount = maxExprChildCount;
    }

    private List<Token> parameterTokens;
    private int[] orderedParameterOffsets;

    @Override
    public void exitParameter(StarRocksParser.ParameterContext context) {
        if (parameterTokens == null) {
            parameterTokens = new ArrayList<>();
        }
        parameterTokens.add(context.start);
    }

    LexicalParameterContext parameterContext(ParserRuleContext rule) {
        if (parameterTokens == null) {
            return null;
        }
        int start = rule.start.getStartIndex();
        int stop = rule.stop.getStopIndex();
        if (orderedParameterOffsets == null) {
            orderedParameterOffsets = parameterTokens.stream().mapToInt(Token::getStartIndex).sorted().toArray();
        }
        int first = Arrays.binarySearch(orderedParameterOffsets, start);
        if (first < 0) {
            first = -first - 1;
        }
        int last = Arrays.binarySearch(orderedParameterOffsets, stop);
        if (last < 0) {
            last = -last - 1;
        } else {
            last++;
        }
        return first == last ? null :
                LexicalParameterContext.fromOffsets(Arrays.copyOfRange(orderedParameterOffsets, first, last));
    }

    // Retain only invalid calls; valid aggregate syntax does not allocate a collection.
    private List<StarRocksParser.SimpleFunctionCallContext> invalidCallContexts;

    @Override
    public void exitSimpleFunctionCall(StarRocksParser.SimpleFunctionCallContext context) {
        if (AggregateCallSyntax.classify(context) == AggregateCallSyntax.INVALID) {
            if (invalidCallContexts == null) {
                invalidCallContexts = new ArrayList<>();
            }
            invalidCallContexts.add(context);
        }
    }

    // Factoring moved tuple-IN from predicate to primary; retain its original category.
    // Recursive parents are finalized after exit callbacks, so defer validation.
    // Allocate only for tuple-IN, not ordinary grouping.
    private List<StarRocksParser.ParenthesizedExpressionContext> tupleContexts;

    @Override
    public void exitParenthesizedExpression(StarRocksParser.ParenthesizedExpressionContext context) {
        if (context.queryRelation() != null) {
            if (tupleContexts == null) {
                tupleContexts = new ArrayList<>();
            }
            tupleContexts.add(context);
        }
    }

    /** Called only after the chosen parse entry finishes and recursion parents are final. */
    public void validateTupleContexts() {
        if (invalidCallContexts != null && !invalidCallContexts.isEmpty()) {
            var context = invalidCallContexts.get(0);
            throw new ParsingException("Call syntax is outside the original aggregate/generic contract",
                    new NodePosition(context.start, context.stop));
        }
        if (tupleContexts != null) {
            for (var context : tupleContexts) {
                validateTupleContext(context);
            }
        }
    }

    /** The abandoned SLL tree must not participate in LL validation. */
    public void resetTupleContexts() {
        tupleContexts = null;
        parameterTokens = null;
        orderedParameterOffsets = null;
        invalidCallContexts = null;
    }

    static void validateTupleContext(StarRocksParser.ParenthesizedExpressionContext context) {
        var parent = context.getParent();
        if (parent instanceof StarRocksParser.BooleanExpressionDefaultContext) {
            var enclosing = parent.getParent();
            if (!(enclosing instanceof StarRocksParser.FlatArithmeticBinaryContext) &&
                    !(enclosing instanceof StarRocksParser.PredicatedBooleanExpressionContext)) {
                return;
            }
        } else if (parent instanceof StarRocksParser.ValueExpressionDefaultContext &&
                parent.getParent() instanceof StarRocksParser.PredicateContext predicate &&
                predicate.predicateOperations() == null) {
            return;
        }
        throw new ParsingException("Parentheses required around predicate", new NodePosition(context.start, context.stop));
    }

    @Override
    public void visitTerminal(TerminalNode node) {
        Token token = node.getSymbol();
        int index = token.getTokenIndex();
        if (index >= maxTokensNum) {
            throw new ParsingException(PARSER_ERROR_MSG.tokenExceedLimit());
        }
    }

    @Override
    public void exitExpressionList(com.starrocks.sql.parser.StarRocksParser.ExpressionListContext ctx) {
        long childCount = ctx.children.stream()
                .filter(child -> child instanceof com.starrocks.sql.parser.StarRocksParser.ExpressionContext).count();
        if (childCount > maxExprChildCount) {
            NodePosition pos = new NodePosition(ctx.start, ctx.stop);
            throw new ParsingException(PARSER_ERROR_MSG.exprsExceedLimit(childCount, maxExprChildCount), pos);
        }
    }

    @Override
    public void exitExpressionsWithDefault(com.starrocks.sql.parser.StarRocksParser.ExpressionsWithDefaultContext ctx) {
        long childCount = ctx.expressionOrDefault().size();
        if (childCount > maxExprChildCount) {
            NodePosition pos = new NodePosition(ctx.start, ctx.stop);
            throw new ParsingException(PARSER_ERROR_MSG.argsOfExprExceedLimit(childCount, maxExprChildCount), pos);
        }
    }

    @Override
    public void exitInsertStatement(com.starrocks.sql.parser.StarRocksParser.InsertStatementContext ctx) {
        long childCount = ctx.expressionsWithDefault().size();
        if (childCount > maxExprChildCount) {
            NodePosition pos = new NodePosition(ctx.start, ctx.stop);
            throw new ParsingException(PARSER_ERROR_MSG.insertRowsExceedLimit(childCount, maxExprChildCount), pos);
        }
    }

    @Override
    public void exitIntegerList(com.starrocks.sql.parser.StarRocksParser.IntegerListContext ctx) {
        long childCount = ctx.INTEGER_VALUE().size();
        if (childCount > maxExprChildCount) {
            NodePosition pos = new NodePosition(ctx.start, ctx.stop);
            throw new ParsingException(PARSER_ERROR_MSG.argsOfExprExceedLimit(childCount, maxExprChildCount), pos);
        }
    }

    @Override
    public void exitStringList(com.starrocks.sql.parser.StarRocksParser.StringListContext ctx) {
        long childCount = ctx.string().size();
        if (childCount > maxExprChildCount) {
            NodePosition pos = new NodePosition(ctx.start, ctx.stop);
            throw new ParsingException(PARSER_ERROR_MSG.argsOfExprExceedLimit(childCount, maxExprChildCount), pos);
        }
    }
}