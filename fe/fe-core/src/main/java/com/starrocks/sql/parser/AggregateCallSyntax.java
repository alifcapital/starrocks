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

import org.antlr.v4.runtime.tree.TerminalNode;

/** Classifies the original aggregate/generic syntax union without parsing arguments twice. */
final class AggregateCallSyntax {
    static final int GENERIC = 0;
    static final int AGGREGATE = 1;
    static final int INVALID = 2;

    private AggregateCallSyntax() {
    }

    static int classify(StarRocksParser.SimpleFunctionCallContext context) {
        if (context.functionName() == null || context.functionName().start == null) {
            // A callback can also receive an abandoned or incomplete parse context.
            return GENERIC;
        }
        var qualified = context.functionName().qualifiedName();
        boolean unqualified = qualified == null || qualified.getChildCount() == 1;
        int head = context.functionName().start.getType();
        int argumentCount = 0;
        boolean quantifier = false;
        boolean star = false;
        boolean hint = false;
        boolean order = false;
        boolean separator = false;
        boolean filter = false;
        // expression() builds a list. Count direct children instead, without allocation.
        // This scan is O(number of direct call children), not O(1).
        if (context.children != null) {
            for (var child : context.children) {
                if (child instanceof StarRocksParser.ExpressionContext) {
                    argumentCount++;
                } else if (child instanceof StarRocksParser.SetQuantifierContext) {
                    quantifier = true;
                } else if (child instanceof StarRocksParser.BracketHintContext) {
                    hint = true;
                } else if (child instanceof StarRocksParser.FilterContext) {
                    filter = true;
                } else if (child instanceof TerminalNode terminal) {
                    int type = terminal.getSymbol().getType();
                    if (type == StarRocksParser.ASTERISK_SYMBOL) {
                        star = true;
                    } else if (type == StarRocksParser.ORDER) {
                        order = true;
                    } else if (type == StarRocksParser.SEPARATOR) {
                        separator = true;
                    }
                }
            }
        }
        if (separator) {
            // The separator expression is not a base argument.
            argumentCount--;
        }
        return classify(head, unqualified, argumentCount, quantifier, star, hint, order, separator, filter);
    }

    static int classify(int head, boolean unqualified, int argumentCount, boolean quantifier, boolean star,
                        boolean hint, boolean order, boolean separator, boolean filter) {
        boolean aggregateHead = unqualified && switch (head) {
            case StarRocksParser.AVG, StarRocksParser.MIN, StarRocksParser.MAX, StarRocksParser.SUM,
                    StarRocksParser.ARRAY_AGG, StarRocksParser.ARRAY_AGG_DISTINCT,
                    StarRocksParser.GROUP_CONCAT, StarRocksParser.COUNT -> true;
            default -> false;
        };
        boolean modifiers = quantifier || star || hint || order || separator || filter;
        if (!aggregateHead) {
            return modifiers ? INVALID : GENERIC;
        }
        if (head == StarRocksParser.COUNT) {
            return order || separator || (star && (argumentCount != 0 || quantifier || hint)) || (hint && !quantifier)
                    ? INVALID : AGGREGATE;
        }
        if (star || hint) {
            return INVALID;
        }
        if (head == StarRocksParser.GROUP_CONCAT) {
            return argumentCount > 0 ? AGGREGATE : modifiers ? INVALID : GENERIC;
        }
        if (separator) {
            return INVALID;
        }
        if (head == StarRocksParser.ARRAY_AGG_DISTINCT && quantifier) {
            return INVALID;
        }
        if (head != StarRocksParser.ARRAY_AGG && head != StarRocksParser.ARRAY_AGG_DISTINCT && order) {
            return INVALID;
        }
        if (argumentCount == 1) {
            return AGGREGATE;
        }
        return modifiers ? INVALID : GENERIC;
    }
}
