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

/** Shared conservative attempt budget. Exceeding it requests whole original fallback. */
public final class DirectParseBudget {
    private int expressions;
    private int queries;
    private org.antlr.v4.runtime.CommonTokenStream intervalInput;
    private IntervalState intervalState;

    static final class IntervalState {
        final long limit;
        long spent;
        // 0 unknown, 1 literal, 2 function call; fastutil default return value is 0.
        final it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap classification =
                new it.unimi.dsi.fastutil.ints.Int2ByteOpenHashMap(2);
        // ((close + 1L) << 1) | comma; 0 is absent because close is nonnegative.
        final it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap boundaries =
                new it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap(4);

        IntervalState(int originalTokenCount) {
            limit = 8L * Math.max(1, originalTokenCount);
        }

        void charge(long work) {
            if (work < 0 || work > limit - spent) {
                throw new DirectExpressionParser.UnsupportedExpression(
                        "interval speculative work requires original path");
            }
            spent += work;
        }
    }

    IntervalState intervalState(org.antlr.v4.runtime.CommonTokenStream input) {
        if (intervalInput != input) {
            intervalInput = input;
            intervalState = new IntervalState(input.size());
        }
        return intervalState;
    }

    private long skewLimit;
    private long skewSpent;

    /** Charged only while scanning an actual skew hint; ordinary tokens pay no cost. */
    void chargeSkewScan(int tokenCount) {
        skewLimit = Math.max(skewLimit, 8L * Math.max(1, tokenCount));
        if (skewSpent == skewLimit) {
            throw new DirectQueryParser.UnsupportedQuery("skew hint work requires original path");
        }
        skewSpent++;
    }

    void enterExpression() {
        if (expressions + queries >= 512) {
            throw new DirectExpressionParser.UnsupportedExpression(
                    "shared query/expression nesting limit 512");
        }
        expressions++;
    }

    void exitExpression() {
        expressions--;
    }

    void enterQuery() {
        if (queries >= 64 || expressions + queries >= 512) {
            throw new DirectQueryParser.UnsupportedQuery(
                    "shared query nesting budget requires original path");
        }
        queries++;
    }

    void exitQuery() {
        queries--;
    }

    int queryDepth() {
        return queries;
    }
}
