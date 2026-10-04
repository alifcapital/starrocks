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

import com.starrocks.common.Config;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The prediction cache that the ANTLR parsers share: the DFA of every decision and the prediction contexts.
 * ANTLR keeps this cache in static fields and never shrinks it, and one large generated statement can add
 * millions of states to it. We keep the cache here instead, so we can measure it and replace it with an empty
 * one when it grows over parser_dfa_cache_max_states. A parse that runs during the replacement keeps the cache
 * it started with, and the old cache is collected when no parse uses it.
 */
public final class ParserDfaCache {
    private static final Logger LOG = LogManager.getLogger(ParserDfaCache.class);

    // We count the states after this many parses, so the count does not add to every parse.
    private static final int CHECK_INTERVAL = 64;

    private record Generation(DFA[] decisions, PredictionContextCache contexts) {
    }

    private static volatile Generation current = newGeneration();
    private static final AtomicInteger PARSES = new AtomicInteger();
    private static final AtomicLong CLEARS = new AtomicLong();

    private ParserDfaCache() {
    }

    private static Generation newGeneration() {
        ATN atn = StarRocksParser._ATN;
        DFA[] decisions = new DFA[atn.getNumberOfDecisions()];
        for (int i = 0; i < decisions.length; i++) {
            decisions[i] = new DFA(atn.getDecisionState(i), i);
        }
        return new Generation(decisions, new PredictionContextCache());
    }

    static ParserATNSimulator interpreter(StarRocksParser parser) {
        Generation generation = current;
        return new ParserATNSimulator(parser, parser.getATN(), generation.decisions(), generation.contexts());
    }

    /** Replaces the cache with an empty one when it has more states than parser_dfa_cache_max_states. */
    static void afterParse() {
        if (PARSES.incrementAndGet() % CHECK_INTERVAL != 0) {
            return;
        }
        long limit = Config.parser_dfa_cache_max_states;
        Generation generation = current;
        long states = states(generation.decisions());
        if (limit <= 0 || states <= limit) {
            return;
        }
        synchronized (ParserDfaCache.class) {
            if (current != generation) {
                return;
            }
            current = newGeneration();
        }
        CLEARS.incrementAndGet();
        LOG.warn("The parser DFA cache has {} states and {} prediction contexts, more than " +
                        "parser_dfa_cache_max_states = {}. The cache is cleared.", states,
                generation.contexts().size(), limit);
    }

    // DFA.states is a map that ANTLR updates under its own lock. We read its size without the lock, which can be
    // a little stale, and that is enough for a limit.
    private static long states(DFA[] decisions) {
        long states = 0;
        for (DFA decision : decisions) {
            states += decision.states.size();
        }
        return states;
    }

    public static long parserStates() {
        return states(current.decisions());
    }

    public static long parserContexts() {
        return current.contexts().size();
    }

    public static long lexerStates() {
        return states(StarRocksLexer._decisionToDFA);
    }

    public static long clears() {
        return CLEARS.get();
    }

    // Tests check the limit without parsing CHECK_INTERVAL statements.
    static void checkNow() {
        PARSES.set(CHECK_INTERVAL - 1);
        afterParse();
    }
}
