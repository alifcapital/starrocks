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

import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.dfa.DFAState;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.IntervalSet;

import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

// Cache Unicode lexer transitions by equivalent ATN character classes.
public final class UnicodeLexerATNSimulator extends LexerATNSimulator {
    private static final Map<ATN, int[]> CLASSES = new ConcurrentHashMap<>();
    private final int[] boundaries;
    private final IdentityHashMap<DFAState, DFAState[]> unicodeEdges = new IdentityHashMap<>();

    public UnicodeLexerATNSimulator(Lexer lexer, ATN atn, DFA[] dfa, PredictionContextCache contexts) {
        super(lexer, atn, dfa, contexts);
        boundaries = CLASSES.computeIfAbsent(atn, UnicodeLexerATNSimulator::boundaries);
    }

    // Between consecutive boundaries every transition has identical character membership.
    // This includes exclusions (NotSetTransition) and supplementary Unicode characters.
    private static int[] boundaries(ATN atn) {
        TreeSet<Integer> values = new TreeSet<>();
        values.add(128);
        values.add(0x110000);
        for (ATNState state : atn.states) {
            if (state == null) {
                continue;
            }
            for (var transition : state.getTransitions()) {
                IntervalSet labels = transition.label();
                if (labels == null) {
                    continue;
                }
                for (Interval interval : labels.getIntervals()) {
                    if (interval.a > 128 && interval.a < 0x110000) {
                        values.add(interval.a);
                    }
                    if (interval.b >= 128 && interval.b < 0x10ffff) {
                        values.add(interval.b + 1);
                    }
                }
            }
        }
        return values.stream().mapToInt(Integer::intValue).toArray();
    }

    private int characterClass(int character) {
        int index = Arrays.binarySearch(boundaries, character);
        return index >= 0 ? index : -index - 2;
    }

    @Override
    protected DFAState getExistingTargetState(DFAState source, int character) {
        if (character <= MAX_DFA_EDGE) {
            return super.getExistingTargetState(source, character);
        }
        DFAState[] edges = unicodeEdges.get(source);
        return edges == null ? null : edges[characterClass(character)];
    }

    @Override
    protected void addDFAEdge(DFAState source, int character, DFAState target) {
        if (character <= MAX_DFA_EDGE) {
            super.addDFAEdge(source, character, target);
            return;
        }
        // The superclass invokes this only when caching is safe: predicate-dependent
        // transitions retain its existing uncached behavior. Edges live with this lexer.
        unicodeEdges.computeIfAbsent(source, ignored -> new DFAState[boundaries.length - 1])
                [characterClass(character)] = target;
    }

    @Override
    public void clearDFA() {
        super.clearDFA();
        unicodeEdges.clear();
    }

    static void install(StarRocksLexer lexer) {
        lexer.setInterpreter(new UnicodeLexerATNSimulator(lexer, lexer.getATN(),
                StarRocksLexer._decisionToDFA, StarRocksLexer._sharedContextCache));
    }
}
