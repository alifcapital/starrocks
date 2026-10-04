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

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNState;
import org.antlr.v4.runtime.atn.AtomTransition;
import org.antlr.v4.runtime.atn.NotSetTransition;
import org.antlr.v4.runtime.atn.PrecedencePredicateTransition;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.atn.RuleStopState;
import org.antlr.v4.runtime.atn.RuleTransition;
import org.antlr.v4.runtime.atn.SetTransition;
import org.antlr.v4.runtime.atn.Transition;
import org.antlr.v4.runtime.misc.IntervalSet;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The part of the ANTLR grammar that the fast query parser owns: every rule reachable from the query and DML
 * statement rules. Two facts about it are checked against the generated parser:
 * - a fingerprint of each rule, so a grammar change in a new version names the rules to port;
 * - the grammar elements of each rule (labeled alternatives, tokens, called rules), so the fixtures can show
 *   that the fast parser accepts every element of the grammar it owns.
 */
final class QueryGrammar {
    static final List<String> ROOTS = List.of("rootQueryOrDmlStatement", "insertStatement");

    private static final ATN PARSER_ATN = StarRocksParser._ATN;
    private static final Vocabulary VOCABULARY = StarRocksParser.VOCABULARY;
    private static final String[] RULES = StarRocksParser.ruleNames;

    private QueryGrammar() {
    }

    static Set<Integer> reachableRules() {
        Set<Integer> rules = new TreeSet<>();
        Deque<Integer> pending = new ArrayDeque<>();
        for (String root : ROOTS) {
            pending.add(ruleIndex(root));
        }
        while (!pending.isEmpty()) {
            int rule = pending.pop();
            if (!rules.add(rule)) {
                continue;
            }
            for (ATNState state : ruleStates(rule)) {
                for (Transition transition : state.getTransitions()) {
                    if (transition instanceof RuleTransition call) {
                        pending.add(call.target.ruleIndex);
                    }
                }
            }
        }
        return rules;
    }

    /** Rule name to fingerprint of its ATN. Token and rule names are used instead of their numbers. */
    static Map<String, String> fingerprints() {
        Map<String, String> fingerprints = new TreeMap<>();
        for (int rule : reachableRules()) {
            fingerprints.put(RULES[rule], fingerprint(rule));
        }
        return fingerprints;
    }

    /** Elements of the owned rules that a fixture accepted by the fast parser must use at least once. */
    static Set<String> elements() {
        Set<String> elements = new TreeSet<>();
        for (int rule : reachableRules()) {
            for (ATNState state : ruleStates(rule)) {
                for (Transition transition : state.getTransitions()) {
                    if (transition instanceof RuleTransition call) {
                        elements.add(call(rule, call.target.ruleIndex));
                    } else if (transition instanceof AtomTransition
                            || transition instanceof SetTransition && !(transition instanceof NotSetTransition)) {
                        IntervalSet label = transition.label();
                        for (int type : label.toArray()) {
                            elements.add(token(rule, type));
                        }
                    }
                }
            }
            for (Class<?> alternative : labeledAlternatives(rule)) {
                elements.add(alternative(rule, alternative));
            }
        }
        return elements;
    }

    /** Key words that the grammar also accepts as an identifier, as they are written in SQL. */
    static List<String> nonReservedWords() {
        IntervalSet types = PARSER_ATN.nextTokens(PARSER_ATN.ruleToStartState[ruleIndex("nonReserved")]);
        List<String> words = new ArrayList<>();
        for (int type : types.toArray()) {
            String literal = VOCABULARY.getLiteralName(type);
            if (literal != null) {
                words.add(literal.substring(1, literal.length() - 1));
            }
        }
        return words;
    }

    /** Adds the grammar elements that the ANTLR parse tree of the SQL uses. */
    static void collect(String sql, long sqlMode, Set<String> out) {
        StarRocksLexer lexer = new StarRocksLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
        lexer.removeErrorListeners();
        lexer.setSqlMode(sqlMode);
        StarRocksParser parser = new StarRocksParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        parser.removeParseListeners();
        parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        Deque<ParseTree> pending = new ArrayDeque<>();
        pending.add(parser.sqlStatements());
        while (!pending.isEmpty()) {
            ParseTree node = pending.pop();
            if (!(node instanceof ParserRuleContext context)) {
                continue;
            }
            int rule = context.getRuleIndex();
            if (context.getClass() != ruleContext(rule)) {
                out.add(alternative(rule, context.getClass()));
            }
            for (int i = 0; i < context.getChildCount(); i++) {
                ParseTree child = context.getChild(i);
                if (child instanceof TerminalNode terminal) {
                    out.add(token(rule, terminal.getSymbol().getType()));
                } else if (child instanceof ParserRuleContext called) {
                    out.add(call(rule, called.getRuleIndex()));
                    pending.add(called);
                }
            }
        }
    }

    private static String token(int rule, int type) {
        return RULES[rule] + " token " + VOCABULARY.getDisplayName(type);
    }

    private static String call(int rule, int called) {
        return RULES[rule] + " calls " + RULES[called];
    }

    private static String alternative(int rule, Class<?> context) {
        return RULES[rule] + " alternative " + context.getSimpleName();
    }

    private static int ruleIndex(String name) {
        for (int i = 0; i < RULES.length; i++) {
            if (RULES[i].equals(name)) {
                return i;
            }
        }
        throw new IllegalArgumentException("no grammar rule " + name);
    }

    private static Class<?> ruleContext(int rule) {
        String name = Character.toUpperCase(RULES[rule].charAt(0)) + RULES[rule].substring(1) + "Context";
        try {
            return Class.forName(StarRocksParser.class.getName() + "$" + name);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }
    }

    private static List<Class<?>> labeledAlternatives(int rule) {
        Class<?> base = ruleContext(rule);
        List<Class<?>> alternatives = new ArrayList<>();
        for (Class<?> nested : StarRocksParser.class.getDeclaredClasses()) {
            if (nested.getSuperclass() == base) {
                alternatives.add(nested);
            }
        }
        return alternatives;
    }

    // States of one rule in a stable order: depth-first from the rule start, following transitions in order.
    private static List<ATNState> ruleStates(int rule) {
        List<ATNState> states = new ArrayList<>();
        IdentityHashMap<ATNState, Boolean> seen = new IdentityHashMap<>();
        Deque<ATNState> pending = new ArrayDeque<>();
        pending.push(PARSER_ATN.ruleToStartState[rule]);
        while (!pending.isEmpty()) {
            ATNState state = pending.pop();
            if (seen.put(state, Boolean.TRUE) != null) {
                continue;
            }
            states.add(state);
            if (state instanceof RuleStopState) {
                continue;
            }
            for (int i = state.getNumberOfTransitions() - 1; i >= 0; i--) {
                Transition transition = state.transition(i);
                ATNState next = transition instanceof RuleTransition call ? call.followState : transition.target;
                pending.push(next);
            }
        }
        return states;
    }

    private static String fingerprint(int rule) {
        List<ATNState> states = ruleStates(rule);
        IdentityHashMap<ATNState, Integer> number = new IdentityHashMap<>();
        for (ATNState state : states) {
            number.put(state, number.size());
        }
        StringBuilder text = new StringBuilder();
        for (ATNState state : states) {
            text.append(number.get(state)).append(' ').append(state.getClass().getSimpleName()).append(':');
            for (Transition transition : state.getTransitions()) {
                text.append(' ').append(transition.getClass().getSimpleName());
                if (transition instanceof RuleTransition call) {
                    text.append('(').append(RULES[call.target.ruleIndex]).append(' ').append(call.precedence)
                            .append(")->").append(number.get(call.followState));
                    continue;
                }
                if (transition instanceof PrecedencePredicateTransition predicate) {
                    text.append('(').append(predicate.precedence).append(')');
                }
                IntervalSet label = transition.label();
                if (label != null) {
                    text.append('[');
                    for (int type : label.toArray()) {
                        text.append(VOCABULARY.getDisplayName(type)).append(',');
                    }
                    text.append(']');
                }
                text.append("->").append(number.get(transition.target));
            }
            text.append('\n');
        }
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(text.toString().getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }
}
