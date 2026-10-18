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
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.LikePredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.ConsolidateLikesRule;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.function.Supplier;

/**
 * The rewrites of a condition into the forms the estimators know: a REGEXP of anchored prefixes, a prefix of a column of
 * dates equal to a constant, an IN over an expression whose equality is read, a LIKE of a prefix on a column of dates,
 * and the ranges of one column that keep one value. The tables are a column of strings with the format of its dates
 * and columns of whole numbers and days.
 */
public class EstimatePredicateNormalizerTest {
    private static final ColumnRefOperator TEXT = new ColumnRefOperator(1, VarcharType.VARCHAR, "created_at", true);
    private static final ColumnRefOperator NAME = new ColumnRefOperator(2, VarcharType.VARCHAR, "name", true);
    private static final ColumnRefOperator D = new ColumnRefOperator(3, IntegerType.INT, "d", true);
    private static final ColumnRefOperator E = new ColumnRefOperator(4, IntegerType.INT, "e", true);
    private static final ColumnRefOperator DAY = new ColumnRefOperator(5, DateType.DATE, "day", true);
    private static final ColumnRefOperator UNKNOWN = new ColumnRefOperator(6, IntegerType.INT, "u", true);
    private static final StringDateFormat ISO = StringDateFormat.parse("%Y-%m-%dT%H:%i:%s.%fZ");

    private TimeZone savedZone;

    @BeforeEach
    public void setUp() {
        savedZone = TimeZone.getDefault();
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        new ConnectContext().setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        TimeZone.setDefault(savedZone);
        ConnectContext.remove();
    }

    private static Statistics statistics() {
        return Statistics.builder().setOutputRowCount(1_000_000)
                .addColumnStatistic(TEXT, ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(27)
                        .setDistinctValuesCount(1000).setStringBounds(new StringBounds("2026-08-11T00:00:00.000000Z",
                                "2026-08-19T23:00:00.000000Z", 200, false)).build())
                .addColumnStatistic(NAME, ColumnStatistic.builder().setNullsFraction(0).setAverageRowSize(8)
                        .setDistinctValuesCount(1000).build())
                .addColumnStatistic(D, new ColumnStatistic(17197, 20727, 0, 4, 805))
                .addColumnStatistic(E, new ColumnStatistic(0, 5, 0, 4, 6))
                .addColumnStatistic(DAY, new ColumnStatistic(1.7e9, 1.8e9, 0, 4, 805))
                .build();
    }

    private static Statistics withUnknown() {
        return Statistics.buildFrom(statistics()).addColumnStatistic(UNKNOWN, ColumnStatistic.unknown()).build();
    }

    private static <T> T inScope(Supplier<T> action) {
        return PredicateStatisticsCalculator.withStringDateFormats(column -> TEXT.equals(column) ? ISO : null, action);
    }

    private static ScalarOperator normalize(ScalarOperator predicate) {
        return inScope(() -> EstimatePredicateNormalizer.normalize(predicate, statistics()));
    }

    private static ScalarOperator normalize(ScalarOperator predicate, Statistics statistics) {
        return inScope(() -> EstimatePredicateNormalizer.normalize(predicate, statistics));
    }

    // The same predicate comes back, and a second pass changes nothing.
    private static void assertUnchanged(ScalarOperator predicate) {
        Assertions.assertSame(predicate, normalize(predicate), predicate.toString());
    }

    private static void assertNormalizesTo(ScalarOperator expected, ScalarOperator predicate) {
        ScalarOperator normalized = normalize(predicate);
        Assertions.assertEquals(expected, normalized, predicate.toString());
        Assertions.assertSame(normalized, normalize(normalized), "second pass of " + predicate);
    }

    private static ConstantOperator text(String value) {
        return ConstantOperator.createVarchar(value);
    }

    private static LikePredicateOperator like(ScalarOperator left, String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.LIKE, left, text(pattern));
    }

    private static LikePredicateOperator regexp(ScalarOperator left, String pattern) {
        return new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, left, text(pattern));
    }

    private static ScalarOperator eq(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    private static ScalarOperator compare(BinaryType type, ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(type, left, right);
    }

    private static ScalarOperator in(boolean not, ScalarOperator left, ScalarOperator... values) {
        List<ScalarOperator> children = new ArrayList<>();
        children.add(left);
        children.addAll(List.of(values));
        return new InPredicateOperator(not, children.toArray(new ScalarOperator[0]));
    }

    private static ScalarOperator call(String name, Type returnType, ScalarOperator... arguments) {
        Type[] types = new Type[arguments.length];
        for (int i = 0; i < arguments.length; i++) {
            types[i] = arguments[i].getType();
        }
        Function function = ExprUtils.getBuiltinFunction(name, types, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        return new CallOperator(name, returnType, List.of(arguments), function);
    }

    private static ScalarOperator prefix(String name, ScalarOperator column, int length) {
        return name.startsWith("sub")
                ? call(name, VarcharType.VARCHAR, column, ConstantOperator.createInt(1), ConstantOperator.createInt(length))
                : call(name, VarcharType.VARCHAR, column, ConstantOperator.createInt(length));
    }

    // days_add('1970-01-01', d), the day of the column as a DATETIME.
    private static ScalarOperator dayOfD() {
        return call("days_add", DateType.DATETIME, ConstantOperator.createDatetime(LocalDateTime.of(1970, 1, 1, 0, 0)), D);
    }

    private static ConstantOperator number(int value) {
        return ConstantOperator.createInt(value);
    }

    @Test
    public void testRegexpOfAnchoredPrefixesIsTheOrOfTheirLikeConditions() {
        assertNormalizesTo(like(NAME, "app%"), regexp(NAME, "^app"));
        assertNormalizesTo(Utils.compoundOr(like(NAME, "app%"), like(NAME, "b\\%\\_%")), regexp(NAME, "^app|^b%_"));
        assertNormalizesTo(Utils.compoundOr(List.of(like(NAME, "a%"), like(NAME, "b%"), like(NAME, "c%"))),
                regexp(NAME, "^a|^b|^c"));
        // The left side is any expression, which the LIKE conditions keep.
        ScalarOperator left = prefix("left", NAME, 5);
        assertNormalizesTo(Utils.compoundOr(like(left, "ab%"), like(left, "cd%")), regexp(left, "^ab|^cd"));
        // On a column of dates the LIKE conditions of the prefixes stay LIKE conditions: the estimate of the LIKE chooses
        // the interval of the times for the statistics of the column.
        assertNormalizesTo(like(TEXT, "2026-08-15%"), regexp(TEXT, "^2026-08-15"));
        assertNormalizesTo(Utils.compoundOr(like(TEXT, "2026-08-15%"), like(TEXT, "2026-09%")),
                regexp(TEXT, "^2026-08-15|^2026-09"));
    }

    @Test
    public void testRegexpThatIsNotAnchoredPrefixesStays() {
        for (String pattern : List.of("^103$", "103", "^1(03)", "^103.*", "^103|104", "^[0-9]", "^\\d", "", "^103|",
                "|^103", "^1+03", "^1{2}", "^1?03", "^10\\x", "^10^3")) {
            assertUnchanged(regexp(NAME, pattern));
        }
        // A pattern that is not a constant.
        assertUnchanged(new LikePredicateOperator(LikePredicateOperator.LikeType.REGEXP, NAME, NAME));
    }

    @Test
    public void testAnchoredPrefixesOfARegexp() {
        Assertions.assertEquals(Optional.of(List.of("abc")), EstimatePredicateNormalizer.anchoredPrefixes("^abc"));
        Assertions.assertEquals(Optional.of(List.of("42894", "40406")),
                EstimatePredicateNormalizer.anchoredPrefixes("^42894|^40406"));
        Assertions.assertEquals(Optional.of(List.of("")), EstimatePredicateNormalizer.anchoredPrefixes("^"));
        // A backslash makes a metacharacter a literal, and the prefix is escaped in the LIKE pattern.
        Assertions.assertEquals(Optional.of(List.of("a.b", "c|d", "x\\y", "$1", "^2", "a+b")),
                EstimatePredicateNormalizer.anchoredPrefixes("^a\\.b|^c\\|d|^x\\\\y|^\\$1|^\\^2|^a\\+b"));
        Assertions.assertEquals("\\%\\_\\\\a%", EstimatePredicateNormalizer.likeOfPrefix("%_\\a"));
        // Anything else is not a prefix.
        for (String other : List.of("", "abc", "^abc$", "^a.c", "^a*", "^a+", "^a?", "^(a|b)", "^[ab]", "^a{2}", "^a|b",
                "b|^a", "^a|", "|^a", "^a||^b", "^a\\d", "^a\\", "^a^b", "^a\\-")) {
            Assertions.assertEquals(Optional.empty(), EstimatePredicateNormalizer.anchoredPrefixes(other), other);
        }
        Assertions.assertEquals(Optional.of(List.of("ab")),
                EstimatePredicateNormalizer.anchoredPrefixes(regexp(NAME, "^ab")));
        // A pattern of LIKE is not a regular expression.
        Assertions.assertEquals(Optional.empty(), EstimatePredicateNormalizer.anchoredPrefixes(like(NAME, "^a")));
    }

    @Test
    public void testAnchoredPrefixesOfTheRegexpThatConsolidateLikesRuleWrites() {
        Assertions.assertEquals(Optional.of(List.of("p", "q")),
                EstimatePredicateNormalizer.anchoredPrefixes("^((p.*.*)|(q.*.*))$"));
        Assertions.assertEquals(Optional.of(List.of("42894", "40406", "4")),
                EstimatePredicateNormalizer.anchoredPrefixes("^((42894.*.*)|(40406.*.*.*)|(4.*))$"));
        // The pattern '%%' is every string, and the group of it is the empty prefix.
        Assertions.assertEquals(Optional.of(List.of("", "a")),
                EstimatePredicateNormalizer.anchoredPrefixes("^((.*.*)|(a.*.*))$"));
        // The characters of a regular expression after a backslash, a % and an _ after a backslash, and the line break,
        // the return and the tab that the rule writes as \n, \r and \t.
        Assertions.assertEquals(Optional.of(List.of("a.b", "c|d", "x\\y", "$1", "e_f", "g%h", "i\nj", "k\rl", "m\tn", "(o)",
                "[p]", "{q}", "r*s", "t+u", "v?w", "^x")),
                EstimatePredicateNormalizer.anchoredPrefixes("^((a\\.b.*.*)|(c\\|d.*.*)|(x\\\\y.*.*)|(\\$1.*.*)|(e\\_f.*.*)|"
                        + "(g\\%h.*.*)|(i\\nj.*.*)|(k\\rl.*.*)|(m\\tn.*.*)|(\\(o\\).*.*)|(\\[p\\].*.*)|(\\{q\\}.*.*)|"
                        + "(r\\*s.*.*)|(t\\+u.*.*)|(v\\?w.*.*)|(\\^x.*.*))$"));
        // Any other group leaves the whole expression as it is: an exact string, a dot that is an _, a wildcard before
        // the end, a group that is not the one of the rule, a class, an end anchor, an unknown escape.
        for (String other : List.of("^((a.*.*)|(b))$", "^((a.*.*)|(b.c.*))$", "^((a.*.*)|(b.*c.*))$", "^((a.*.*)|(^$))$",
                "^(a.*.*)$", "^((a.*.*)|b.*)$", "^((a.*.*)(b.*.*))$", "^((a.*.*)|(b.*.*)$", "^((a.*.*)|(b.*.*)))$",
                "^((a.*.*)|(b(c).*))$", "^((a.*.*)|(b[c].*))$", "^((a.*.*)|(b.*$))$", "^((a.*.*)|(b\\d.*))$",
                "^((a.*.*)|(b\\.*))$", "^((a.*.*)|())$", "^(())$", "^()$", "^((a.*.*)|)$", "^((a.*.*)||(b.*.*))$",
                "((p.*.*)|(q.*.*))$", "^((p.*.*)|(q.*.*))", "^((p.*.*)|(q.*.*)) $", "^((a.+))$", "^((a.))$")) {
            Assertions.assertEquals(Optional.empty(), EstimatePredicateNormalizer.anchoredPrefixes(other), other);
        }
    }

    // The pattern that ConsolidateLikesRule writes when it ends it with \z and not with $.
    private static String endedWithEndOfText(String regexp) {
        Assertions.assertTrue(regexp.endsWith(")$"), regexp);
        return regexp.substring(0, regexp.length() - 1) + "\\z";
    }

    @Test
    public void testRegexpEndedWithEndOfTextIsReadAsTheOneEndedWithDollar() {
        List<String> read = List.of("^((p.*.*)|(q.*.*))$", "^((42894.*.*)|(40406.*.*.*)|(4.*))$", "^((.*.*)|(a.*.*))$",
                "^((a\\.b.*.*)|(c\\|d.*.*)|(x\\\\y.*.*)|(\\$1.*.*)|(e\\_f.*.*)|(g\\%h.*.*)|(i\\nj.*.*)|(k\\rl.*.*)|"
                        + "(m\\tn.*.*)|(\\(o\\).*.*)|(\\^x.*.*))$", "^((a.*.*))$");
        for (String dollar : read) {
            String endOfText = endedWithEndOfText(dollar);
            Assertions.assertTrue(endOfText.endsWith(")\\z"), endOfText);
            Optional<List<String>> prefixes = EstimatePredicateNormalizer.anchoredPrefixes(dollar);
            Assertions.assertTrue(prefixes.isPresent(), dollar);
            Assertions.assertEquals(prefixes, EstimatePredicateNormalizer.anchoredPrefixes(endOfText), endOfText);
            // The OR of the LIKE conditions of the same prefixes, as the optimizer writes them.
            ScalarOperator normalized = normalize(regexp(NAME, endOfText));
            Assertions.assertEquals(normalize(regexp(NAME, dollar)), normalized, endOfText);
            Assertions.assertEquals(Utils.compoundOr(prefixes.get().stream()
                    .map(prefix -> (ScalarOperator) like(NAME, EstimatePredicateNormalizer.likeOfPrefix(prefix))).toList()),
                    normalized, endOfText);
        }
        // What is not read with $ is not read with \z.
        for (String other : List.of("^((a.*.*)|(b))$", "^((a.*.*)|(b.c.*))$", "^((a.*.*)|(b.*c.*))$", "^(a.*.*)$",
                "^((a.*.*)|(b.*.*)$", "^((a.*.*)|(b.*.*)))$", "^((a.*.*)|(b[c].*))$", "^((a.*.*)|(b\\d.*))$", "^((a.*.*)|())$",
                "^(())$", "^()$", "^((a.*.*)|)$", "((p.*.*)|(q.*.*))$", "^((a.+))$", "^((a.))$")) {
            Assertions.assertEquals(Optional.empty(), EstimatePredicateNormalizer.anchoredPrefixes(endedWithEndOfText(other)),
                    other);
        }
        // Other endings are not read: no ending, an escaped dollar, \Z, \z with a backslash before it, and \z in a group.
        for (String other : List.of("^((p.*.*)|(q.*.*))", "^((p.*.*)|(q.*.*))\\$", "^((p.*.*)|(q.*.*))\\Z",
                "^((p.*.*)|(q.*.*))\\\\z", "^((p.*.*)|(q.*.*))z", "^((p.*.*)|(q\\z.*.*))$", "^((p.*.*)|(q.*.*))\\z$",
                "^((p.*.*)|(q.*.*)\\z", "^((p.*.*)|(q.*.*)x\\z", "^((p.*.*)|(q.*.*)x$")) {
            Assertions.assertEquals(Optional.empty(), EstimatePredicateNormalizer.anchoredPrefixes(other), other);
        }
        // The REGEXP that is not merged is the same with either ending, that is, it stays.
        assertUnchanged(regexp(NAME, "^((a.*.*)|(b))\\z"));
    }

    @Test
    public void testRegexpThatConsolidateLikesRuleEndsWithEndOfTextIsTheOrOfTheLikeConditionsOfItsPrefixes() {
        ScalarOperator likes = Utils.compoundOr(like(NAME, "p%%"), like(NAME, "q%%"));
        LikePredicateOperator merged = (LikePredicateOperator) ConsolidateLikesRule.INSTANCE.apply(likes, null);
        // The pattern of the rule in the branch where it ends with \z: ^((p.*.*)|(q.*.*))\z.
        ScalarOperator endOfText = regexp(NAME, endedWithEndOfText(((ConstantOperator) merged.getChild(1)).getVarchar()));
        Assertions.assertEquals(regexp(NAME, "^((p.*.*)|(q.*.*))\\z"), endOfText);
        assertNormalizesTo(Utils.compoundOr(like(NAME, "p%"), like(NAME, "q%")), endOfText);
        // The AND of the negations is the NOT of the REGEXP.
        assertNormalizesTo(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                Utils.compoundOr(like(NAME, "p%"), like(NAME, "q%"))),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, endOfText));
        // Several patterns, as the rule writes them for the OR of three.
        ScalarOperator three = ConsolidateLikesRule.INSTANCE.apply(Utils.compoundOr(List.of(like(NAME, "a%%"),
                like(NAME, "b%%%"), like(NAME, "c%%"))), null);
        assertNormalizesTo(Utils.compoundOr(List.of(like(NAME, "a%"), like(NAME, "b%"), like(NAME, "c%"))),
                regexp(NAME, endedWithEndOfText(((ConstantOperator) three.getChild(1)).getVarchar())));
    }

    @Test
    public void testRegexpThatConsolidateLikesRuleWritesIsTheOrOfTheLikeConditionsOfItsPrefixes() {
        ScalarOperator likes = Utils.compoundOr(like(NAME, "p%%"), like(NAME, "q%%"));
        ScalarOperator consolidated = ConsolidateLikesRule.INSTANCE.apply(likes, null);
        Assertions.assertEquals(regexp(NAME, "^((p.*.*)|(q.*.*))$"), consolidated);
        assertNormalizesTo(Utils.compoundOr(like(NAME, "p%"), like(NAME, "q%")), consolidated);
        // The rule writes the same for the OR of several patterns, and for the AND of their negations.
        assertNormalizesTo(Utils.compoundOr(List.of(like(NAME, "a%"), like(NAME, "b%"), like(NAME, "c%"))),
                ConsolidateLikesRule.INSTANCE.apply(Utils.compoundOr(List.of(like(NAME, "a%%"), like(NAME, "b%%%"),
                        like(NAME, "c%%"))), null));
        ScalarOperator notLikes = Utils.compoundAnd(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                like(NAME, "p%%")), new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                like(NAME, "q%%")));
        ScalarOperator consolidatedNot = ConsolidateLikesRule.INSTANCE.apply(notLikes, null);
        Assertions.assertEquals(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                regexp(NAME, "^((p.*.*)|(q.*.*))$")), consolidatedNot);
        assertNormalizesTo(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT,
                Utils.compoundOr(like(NAME, "p%"), like(NAME, "q%"))), consolidatedNot);
        // A pattern that is not a prefix next to the prefixes: the REGEXP stays as the rule wrote it.
        ScalarOperator mixed = ConsolidateLikesRule.INSTANCE.apply(Utils.compoundOr(List.of(like(NAME, "p%%"),
                like(NAME, "q%%"), like(NAME, "r"))), null);
        assertUnchanged(mixed);
        ScalarOperator inside = ConsolidateLikesRule.INSTANCE.apply(Utils.compoundOr(List.of(like(NAME, "p%%"),
                like(NAME, "q_%"))), null);
        assertUnchanged(inside);
    }

    @Test
    public void testRegexpThatConsolidateLikesRuleWritesKeepsTheStringsOfTheLikeConditions() {
        var random = new java.util.Random(91);
        String[] literals = {"a", "b", "1", ".", "%", "_", "\\", "|", "$", "^", "+", "(", ")", "*", "[", "?", "-", " ", "\n"};
        String[] texts = {"a", "b", "1", ".", "%", "_", "\\", "|", "$", "^", "+", "(", ")", "*", "[", "?", "-", " ", "\n", "x"};
        int compared = 0;
        for (int iteration = 0; iteration < 4000; iteration++) {
            List<ScalarOperator> likes = new ArrayList<>();
            for (int part = 2 + random.nextInt(2); part > 0; part--) {
                var pattern = new StringBuilder();
                for (int i = random.nextInt(4); i > 0; i--) {
                    String literal = literals[random.nextInt(literals.length)];
                    pattern.append(literal.equals("%") || literal.equals("_") || literal.equals("\\") ? "\\" + literal : literal);
                }
                pattern.append("%".repeat(2 + random.nextInt(2)));
                likes.add(like(NAME, pattern.toString()));
            }
            ScalarOperator consolidated = ConsolidateLikesRule.INSTANCE.apply(Utils.compoundOr(likes), null);
            Assertions.assertTrue(consolidated instanceof LikePredicateOperator regexp && regexp.isRegexp(),
                    likes.toString());
            ScalarOperator normalized = normalize(consolidated);
            Assertions.assertNotSame(consolidated, normalized, consolidated.toString());
            var text = new StringBuilder();
            for (int i = random.nextInt(6); i > 0; i--) {
                text.append(texts[random.nextInt(texts.length)]);
            }
            boolean expected = likes.stream().anyMatch(part -> LikePatternEstimator
                    .pattern((LikePredicateOperator) part).orElseThrow().matches(text.toString()));
            boolean actual = Utils.extractDisjunctive(normalized).stream().anyMatch(part -> LikePatternEstimator
                    .pattern((LikePredicateOperator) part).orElseThrow().matches(text.toString()));
            Assertions.assertEquals(expected, actual, likes + " / " + consolidated + " / " + text);
            compared++;
        }
        Assertions.assertEquals(4000, compared);
    }

    @Test
    public void testRegexpOfAnchoredPrefixesMatchesAsTheRegularExpressionDoes() {
        var random = new java.util.Random(77);
        String[] literals = {"a", "b", "1", ".", "%", "_", "\\", "|", "$", "^", "+"};
        String[] texts = {"a", "b", "1", ".", "%", "_", "\\", "|", "$", "^", "+", "😀", "\n"};
        int compared = 0;
        for (int iteration = 0; iteration < 5000; iteration++) {
            var regexp = new StringBuilder();
            for (int part = 1 + random.nextInt(3); part > 0; part--) {
                regexp.append(regexp.length() > 0 ? "|^" : "^");
                for (int i = random.nextInt(4); i > 0; i--) {
                    String literal = literals[random.nextInt(literals.length)];
                    regexp.append("%_ab1".contains(literal) ? literal : "\\" + literal);
                }
            }
            var text = new StringBuilder();
            for (int i = random.nextInt(7); i > 0; i--) {
                text.append(texts[random.nextInt(texts.length)]);
            }
            ScalarOperator likes = normalize(regexp(NAME, regexp.toString()));
            boolean matches = Utils.extractDisjunctive(likes).stream().anyMatch(part -> LikePatternEstimator
                    .pattern((LikePredicateOperator) part).orElseThrow().matches(text.toString()));
            Assertions.assertEquals(java.util.regex.Pattern.compile(regexp.toString()).matcher(text).find(), matches,
                    regexp + " / " + text);
            compared++;
        }
        Assertions.assertEquals(5000, compared);
    }

    private static final ColumnRefOperator VIRTUAL = new ColumnRefOperator(-1, VarcharType.VARCHAR, "expr#1", true);

    // The format of dates for every column of the query, the virtual column and the columns of other types as well.
    private static <T> T withFormatForEveryColumn(StringDateFormat format, Supplier<T> action) {
        return PredicateStatisticsCalculator.withStringDateFormats(column -> format, action);
    }

    private static Statistics withVirtual() {
        return Statistics.buildFrom(statistics()).addColumnStatistic(VIRTUAL, statistics().getColumnStatistic(TEXT)).build();
    }

    @Test
    public void testColumnOfDatesIsAStringColumnWithADayThatIsNotVirtual() {
        StringDateFormat month = StringDateFormat.parse("%Y-%m");
        Assertions.assertSame(ISO, withFormatForEveryColumn(ISO, () -> PredicateStatisticsCalculator.dayFormatOf(TEXT)));
        Assertions.assertSame(ISO, withFormatForEveryColumn(ISO, () -> PredicateStatisticsCalculator.dayFormatOf(NAME)));
        // The virtual column holds the first characters of a column, a format with no day casts to NULL, a column of
        // another type is not a string, and a query without a format has no column of dates.
        Assertions.assertNull(withFormatForEveryColumn(ISO, () -> PredicateStatisticsCalculator.dayFormatOf(VIRTUAL)));
        Assertions.assertNull(withFormatForEveryColumn(ISO, () -> PredicateStatisticsCalculator.dayFormatOf(D)));
        Assertions.assertNull(withFormatForEveryColumn(month, () -> PredicateStatisticsCalculator.dayFormatOf(TEXT)));
        Assertions.assertNull(PredicateStatisticsCalculator.dayFormatOf(TEXT));
        Assertions.assertNull(withFormatForEveryColumn(null, () -> PredicateStatisticsCalculator.dayFormatOf(TEXT)));
    }

    @Test
    public void testFourReadersOfAColumnOfDatesAgreeOnWhichColumnIsOne() {
        StringDateFormat month = StringDateFormat.parse("%Y-%m");
        ScalarOperator inOf = in(false, TEXT, text("2026-08-15"), text("2026-08-16"));
        for (ColumnRefOperator column : List.of(TEXT, NAME, VIRTUAL, D)) {
            boolean isDate = column == TEXT || column == NAME;
            ScalarOperator in = in(false, column, text("2026-08-15"), text("2026-08-16"));
            ScalarOperator prefixEquality = eq(prefix("left", column, 10), text("2026-08-15"));
            ScalarOperator comparison = compare(BinaryType.GE, column, text("2026-08-15"));
            for (StringDateFormat format : new StringDateFormat[] {ISO, month}) {
                boolean expected = isDate && format == ISO;
                String message = column.getName() + " " + format;
                // The IN of the column of dates is the OR of equalities, and the candidate of the normalizer.
                Assertions.assertEquals(expected, withFormatForEveryColumn(format,
                        () -> EstimatePredicateNormalizer.inEqualities((InPredicateOperator) in).isPresent()), message);
                Assertions.assertEquals(expected, withFormatForEveryColumn(format,
                        () -> EstimatePredicateNormalizer.hasCandidate(in)), message);
                // The prefix of the column is its LIKE.
                Assertions.assertEquals(expected, withFormatForEveryColumn(format,
                        () -> EstimatePredicateNormalizer.normalize(prefixEquality, withVirtual()) != prefixEquality), message);
                // The comparison of the column is the comparison of its cast to the time.
                Assertions.assertEquals(expected, withFormatForEveryColumn(format, () -> PredicateStatisticsCalculator
                        .asDatetimeComparison((BinaryPredicateOperator) comparison) != null), message);
            }
        }
        // The column of dates is a candidate with a format, and is not one without.
        Assertions.assertTrue(inScope(() -> EstimatePredicateNormalizer.hasCandidate(inOf)));
        Assertions.assertFalse(EstimatePredicateNormalizer.hasCandidate(inOf));
        // A string column of a table with no format of dates is no candidate, whatever the list.
        Assertions.assertFalse(inScope(() -> EstimatePredicateNormalizer.hasCandidate(in(false, NAME, text("a"), text("b")))));
    }

    @Test
    public void testPeriodOfAPrefixIsEstimatedOnlyForAColumnOfDates() {
        // The LIKE of a prefix on a column of dates is estimated as the period, and on the virtual column, whatever the
        // formats of the query say, as on any column without one.
        ScalarOperator onText = like(TEXT, "2026-08-1%");
        ScalarOperator onVirtual = like(VIRTUAL, "2026-08-1%");
        double text = withFormatForEveryColumn(ISO, () -> PredicateStatisticsCalculator
                .statisticsCalculate(onText, withVirtual()).getOutputRowCount());
        double textWithoutFormat = PredicateStatisticsCalculator.statisticsCalculate(onText, withVirtual())
                .getOutputRowCount();
        Assertions.assertNotEquals(textWithoutFormat, text, 1.0);
        double virtual = withFormatForEveryColumn(ISO, () -> PredicateStatisticsCalculator
                .statisticsCalculate(onVirtual, withVirtual()).getOutputRowCount());
        double virtualWithoutFormat = PredicateStatisticsCalculator.statisticsCalculate(onVirtual, withVirtual())
                .getOutputRowCount();
        Assertions.assertEquals(virtualWithoutFormat, virtual, 1e-6);
    }

    @Test
    public void testPrefixEqualityOnAColumnOfDatesIsItsLike() {
        for (String name : List.of("left", "strleft", "substr", "substring")) {
            assertNormalizesTo(like(TEXT, "2026-08-15%"), eq(prefix(name, TEXT, 10), text("2026-08-15")));
            assertNormalizesTo(like(TEXT, "2026-08%"), eq(prefix(name, TEXT, 7), text("2026-08")));
        }
        // The prefix of the constant is escaped as a LIKE pattern.
        assertNormalizesTo(like(TEXT, "2026\\_\\%%"), eq(prefix("left", TEXT, 6), text("2026_%")));
    }

    @Test
    public void testPrefixIsReadFromTheCanonicalFormOfTheStatisticsOnPrefixes() {
        ScalarOperator canonicalPrefix = prefix("substr", TEXT, 10);
        // Every spelling that the canonical form writes as substr(c, 1, n), and the cast of the column to VARCHAR that it
        // takes off, is the prefix of the column; so is an upper case name.
        ScalarOperator castColumn = new CastOperator(VarcharType.VARCHAR, TEXT);
        for (ScalarOperator spelled : List.of(prefix("left", castColumn, 10), prefix("strleft", castColumn, 10),
                prefix("substr", castColumn, 10), prefix("substring", castColumn, 10),
                call("LEFT", VarcharType.VARCHAR, TEXT, ConstantOperator.createInt(10)),
                call("substr", VarcharType.VARCHAR, TEXT, ConstantOperator.createBigint(1),
                        ConstantOperator.createInt(10)))) {
            assertNormalizesTo(like(TEXT, "2026-08-15%"), eq(spelled, text("2026-08-15")));
        }
        // They agree with the rule that the statistics on prefixes write the conditions with.
        Assertions.assertArrayEquals(new int[] {1, 10}, PrefixDerivationRule.substrPositions(
                ExpressionStatisticsCanonicalizer.canonicalize(prefix("left", TEXT, 10))));
        Assertions.assertArrayEquals(new int[] {1, 10}, PrefixDerivationRule.substrPositions(
                ExpressionStatisticsCanonicalizer.canonicalize(canonicalPrefix)));
        // The prefix of no characters is every string that is not NULL.
        assertNormalizesTo(like(TEXT, "%"), eq(prefix("left", TEXT, 0), text("")));
        // The first characters of an expression, or from another place, are not the prefix of a column.
        assertUnchanged(eq(prefix("left", call("lower", VarcharType.VARCHAR, TEXT), 10), text("2026-08-15")));
        assertUnchanged(eq(call("substr", VarcharType.VARCHAR, TEXT, ConstantOperator.createInt(0),
                ConstantOperator.createInt(10)), text("2026-08-15")));
        assertUnchanged(eq(call("substr", VarcharType.VARCHAR, TEXT, D, ConstantOperator.createInt(10)),
                text("2026-08-15")));
        assertUnchanged(eq(call("substr", VarcharType.VARCHAR, TEXT, ConstantOperator.createInt(1)),
                text("2026-08-15")));
        assertUnchanged(eq(prefix("left", TEXT, -1), text("")));
    }

    @Test
    public void testPrefixEqualityThatIsNotAPrefixOfDatesStays() {
        // The constant is not as long as the prefix, the call is not a prefix from the first character, the column has
        // no format of dates, or the comparison is not an equality or has no constant.
        assertUnchanged(eq(prefix("left", TEXT, 10), text("2026-08")));
        assertUnchanged(eq(call("substr", VarcharType.VARCHAR, TEXT, ConstantOperator.createInt(2),
                ConstantOperator.createInt(4)), text("026-")));
        assertUnchanged(eq(prefix("left", NAME, 4), text("abcd")));
        assertUnchanged(compare(BinaryType.GE, prefix("left", TEXT, 10), text("2026-08-15")));
        assertUnchanged(eq(prefix("left", TEXT, 10), ConstantOperator.createNull(VarcharType.VARCHAR)));
        assertUnchanged(eq(prefix("left", TEXT, 10), prefix("left", NAME, 10)));
        // Without the format of dates in the scope of the estimate.
        ScalarOperator equality = eq(prefix("left", TEXT, 10), text("2026-08-15"));
        Assertions.assertSame(equality, EstimatePredicateNormalizer.normalize(equality, statistics()));
    }

    @Test
    public void testLikeOnAColumnOfDatesStays() {
        // The interval of the times is chosen by the estimate of the LIKE for the statistics of the column, after the
        // statistics on expressions have read the prefix.
        for (String pattern : List.of("2026-08-15%", "2026-08-1%", "2026-0%", "a\\%b%", "2026-08-1_%", "%2026",
                "2026-08-15")) {
            assertUnchanged(like(TEXT, pattern));
        }
        assertUnchanged(like(NAME, "2026-08-15%"));
        assertUnchanged(like(prefix("left", TEXT, 10), "2026-08-15%"));
        ScalarOperator like = like(TEXT, "2026-08-15%");
        Assertions.assertSame(like, EstimatePredicateNormalizer.normalize(like, statistics()));
    }

    @Test
    public void testInOverAColumnOfDatesAndItsPrefixIsTheOrOfEqualities() {
        String first = "2026-08-15T00:00:00.000000Z";
        String second = "2026-08-16T00:00:00.000000Z";
        assertNormalizesTo(Utils.compoundOr(eq(TEXT, text(first)), eq(TEXT, text(second))),
                in(false, TEXT, text(first), text(second)));
        assertNormalizesTo(Utils.compoundOr(like(TEXT, "2026-08-15%"), like(TEXT, "2026-08-16%")),
                in(false, prefix("left", TEXT, 10), text("2026-08-15"), text("2026-08-16")));
        // A constant of another length stays an equality of the prefix.
        assertNormalizesTo(Utils.compoundOr(like(TEXT, "2026-08-15%"),
                eq(prefix("left", TEXT, 10), text("2026-08"))), in(false, prefix("left", TEXT, 10), text("2026-08-15"),
                text("2026-08")));
        // A NULL and a repeated value are not values.
        assertNormalizesTo(eq(TEXT, text(first)), in(false, TEXT, text(first), text(first),
                ConstantOperator.createNull(VarcharType.VARCHAR)));
        assertUnchanged(in(false, TEXT, ConstantOperator.createNull(VarcharType.VARCHAR)));
    }

    @Test
    public void testInOverAFunctionOfAColumnIsTheOrOfEqualities() {
        ConstantOperator first = ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 1, 0, 0));
        ConstantOperator second = ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 2, 0, 0));
        assertNormalizesTo(Utils.compoundOr(eq(dayOfD(), first), eq(dayOfD(), second)),
                in(false, dayOfD(), first, second));
        // A function whose comparison with a constant reads no bound on its column.
        ScalarOperator lower = call("lower", VarcharType.VARCHAR, NAME);
        assertUnchanged(in(false, lower, text("a"), text("b")));
    }

    @Test
    public void testInThatStaysIn() {
        String first = "2026-08-15T00:00:00.000000Z";
        String second = "2026-08-16T00:00:00.000000Z";
        // NOT IN is the complement of the IN, which the estimate of the IN gives.
        assertUnchanged(in(true, TEXT, text(first), text(second)));
        assertUnchanged(in(true, dayOfD(), ConstantOperator.createDatetime(LocalDateTime.of(2026, 8, 1, 0, 0))));
        // Next to a condition that is rewritten, the NOT IN stays as it is.
        ScalarOperator notIn = in(true, TEXT, text(first), text(second));
        ScalarOperator around = normalize(Utils.compoundAnd(regexp(NAME, "^a|^b"), notIn));
        Assertions.assertEquals(Utils.compoundOr(like(NAME, "a%"), like(NAME, "b%")), around.getChild(0));
        Assertions.assertSame(notIn, around.getChild(1));
        // A column that is not of dates, a list of an expression, a list longer than the limit.
        assertUnchanged(in(false, NAME, text("a"), text("b")));
        assertUnchanged(in(false, TEXT, text(first), NAME));
        List<ScalarOperator> values = new ArrayList<>();
        for (int i = 0; i <= StatisticsEstimateCoefficient.IN_AS_OR_LIMIT; i++) {
            values.add(text(String.format("2026-08-15T00:00:00.%06dZ", i)));
        }
        assertUnchanged(in(false, TEXT, values.toArray(new ScalarOperator[0])));
        assertNormalizesTo(Utils.compoundOr(values.subList(0, StatisticsEstimateCoefficient.IN_AS_OR_LIMIT).stream()
                        .map(value -> eq(TEXT, value)).toList()),
                in(false, TEXT, values.subList(0, StatisticsEstimateCoefficient.IN_AS_OR_LIMIT)
                        .toArray(new ScalarOperator[0])));
    }

    @Test
    public void testSidesOfAPointAreTheEquality() {
        ScalarOperator five = eq(D, number(5));
        assertNormalizesTo(five, Utils.compoundAnd(compare(BinaryType.GE, D, number(5)),
                compare(BinaryType.LE, D, number(5))));
        assertNormalizesTo(five, Utils.compoundAnd(compare(BinaryType.LE, D, number(5)),
                compare(BinaryType.GE, D, number(5))));
        assertNormalizesTo(five, Utils.compoundAnd(compare(BinaryType.GT, D, number(4)),
                compare(BinaryType.LT, D, number(6))));
        assertNormalizesTo(five, Utils.compoundAnd(compare(BinaryType.GE, D, number(5)),
                compare(BinaryType.LT, D, number(6))));
        // The equality stands at the place of the first side, and the other conjuncts keep their places.
        assertNormalizesTo(Utils.compoundAnd(List.of(eq(E, number(1)), five, compare(BinaryType.GT, E, number(0)))),
                Utils.compoundAnd(List.of(eq(E, number(1)), compare(BinaryType.GE, D, number(5)),
                        compare(BinaryType.GT, E, number(0)), compare(BinaryType.LE, D, number(5)))));
        // Two columns.
        assertNormalizesTo(Utils.compoundAnd(five, eq(E, number(2))), Utils.compoundAnd(List.of(
                compare(BinaryType.GE, D, number(5)), compare(BinaryType.GE, E, number(2)),
                compare(BinaryType.LE, D, number(5)), compare(BinaryType.LE, E, number(2)))));
        // Days.
        ConstantOperator march = ConstantOperator.createDate(LocalDateTime.of(2026, 3, 1, 0, 0));
        ConstantOperator april = ConstantOperator.createDate(LocalDateTime.of(2026, 3, 2, 0, 0));
        assertNormalizesTo(eq(DAY, march), Utils.compoundAnd(compare(BinaryType.GE, DAY, march),
                compare(BinaryType.LT, DAY, april)));
        // A function of the column that reduces to the bounds on it.
        ConstantOperator day = ConstantOperator.createDatetime(LocalDate.ofEpochDay(20000).atStartOfDay());
        assertNormalizesTo(eq(D, number(20000)), Utils.compoundAnd(compare(BinaryType.GE, dayOfD(), day),
                compare(BinaryType.LE, dayOfD(), day)));
        // Within an OR and a NOT.
        ScalarOperator sides = Utils.compoundAnd(compare(BinaryType.GE, D, number(5)),
                compare(BinaryType.LE, D, number(5)));
        assertNormalizesTo(Utils.compoundOr(five, eq(E, number(1))), Utils.compoundOr(sides, eq(E, number(1))));
        assertNormalizesTo(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, five),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, sides));
    }

    @Test
    public void testSidesThatKeepMoreThanAPointStay() {
        assertUnchanged(Utils.compoundAnd(compare(BinaryType.GE, D, number(5)), compare(BinaryType.LE, D, number(6))));
        assertUnchanged(Utils.compoundAnd(compare(BinaryType.GE, D, number(5)), compare(BinaryType.LE, E, number(5))));
        assertUnchanged(Utils.compoundAnd(compare(BinaryType.GE, D, number(5)), eq(D, number(5))));
        assertUnchanged(Utils.compoundAnd(compare(BinaryType.GE, D, number(5)), compare(BinaryType.GE, E, number(5))));
        // A column without statistics keeps its ranges.
        ScalarOperator unknown = Utils.compoundAnd(compare(BinaryType.GE, UNKNOWN, number(5)),
                compare(BinaryType.LE, UNKNOWN, number(5)));
        Assertions.assertSame(unknown, normalize(unknown, withUnknown()));
        // The ends of the longs are not read.
        ColumnRefOperator big = new ColumnRefOperator(7, IntegerType.BIGINT, "big", true);
        ScalarOperator end = Utils.compoundAnd(compare(BinaryType.GT, big, ConstantOperator.createBigint(Long.MAX_VALUE)),
                compare(BinaryType.LE, big, ConstantOperator.createBigint(Long.MAX_VALUE)));
        Assertions.assertSame(end, normalize(end, Statistics.buildFrom(statistics())
                .addColumnStatistic(big, new ColumnStatistic(0, 1e12, 0, 8, 100)).build()));
    }

    @Test
    public void testNothingToRewriteIsTheSameObject() {
        ScalarOperator predicate = Utils.compoundAnd(List.of(compare(BinaryType.GE, D, number(5)),
                Utils.compoundOr(eq(E, number(1)), like(NAME, "ab%")), in(false, E, number(1), number(2)),
                in(true, NAME, text("a")), regexp(NAME, "^a.*"), new CompoundPredicateOperator(
                        CompoundPredicateOperator.CompoundType.NOT, eq(prefix("left", NAME, 2), text("ab")))));
        assertUnchanged(predicate);
        Assertions.assertNull(EstimatePredicateNormalizer.normalize(null, statistics()));
    }

    @Test
    public void testAnAtomThatIsNotEvaluatedStays() {
        LikePredicateOperator skipped = regexp(NAME, "^a|^b");
        skipped.setNotEvalEstimate(true);
        assertUnchanged(skipped);
        // Next to an atom that is rewritten, it stays.
        LikePredicateOperator next = regexp(NAME, "^c|^d");
        ScalarOperator mixed = normalize(Utils.compoundAnd(List.of(eq(E, number(1)), skipped, next)));
        Assertions.assertEquals(Utils.compoundAnd(List.of(eq(E, number(1)), skipped,
                Utils.compoundOr(like(NAME, "c%"), like(NAME, "d%")))), mixed);
        Assertions.assertSame(skipped, mixed.getChild(0).getChild(1));
        // A conjunction that is skipped whole stays with the atoms inside it, next to an atom that is rewritten.
        CompoundPredicateOperator skippedAnd = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                regexp(NAME, "^e|^f"), eq(E, number(2)));
        skippedAnd.setNotEvalEstimate(true);
        ScalarOperator around = normalize(Utils.compoundAnd(regexp(NAME, "^c|^d"), skippedAnd));
        Assertions.assertEquals(Utils.compoundOr(like(NAME, "c%"), like(NAME, "d%")), around.getChild(0));
        Assertions.assertSame(skippedAnd, around.getChild(1));
        // The atoms of a conjunction that is skipped whole stay as well.
        CompoundPredicateOperator and = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.AND,
                compare(BinaryType.GE, D, number(5)), compare(BinaryType.LE, D, number(5)));
        and.setNotEvalEstimate(true);
        assertUnchanged(and);
        // A range derived from other conditions is not evaluated while a column has unknown statistics.
        LikePredicateOperator derived = regexp(NAME, "^a|^b");
        derived.setFromPredicateRangeDerive(true);
        Assertions.assertSame(derived, normalize(derived, withUnknown()));
        Assertions.assertNotSame(derived, normalize(derived, statistics()));
    }

    @Test
    public void testARewrittenAtomTakesTheFlagsOfTheAtom() {
        LikePredicateOperator regexp = regexp(NAME, "^a|^b");
        regexp.setRedundant(true);
        regexp.setIsPushdown(true);
        ScalarOperator likes = normalize(regexp);
        List<ScalarOperator> flagged = new ArrayList<>(List.of(likes));
        flagged.addAll(Utils.extractDisjunctive(likes));
        for (ScalarOperator operator : flagged) {
            Assertions.assertTrue(operator.isRedundant(), operator.toString());
            Assertions.assertTrue(operator.isPushdown(), operator.toString());
            Assertions.assertFalse(operator.isFromPredicateRangeDerive(), operator.toString());
        }
        // The LIKE of a prefix equality takes them.
        ScalarOperator equality = eq(prefix("left", TEXT, 10), text("2026-08-15"));
        equality.setFromPredicateRangeDerive(true);
        equality.setCorrelated(true);
        ScalarOperator prefixLike = normalize(equality);
        Assertions.assertEquals(like(TEXT, "2026-08-15%"), prefixLike);
        Assertions.assertTrue(prefixLike.isFromPredicateRangeDerive());
        Assertions.assertTrue(prefixLike.isCorrelated());
        // The equality of a point stands at the place of the first side and has its flags.
        ScalarOperator first = compare(BinaryType.GE, D, number(5));
        first.setFromPredicateRangeDerive(true);
        ScalarOperator point = normalize(Utils.compoundAnd(first, compare(BinaryType.LE, D, number(5))));
        Assertions.assertEquals(eq(D, number(5)), point);
        Assertions.assertTrue(point.isFromPredicateRangeDerive());
        // The compound conditions that are rebuilt keep theirs.
        CompoundPredicateOperator or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR,
                regexp(NAME, "^a|^b"), eq(E, number(1)));
        or.setRedundant(true);
        ScalarOperator rebuilt = normalize(or);
        Assertions.assertNotSame(or, rebuilt);
        Assertions.assertTrue(rebuilt.isRedundant());
    }

    @Test
    public void testConditionsInsideAndOrNotAndIfAreRewritten() {
        ScalarOperator regexp = regexp(NAME, "^a|^b");
        ScalarOperator likes = Utils.compoundOr(like(NAME, "a%"), like(NAME, "b%"));
        ScalarOperator other = eq(E, number(1));
        assertNormalizesTo(Utils.compoundAnd(other, likes), Utils.compoundAnd(other, regexp));
        assertNormalizesTo(Utils.compoundOr(other, likes), Utils.compoundOr(other, regexp));
        assertNormalizesTo(new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, likes),
                new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.NOT, regexp));
        // A conjunct that does not change keeps its place in the chain, and the chain its shape.
        ScalarOperator chain = Utils.compoundAnd(List.of(other, regexp, compare(BinaryType.GT, E, number(0))));
        ScalarOperator normalized = normalize(chain);
        Assertions.assertEquals(Utils.compoundAnd(List.of(other, likes, compare(BinaryType.GT, E, number(0)))),
                normalized);
        Assertions.assertSame(chain.getChild(0).getChild(0), normalized.getChild(0).getChild(0));
        // The rebuilt nodes can be changed in place by a rewrite of the estimate, as any node.
        Assertions.assertDoesNotThrow(() -> normalized.getChild(0).setChild(0, other));
        Assertions.assertDoesNotThrow(() -> normalized.setChild(1, other));
        // The arguments of IF are conditions of the estimate.
        ScalarOperator conditional = new CallOperator("if", BooleanType.BOOLEAN, List.of(other, regexp,
                eq(E, number(2))));
        ScalarOperator rewritten = normalize(conditional);
        Assertions.assertNotSame(conditional, rewritten);
        Assertions.assertEquals(likes, rewritten.getChild(1));
        Assertions.assertSame(other, rewritten.getChild(0));
        Assertions.assertSame(rewritten, normalize(rewritten));
    }

    @Test
    public void testAFailureLeavesThePredicateAsItIs() {
        ScalarOperator equality = eq(prefix("left", TEXT, 10), text("2026-08-15"));
        ScalarOperator regexp = regexp(NAME, "^a|^b");
        ScalarOperator both = Utils.compoundAnd(equality, regexp);
        ScalarOperator result = PredicateStatisticsCalculator.withStringDateFormats(column -> {
            throw new IllegalStateException("no format");
        }, () -> EstimatePredicateNormalizer.normalize(both, statistics()));
        Assertions.assertSame(both, result);
    }
}
