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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Range;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.zone.ZoneOffsetTransition;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Computes the value range of a monotonic expression from the range of its input column.
 * <p>
 * Example:
 * <pre>
 *   expr        = date_format(e.datadate, '%Y%m')
 *   column      = e.datadate
 *   columnRange = ['2024-03-05', '2024-04-10']    -- from WHERE datadate BETWEEN ... AND ...
 * </pre>
 * The expression is monotonic in datadate, so its value always stays between the values at
 * the two endpoints:
 * <pre>
 *   fold(expr, datadate := '2024-03-05') = '202403'
 *   fold(expr, datadate := '2024-04-10') = '202404'
 *   image = ['202403', '202404']
 * </pre>
 * If the caller has an equality {@code something = expr}, it can bound that something by the
 * image. This is how a filter on one join side moves to the other side.
 * <p>
 * The monotonicity of calls comes from the {@code @ConstantFunction(isMonotonic = true)}
 * registry. Casts are checked separately against a whitelist here, because the registry
 * accepts every cast, and e.g. cast(int as varchar) breaks order: 999 &lt; 1001 as ints,
 * but '999' &gt; '1001' as strings.
 * <p>
 * Monotonicity here is non-strict: date_trunc maps both '2024-03-05' and '2024-03-20' to
 * '2024-03-01'. So the image is always closed on both ends, and the caller must not build
 * strict bounds from it.
 */
public class MonotonicImage {

    private MonotonicImage() {
    }

    /**
     * Range of {@code expr} values when {@code column} stays inside {@code columnDomain}.
     * Empty when soundness cannot be proven. A refused derivation only loses the
     * optimization; it never fails the query.
     */
    public static Optional<Range<ConstantOperator>> imageRange(ScalarOperator expr, ColumnRefOperator column,
                                                               MinMax columnDomain) {
        // Need both endpoints. The registry does not say whether the function increases or
        // decreases, so a single endpoint could turn into either a lower or an upper bound.
        // With two endpoints we fold both and sort the results, so direction does not matter.
        if (columnDomain.getMin().isEmpty() || columnDomain.getMax().isEmpty()) {
            return Optional.empty();
        }
        // The column must occur exactly once. Counter-example with two occurrences:
        // datediff(d, date_trunc('month', d)) is the day of month, it goes 0..30 inside every
        // month, but folding the endpoints '2024-03-05' and '2024-04-10' gives just [4, 9].
        // With one occurrence the tree is one chain of monotonic steps, and such a chain is
        // monotonic as a whole.
        if (expr.getColumnRefs().stream().filter(column::equals).count() != 1) {
            return Optional.empty();
        }
        // No string columns. String order is lexicographic, but the admitted functions read
        // the string as a date, and the two orders disagree on non-padded values:
        // '2024-12-31' is INSIDE the string interval ['2024-1-1', '2024-3-5'], but its date
        // is outside the dates of the endpoints.
        if (column.getType().isStringType()) {
            return Optional.empty();
        }
        // cast(int_col as date) is admitted only when the whole domain sits inside the
        // positional YYYYMMDD segment (see intDateCastAdmitted); other integer
        // expressions follow the normal rules.
        ColumnRefOperator intDateCastColumn = intDateCastAdmitted(column, columnDomain) ? column : null;
        // No proof of monotonicity - no image. E.g. date_format(d, '%d') over
        // ['2024-03-05','2024-04-10'] produces '01'..'31', far outside ['05','10'].
        if (!isMonotonicExpression(expr, intDateCastColumn)) {
            return Optional.empty();
        }
        // Fold the expression at both endpoints:
        //   date_format('2024-03-05', '%Y%m') -> '202403'
        //   date_format('2024-04-10', '%Y%m') -> '202404'
        Optional<ConstantOperator> first = foldAt(expr, column, columnDomain.getMin().get());
        Optional<ConstantOperator> second = foldAt(expr, column, columnDomain.getMax().get());
        if (first.isEmpty() || second.isEmpty()) {
            return Optional.empty();
        }
        ConstantOperator a = first.get();
        ConstantOperator b = second.get();
        // Both endpoints must fold into the same comparable type, otherwise some fold step
        // did not finish and we cannot build a typed range.
        if (!a.getType().matchesType(b.getType())) {
            return Optional.empty();
        }
        // unix_timestamp clamps out-of-range input to 0 instead of NULL: a 0 endpoint may
        // sit on the clamp plateau, where the endpoint fold is not a bound of the image.
        // Only whitelisted casts can sit above it in the chain, and they keep 0 recognizable.
        if (chainContains(expr, FunctionSet.UNIX_TIMESTAMP) && (isZeroValue(a) || isZeroValue(b))) {
            return Optional.empty();
        }
        // from_unixtime renders the epoch as session-zone wall clock: across a zone-rule
        // transition the rendering is not monotonic (a fall-back repeats an hour). The
        // domain endpoints are epoch values, so admit only transition-free windows.
        boolean fromUnixSeconds = chainContains(expr, FunctionSet.FROM_UNIXTIME);
        boolean fromUnixMillis = chainContains(expr, FunctionSet.FROM_UNIXTIME_MS);
        if ((fromUnixSeconds || fromUnixMillis)
                && !epochWindowHasNoTransition(columnDomain, fromUnixMillis ? 1000L : 1L)) {
            return Optional.empty();
        }
        try {
            // Sort the two results: an increasing expr gives [f(lo), f(hi)], a decreasing one
            // gives [f(hi), f(lo)]. Closed on both ends because monotonicity is non-strict.
            return Optional.of(a.compareTo(b) <= 0 ? Range.closed(a, b) : Range.closed(b, a));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /**
     * True when the expression is a single monotonic chain: at every call exactly one argument
     * carries the column, that argument is in a position allowed by
     * {@link MonotonicFunctionRegistry#dataArgPositions}, all other arguments are constants,
     * every call passes the registry, and every cast is order-preserving.
     * <p>
     * For the example the tree is:
     * <pre>
     *   date_format          -- registry: isMonotonic, '%Y%m' passes the format order check;
     *   ├── e.datadate       --   the column is in arg 0, allowed by the table
     *   └── '%Y%m'           -- constant
     * </pre>
     */
    public static boolean isMonotonicExpression(ScalarOperator op) {
        return isMonotonicExpression(op, null);
    }

    private static boolean isMonotonicExpression(ScalarOperator op, ColumnRefOperator intDateCastColumn) {
        if (op.isColumnRef() || op.isConstantRef()) {
            return true;
        }
        // CastOperator extends CallOperator, so this branch must come first. Casts go through
        // the whitelist below, not through the registry: the registry accepts every cast.
        if (op instanceof CastOperator) {
            CastOperator cast = (CastOperator) op;
            if (isOrderPreservingCast(cast)) {
                return isMonotonicExpression(cast.getChild(0), intDateCastColumn);
            }
            // cast(int_col as date/datetime): only for the domain column itself, and only
            // when the caller proved the whole domain reads as positional YYYYMMDD
            // (see intDateCastAdmitted)
            if (intDateCastColumn != null && cast.getType().isDateType()
                    && intDateCastColumn.equals(cast.getChild(0))) {
                return true;
            }
            // a numeric cast over a digits-only date rendering: the images of
            // date_format(x, '%Y%m') are fixed-width digit strings, so their numeric value
            // is ordered exactly like the dates. Comes up when an implicit comparison casts
            // date_format against an integer column (both sides land on DECIMAL).
            if (cast.getType().isNumericType() && cast.getChild(0) instanceof CallOperator
                    && !(cast.getChild(0) instanceof CastOperator)) {
                CallOperator inner = (CallOperator) cast.getChild(0);
                if (FunctionSet.DATE_FORMAT.equals(inner.getFnName().toLowerCase())
                        && inner.getChildren().size() == 2 && inner.getChild(1).isConstantRef()
                        && !((ConstantOperator) inner.getChild(1)).isNull()
                        && MonotonicFunctionRegistry.isDigitsOnlyFormat(
                                ((ConstantOperator) inner.getChild(1)).getVarchar())) {
                    return isMonotonicExpression(inner, intDateCastColumn);
                }
            }
            return false;
        }
        if (op instanceof CallOperator) {
            CallOperator call = (CallOperator) op;
            String fnName = call.getFnName();
            // rand(), uuid() etc: the folded endpoint means nothing
            if (FunctionSet.nonDeterministicFunctions.contains(fnName.toLowerCase())) {
                return false;
            }
            // unix_timestamp is admitted by this registry, not by the evaluator annotation:
            // it clamps out-of-range input to 0 instead of NULL, so it is monotonic only off
            // the clamp plateau - imageRange refuses when an endpoint folds to 0. Wall clock
            // to epoch never runs backwards, so no timezone condition is needed here.
            if (!FunctionSet.UNIX_TIMESTAMP.equals(fnName.toLowerCase())
                    && !ScalarOperatorEvaluator.INSTANCE.isMonotonicFunction(call)) {
                return false;
            }
            // The evaluator checks the format only for the two-argument shapes.
            // from_unixtime(ts, '%d/%m/%Y', 'UTC') has three arguments and its day-first
            // format goes through unchecked. So only the two-argument forms are allowed.
            if (MonotonicFunctionRegistry.hasFormatArg(fnName) && call.getChildren().size() != 2) {
                return false;
            }
            Set<Integer> admittedArgs = MonotonicFunctionRegistry.dataArgPositions(fnName);
            if (admittedArgs == null) {
                return false;
            }
            // exactly one argument may be non-constant, in an admitted position, itself a
            // monotonic chain. The one-non-constant rule matters for functions with several
            // admitted positions: datediff(d, date_trunc('month', d)) is the day of month,
            // it rises within a month and drops at the next, even though both children are
            // monotonic chains.
            int nonConstant = 0;
            for (int i = 0; i < call.getChildren().size(); i++) {
                ScalarOperator child = call.getChild(i);
                if (child.isConstantRef()) {
                    continue;
                }
                nonConstant++;
                if (nonConstant > 1 || !admittedArgs.contains(i)
                        || !isMonotonicExpression(child, intDateCastColumn)) {
                    return false;
                }
            }
            return true;
        }
        // everything else (case-when, lambdas, subqueries, ...) is out of scope
        return false;
    }

    private static boolean chainContains(ScalarOperator expr, String fnName) {
        return Utils.collect(expr, CallOperator.class).stream()
                .anyMatch(c -> fnName.equals(c.getFnName().toLowerCase()));
    }

    private static boolean epochWindowHasNoTransition(MinMax domain, long unitsPerSecond) {
        try {
            long min = domain.getMin().flatMap(c -> c.castTo(IntegerType.BIGINT))
                    .map(ConstantOperator::getBigint).orElseThrow();
            long max = domain.getMax().flatMap(c -> c.castTo(IntegerType.BIGINT))
                    .map(ConstantOperator::getBigint).orElseThrow();
            ZoneOffsetTransition next = TimeUtils.getTimeZone().toZoneId().getRules()
                    .nextTransition(Instant.ofEpochSecond(min / unitsPerSecond));
            return next == null || next.getInstant().isAfter(Instant.ofEpochSecond(max / unitsPerSecond));
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isZeroValue(ConstantOperator value) {
        try {
            return new BigDecimal(value.toString()).signum() == 0;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * True when every value of the integer domain reads as positional YYYYMMDD under
     * cast(int as date). BE (date::standardize_date) parses 8-digit numbers positionally,
     * but smaller numbers as MySQL two-digit-year forms (690315 is 2069-03-15,
     * 700520 is 1970-05-20: numeric order breaks on the 69/70 boundary) and 9+ digit
     * numbers as compact datetimes. An interval with both endpoints inside
     * [10000101, 99991231] contains 8-digit numbers only, so numeric order equals date
     * order on the whole interval. Invalid combinations inside it (20260732) cast to
     * NULL, and NULL never matches the equality join that consumes the image.
     */
    private static boolean intDateCastAdmitted(ColumnRefOperator column, MinMax domain) {
        if (!column.getType().isIntegerType()) {
            return false;
        }
        return inPositionalDateSegment(domain.getMin()) && inPositionalDateSegment(domain.getMax());
    }

    private static boolean inPositionalDateSegment(Optional<ConstantOperator> bound) {
        if (bound.isEmpty() || bound.get().isNull()) {
            return false;
        }
        Optional<ConstantOperator> asLong = bound.get().castTo(IntegerType.BIGINT);
        if (asLong.isEmpty()) {
            return false;
        }
        long v = asLong.get().getBigint();
        return v >= 10000101L && v <= 99991231L;
    }

    /**
     * Casts whose output order matches the input order on the whole input domain. Narrower on
     * purpose than {@code ScalarOperatorEvaluator.isMonotonicFunction}, which accepts every
     * cast.
     */
    public static boolean isOrderPreservingCast(CastOperator cast) {
        Type from = cast.getChild(0).getType();
        Type to = cast.getType();
        // same primitive type (e.g. varchar length change) does not reorder values
        if (from.getPrimitiveType() == to.getPrimitiveType()) {
            return true;
        }
        // integer widening keeps numeric order; narrowing may wrap and is rejected
        if (from.isIntegerType() && to.isIntegerType()) {
            return from.getPrimitiveType().getSlotSize() <= to.getPrimitiveType().getSlotSize();
        }
        // integer -> decimal is exact when the integer part fits every value of the source
        // type (BIGINT needs 19 digits; LARGEINT does not fit and stays out); implicit
        // numeric comparisons produce these casts
        if (from.isIntegerType() && to.isDecimalOfAnyVersion() && !from.isLargeIntType()) {
            ScalarType decimal = (ScalarType) to;
            return decimal.getScalarPrecision() - decimal.getScalarScale() >= 19;
        }
        // date -> datetime is exact; datetime -> date truncates, which is non-strict monotonic
        if (from.isDateType() && to.isDateType()) {
            return true;
        }
        // string -> date is out: parsing accepts non-padded strings, and string order breaks
        // ('2024-12-31' < '2024-3-5' as strings, the dates go the other way).
        // date -> string is fine: the rendered format is fixed-width, so string order matches
        // date order.
        return from.isDateType() && to.isStringType();
        // everything else is out, notably int -> varchar and floating point
    }

    /**
     * Substitutes {@code value} for the column and constant-folds:
     * {@code date_format(datadate, '%Y%m')} at {@code datadate := '2024-03-05'} becomes
     * {@code date_format('2024-03-05', '%Y%m')} and folds to '202403'. Empty when the result
     * is not a non-null constant: unregistered function, NULL from a bad value, overflow like
     * years_add(d, 9000) leaving the DATE range.
     */
    private static Optional<ConstantOperator> foldAt(ScalarOperator expr, ColumnRefOperator column,
                                                     ConstantOperator value) {
        try {
            ReplaceColumnRefRewriter replacer = new ReplaceColumnRefRewriter(Map.of(column, value));
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            ScalarOperator folded = replacer.rewrite(expr);
            // align types first (the endpoint constant may need an implicit cast), then fold
            folded = rewriter.rewrite(folded, ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
            folded = rewriter.rewrite(folded, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);
            if (folded instanceof ConstantOperator && !((ConstantOperator) folded).isNull()) {
                return Optional.of((ConstantOperator) folded);
            }
            return Optional.empty();
        } catch (Exception e) {
            // folding may throw on overflow or invalid dates: give up on this derivation,
            // never fail the query
            return Optional.empty();
        }
    }
}
