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
import com.starrocks.common.util.StringDateFormat;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.time.zone.ZoneOffsetTransition;
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
        if (columnDomain.getMin().isEmpty() || columnDomain.getMax().isEmpty()
                || columnDomain.getMin().get().isNull() || columnDomain.getMax().get().isNull()) {
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
        // A VARCHAR domain alone proves no date ordering: MySQL accepts short years and
        // variable-width compact forms. The session option is an explicit data-format
        // assertion; matching, canonical endpoints are still required for this derivation.
        boolean canonicalString = canonicalStringDateDomain(column, columnDomain);
        if (column.getType().isStringType() && !canonicalString) {
            return Optional.empty();
        }
        ColumnRefOperator dateCastColumn = intDateCastAdmitted(column, columnDomain) || canonicalString ? column : null;
        if (!isMonotonicExpression(expr, dateCastColumn)) {
            return Optional.empty();
        }
        return imageOf(expr, column, columnDomain);
    }

    // Evaluate and validate each link on its own input range. Checking only the final
    // rendering hides unix_timestamp's clamp-to-zero, and checking the original column's
    // range gives the wrong units when an epoch conversion is nested inside another call.
    private static Optional<Range<ConstantOperator>> imageOf(ScalarOperator expr, ColumnRefOperator column,
                                                             MinMax columnDomain) {
        try {
            if (expr.equals(column)) {
                return Optional.of(Range.closed(columnDomain.getMin().orElseThrow(),
                        columnDomain.getMax().orElseThrow()));
            }
            if (!(expr instanceof CallOperator)) {
                return Optional.empty();
            }
            CallOperator call = (CallOperator) expr;
            ScalarOperator child = call instanceof CastOperator ? call.getChild(0)
                    : MonotonicFunctionRegistry.dataChildOf(call);
            if (child == null) {
                return Optional.empty();
            }
            // The canonical-string assertion applies to CAST, not arbitrary format parsers.
            if (child.equals(column) && column.getType().isStringType()
                    && (!(call instanceof CastOperator) || !call.getType().isDateType())) {
                return Optional.empty();
            }
            Optional<Range<ConstantOperator>> input = imageOf(child, column, columnDomain);
            if (input.isEmpty()) {
                return Optional.empty();
            }
            Range<ConstantOperator> range = input.get();
            if (MonotonicFunctionRegistry.isEpochRendering(call.getFnName())
                    && !epochWindowHasNoTransition(MinMax.of(range),
                            FunctionSet.FROM_UNIXTIME_MS.equalsIgnoreCase(call.getFnName()) ? 1000L : 1L)) {
                return Optional.empty();
            }
            if (FunctionSet.UNIX_TIMESTAMP.equalsIgnoreCase(call.getFnName()) && !wallWindowHasNoTransition(range)) {
                return Optional.empty();
            }
            Optional<ConstantOperator> first = foldArgument(call, child, range.lowerEndpoint());
            Optional<ConstantOperator> second = foldArgument(call, child, range.upperEndpoint());
            if (first.isEmpty() || second.isEmpty() || !first.get().getType().matchesType(second.get().getType())) {
                return Optional.empty();
            }
            ConstantOperator a = first.get();
            ConstantOperator b = second.get();
            if (FunctionSet.UNIX_TIMESTAMP.equalsIgnoreCase(call.getFnName())
                    && (a.isZero() || b.isZero())) {
                return Optional.empty();
            }
            return Optional.of(a.compareTo(b) <= 0 ? Range.closed(a, b) : Range.closed(b, a));
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    private static boolean wallWindowHasNoTransition(Range<ConstantOperator> input) {
        try {
            ZoneId zone = TimeUtils.getTimeZone().toZoneId();
            LocalDateTime lo = input.lowerEndpoint().getDatetime();
            LocalDateTime hi = input.upperEndpoint().getDatetime();
            if (zone.getRules().getValidOffsets(lo).size() != 1 || zone.getRules().getValidOffsets(hi).size() != 1) {
                return false;
            }
            Instant from = lo.atZone(zone).toInstant();
            Instant to = hi.atZone(zone).toInstant();
            ZoneOffsetTransition next = zone.getRules().nextTransition(from);
            return !to.isBefore(from) && (next == null || next.getInstant().isAfter(to));
        } catch (Exception e) {
            return false;
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

    private static boolean isMonotonicExpression(ScalarOperator op, ColumnRefOperator dateCastColumn) {
        if (op.isColumnRef() || op.isConstantRef()) {
            return true;
        }
        // CastOperator extends CallOperator, so this branch must come first. Casts go through
        // the whitelist below, not through the registry: the registry accepts every cast.
        if (op instanceof CastOperator) {
            CastOperator cast = (CastOperator) op;
            if (isOrderPreservingCast(cast)) {
                return isMonotonicExpression(cast.getChild(0), dateCastColumn);
            }
            // Cast the domain column itself only after proving its INT segment or applying
            // the explicit canonical-VARCHAR assertion to matching validated endpoints.
            if (dateCastColumn != null && cast.getType().isDateType()
                    && dateCastColumn.equals(cast.getChild(0))) {
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
                    return isMonotonicExpression(inner, dateCastColumn);
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
            // unix_timestamp's image additionally needs per-call clamp and timezone checks.
            // A function annotation alone is not sufficient proof for an input domain.
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
                        || !isMonotonicExpression(child, dateCastColumn)) {
                    return false;
                }
            }
            return true;
        }
        // everything else (case-when, lambdas, subqueries, ...) is out of scope
        return false;
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

    private static boolean canonicalStringDateDomain(ColumnRefOperator column, MinMax domain) {
        ConnectContext context = ConnectContext.get();
        if (!column.getType().isStringType() || context == null
                || !context.getSessionVariable().isEnableStringDateJoinPruning()) {
            return false;
        }
        String lo = domain.getMin().orElseThrow().getVarchar();
        String hi = domain.getMax().orElseThrow().getVarchar();
        if (context.getSessionVariable().isEnableStringDatePredicatePushdown()) {
            StringDateFormat declared = StringDateFormat.fromFormat(
                    context.getSessionVariable().getStringDatePredicateFormat());
            if (declared != null) {
                return declared.isSupportedInCurrentTimezone() && declared.matches(lo) && declared.matches(hi);
            }
        }
        for (StringDateFormat format : StringDateFormat.values()) {
            if (!format.hasUtcSuffix() && format.getPrecision() != ChronoUnit.MICROS
                    && format.matches(lo) && format.matches(hi)) {
                return true;
            }
        }
        return false;
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

    private static Optional<ConstantOperator> foldArgument(CallOperator call, ScalarOperator child,
                                                            ConstantOperator value) {
        try {
            CallOperator foldedCall = (CallOperator) call.clone();
            for (int i = 0; i < call.getChildren().size(); i++) {
                if (call.getChild(i) == child) {
                    foldedCall.setChild(i, value);
                }
            }
            ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
            ScalarOperator folded = rewriter.rewrite(foldedCall, ScalarOperatorRewriter.DEFAULT_TYPE_CAST_RULE);
            folded = rewriter.rewrite(folded, ScalarOperatorRewriter.FOLD_CONSTANT_RULES);
            if (folded instanceof ConstantOperator && !((ConstantOperator) folded).isNull()) {
                return Optional.of((ConstantOperator) folded);
            }
            return Optional.empty();
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
