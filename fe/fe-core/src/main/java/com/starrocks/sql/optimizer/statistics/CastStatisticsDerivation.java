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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.starrocks.sql.optimizer.ConstantOperatorUtils;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.statistics.CastStatisticsRules.Order;
import com.starrocks.type.DateType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

/**
 * The statistics of CAST(x AS T) from the statistics of x, following the order the cast keeps, see
 * CastStatisticsRules. A cast that keeps the order maps the range of x and its histogram value by value with the
 * semantics of the BE, see StatisticsCastEvaluator, so a comparison over the cast is estimated from the distribution
 * of x. A cast that reorders the values keeps only the NDV and the NULL rows of x.
 */
final class CastStatisticsDerivation {
    private static final Logger LOG = LogManager.getLogger(CastStatisticsDerivation.class);
    private static final AtomicBoolean FAILURE_LOGGED = new AtomicBoolean();

    // We expect the statistics of one cast to be derived many times per query from the same histogram of a column,
    // and the image of a histogram casts each of its values. So the image stays while the histogram lives. An image
    // is as large as its histogram: we keep the images of a bounded number of histograms, and a few casts of each,
    // as a column is cast to few types.
    private static final int IMAGES_PER_HISTOGRAM = 4;
    private static final Cache<Histogram, Map<List<Type>, Optional<Image>>> IMAGES =
            CacheBuilder.newBuilder().weakKeys().maximumSize(1000).build();

    /** A histogram mapped through a cast, and the rows of its most common values that the cast turns into NULL. */
    record Image(Histogram histogram, long nullRows) {
    }

    // The ends of the range of a cast, and the share of the range of its source that the cast turns into NULL
    private record Range(double min, double max, double nullShare) {
    }

    // The seconds of the ends of a bucket of times
    private record Times(double lower, double upper) {
        // The part of the time of the bucket between two seconds
        double share(double from, double to) {
            return Math.max(0, Math.min(1, (Math.min(to, upper) - Math.max(from, lower)) / (upper - lower)));
        }
    }

    private CastStatisticsDerivation() {
    }

    /**
     * The statistics of the cast of a value with the given statistics, or null when the rules do not cover the pair
     * or the derivation fails; the caller then keeps its own estimate.
     */
    static ColumnStatistic derive(ColumnStatistic source, Type from, Type to) {
        try {
            Order order = CastStatisticsRules.order(from, to);
            switch (order) {
                case PRESERVED:
                    return ColumnStatistic.buildFrom(source).setAverageRowSize(to.getTypeSize()).build();
                case STRICT:
                case NON_DECREASING:
                case SEGMENTED:
                    return to.isStringType() ? dateToText(source, from, to) : ordered(source, from, to, order);
                case LAYOUT:
                    return textToDate(source, from, to);
                case NONE:
                    return unordered(source, to);
                default:
                    return null;
            }
        } catch (RuntimeException e) {
            logFailure(from, to, e);
            return null;
        }
    }

    // The availability of the estimate comes first: a failure keeps the previous estimate and is logged once.
    static void logFailure(Type from, Type to, RuntimeException e) {
        if (FAILURE_LOGGED.compareAndSet(false, true)) {
            LOG.warn("Failed to derive the statistics of a cast from {} to {}", from, to, e);
        } else {
            LOG.debug("Failed to derive the statistics of a cast from {} to {}", from, to, e);
        }
    }

    // The values move to other places in the order: the range and the histogram of the source say nothing about them.
    private static ColumnStatistic unordered(ColumnStatistic source, Type to) {
        return unorderedBuilder(source, to).build();
    }

    private static ColumnStatistic.Builder unorderedBuilder(ColumnStatistic source, Type to) {
        return ColumnStatistic.buildFrom(source).setHistogram(null).setMinString(null).setMaxString(null)
                .setStringBounds(null).setMinValue(NEGATIVE_INFINITY).setMaxValue(POSITIVE_INFINITY)
                .setAverageRowSize(to.getTypeSize());
    }

    /**
     * A date written as text keeps its order, so its histogram maps to a histogram of strings, which the estimate of
     * a comparison of strings reads like that of a column. The range of the strings stays unknown: only the casts of
     * text to dates read bounds of strings.
     */
    private static ColumnStatistic dateToText(ColumnStatistic source, Type from, Type to) {
        ColumnStatistic.Builder builder = unorderedBuilder(source, to);
        Histogram histogram = source.getHistogram();
        if (histogram != null) {
            image(histogram, from, to).ifPresent(image -> builder.setHistogram(image.histogram()));
        }
        return builder.build();
    }

    private static String text(ConstantOperator value, Type to) {
        ConstantOperator text = image(value, to);
        return text == null || text.isNull() ? null : text.getVarchar();
    }

    /**
     * Text read as a date keeps its order among strings of one layout, see StringDateLayout. When the bounds of the
     * column, or the ends of the buckets of its string histogram, share one layout, the dates they hold bound the
     * cast, and the buckets map to the dates of their ends. A DATE then has at most one value per day of the range.
     */
    private static ColumnStatistic textToDate(ColumnStatistic source, Type from, Type to) {
        ColumnStatistic.Builder builder = unorderedBuilder(source, to);
        StringBounds bounds = source.getStringBounds();
        Histogram histogram = source.getHistogram();
        boolean stringHistogram = histogram != null && histogram.hasStringValues();
        StringBucket first = stringHistogram && !histogram.getBuckets().isEmpty()
                ? (StringBucket) histogram.getBuckets().get(0) : null;
        StringBucket last = first == null ? null
                : (StringBucket) histogram.getBuckets().get(histogram.getBuckets().size() - 1);
        LocalDateTime low;
        LocalDateTime high;
        if (bounds != null) {
            if (!StringDateLayout.sameLayout(bounds.min(), bounds.minMayBeCut(), StringDateLayout.upperText(bounds),
                    bounds.maxMayBeCut())) {
                return builder.build();
            }
            low = StatisticsDatetimeParser.parse(bounds.min());
            high = StringDateLayout.upper(bounds);
        } else if (first != null) {
            if (!StringDateLayout.sameLayout(first.getLowerString(), false, last.getUpperString(), false)) {
                return builder.build();
            }
            low = StatisticsDatetimeParser.parse(first.getLowerString());
            high = StatisticsDatetimeParser.parse(last.getUpperString());
            // The most common values can lie beyond the buckets, as the last days of a table that grows
            for (String value : histogram.getMCV().keySet()) {
                LocalDateTime time = low == null || high == null ? null : StatisticsDatetimeParser.parse(value);
                if (time != null) {
                    low = time.isBefore(low) ? time : low;
                    high = time.isAfter(high) ? time : high;
                }
            }
        } else {
            return builder.build();
        }
        if (low == null || high == null || low.isAfter(high)) {
            return builder.build();
        }
        double min = dateValue(low, to);
        double max = dateValue(high, to);
        double distinct = to.isDate()
                ? Math.min(source.getDistinctValuesCount(), CastStatisticsRules.valuesInRange(to, min, max))
                : source.getDistinctValuesCount();
        builder.setMinValue(min).setMaxValue(max).setDistinctValuesCount(distinct);
        double nulls = source.getNullsFraction();
        // The buckets hold the order of the bounds only when they share their layout
        if (stringHistogram && (first == null || bounds == null
                || StringDateLayout.sameLayout(bounds.min(), bounds.minMayBeCut(), first.getLowerString(), false))) {
            Optional<Image> image = image(histogram, from, to);
            if (image.isPresent()) {
                builder.setHistogram(image.get().histogram());
                nulls += (1 - nulls) * image.get().nullRows() / (double) histogram.getTotalRows();
            }
        }
        return builder.setNullsFraction(nulls).build();
    }

    // The statistic value of a date or datetime of the target type: seconds since the epoch, see
    // ConstantOperatorUtils.doubleValueFromConstant
    private static double dateValue(LocalDateTime value, Type to) {
        return Utils.getLongFromDateTime(to.isDate() ? value.toLocalDate().atStartOfDay() : value);
    }

    private static ColumnStatistic ordered(ColumnStatistic source, Type from, Type to, Order order) {
        ColumnStatistic.Builder builder = ColumnStatistic.buildFrom(source).setHistogram(null).setMinString(null)
                .setMaxString(null).setStringBounds(null).setAverageRowSize(to.getTypeSize());
        double min = source.getMinValue();
        double max = source.getMaxValue();
        Range range = range(min, max, from, to, order);
        if (range == null) {
            return builder.setMinValue(NEGATIVE_INFINITY).setMaxValue(POSITIVE_INFINITY).build();
        }
        if (range.nullShare() >= 1) {
            return builder.setMinValue(NEGATIVE_INFINITY).setMaxValue(POSITIVE_INFINITY).setNullsFraction(1)
                    .setDistinctValuesCount(0).build();
        }
        double nulls = source.getNullsFraction() + (1 - source.getNullsFraction()) * range.nullShare();
        double distinct = Math.min(source.getDistinctValuesCount(),
                distinctValues(from, to, min, max, range.min(), range.max()));
        builder.setMinValue(range.min()).setMaxValue(range.max());
        Histogram histogram = source.getHistogram();
        if (range.nullShare() == 0 && histogram != null) {
            Optional<Image> image = image(histogram, from, to);
            if (image.isPresent()) {
                builder.setHistogram(image.get().histogram());
                nulls += (1 - nulls) * image.get().nullRows() / (double) histogram.getTotalRows();
            }
        }
        return builder.setNullsFraction(nulls).setDistinctValuesCount(distinct).build();
    }

    /**
     * The range of the cast of a source with the given range: the images of its ends. When the cast turns the ends
     * into NULL, as a narrowing does for values outside the target type, the range is cut to the values that
     * stay, and the share cut off becomes NULL, spread evenly over the source range. Null when not known.
     */
    private static Range range(double min, double max, Type from, Type to, Order order) {
        if (!(min <= max) || Double.isInfinite(min) || Double.isInfinite(max)) {
            return null;
        }
        if (order == Order.SEGMENTED && !inOneSegment(min, max)) {
            return null;
        }
        ConstantOperator low = image(toConstant(min, from), to);
        ConstantOperator high = image(toConstant(max, from), to);
        if (low == null || high == null) {
            return null;
        }
        if (!low.isNull() && !high.isNull()) {
            return range(low, high, 0);
        }
        if (order == Order.SEGMENTED) {
            // A date that does not exist at an end, such as 20260230: the next valid one is not known here
            return null;
        }
        if (from.isDateType()) {
            // A date is a positive literal and becomes NULL only above the integer of the target
            return low.isNull() ? new Range(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1) : null;
        }
        double[] valid = validSourceRange(from, to);
        if (valid == null) {
            return null;
        }
        double clippedMin = Math.max(min, valid[0]);
        double clippedMax = Math.min(max, valid[1]);
        if (clippedMin > clippedMax) {
            return new Range(NEGATIVE_INFINITY, POSITIVE_INFINITY, 1);
        }
        ConstantOperator clippedLow = image(toConstant(clippedMin, from), to);
        ConstantOperator clippedHigh = image(toConstant(clippedMax, from), to);
        if (clippedLow == null || clippedHigh == null || clippedLow.isNull() || clippedHigh.isNull()) {
            return null;
        }
        double nullShare = max > min ? 1 - (clippedMax - clippedMin) / (max - min) : 0;
        return range(clippedLow, clippedHigh, nullShare);
    }

    private static Range range(ConstantOperator low, ConstantOperator high, double nullShare) {
        OptionalDouble min = ConstantOperatorUtils.doubleValueFromConstant(low);
        OptionalDouble max = ConstantOperatorUtils.doubleValueFromConstant(high);
        if (min.isEmpty() || max.isEmpty() || min.getAsDouble() > max.getAsDouble()) {
            return null;
        }
        return new Range(min.getAsDouble(), max.getAsDouble(), nullShare);
    }

    // The source values a narrowing keeps: the target integer, the physical integer of the target DECIMAL, or the
    // finite FLOAT values; null when the cast has no such limit.
    private static double[] validSourceRange(Type from, Type to) {
        if (to.isFixedPointType()) {
            double bound = Math.scalb(1.0, StatisticsCastEvaluator.integerBits(to) - 1);
            return new double[] {-bound, bound - 1};
        }
        if (to.isDecimalV3()) {
            double bound = (Math.scalb(1.0, StatisticsCastEvaluator.decimalBits(to) - 1) - 1)
                    / Math.pow(10, ((ScalarType) to).getScalarScale());
            return new double[] {-bound, bound};
        }
        if (to.isFloat() && from.isFloatingPointType()) {
            return new double[] {-Float.MAX_VALUE, Float.MAX_VALUE};
        }
        return null;
    }

    // A number read as a date keeps its order inside the eight digit dates and inside the fourteen digit datetimes,
    // after the fraction is cut off.
    private static boolean inOneSegment(double min, double max) {
        double low = min < 0 ? Math.ceil(min) : Math.floor(min);
        double high = max < 0 ? Math.ceil(max) : Math.floor(max);
        return (low >= CastStatisticsRules.DATE_SEGMENT_LOW && high <= CastStatisticsRules.DATE_SEGMENT_HIGH)
                || (low >= CastStatisticsRules.DATETIME_SEGMENT_LOW && high <= CastStatisticsRules.DATETIME_SEGMENT_HIGH);
    }

    /**
     * The most distinct values the cast of a source range can have: those of the target type in the range of the
     * cast, and those of the source in its own range. A cast of a number to DATETIME gives whole seconds, and a
     * cast of a DATETIME to anything else drops the microseconds.
     */
    private static double distinctValues(Type from, Type to, double min, double max, double castMin, double castMax) {
        double values = targetValues(from, to, castMin, castMax);
        if (from.isDatetime()) {
            return to.isDatetime() ? values : Math.min(values, CastStatisticsRules.secondsInRange(min, max));
        }
        return Math.min(values, CastStatisticsRules.valuesInRange(from, min, max));
    }

    private static double targetValues(Type from, Type to, double castMin, double castMax) {
        if (to.isDatetime()) {
            return CastStatisticsRules.isNumber(from) ? CastStatisticsRules.secondsInRange(castMin, castMax)
                    : POSITIVE_INFINITY;
        }
        return CastStatisticsRules.valuesInRange(to, castMin, castMax);
    }

    // The statistic value as a constant of the type, see ConstantOperatorUtils.doubleValueFromConstant; null when it
    // is not a value of the type.
    static ConstantOperator toConstant(double value, Type type) {
        try {
            if (type.isDate() || type.isDatetime()) {
                Optional<LocalDateTime> dateTime = Utils.getDatetimeFromStatistic(Math.floor(value));
                if (dateTime.isEmpty()) {
                    return null;
                }
                return type.isDate()
                        ? ConstantOperator.createDate(dateTime.get().toLocalDate().atStartOfDay())
                        : ConstantOperator.createDatetime(dateTime.get());
            }
            if (type.isFixedPointType()) {
                if (value != Math.rint(value)) {
                    return null;
                }
                BigInteger integer = new BigDecimal(value).toBigInteger();
                return integer.bitLength() < StatisticsCastEvaluator.integerBits(type)
                        ? StatisticsCastEvaluator.integerConstant(integer, type) : null;
            }
            if (type.isFloat()) {
                return ConstantOperator.createFloat(value);
            }
            if (type.isDouble()) {
                return ConstantOperator.createDouble(value);
            }
            if (type.isDecimalV3()) {
                return ConstantOperator.createDecimal(BigDecimal.valueOf(value)
                        .setScale(((ScalarType) type).getScalarScale(), RoundingMode.HALF_UP), type);
            }
        } catch (RuntimeException e) {
            LOG.debug("cannot turn statistic value {} into {}", value, type, e);
        }
        return null;
    }

    // The cast of a constant as the BE computes it; null when not supported
    private static ConstantOperator image(ConstantOperator value, Type to) {
        return value == null ? null : StatisticsCastEvaluator.cast(value, to).orElse(null);
    }

    // The statistic value of the cast of an end of a bucket; null when it is NULL, not supported, or a string of
    // another layout than the given one
    private static Double endValue(Bucket bucket, boolean upper, String layout, Type from, Type to) {
        ConstantOperator value;
        if (bucket instanceof StringBucket text) {
            String end = upper ? text.getUpperString() : text.getLowerString();
            if (layout == null || !StringDateLayout.sameLayout(layout, false, end, false)) {
                return null;
            }
            value = ConstantOperator.createVarchar(end);
        } else {
            value = toConstant(upper ? bucket.getUpper() : bucket.getLower(), from);
        }
        ConstantOperator image = image(value, to);
        if (image == null || image.isNull()) {
            return null;
        }
        OptionalDouble number = ConstantOperatorUtils.doubleValueFromConstant(image);
        return number.isPresent() ? number.getAsDouble() : null;
    }

    /**
     * The histogram of the cast: each most common value cast, with the counts of values that meet summed, and each
     * bucket mapped to the images of its ends with the same cumulative rows. A bucket whose image starts on the last
     * value of the one before, as times of one day in two buckets cast to DATE, gives its rows of that value to the
     * bucket before. The buckets of a string histogram map only when their ends share one layout. Empty when a value
     * cannot be cast.
     */
    static Optional<Image> image(Histogram histogram, Type from, Type to) {
        if (histogram.hasUnknownRange() || histogram.hasStringValues() != from.isStringType()) {
            return Optional.empty();
        }
        Map<List<Type>, Optional<Image>> images =
                IMAGES.asMap().computeIfAbsent(histogram, ignored -> new ConcurrentHashMap<>());
        List<Type> key = List.of(from, to);
        Optional<Image> image = images.get(key);
        if (image == null) {
            image = computeImage(histogram, from, to);
            if (images.size() < IMAGES_PER_HISTOGRAM) {
                images.put(key, image);
            }
        }
        return image;
    }

    private static Optional<Image> computeImage(Histogram histogram, Type from, Type to) {
        Map<String, Long> mcv = new HashMap<>();
        // The statistic value of each most common value of the image, to find the buckets that hold it
        Map<String, Double> mcvValues = new HashMap<>();
        long nullRows = 0;
        for (Map.Entry<String, Long> entry : histogram.getMCV().entrySet()) {
            ConstantOperator value = StatisticsCastEvaluator.cast(ConstantOperator.createVarchar(entry.getKey()), from)
                    .orElse(null);
            ConstantOperator image = value == null || value.isNull() ? null : image(value, to);
            if (image == null) {
                return Optional.empty();
            }
            if (image.isNull()) {
                nullRows += entry.getValue();
            } else {
                String text = MultiColumnMcvEstimator.constantText(image);
                mcv.merge(text, entry.getValue(), Long::sum);
                OptionalDouble number = ConstantOperatorUtils.doubleValueFromConstant(image);
                if (number.isPresent()) {
                    mcvValues.put(text, number.getAsDouble());
                }
            }
        }
        List<Bucket> buckets = new ArrayList<>(histogram.getBuckets().size());
        if (to.isStringType()) {
            // A date written as text keeps its order and its distinct values: the buckets keep their rows. A DATETIME
            // statistic holds whole seconds, so the upper end of a bucket covers the microseconds of its last second.
            for (Bucket bucket : histogram.getBuckets()) {
                String lower = text(toConstant(bucket.getLower(), from), to);
                ConstantOperator end = toConstant(bucket.getUpper(), from);
                if (end != null && from.isDatetime()) {
                    end = ConstantOperator.createDatetime(end.getDatetime().withNano(999_999_000), from);
                }
                String upper = text(end, to);
                if (lower == null || upper == null) {
                    return Optional.empty();
                }
                buckets.add(new StringBucket(lower, upper, bucket.getCount(), bucket.getUpperRepeats(),
                        bucket.getDistinctCount().orElse(1L)));
            }
            return Optional.of(new Image(Histogram.forStrings(buckets, mcv), nullRows));
        }
        String layout = from.isStringType() && !histogram.getBuckets().isEmpty()
                ? ((StringBucket) histogram.getBuckets().get(0)).getLowerString() : null;
        boolean merges = layout != null ? textMergesValues(histogram, to) : CastStatisticsRules.mergesValues(from, to);
        long previous = 0;
        for (Bucket bucket : histogram.getBuckets()) {
            Double lower = endValue(bucket, false, layout, from, to);
            Double upper = endValue(bucket, true, layout, from, to);
            if (lower == null || upper == null || lower > upper) {
                return Optional.empty();
            }
            long rows = bucket.getCount() - previous;
            previous = bucket.getCount();
            double low = lower;
            double high = upper;
            double values = imageValues(from, to, low, high);
            Times times = merges && to.isDate() ? times(bucket, layout, from) : null;
            long repeats = upperRows(bucket, rows, values, merges, high, times);
            Optional<Long> distinct = bucket.getDistinctCount();
            Bucket last = buckets.isEmpty() ? null : buckets.get(buckets.size() - 1);
            if (last != null && low <= last.getUpper()) {
                double next = CastStatisticsRules.nextValue(to, last.getUpper());
                if (high <= last.getUpper()) {
                    // The whole image lies in the last bucket, as the times of a bucket within the last day of it
                    long onUpper = high < last.getUpper() ? 0 : low == high ? rows : repeats;
                    buckets.set(buckets.size() - 1, bucket(last.getLower(), last.getUpper(), bucket.getCount(),
                            last.getUpperRepeats() + onUpper, last.getDistinctCount()));
                    continue;
                }
                if (next <= high) {
                    // The bucket starts on the last value of the image of the bucket before it, as a bucket that
                    // starts at noon of the last day of the one before. We fear that merging the two buckets would
                    // spread the rows of a short recent bucket over a long old one, so we give the rows of the
                    // shared value to the bucket before and start this one at the next value.
                    long shared = lowerRows(bucket, rows, repeats, values, next, times);
                    buckets.set(buckets.size() - 1, bucket(last.getLower(), last.getUpper(),
                            last.getCount() + shared, last.getUpperRepeats() + shared, last.getDistinctCount()));
                    low = next;
                } else {
                    // A target without a next value, such as a DOUBLE: the two buckets merge
                    buckets.remove(buckets.size() - 1);
                    low = last.getLower();
                    if (last.getUpper() == high) {
                        repeats += last.getUpperRepeats();
                    }
                    distinct = distinct.isPresent() && last.getDistinctCount().isPresent()
                            ? Optional.of(distinct.get() + last.getDistinctCount().get()) : Optional.empty();
                }
            }
            if (distinct.isPresent()) {
                distinct = Optional.of((long) Math.max(1, Math.min(distinct.get(), imageValues(from, to, low, high))));
            }
            buckets.add(bucket(low, high, bucket.getCount(), repeats, distinct));
        }
        if (merges && !buckets.isEmpty() && !mcvValues.isEmpty()) {
            buckets = withoutMostCommonValues(buckets, mcv, mcvValues, to);
        }
        Histogram image = buckets.isEmpty() ? new Histogram(mcv) : new Histogram(buckets, mcv);
        return Optional.of(new Image(image, nullRows));
    }

    /**
     * We expect a most common value of a histogram to hold all the rows of its value and the buckets to hold none of
     * them, as the estimate of an equality reads the list alone for a value that is in it. A cast that merges values
     * breaks this: the most common time of a day is a most common value of the day, and the buckets still hold the
     * other times of the day. We fear that an equality on the day would then count the rows of that time only, so each
     * bucket gives the rows it holds for each most common value inside it to the list: for a value below its upper end,
     * the rows that a range of that value takes from it, and for its upper end, the rows of the end. The rows are
     * taken out of the bucket and out of the counts of the buckets after it, so the rows of the image are those of the
     * histogram. A value below the upper end is no longer one of the distinct values of the bucket, so an equality on
     * another value of the bucket reads the same rows as before. A target with no next value, as a DOUBLE, has no
     * value to take. We accept that a range of the other days of the bucket is estimated lower by about 1 / the days of
     * the bucket, because the bucket keeps spreading the rows it has left over its whole range, the given days included.
     */
    private static List<Bucket> withoutMostCommonValues(List<Bucket> buckets, Map<String, Long> mcv,
                                                        Map<String, Double> mcvValues, Type to) {
        // The values in their order, so that the cap on the rows a bucket gives does not depend on the order of the map
        List<Map.Entry<String, Double>> values = new ArrayList<>(mcvValues.entrySet());
        values.sort(Map.Entry.comparingByValue());
        List<Bucket> result = new ArrayList<>(buckets.size());
        long previous = 0;
        long moved = 0;
        for (Bucket bucket : buckets) {
            long rows = bucket.getCount() - previous;
            previous = bucket.getCount();
            long repeats = Math.min(bucket.getUpperRepeats(), rows);
            // The rows other than those of the upper end, which the bucket spreads evenly between its ends
            long spread = rows - repeats;
            long fromSpread = 0;
            long fromUpper = 0;
            long valuesInside = 0;
            for (Map.Entry<String, Double> entry : values) {
                double value = entry.getValue();
                double next = CastStatisticsRules.nextValue(to, value);
                if (value < bucket.getLower() || value > bucket.getUpper() || Double.isNaN(next)) {
                    continue;
                }
                long share;
                if (value == bucket.getUpper()) {
                    share = repeats;
                    fromUpper = share;
                } else {
                    share = Math.min(spread - fromSpread, Math.round(bucket.spreadRowsOn(spread, value, next)));
                    fromSpread += share;
                    valuesInside++;
                }
                mcv.merge(entry.getKey(), share, Long::sum);
            }
            Optional<Long> distinct = bucket.getDistinctCount();
            if (distinct.isPresent() && valuesInside > 0) {
                distinct = Optional.of(Math.max(1L, distinct.get() - valuesInside));
            }
            long taken = fromSpread + fromUpper;
            result.add(bucket(bucket.getLower(), bucket.getUpper(), bucket.getCount() - moved - taken,
                    bucket.getUpperRepeats() - fromUpper, distinct));
            moved += taken;
        }
        return result;
    }

    // Text read as a DATE merges the times of a day. A date and a datetime of the same fields share a layout, so we
    // look at the ends of all buckets.
    private static boolean textMergesValues(Histogram histogram, Type to) {
        if (!to.isDate()) {
            return false;
        }
        for (Bucket bucket : histogram.getBuckets()) {
            StringBucket text = (StringBucket) bucket;
            if (StringDateLayout.hasTime(text.getLowerString()) || StringDateLayout.hasTime(text.getUpperString())) {
                return true;
            }
        }
        return false;
    }

    // The seconds of the ends of a bucket of times, a DATETIME or text read as one; null for other sources
    private static Times times(Bucket bucket, String layout, Type from) {
        if (from.isDatetime()) {
            return bucket.getLower() < bucket.getUpper() ? new Times(bucket.getLower(), bucket.getUpper()) : null;
        }
        if (!from.isStringType()) {
            return null;
        }
        Double lower = endValue(bucket, false, layout, from, DateType.DATETIME);
        Double upper = endValue(bucket, true, layout, from, DateType.DATETIME);
        return lower != null && upper != null && lower < upper ? new Times(lower, upper) : null;
    }

    private static Bucket bucket(double lower, double upper, long count, long repeats, Optional<Long> distinct) {
        return distinct.isPresent() ? new Bucket(lower, upper, count, repeats, distinct.get())
                : new Bucket(lower, upper, count, repeats);
    }

    // The most distinct values of the target between two images
    private static double imageValues(Type from, Type to, double low, double high) {
        return from.isStringType() ? CastStatisticsRules.valuesInRange(to, low, high) : targetValues(from, to, low, high);
    }

    /**
     * The rows of the upper end of the image of a bucket. Where the cast merges values, as the times of the last day
     * of a bucket cast to DATE, other values of the bucket meet the upper end. A bucket spreads the rows other than
     * those of its upper end evenly between its ends: over the times of a bucket of times, so the last day takes
     * its part of the time the bucket covers. Over other values we expect each value of the image to take the same
     * number of the source values; a bucket with no more source values than values of the image keeps the rows of
     * its upper end, and so does one whose values of the image are not known.
     */
    private static long upperRows(Bucket bucket, long rows, double imageValues, boolean merges, double upperImage,
                                  Times times) {
        long repeats = bucket.getUpperRepeats();
        if (!merges || rows <= repeats) {
            return repeats;
        }
        double share;
        if (times != null) {
            share = times.share(upperImage, times.upper());
        } else if (Double.isInfinite(imageValues)) {
            return repeats;
        } else if (bucket.getDistinctCount().isPresent()) {
            double distinct = bucket.getDistinctCount().get();
            if (distinct <= imageValues) {
                return repeats;
            }
            share = (distinct / imageValues - 1) / (distinct - 1);
        } else {
            share = 1 / Math.max(1, imageValues);
        }
        return Math.min(rows, repeats + Math.round((rows - repeats) * share));
    }

    /**
     * The rows of the lower end of the image of a bucket, the values below the next one: their part of the time a
     * bucket of times covers, or else the rows other than those of the upper end spread evenly over the other
     * values of the image the bucket can hold.
     */
    private static long lowerRows(Bucket bucket, long rows, long upperRows, double imageValues, double next,
                                  Times times) {
        long others = Math.max(0, rows - upperRows);
        if (times != null) {
            return Math.min(others, Math.round((rows - bucket.getUpperRepeats()) * times.share(times.lower(), next)));
        }
        double values = Math.min(imageValues, bucket.getDistinctCount().map(Long::doubleValue)
                .orElse(Double.POSITIVE_INFINITY));
        return values <= 2 ? others : Math.min(others, Math.round(others / (values - 1)));
    }
}
