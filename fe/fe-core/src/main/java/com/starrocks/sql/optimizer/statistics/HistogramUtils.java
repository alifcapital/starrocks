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

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.util.DateUtils;
import com.starrocks.type.Type;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.Utils.getLongFromDateTime;

public class HistogramUtils {
    private static final Logger LOG = LogManager.getLogger(HistogramUtils.class);

    public static List<Bucket> convertBuckets(String histogramString, Type type) throws AnalysisException {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();

        JsonElement jsonElement = jsonObject.get("buckets");
        if (jsonElement.isJsonNull()) {
            return Collections.emptyList();
        }

        JsonArray histogramObj = (JsonArray) jsonElement;
        List<Bucket> buckets = Lists.newArrayList();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();
            try {
                double low;
                double high;
                if (type.isDate()) {
                    low = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                            bucketJsonArray.get(0).getAsString(), DateUtils.DATE_FORMATTER_UNIX));
                    high = (double) getLongFromDateTime(DateUtils.parseStringWithDefaultHSM(
                            bucketJsonArray.get(1).getAsString(), DateUtils.DATE_FORMATTER_UNIX));
                } else if (type.isDatetime()) {
                    low = (double) getLongFromDateTime(DateUtils.parseDatTimeString(
                            bucketJsonArray.get(0).getAsString()));
                    high = (double) getLongFromDateTime(DateUtils.parseDatTimeString(
                            bucketJsonArray.get(1).getAsString()));
                } else {
                    low = Double.parseDouble(bucketJsonArray.get(0).getAsString());
                    high = Double.parseDouble(bucketJsonArray.get(1).getAsString());
                }

                // #76670 stores string tail mass as [Infinity, Infinity, count, 0].
                // These are not string or numeric endpoints.
                if (type.isStringType() && low == Double.POSITIVE_INFINITY && high == Double.POSITIVE_INFINITY
                        && bucketJsonArray.get(3).getAsLong() == 0) {
                    buckets.add(new UnknownRangeBucket(bucketJsonArray.get(2).getAsLong()));
                    continue;
                }

                if (bucketJsonArray.size() == 5) {
                    Bucket bucket = new Bucket(low, high,
                            Long.parseLong(bucketJsonArray.get(2).getAsString()),
                            Long.parseLong(bucketJsonArray.get(3).getAsString()),
                            Long.parseLong(bucketJsonArray.get(4).getAsString()));
                    buckets.add(bucket);
                } else {
                    Bucket bucket = new Bucket(low, high,
                            Long.parseLong(bucketJsonArray.get(2).getAsString()),
                            Long.parseLong(bucketJsonArray.get(3).getAsString()));
                    buckets.add(bucket);
                }
            } catch (Exception e) {
                LOG.warn("Failed to parse histogram bucket: {}", bucketJsonArray, e);
            }
        }
        return buckets;
    }

    public static Map<String, Long> convertMCV(String histogramString) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();
        JsonElement jsonElement = jsonObject.get("mcv");
        if (jsonElement.isJsonNull()) {
            return Collections.emptyMap();
        }

        JsonArray histogramObj = (JsonArray) jsonElement;
        Map<String, Long> mcv = new HashMap<>();
        for (int i = 0; i < histogramObj.size(); ++i) {
            JsonArray bucketJsonArray = histogramObj.get(i).getAsJsonArray();
            mcv.put(bucketJsonArray.get(0).getAsString(), Long.parseLong(bucketJsonArray.get(1).getAsString()));
        }
        return mcv;
    }

    // Query-dump representation: numeric endpoints stay doubles; string intervals carry their type and inclusivity.
    public static String serializeHistogram(Histogram histogram) {
        JsonObject root = new JsonObject();
        if (histogram.hasStringValues()) {
            root.addProperty("bucket_type", "string");
        }

        JsonArray bucketsArray = new JsonArray();
        if (histogram.getBuckets() != null) {
            for (Bucket bucket : histogram.getBuckets()) {
                JsonArray bucketArray = new JsonArray();
                if (bucket instanceof StringBucket stringBucket) {
                    bucketArray.add(stringBucket.getLowerString());
                    bucketArray.add(stringBucket.getUpperString());
                } else {
                    bucketArray.add(Double.toString(bucket.getLower()));
                    bucketArray.add(Double.toString(bucket.getUpper()));
                }
                bucketArray.add(Long.toString(bucket.getCount()));
                bucketArray.add(Long.toString(bucket.getUpperRepeats()));
                bucket.getDistinctCount().ifPresent(distinctCount -> bucketArray.add(Long.toString(distinctCount)));
                if (bucket instanceof StringBucket stringBucket) {
                    bucketArray.add(stringBucket.isLowerInclusive());
                    bucketArray.add(stringBucket.isUpperInclusive());
                }
                bucketsArray.add(bucketArray);
            }
        }
        root.add("buckets", bucketsArray);

        JsonArray mcvArray = new JsonArray();
        if (histogram.getMCV() != null) {
            for (Map.Entry<String, Long> entry : histogram.getMCV().entrySet()) {
                JsonArray mcvEntry = new JsonArray();
                mcvEntry.add(entry.getKey());
                mcvEntry.add(Long.toString(entry.getValue()));
                mcvArray.add(mcvEntry);
            }
        }
        root.add("mcv", mcvArray);

        return root.toString();
    }

    // Inverse of serializeHistogram; the dump carries the endpoint type, so no column Type is needed.
    public static Histogram deserializeHistogram(String histogramString) {
        JsonObject jsonObject = JsonParser.parseString(histogramString).getAsJsonObject();

        List<Bucket> buckets = Lists.newArrayList();
        JsonElement bucketsElement = jsonObject.get("buckets");
        if (bucketsElement != null && !bucketsElement.isJsonNull()) {
            JsonArray bucketsArray = bucketsElement.getAsJsonArray();
            for (int i = 0; i < bucketsArray.size(); ++i) {
                JsonArray bucketArray = bucketsArray.get(i).getAsJsonArray();
                if (jsonObject.has("bucket_type") && "string".equals(jsonObject.get("bucket_type").getAsString())) {
                    buckets.add(new StringBucket(bucketArray.get(0).getAsString(), bucketArray.get(1).getAsString(),
                            bucketArray.get(2).getAsLong(), bucketArray.get(3).getAsLong(), bucketArray.get(4).getAsLong(),
                            bucketArray.get(5).getAsBoolean(), bucketArray.get(6).getAsBoolean()));
                    continue;
                }
                double lower = Double.parseDouble(bucketArray.get(0).getAsString());
                double upper = Double.parseDouble(bucketArray.get(1).getAsString());
                long count = Long.parseLong(bucketArray.get(2).getAsString());
                long upperRepeats = Long.parseLong(bucketArray.get(3).getAsString());
                // Also read old dumps: a positive-infinity point with zero repeats is the legacy
                // unknown-tail encoding, not a bucket containing real infinity values.
                if (lower == Double.POSITIVE_INFINITY && upper == Double.POSITIVE_INFINITY && upperRepeats == 0) {
                    buckets.add(new UnknownRangeBucket(count));
                } else if (bucketArray.size() == 5) {
                    buckets.add(new Bucket(lower, upper, count, upperRepeats,
                            Long.parseLong(bucketArray.get(4).getAsString())));
                } else {
                    buckets.add(new Bucket(lower, upper, count, upperRepeats));
                }
            }
        }

        Map<String, Long> mcv = convertMCV(histogramString);
        if (jsonObject.has("bucket_type") && "string".equals(jsonObject.get("bucket_type").getAsString())) {
            return Histogram.forStrings(buckets, mcv);
        }
        return buckets.isEmpty() ? new Histogram(mcv) : new Histogram(buckets, mcv);
    }
}
