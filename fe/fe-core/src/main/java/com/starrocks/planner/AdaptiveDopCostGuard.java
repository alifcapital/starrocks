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

package com.starrocks.planner;

import com.starrocks.thrift.TFunction;
import com.starrocks.thrift.TFunctionBinaryType;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.meta_data.FieldMetaData;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Conservative function-cost guard, evaluated before FE enables runtime adaptive DOP. */
public final class AdaptiveDopCostGuard {
    // Keep in sync with BE two-phase classification in case_expensive_functions.inc.
    // AdaptiveDopCostGuardTest verifies parity, including aliases and newly registered functions.
    static final Set<String> EXPENSIVE_FUNCTIONS = Set.of(
            "parse_json", "json_query", "json_string", "to_json",
            "json_query_from_string", "json_query_many_from_string", "get_json_bool", "get_json_double",
            "get_json_int", "get_json_object", "get_json_string", "json_array",
            "json_object", "json_contains", "json_exists", "json_keys",
            "json_length", "json_pretty", "json_remove", "json_set",
            "is_json_scalar", "get_variant", "variant_query", "variant_typeof",
            "get_variant_bool", "get_variant_int", "get_variant_double", "get_variant_string",
            "get_variant_date", "get_variant_datetime", "get_variant_time", "regexp",
            "regexp_count", "regexp_extract", "regexp_extract_all", "regexp_replace",
            "regexp_split", "replace_old", "tokenize", "ngram_search",
            "ngram_search_case_insensitive", "levenshtein_distance", "levenshtein_ratio", "levenshtein_tj_distance",
            "levenshtein_tj_ratio", "cosine_similarity", "cosine_similarity_norm", "approx_cosine_similarity",
            "l2_distance", "approx_l2_distance", "parse_url", "url_extract_host",
            "url_extract_parameter", "str_to_date", "str2date", "str_to_jodatime",
            "str_to_map", "to_tera_date", "to_tera_timestamp", "md5",
            "md5sum", "md5sum_numeric", "sha2", "sm3",
            "aes_encrypt", "aes_decrypt", "to_base64", "from_base64",
            "base64_decode_binary", "base64_decode_string", "encode_fingerprint_sha256", "to_bitmap",
            "array_to_bitmap", "base64_to_bitmap", "sub_bitmap", "bitmap_and",
            "bitmap_andnot", "bitmap_contains", "bitmap_count", "bitmap_empty",
            "bitmap_from_binary", "bitmap_from_string", "bitmap_has_any", "bitmap_hash",
            "bitmap_hash64", "bitmap_max", "bitmap_min", "bitmap_or",
            "bitmap_remove", "bitmap_subset_in_range", "bitmap_subset_limit", "bitmap_to_array",
            "bitmap_to_base64", "bitmap_to_binary", "bitmap_to_string", "bitmap_xor",
            "hll_cardinality", "hll_empty", "hll_hash", "hll_serialize",
            "hll_deserialize", "percentile_approx_raw", "percentile_empty", "percentile_hash",
            "percentile_union", "st_astext", "st_aswkt", "st_circle",
            "st_contains", "st_distance_sphere", "st_geomfromtext", "st_geometryfromtext",
            "st_linefromtext", "st_linestringfromtext", "st_polyfromtext", "st_point",
            "st_polygon", "st_polygonfromtext", "st_x", "st_y",
            "all_match", "any_match", "array_distinct", "array_filter",
            "array_intersect", "array_join", "array_map", "array_sort",
            "array_sortby", "map_apply", "map_filter", "transform_keys",
            "transform_values", "distinct_map_keys", "array_sort_lambda", "array_generate",
            "array_repeat", "array_top_n", "array_flatten", "array_concat",
            "arrays_zip", "map_from_arrays", "map_concat", "map_entries",
            "dict_mapping", "dictionary_get", "lookup_string", "http_request",
            "ai_query");

    private AdaptiveDopCostGuard() {
    }

    /**
     * Inspect the finalized execution expressions, including nested calls, common expressions,
     * aggregate arguments, scan predicates and sink partition expressions. Walking the Thrift
     * objects avoids maintaining a second, incomplete list of expression fields for every node.
     * This does not serialize bytes or alter the plan. Scalar values and binary payloads are leaves.
     */
    public static boolean containsExpensiveFunction(Object value) {
        if (value instanceof TFunction function) {
            return function.getBinary_type() != TFunctionBinaryType.BUILTIN
                    || function.getName() == null
                    || function.getName().getFunction_name() == null
                    || EXPENSIVE_FUNCTIONS.contains(function.getName().getFunction_name().toLowerCase(Locale.ROOT));
        }
        if (value instanceof TBase<?, ?> struct) {
            return containsExpensiveFunctionInStruct(struct);
        }
        if (value instanceof Iterable<?> elements) {
            for (Object element : elements) {
                if (containsExpensiveFunction(element)) {
                    return true;
                }
            }
        } else if (value instanceof Map<?, ?> map) {
            for (Object element : map.values()) {
                if (containsExpensiveFunction(element)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static <T extends TBase<T, F>, F extends TFieldIdEnum>
            boolean containsExpensiveFunctionInStruct(TBase<T, F> struct) {
        @SuppressWarnings("unchecked")
        Class<T> type = (Class<T>) struct.getClass();
        for (F field : FieldMetaData.getStructMetaDataMap(type).keySet()) {
            if (struct.isSet(field) && containsExpensiveFunction(struct.getFieldValue(field))) {
                return true;
            }
        }
        return false;
    }
}
