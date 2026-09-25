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

// Scalar functions whose per-row cost justifies lazy (filtered) evaluation inside a CASE/IF/IFNULL/
// COALESCE value-branch (two-phase conditional evaluation; see case_expr.cpp / condition_expr.cpp).
//
// Names are the lowercase canonical function names seen at runtime as _fn.name.function_name
// (FunctionName lowercases at registration, FunctionName.java). Matching is by name, overload-independent.
// Hand-maintained; intentionally NOT generated. Rationale and the CHEAP complement live in
// handbook/notes/function-cost-classification.md. EXPENSIVE iff, per row, the function parses structure
// out of text, runs a regex, tokenizes text, does crypto/crypto-hash/payload-encoding, manipulates a
// bitmap/hll/percentile structure, does geospatial work, runs a higher-order/collection-building array or
// map op, or makes an external call. Everything else is CHEAP and absent here. UDFs (java/python/arrow)
// are detected structurally, not by name (see *_function_call_expr is_expensive_node overrides).
//
// This file is meant to be #included inside a string-set initializer list.

// JSON parse / query / construct
"parse_json", "json_query", "json_string", "to_json",
"json_query_from_string", "json_query_many_from_string",
"get_json_bool", "get_json_double", "get_json_int", "get_json_object", "get_json_string",
"json_array", "json_object", "json_contains", "json_exists", "json_keys", "json_length",
"json_pretty", "json_remove", "json_set", "is_json_scalar",
// Variant (`->` arrow operator lowers to json_query in FE, so it is covered above)
"get_variant", "variant_query", "variant_typeof",
// Regular expressions
"regexp", "regexp_count", "regexp_extract", "regexp_extract_all", "regexp_replace", "regexp_split",
// Text tokenization / n-gram similarity
"tokenize", "ngram_search", "ngram_search_case_insensitive",
// URL parsing (codecs url_encode/url_decode are CHEAP and absent)
"parse_url", "url_extract_host", "url_extract_parameter",
// Date/time parsed out of arbitrary text (format/extract helpers are CHEAP and absent)
"str_to_date", "str2date", "str_to_jodatime", "str_to_map", "to_tera_date", "to_tera_timestamp",
// Cryptography / crypto-hash / payload encoding (fast non-crypto hashes crc32/murmur/xxhash are CHEAP)
"md5", "md5sum", "md5sum_numeric", "sha2", "sm3", "aes_encrypt", "aes_decrypt",
"to_base64", "from_base64", "base64_decode_binary", "base64_decode_string",
// Bitmap
"to_bitmap", "array_to_bitmap", "base64_to_bitmap", "sub_bitmap",
"bitmap_and", "bitmap_andnot", "bitmap_contains", "bitmap_count", "bitmap_empty",
"bitmap_from_binary", "bitmap_from_string", "bitmap_has_any", "bitmap_hash", "bitmap_hash64",
"bitmap_max", "bitmap_min", "bitmap_or", "bitmap_remove", "bitmap_subset_in_range",
"bitmap_subset_limit", "bitmap_to_array", "bitmap_to_base64", "bitmap_to_binary",
"bitmap_to_string", "bitmap_xor",
// HLL
"hll_cardinality", "hll_empty", "hll_hash",
// Percentile (scalar ops)
"percentile_approx_raw", "percentile_empty", "percentile_hash", "percentile_union",
// Geospatial
"st_astext", "st_aswkt", "st_circle", "st_contains", "st_distance_sphere",
"st_geomfromtext", "st_geometryfromtext", "st_linefromtext", "st_linestringfromtext",
"st_polyfromtext", "st_point", "st_polygon", "st_polygonfromtext", "st_x", "st_y",
// Higher-order / collection-building array & map
"all_match", "any_match", "array_distinct", "array_filter", "array_intersect", "array_join",
"array_map", "array_sort", "array_sortby", "map_apply", "map_filter", "transform_keys",
"transform_values", "distinct_map_keys",
// External lookups / HTTP (UDFs handled structurally elsewhere)
"dict_mapping", "dictionary_get", "lookup_string", "http_request",
