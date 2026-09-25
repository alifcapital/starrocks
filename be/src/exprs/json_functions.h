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

#pragma once

#include <re2/re2.h>
#include <simdjson.h>
#include <velocypack/vpack.h>

#include <utility>

#include "common/status.h"
#include "exprs/function_context.h"
#include "exprs/function_helper.h"
#include "exprs/selected_column.h"
#include "types/logical_type.h"
#include "util/faststring.h"

namespace starrocks {

// Forward declarations
struct JsonPath;
template <LogicalType LT>
class ColumnBuilder;
namespace vpack = arangodb::velocypack;

// Pre-planned move step for fast simdjson path. Owned by NativeJsonState (FRAGMENT_LOCAL),
// shared read-only between cloned drivers.
struct JsonMoveStep {
    enum class Kind : uint8_t { Field, ArrayIndex };
    Kind kind;
    std::string field;  // owned; non-empty only when kind==Field
    int32_t index = -1; // valid only when kind==ArrayIndex
};

enum class JsonPathShape : uint8_t {
    Unsupported, // wildcard / slice / unrecognized -> legacy fallback
    SimpleFlat,  // only Field steps
    HasIndex,    // Field + ArrayIndex steps (chained selectors OK)
};

// Mutable scratch shared by calls on one OS thread. Columns own copies of extracted values.
struct JsonGetThreadState {
    simdjson::ondemand::parser parser; // reused across calls on this OS thread
    faststring padded_scratch;         // input copy + SIMDJSON_PADDING zero tail
    faststring unescape_scratch;       // backs value_get_string_safe outputs
    faststring key_scratch;            // backs field_unescaped_key_safe during object descent
    vpack::Builder leaf_builder;       // .clear()-ed before each leaf conversion
};

extern const re2::RE2 SIMPLE_JSONPATH_PATTERN;

struct SimpleJsonPath {
    std::string key; // key of a json object
    int idx;         // array index of a json array, -1 means not set, -2 means *
    bool is_valid;   // true if the path is successfully parsed

    SimpleJsonPath(std::string key_, int idx_, bool is_valid_) : key(std::move(key_)), idx(idx_), is_valid(is_valid_) {}

    std::string to_string() const {
        std::stringstream ss;
        if (!is_valid) {
            return "INVALID";
        }
        if (!key.empty()) {
            ss << key;
        }
        if (idx == -2) {
            ss << "[*]";
        } else if (idx > -1) {
            ss << "[" << idx << "]";
        }
        return ss.str();
    }

    std::string debug_string() const {
        std::stringstream ss;
        ss << "key: " << key << ", idx: " << idx << ", valid: " << is_valid;
        return ss.str();
    }
};

class JsonFunctions {
public:
    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: type column
     */
    DEFINE_VECTORIZED_FN(get_json_int);
    static StatusOr<ColumnPtr> get_json_int_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_json_int_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(get_json_bigint);
    static StatusOr<ColumnPtr> get_json_bigint_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_json_bigint_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(get_json_double);
    static StatusOr<ColumnPtr> get_json_double_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_json_double_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(get_json_string);
    static StatusOr<ColumnPtr> get_json_string_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_json_string_impl(FunctionContext*, const Inputs&);
    static StatusOr<ColumnPtr> get_json_bool_selected(FunctionContext*, const SelectedColumns&, size_t);
    DEFINE_VECTORIZED_FN(get_json_bool); // (VARCHAR, VARCHAR) -> BOOLEAN
    static StatusOr<ColumnPtr> json_query_many_from_string_selected(FunctionContext*, const SelectedColumns&, size_t);
    DEFINE_VECTORIZED_FN(json_query_many_from_string);
    static StatusOr<ColumnPtr> json_query_from_string_selected(FunctionContext*, const SelectedColumns&, size_t);
    DEFINE_VECTORIZED_FN(json_query_from_string); // (VARCHAR, VARCHAR) -> JSON, FE-fusion target

    /**
     * @param: [json, tagged_value]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: type column
     */
    DEFINE_VECTORIZED_FN(get_native_json_bool);
    static StatusOr<ColumnPtr> get_native_json_bool_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_native_json_bool_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(get_native_json_int);
    static StatusOr<ColumnPtr> get_native_json_int_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_native_json_int_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(get_native_json_bigint);
    static StatusOr<ColumnPtr> get_native_json_bigint_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_native_json_bigint_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(get_native_json_double);
    static StatusOr<ColumnPtr> get_native_json_double_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_native_json_double_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(get_native_json_string);
    static StatusOr<ColumnPtr> get_native_json_string_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> get_native_json_string_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(json_query);
    static StatusOr<ColumnPtr> json_query_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_query_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [json_string]
     * @paramType: [BinaryColumn]
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(parse_json);
    static StatusOr<ColumnPtr> parse_json_selected(FunctionContext* context, const SelectedColumns& columns,
                                                   size_t size);

    /**
     * @param: [json_column]
     * @paramType: [JsonColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(json_pretty);
    static StatusOr<ColumnPtr> json_pretty_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_pretty_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [json_column]
     * @paramType: [JsonColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(json_string);
    static StatusOr<ColumnPtr> json_string_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_string_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(json_exists);
    static StatusOr<ColumnPtr> json_exists_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_exists_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [json_object, json_value]
     * @paramType: [JsonColumn, JsonColumn]
     * @return: BooleanColumn
     */
    DEFINE_VECTORIZED_FN(json_contains);
    static StatusOr<ColumnPtr> json_contains_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_contains_impl(FunctionContext*, const Inputs&);

    /**
     * Build json object from json values
     * @param: [field_name, field_value, ...]
     * @paramType: [JsonColumn, JsonColumn, ...]
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(json_object);
    static StatusOr<ColumnPtr> json_object_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_object_impl(FunctionContext*, const Inputs&);

    /**
     * Build empty json object 
     * @param: 
     * @paramType: 
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(json_object_empty);

    /**
     * Build json array from json values
     * @param: [json_object, ...]
     * @paramType: [JsonColumn, ...]
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(json_array);
    static StatusOr<ColumnPtr> json_array_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_array_impl(FunctionContext*, const Inputs&);

    /**
     * Build empty json array 
     * @param: 
     * @paramType: 
     * @return: JsonColumn
     */
    DEFINE_VECTORIZED_FN(json_array_empty);

    /**
     * Return number of elements in a JSON object/array
     * @param JSON, JSONPath
     * @return number of elements if it's object or array, otherwise return 1
     */
    DEFINE_VECTORIZED_FN(json_length);
    static StatusOr<ColumnPtr> json_length_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_length_impl(FunctionContext*, const Inputs&);

    /**
     * Returns the keys from the top-level value of a JSON object as a JSON array
     * 
     */
    DEFINE_VECTORIZED_FN(json_keys);
    static StatusOr<ColumnPtr> json_keys_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_keys_impl(FunctionContext*, const Inputs&);

    /**
     * Remove data from a JSON document at one or more specified JSON paths
     * @param JSON, JSONPath, [JSONPath, ...]
     * @return JSON with specified paths removed
     */
    DEFINE_VECTORIZED_FN(json_remove);
    static StatusOr<ColumnPtr> json_remove_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_remove_impl(FunctionContext*, const Inputs&);

    /**
     * Inserts or updates data in a JSON document at one or more specified JSON paths
     * @param JSON, JSONPath, Value, [JSONPath, Value, ...]
     * @return Modified JSON
     */
    DEFINE_VECTORIZED_FN(json_set);
    static StatusOr<ColumnPtr> json_set_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_set_impl(FunctionContext*, const Inputs&);


    /**
     * Return json built from struct/map
     */
    DEFINE_VECTORIZED_FN(to_json);
    static StatusOr<ColumnPtr> to_json_selected(FunctionContext*, const SelectedColumns&, size_t);

    static Status json_query_many_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status json_query_many_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    static Status native_json_path_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status native_json_path_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    // extract_from_object extracts value from object according to the json path.
    // Now, we do not support complete functions of json path.
    static Status extract_from_object(simdjson::ondemand::object& obj, const std::vector<SimpleJsonPath>& jsonpath,
                                      simdjson::ondemand::value* value) noexcept;

    static Status parse_json_paths(const std::string& path_strings, std::vector<SimpleJsonPath>* parsed_paths);

    // jsonpaths_to_string serializes json patsh to std::string. Setting sub_index to serializes paritially json paths.
    static std::string jsonpaths_to_string(const std::vector<SimpleJsonPath>& jsonpaths, size_t sub_index = -1);

    template <typename ValueType>
    static std::string_view to_json_string(ValueType&& val, size_t limit) {
        std::string_view sv = simdjson::to_json_string(std::forward<ValueType>(val));
        if (sv.size() > limit) {
            return sv.substr(0, limit);
        }
        return sv;
    }

private:
    template <typename Inputs>
    static StatusOr<ColumnPtr> json_query_many_from_string_impl(FunctionContext*, const Inputs&);
    template <LogicalType ResultType, typename Inputs>
    static StatusOr<ColumnPtr> _json_query_impl(FunctionContext* context, const Inputs& columns);

    template <LogicalType RresultType, typename Inputs>
    static StatusOr<ColumnPtr> _flat_json_query_impl(FunctionContext* context, const Inputs& columns);

    template <LogicalType RresultType, typename Inputs>
    static StatusOr<ColumnPtr> _full_json_query_impl(FunctionContext* context, const Inputs& columns);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: BooleanColumn
     */
    template <typename Inputs>
    static StatusOr<ColumnPtr> _flat_json_exists(FunctionContext*, const Inputs&);
    template <typename Inputs>
    static StatusOr<ColumnPtr> _full_json_exists(FunctionContext*, const Inputs&);

    /**
     * Return number of elements in a JSON object/array
     * @param JSON, JSONPath
     * @return number of elements if it's object or array, otherwise return 1
     */
    template <typename Inputs>
    static StatusOr<ColumnPtr> _flat_json_length(FunctionContext*, const Inputs&);
    template <typename Inputs>
    static StatusOr<ColumnPtr> _full_json_length(FunctionContext*, const Inputs&);

    /**
     * Returns the keys from the top-level value of a JSON object as a JSON array
     */
    template <typename Inputs>
    static StatusOr<ColumnPtr> _json_keys_without_path(FunctionContext*, const Inputs&);
    template <typename Inputs>
    static StatusOr<ColumnPtr> _flat_json_keys_with_path(FunctionContext*, const Inputs&);
    template <typename Inputs>
    static StatusOr<ColumnPtr> _full_json_keys_with_path(FunctionContext*, const Inputs&);

    template <LogicalType RresultType, typename Inputs>
    static StatusOr<ColumnPtr> _get_json_value(FunctionContext*, const Inputs&);

    // Pre-decompose a constant JsonPath into flat moves + classify shape.
    // Sets state->fast_shape and state->fast_moves. Called once at FRAGMENT_LOCAL prepare.
    static void _plan_fast_moves(const JsonPath& path, struct NativeJsonState* state);

    enum class ExtractResult : uint8_t {
        Handled,       // value or NULL appended; row complete
        FallbackRow,   // caller should invoke _fallback_extract_one for this row
        FallbackBatch, // reserved for future; v4 does not use it
    };

    // Per-row fast extraction. Returns Handled when the row produced output via the simdjson path,
    // FallbackRow for bare-scalar / empty / whitespace-only inputs that need legacy parse_json_or_string semantics.
    template <LogicalType ResultType>
    static ExtractResult _fused_extract_one(const Slice& raw_json, const struct NativeJsonState* fragment_state,
                                            JsonGetThreadState* thread_state, ColumnBuilder<ResultType>& out);

    // Per-row fallback that mirrors legacy parse_json_or_string + JsonPath::extract + cast_vpjson_to.
    // Always appends exactly one row (value or NULL). Status::OK is the only return.
    template <LogicalType ResultType>
    static Status _fallback_extract_one(const Slice& raw_json, const struct NativeJsonState* fragment_state,
                                        JsonGetThreadState* thread_state, ColumnBuilder<ResultType>& out);

    // Drives the fully-fused per-row loop. Caller has already verified fast-path eligibility.
    template <LogicalType ResultType, typename Inputs>
    static StatusOr<ColumnPtr> _fused_get_json_value(FunctionContext* context, const Inputs& columns,
                                                     const struct NativeJsonState* fstate, JsonGetThreadState* tstate);

    friend void set_json_fast_path_disabled_for_test(FunctionContext*, bool);

    /**
     * @param: [json_object, json_path]
     * @paramType: [JsonColumn, BinaryColumn]
     * @return: JsonColumn
     */

    static Status _get_parsed_paths(const std::vector<std::string>& path_exprs,
                                    std::vector<SimpleJsonPath>* parsed_paths);

    // Helper function to check if target JSON contains candidate JSON
    static bool json_value_contains(JsonValue* target, JsonValue* candidate);
};

// Test-only: force the fused fast path on/off for the given FunctionContext after
// FRAGMENT_LOCAL prepare. Differential tests and the json-extract benchmark use this
// to feed identical inputs through both paths and compare results.
void set_json_fast_path_disabled_for_test(FunctionContext* ctx, bool disabled);

} // namespace starrocks
