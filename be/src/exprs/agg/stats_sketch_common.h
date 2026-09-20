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

#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>

#include "column/column_helper.h"
#include "column/datum.h"
#include "column/datum_convert.h"
#include "column/type_traits.h"
#include "common/status.h"
#include "datasketches/serde.hpp"
#include "exprs/function_context.h"
#include "runtime/mem_pool.h"
#include "storage/types.h"
#include "types/logical_type.h"
#include "util/hash_util.hpp"
#include "util/slice.h"

// Shared pieces of the sketch-based statistics aggregates (ds_frequent_items, ds_kll_quantiles,
// histogram_by_bounds): the item type a sketch stores for a column, hashing and serialization of
// those items, and the string form of values that the statistics tables keep.

namespace starrocks {

// Item type a DataSketches sketch stores for a column of logical type LT. Strings need an owning
// type because the sketch keeps items after the input chunk is gone; every other supported type
// is a trivially copyable value.
template <LogicalType LT>
using SketchItemType = std::conditional_t<lt_is_string<LT>, std::string, RunTimeCppType<LT>>;

// Hash functor for sketch items and for the MCV lookup table. std::hash is not specialized for
// DateValue, TimestampValue or DecimalV2Value, so fixed-length items are hashed by their bytes.
template <typename T>
struct SketchItemHash {
    size_t operator()(const T& v) const {
        if constexpr (std::is_floating_point_v<T>) {
            // -0.0 == 0.0, so both must hash alike.
            T normalized = v == 0 ? T(0) : v;
            return HashUtil::murmur_hash64A(&normalized, sizeof(normalized), HashUtil::MURMUR_SEED);
        } else {
            return HashUtil::murmur_hash64A(&v, sizeof(v), HashUtil::MURMUR_SEED);
        }
    }
};

template <>
struct SketchItemHash<std::string> {
    size_t operator()(const std::string& v) const { return std::hash<std::string>()(v); }
};

template <>
struct SketchItemHash<Slice> {
    size_t operator()(const Slice& v) const { return HashUtil::murmur_hash64A(v.data, v.size, HashUtil::MURMUR_SEED); }
};

// Byte-copy SerDe for fixed-length sketch items. The serialized sketch only travels between BEs
// of one cluster within a single query, so byte order and alignment are not a concern.
template <typename T>
struct FixedSketchItemSerde {
    void serialize(std::ostream& os, const T* items, unsigned num) const {
        os.write(reinterpret_cast<const char*>(items), sizeof(T) * num);
    }
    void deserialize(std::istream& is, T* items, unsigned num) const {
        is.read(reinterpret_cast<char*>(items), sizeof(T) * num);
    }
    size_t size_of_item(const T&) const { return sizeof(T); }
    size_t serialize(void* ptr, size_t capacity, const T* items, unsigned num) const {
        const size_t bytes = sizeof(T) * num;
        datasketches::check_memory_size(bytes, capacity);
        memcpy(ptr, items, bytes);
        return bytes;
    }
    size_t deserialize(const void* ptr, size_t capacity, T* items, unsigned num) const {
        const size_t bytes = sizeof(T) * num;
        datasketches::check_memory_size(bytes, capacity);
        memcpy(items, ptr, bytes);
        return bytes;
    }
};

template <typename T>
using SketchItemSerde =
        std::conditional_t<std::is_same_v<T, std::string>, datasketches::serde<std::string>, FixedSketchItemSerde<T>>;

// Converts column values to and from the string form the statistics tables store. The column's
// TypeInfo formats decimals with their scale and dates as literals, which is what the FE parses.
template <LogicalType LT>
class StatsValueCodec {
public:
    using CppType = RunTimeCppType<LT>;

    explicit StatsValueCodec(const FunctionContext::TypeDesc* type_desc)
            : _type_info(get_type_info(LT, type_desc->precision, type_desc->scale)) {}

    std::string to_string(const CppType& value) const { return datum_to_string(_type_info.get(), Datum(value)); }

    // String values are copied into mem_pool so the returned Slice outlives `text`.
    Status from_string(const std::string& text, CppType* value, MemPool* mem_pool) const {
        Datum datum;
        RETURN_IF_ERROR(datum_from_string(_type_info.get(), &datum, text, mem_pool));
        *value = datum.get<CppType>();
        if constexpr (!lt_is_string<LT>) {
            // The type's parser reads what it can and reports nothing, so the value must print back
            // as it was given; the texts come from to_string in the first place.
            if (to_string(*value) != text) {
                return Status::InvalidArgument("not a value of the column type: " + text);
            }
        }
        return Status::OK();
    }

private:
    TypeInfoPtr _type_info;
};

// Appends `text` to `out` as a quoted JSON string literal.
inline void append_json_string(std::string& out, std::string_view text) {
    static constexpr char HEX[] = "0123456789abcdef";
    out.push_back('"');
    for (unsigned char c : text) {
        switch (c) {
        case '"':
            out += "\\\"";
            break;
        case '\\':
            out += "\\\\";
            break;
        case '\n':
            out += "\\n";
            break;
        case '\r':
            out += "\\r";
            break;
        case '\t':
            out += "\\t";
            break;
        default:
            if (c < 0x20) {
                out += "\\u00";
                out.push_back(HEX[c >> 4]);
                out.push_back(HEX[c & 0xf]);
            } else {
                out.push_back(static_cast<char>(c));
            }
        }
    }
    out.push_back('"');
}

// Reads the constant INT argument at `idx`; `fallback` when the call has fewer arguments.
inline int32_t stats_const_int_arg(FunctionContext* ctx, const char* fn_name, int idx, int32_t fallback) {
    if (ctx->get_num_args() <= idx) {
        return fallback;
    }
    ColumnPtr column = ctx->get_constant_column(idx);
    if (column == nullptr || column->only_null()) {
        throw std::runtime_error(std::string(fn_name) + ": argument " + std::to_string(idx + 1) +
                                 " must be a non-null constant integer");
    }
    return ColumnHelper::get_const_value<TYPE_INT>(column);
}

// Reads the constant VARCHAR argument at `idx`.
inline std::string stats_const_string_arg(FunctionContext* ctx, const char* fn_name, int idx) {
    ColumnPtr column = ctx->get_num_args() > idx ? ctx->get_constant_column(idx) : nullptr;
    if (column == nullptr || column->only_null()) {
        throw std::runtime_error(std::string(fn_name) + ": argument " + std::to_string(idx + 1) +
                                 " must be a non-null constant string");
    }
    return ColumnHelper::get_const_value<TYPE_VARCHAR>(column).to_string();
}

} // namespace starrocks
