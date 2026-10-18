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

#include "formats/parquet/column_converter.h"

#include <cctz/time_zone.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <sstream>
#include <type_traits>
#include <utility>
#include <vector>

#include "column/binary_column.h"
#include "column/column.h"
#include "column/column_helper.h"
#include "column/fixed_length_column.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "formats/parquet/schema.h"
#include "formats/parquet/types.h"
#include "gutil/casts.h"
#include "gutil/integral_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/time_types.h"
#include "runtime/types.h"
#include "storage/olap_common.h"
#include "types/date_value.h"
#include "types/logical_type.h"
#include "types/timestamp_value.h"
#include "util/bit_util.h"
#include "util/decimal_types.h"
#include "util/int96.h"
#include "util/timezone_utils.h"

namespace starrocks::parquet {

// When doing decimal convert, source scale may not equal with destination scale,
// when destination scale is greater than source, source unscaled data will be scaled up
// to match destination scale.
enum class DecimalScaleType { kNoScale, kScaleUp, kScaleDown };

// The widest decimal of StarRocks is decimal256, so a scale above 76 is not a scale of any decimal.
static constexpr int32_t kMaxDecimalScale = 76;
// The scale factor is a power of ten of int128, and the table of such powers ends at 10^38.
static constexpr int32_t kMaxScaleFactorDigits = 38;

// How a decimal converter gets from the scale of the file to the scale of the destination.
struct DecimalScalePlan {
    DecimalScaleType type = DecimalScaleType::kNoScale;
    // The converter multiplies or divides the unscaled value by 10^digits, and digits is in [0, 38].
    int32_t digits = 0;
    // The scale of the file is more than 38 digits above the scale of the destination, so the divisor is not
    // an int128 and the converter makes every row NULL.
    bool all_null = false;
};

static std::string describe_column(const ParquetField& field, const std::string& file_name) {
    if (file_name.empty()) {
        return strings::Substitute("column '$0'", field.name);
    }
    return strings::Substitute("column '$0' in file '$1'", field.name, file_name);
}

// We fear a scale or precision in the footer that makes the scale factor read outside its table, which holds
// the powers 0..38. We expect some writers to put a scale above the precision; a scale in 0..76 is safe for the
// table lookups, so we read such files and reject only a scale outside 0..76 or a negative precision, with a
// Corruption that names the column and the file. The destination scale must be a scale of int128.
// A scale of the file that is more than 38 digits above the destination scale gets a plan that makes every row
// NULL, as a FIXED_LEN_BYTE_ARRAY wider than int128 does.
static StatusOr<DecimalScalePlan> make_decimal_scale_plan(const ParquetField& field, int32_t dst_scale,
                                                          const std::string& file_name) {
    if (field.scale < 0 || field.scale > kMaxDecimalScale || field.precision < 0) {
        return Status::Corruption(strings::Substitute(
                "Invalid decimal scale $0 and precision $1 of $2, expect 0 <= scale <= $3 and 0 <= precision",
                field.scale, field.precision, describe_column(field, file_name), kMaxDecimalScale));
    }
    if (dst_scale < 0 || dst_scale > kMaxScaleFactorDigits) {
        return Status::InternalError(strings::Substitute("Invalid scale $0 of the destination decimal type of $1",
                                                         dst_scale, describe_column(field, file_name)));
    }
    DecimalScalePlan plan;
    if (field.scale < dst_scale) {
        // The gap is at most dst_scale, which is at most 38.
        plan.type = DecimalScaleType::kScaleUp;
        plan.digits = dst_scale - field.scale;
    } else if (field.scale > dst_scale) {
        if (field.scale - dst_scale > kMaxScaleFactorDigits) {
            plan.all_null = true;
        } else {
            plan.type = DecimalScaleType::kScaleDown;
            plan.digits = field.scale - dst_scale;
        }
    }
    return plan;
}

class Int32ToDateConverter final : public ColumnConverter {
public:
    Int32ToDateConverter() = default;
    ~Int32ToDateConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int32ToTimeConverter final : public ColumnConverter {
public:
    Int32ToTimeConverter() = default;
    ~Int32ToTimeConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int32ToDateTimeConverter final : public ColumnConverter {
public:
    Int32ToDateTimeConverter() = default;
    ~Int32ToDateTimeConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int64ToTimeConverter final : public ColumnConverter {
public:
    Int64ToTimeConverter() = default;
    ~Int64ToTimeConverter() override = default;

    Status convert(const Column* src, Column* dst) override;
};

class Int96ToDateTimeConverter final : public ColumnConverter {
public:
    Int96ToDateTimeConverter() = default;
    ~Int96ToDateTimeConverter() override = default;

    Status init(const std::string& timezone);
    // convert column from int96 to timestamp
    Status convert(const Column* src, Column* dst) override;

private:
    int _offset = 0;
    cctz::time_zone _ctz;
};

class Int64ToDateTimeConverter final : public ColumnConverter {
public:
    Int64ToDateTimeConverter() = default;
    ~Int64ToDateTimeConverter() override = default;

    Status init(const std::string& timezone, const tparquet::SchemaElement& schema_element);
    Status convert(const Column* src, Column* dst) override;

private:
    bool _is_adjusted_to_utc = false;
    int _offset = 0;
    cctz::time_zone _ctz;
    int64_t _second_mask = 0;
    int64_t _scale_to_nano_factor = 0;
};

template <typename SourceType, typename DestType, bool kSourceUnsigned = false>
void convert_int_to_int(const SourceType* __restrict__ src, DestType* __restrict__ dst, size_t size) {
    for (size_t i = 0; i < size; i++) {
        if constexpr (kSourceUnsigned && std::is_integral_v<SourceType>) {
            // Reinterpret the source through its unsigned bit pattern before widening so a
            // high-bit unsigned value is zero-extended instead of sign-extended.
            dst[i] = DestType(static_cast<std::make_unsigned_t<SourceType>>(src[i]));
        } else {
            dst[i] = DestType(src[i]);
        }
    }
}

// Support int => int and float => double
template <typename SourceType, typename DestType, bool kSourceUnsigned = false>
class NumericToNumericConverter final : public ColumnConverter {
public:
    NumericToNumericConverter() = default;
    ~NumericToNumericConverter() override = default;

    Status convert(const Column* src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column =
                ColumnHelper::as_raw_column<FixedLengthColumn<SourceType>>(src_nullable_column->data_column());
        auto* dst_column =
                ColumnHelper::as_raw_column<FixedLengthColumn<DestType>>(dst_nullable_column->data_column_raw_ptr());

        auto& src_data = src_column->get_data();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

        size_t size = dst_null_data.size();
        memcpy(dst_null_data.data(), src_null_data.data(), size);
        convert_int_to_int<SourceType, DestType, kSourceUnsigned>(src_data.data(), dst_data.data(), size);
        dst_nullable_column->set_has_null(src_nullable_column->has_null());
        return Status::OK();
    }
};

template <typename SourceType, LogicalType DestType, bool kSourceUnsigned = false>
class PrimitiveToDecimalConverter final : public ColumnConverter {
public:
    using DestDecimalType = typename RunTimeTypeTraits<DestType>::CppType;
    using DestColumnType = typename RunTimeTypeTraits<DestType>::ColumnType;
    using DestPrimitiveType = typename RunTimeTypeTraits<TYPE_DECIMAL128>::CppType;

    explicit PrimitiveToDecimalConverter(const DecimalScalePlan& plan)
            : _scale_type(plan.type), _all_null(plan.all_null) {
        DCHECK(plan.digits >= 0 && plan.digits <= kMaxScaleFactorDigits);
        if (_scale_type != DecimalScaleType::kNoScale) {
            _scale_factor = get_scale_factor<DestPrimitiveType>(plan.digits);
        }
    }

    Status convert(const Column* src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column =
                ColumnHelper::as_raw_column<FixedLengthColumn<SourceType>>(src_nullable_column->data_column());
        auto* dst_column = ColumnHelper::as_raw_column<DestColumnType>(dst_nullable_column->data_column_raw_ptr());

        auto& src_data = src_column->get_data();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

        bool has_null = false;
        size_t size = src_column->size();
        if (_all_null) {
            memset(dst_null_data.data(), 0x1, size);
            dst_nullable_column->set_has_null(size > 0);
            return Status::OK();
        }
        for (size_t i = 0; i < size; i++) {
            dst_null_data[i] = src_null_data[i];
            if (dst_null_data[i]) {
                has_null = true;
                continue;
            }
            DestPrimitiveType value;
            if constexpr (kSourceUnsigned && std::is_integral_v<SourceType>) {
                // Reinterpret the source through its unsigned bit pattern so a high-bit
                // unsigned value is zero-extended instead of sign-extended.
                value = static_cast<std::make_unsigned_t<SourceType>>(src_data[i]);
            } else {
                value = src_data[i];
            }
            if (_scale_type == DecimalScaleType::kScaleUp) {
                // We fear that a large scale gap makes the product exceed int128, since the scale factor can
                // be 10^38. Signed overflow is undefined behavior, so we multiply in unsigned arithmetic, where
                // an overflow wraps. A value that does not fit the destination type is not detected here:
                // the cast to the destination type keeps its low bits.
                value = static_cast<DestPrimitiveType>(static_cast<uint128_t>(value) *
                                                       static_cast<uint128_t>(_scale_factor));
            } else if (_scale_type == DecimalScaleType::kScaleDown) {
                value /= _scale_factor;
            }
            dst_data[i] = DestDecimalType(value);
        }
        dst_nullable_column->set_has_null(has_null);
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type = DecimalScaleType::kNoScale;
    DestPrimitiveType _scale_factor = 1;
    bool _all_null = false;
};

// This class is to convert *fixed length* binary to decimal
// and for fixed length binary in parquet, string data is contiguous,
// and that's why we can do memcpy 8 bytes without accessing invalid address.
template <LogicalType DestType>
class BinaryToDecimalConverter final : public ColumnConverter {
public:
    using DecimalType = typename RunTimeTypeTraits<DestType>::CppType;
    using ColumnType = typename RunTimeTypeTraits<DestType>::ColumnType;
    using DestPrimitiveType = typename RunTimeTypeTraits<TYPE_DECIMAL128>::CppType;
    BinaryToDecimalConverter(const DecimalScalePlan& plan, int32_t type_length)
            : _scale_type(plan.type), _type_length(type_length), _all_null(plan.all_null) {
        DCHECK(plan.digits >= 0 && plan.digits <= kMaxScaleFactorDigits);
        if (_scale_type != DecimalScaleType::kNoScale) {
            _scale_factor = get_scale_factor<DestPrimitiveType>(plan.digits);
        }
    }

    template <int BINSZ, DecimalScaleType scale_type, typename T, bool has_null>
    void t_convert(size_t size, uint8_t* dst_null_data, uint8_t* src_null_data, DecimalType* dst_data,
                   const uint8* src_data) {
        if constexpr (!has_null) {
            memset(dst_null_data, 0x0, size);
        } else {
            memcpy(dst_null_data, src_null_data, size);
        }

        for (size_t i = 0; i < size; i++) {
            if constexpr (has_null) {
                if (dst_null_data[i]) continue;
            }
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.

            DestPrimitiveType unscale = 0;
            T value = 0;
            static_assert(BINSZ <= sizeof(value));

            // NOTE(yan): since fixed length binary data is contiguous, with condition check (i+8) < size
            // we can assure that there is no illegal memory access
            // UPDATE: bytes are allocated by `RawVectorPad16`, so there will be extra bytes at tail.

            // mempcy 8 bytes to compiler, it's instruction to move 8 bytes from memory to register
            // and follow actions are all in-register instructions.
            memcpy(reinterpret_cast<char*>(&value), src_data, sizeof(value));
            value = BitUtil::big_endian_to_host(value);
            value = value >> ((sizeof(value) - BINSZ) * 8);
            unscale = value;

            src_data += BINSZ;
            // hardware branch predicator works well here.
            if constexpr (scale_type == DecimalScaleType::kScaleUp) {
                // Unsigned multiplication, so that an overflow of a large scale gap wraps and is not
                // undefined behavior, as in PrimitiveToDecimalConverter.
                unscale = static_cast<DestPrimitiveType>(static_cast<uint128_t>(unscale) *
                                                         static_cast<uint128_t>(_scale_factor));
            }
            if constexpr (scale_type == DecimalScaleType::kScaleDown) {
                unscale /= _scale_factor;
            }
            dst_data[i] = DecimalType(unscale);
        }
    }

    Status convert(const Column* src, Column* dst) override {
        auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
        // hive only support null column
        // TODO: support not null
        auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
        dst_nullable_column->resize_uninitialized(src_nullable_column->size());

        auto* src_column = ColumnHelper::as_raw_column<BinaryColumn>(src_nullable_column->data_column());
        auto* dst_column = ColumnHelper::as_raw_column<ColumnType>(dst_nullable_column->data_column_raw_ptr());

        auto src_data = src_column->get_immutable_bytes();
        auto& dst_data = dst_column->get_data();
        auto& src_null_data = src_nullable_column->null_column()->get_data();
        auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

        size_t size = src_column->size();
        // The switch below has a case for each width from 1 to 16 and nothing else, so we must not reach
        // it with a width below 1.
        if (UNLIKELY(_type_length <= 0)) {
            return Status::Corruption(
                    strings::Substitute("Invalid type length $0 of FIXED_LEN_BYTE_ARRAY decimal", _type_length));
        }
        // A value wider than int128, or a scale of the file that has no int128 divisor, cannot be converted, so
        // every row is NULL.
        if (_all_null || _type_length > sizeof(DestPrimitiveType)) {
            memset(dst_null_data.data(), 0x1, size);
            dst_nullable_column->set_has_null(true);
            return Status::OK();
        }

        bool has_null = src_nullable_column->has_null();

        // The conversion walks the bytes of the column with a stride of the type length and reads that
        // many bytes for each non-null row. A value that is shorter than the type length (for example a
        // min/max statistic in a corrupt file) would make it read past the end of the bytes, so we check
        // that the column holds the bytes of every non-null row.
        const size_t num_not_null = size - src_nullable_column->null_count();
        if (UNLIKELY(src_data.size() < num_not_null * static_cast<size_t>(_type_length))) {
            return Status::Corruption(strings::Substitute(
                    "FIXED_LEN_BYTE_ARRAY decimal column holds $0 bytes for $1 non-null values of type length $2",
                    src_data.size(), num_not_null, _type_length));
        }

        // For calling `src_data.get_bytes().data()` , we don't need to call `build_slices` underneath.
        // And notice bytes are allocated by `RawVectorPad16`, there will be extra 16 bytes.
#define M(SZ, K, T)                                                                                            \
    case SZ:                                                                                                   \
        if (has_null) {                                                                                        \
            t_convert<SZ, K, T, true>(size, dst_null_data.data(), const_cast<uint8_t*>(src_null_data.data()),  \
                                      dst_data.data(), src_data.data());                                       \
        } else {                                                                                               \
            t_convert<SZ, K, T, false>(size, dst_null_data.data(), const_cast<uint8_t*>(src_null_data.data()), \
                                       dst_data.data(), src_data.data());                                      \
        }                                                                                                      \
        break;

#define MX(T)          \
    M(1, T, int64_t)   \
    M(2, T, int64_t)   \
    M(3, T, int64_t)   \
    M(4, T, int64_t)   \
    M(5, T, int64_t)   \
    M(6, T, int64_t)   \
    M(7, T, int64_t)   \
    M(8, T, int64_t)   \
    M(9, T, int128_t)  \
    M(10, T, int128_t) \
    M(11, T, int128_t) M(12, T, int128_t) M(13, T, int128_t) M(14, T, int128_t) M(15, T, int128_t) M(16, T, int128_t)

        if (_scale_type == DecimalScaleType::kScaleUp) {
            switch (_type_length) {
                MX(DecimalScaleType::kScaleUp);
            default:
                __builtin_unreachable();
            }
        } else if (_scale_type == DecimalScaleType::kScaleDown) {
            switch (_type_length) {
                MX(DecimalScaleType::kScaleDown);
            default:
                __builtin_unreachable();
            }
        } else {
            switch (_type_length) {
                MX(DecimalScaleType::kNoScale);
            default:
                __builtin_unreachable();
            }
        }
        dst_nullable_column->set_has_null(has_null);
        return Status::OK();
    }

private:
    DecimalScaleType _scale_type = DecimalScaleType::kNoScale;
    DestPrimitiveType _scale_factor = 1;
    int32_t _type_length = 0;
    bool _all_null = false;
};

template <typename DestType>
static std::unique_ptr<ColumnConverter> make_int32_numeric_converter(bool src_unsigned) {
    if (src_unsigned) {
        return std::make_unique<NumericToNumericConverter<int32_t, DestType, true>>();
    }
    return std::make_unique<NumericToNumericConverter<int32_t, DestType, false>>();
}

template <LogicalType DestType>
static std::unique_ptr<ColumnConverter> make_int32_decimal_converter(bool src_unsigned, const DecimalScalePlan& plan) {
    if (src_unsigned) {
        return std::make_unique<PrimitiveToDecimalConverter<int32_t, DestType, true>>(plan);
    }
    return std::make_unique<PrimitiveToDecimalConverter<int32_t, DestType, false>>(plan);
}

Status ColumnConverterFactory::create_converter(const ParquetField& field, const TypeDescriptor& typeDescriptor,
                                                const std::string& timezone,
                                                std::unique_ptr<ColumnConverter>* converter,
                                                const std::string& file_name) {
    LogicalType col_type = typeDescriptor.type;
    bool need_convert = false;
    tparquet::Type::type parquet_type = field.physical_type;
    const auto& schema_element = field.schema_element;

    // the reason why there is down conversion of integer type is
    // assume we create a hive column called `col0` whose type is `tinyint`
    // but when we insert value into `col0`, the physical type in parquet file is actually `INT32`
    // so when we read `col0` from parquet file, we have to do a type conversion from int32_t to int8_t.
    switch (parquet_type) {
    case tparquet::Type::type::BOOLEAN: {
        if (col_type != LogicalType::TYPE_BOOLEAN) {
            need_convert = true;
        }
        break;
    }
    case tparquet::Type::type::INT32: {
        const bool src_unsigned = is_unsigned_integer(schema_element);
        if (col_type != LogicalType::TYPE_INT) {
            need_convert = true;
        }
        switch (col_type) {
        case LogicalType::TYPE_TINYINT:
            *converter = make_int32_numeric_converter<int8_t>(src_unsigned);
            break;
        case LogicalType::TYPE_SMALLINT:
            *converter = make_int32_numeric_converter<int16_t>(src_unsigned);
            break;
        case LogicalType::TYPE_BIGINT:
            *converter = make_int32_numeric_converter<int64_t>(src_unsigned);
            break;
        case LogicalType::TYPE_DOUBLE:
            *converter = make_int32_numeric_converter<double>(src_unsigned);
            break;
        case LogicalType::TYPE_TIME:
            *converter = std::make_unique<Int32ToTimeConverter>();
            break;
        case LogicalType::TYPE_DATE:
            *converter = std::make_unique<Int32ToDateConverter>();
            break;
        case LogicalType::TYPE_DATETIME:
            *converter = std::make_unique<Int32ToDateTimeConverter>();
            break;
            // when decimal precision is greater than 27, precision may be lost in the following
            // process. However to handle most enviroment, we also make progress other than
            // rejection
        case LogicalType::TYPE_DECIMALV2: {
            // All DecimalV2 use scale 9 as scale
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, 9, file_name));
            *converter = make_int32_decimal_converter<TYPE_DECIMALV2>(src_unsigned, plan);
            break;
        }
        case LogicalType::TYPE_DECIMAL32: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = make_int32_decimal_converter<TYPE_DECIMAL32>(src_unsigned, plan);
            break;
        }
        case LogicalType::TYPE_DECIMAL64: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = make_int32_decimal_converter<TYPE_DECIMAL64>(src_unsigned, plan);
            break;
        }
        case LogicalType::TYPE_DECIMAL128: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = make_int32_decimal_converter<TYPE_DECIMAL128>(src_unsigned, plan);
            break;
        }
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT64: {
        if (col_type != LogicalType::TYPE_BIGINT) {
            need_convert = true;
        }
        switch (col_type) {
        case LogicalType::TYPE_TINYINT:
            *converter = std::make_unique<NumericToNumericConverter<int64_t, int8_t>>();
            break;
        case LogicalType::TYPE_SMALLINT:
            *converter = std::make_unique<NumericToNumericConverter<int64_t, int16_t>>();
            break;
        case LogicalType::TYPE_INT:
            *converter = std::make_unique<NumericToNumericConverter<int64_t, int32_t>>();
            break;
            // when decimal precision is greater than 27, precision may be lost in the following
            // process. However to handle most enviroment, we also make progress other than
            // rejection
        case LogicalType::TYPE_DECIMALV2: {
            // All DecimalV2 use scale 9 as scale
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, 9, file_name));
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMALV2>>(plan);
            break;
        }
        case LogicalType::TYPE_DECIMAL32: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL32>>(plan);
            break;
        }
        case LogicalType::TYPE_DECIMAL64: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL64>>(plan);
            break;
        }
        case LogicalType::TYPE_DECIMAL128: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = std::make_unique<PrimitiveToDecimalConverter<int64_t, TYPE_DECIMAL128>>(plan);
            break;
        }

        case LogicalType::TYPE_DATETIME: {
            auto _converter = std::make_unique<Int64ToDateTimeConverter>();
            RETURN_IF_ERROR(_converter->init(timezone, schema_element));
            *converter = std::move(_converter);
            break;
        }
        case LogicalType::TYPE_TIME: {
            auto _converter = std::make_unique<Int64ToTimeConverter>();
            *converter = std::move(_converter);
            break;
        }
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::BYTE_ARRAY: {
        // TODO don't support converter byte_array to decimal
        if (col_type != LogicalType::TYPE_VARCHAR && col_type != LogicalType::TYPE_CHAR &&
            col_type != LogicalType::TYPE_VARBINARY) {
            need_convert = true;
        }
        break;
    }
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY: {
        int32_t type_length = field.type_length;
        if (col_type != LogicalType::TYPE_VARCHAR && col_type != LogicalType::TYPE_CHAR) {
            need_convert = true;
        }
        switch (col_type) {
        case LogicalType::TYPE_DECIMALV2: {
            // All DecimalV2 use scale 9 as scale
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, 9, file_name));
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMALV2>>(plan, type_length);
            break;
        }
        case LogicalType::TYPE_DECIMAL32: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL32>>(plan, type_length);
            break;
        }
        case LogicalType::TYPE_DECIMAL64: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL64>>(plan, type_length);
            break;
        }
        case LogicalType::TYPE_DECIMAL128: {
            ASSIGN_OR_RETURN(auto plan, make_decimal_scale_plan(field, typeDescriptor.scale, file_name));
            *converter = std::make_unique<BinaryToDecimalConverter<TYPE_DECIMAL128>>(plan, type_length);
            break;
        }
        default:
            break;
        }
        break;
    }
    case tparquet::Type::type::INT96: {
        need_convert = true;
        if (col_type == LogicalType::TYPE_DATETIME) {
            auto _converter = std::make_unique<Int96ToDateTimeConverter>();
            RETURN_IF_ERROR(_converter->init(timezone));
            *converter = std::move(_converter);
        }
        break;
    }
    case tparquet::Type::FLOAT: {
        if (col_type != LogicalType::TYPE_FLOAT) {
            need_convert = true;
        }
        if (col_type == LogicalType::TYPE_DOUBLE) {
            auto _converter = std::make_unique<NumericToNumericConverter<float, double>>();
            *converter = std::move(_converter);
            break;
        }
        break;
    }
    case tparquet::Type::DOUBLE: {
        if (col_type != LogicalType::TYPE_DOUBLE) {
            need_convert = true;
        }
        break;
    }
    default:
        need_convert = true;
        break;
    }

    if (need_convert && *converter == nullptr) {
        return Status::NotSupported(
                strings::Substitute("parquet column reader: not supported convert from parquet `$0` to `$1`",
                                    ::tparquet::to_string(parquet_type), type_to_string(col_type)));
    }

    if (!need_convert) {
        *converter = std::make_unique<ColumnConverter>();
    }
    (*converter)->init_info(need_convert, parquet_type);
    return Status::OK();
}

MutableColumnPtr ColumnConverter::create_src_column() {
    MutableColumnPtr data_column = nullptr;
    switch (parquet_type) {
    case tparquet::Type::type::BOOLEAN:
        data_column = FixedLengthColumn<uint8_t>::create();
        break;
    case tparquet::Type::type::INT32:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT32>::CppType>::create();
        break;
    case tparquet::Type::type::INT64:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT64>::CppType>::create();
        break;
    case tparquet::Type::type::INT96:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::INT96>::CppType>::create();
        break;
    case tparquet::Type::type::FLOAT:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::FLOAT>::CppType>::create();
        break;
    case tparquet::Type::type::DOUBLE:
        data_column = FixedLengthColumn<PhysicalTypeTraits<tparquet::Type::DOUBLE>::CppType>::create();
        break;
    case tparquet::Type::type::BYTE_ARRAY:
    case tparquet::Type::type::FIXED_LEN_BYTE_ARRAY:
        data_column = BinaryColumn::create();
        break;
    }
    return NullableColumn::create(std::move(data_column), NullColumn::create());
}

Status parquet::Int32ToDateConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<DateColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();
    memcpy(dst_null_data.data(), src_null_data.data(), size);
    // DateValue's storage is a single int32_t (_julian), so we can stride through
    // dst_data as raw int32 and add the epoch with SIMD.
    const int32_t* src_ptr = src_data.data();
    int32_t* dst_ptr = reinterpret_cast<int32_t*>(dst_data.data());
    const uint32_t epoch = static_cast<uint32_t>(date::UNIX_EPOCH_JULIAN);
    // We add the epoch to every row, so we meet the data of a NULL row, which is uninitialized, and the days of
    // a corrupt file, which can be any int32. Signed overflow is undefined behavior, and a branch on the NULL
    // flag would stop the vectorization, so we add in unsigned arithmetic, where an overflow wraps. The
    // compiler auto-vectorises the broadcast-add.
    for (size_t i = 0; i < size; i++) {
        dst_ptr[i] = static_cast<int32_t>(static_cast<uint32_t>(src_ptr[i]) + epoch);
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int32ToTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<DoubleColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();

    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            dst_data.data()[i] = src_data.data()[i] / 1000;
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status parquet::Int32ToDateTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int32_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<TimestampColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();
    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            int64_t day = src_data[i];
            TimestampValue ep;
            ep.from_unix_second(day * 24 * 60 * 60, 0);
            dst_data[i].set_timestamp(ep.timestamp());
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int96ToDateTimeConverter::init(const std::string& timezone) {
    if (!TimezoneUtils::find_cctz_time_zone(timezone, _ctz)) {
        return Status::InternalError(strings::Substitute("can not find cctz time zone $0", timezone));
    }
    const auto tp = std::chrono::system_clock::now();
    const cctz::time_zone::absolute_lookup al = _ctz.lookup(tp);
    _offset = al.offset;
    return Status::OK();
}

Status Int96ToDateTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int96_t>>(src_nullable_column->data_column());
    auto* dst_column = down_cast<TimestampColumn*>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();

    auto fill_dst_fn = [&]<bool FAST_TZ>() {
        for (size_t i = 0; i < size; i++) {
            dst_null_data[i] = src_null_data[i];
            if (!src_null_data[i]) {
                Timestamp timestamp =
                        (static_cast<uint64_t>(src_data[i].hi) << TIMESTAMP_BITS) | (src_data[i].lo / 1000);
                int offset = _offset;
                if constexpr (!FAST_TZ) {
                    offset = timestamp::get_timezone_offset_by_timestamp(timestamp, _ctz);
                }
                dst_data[i].set_timestamp(timestamp::add<TimeUnit::SECOND>(timestamp, offset));
            }
        }
    };
    // use fixed offset to adjust to local timezone(potentially could get wrong result but faster)
    // or when it's UTC timezone.
    if (config::parquet_fast_timezone_conversion || _offset == 0) {
        fill_dst_fn.operator()<true>();
    } else {
        fill_dst_fn.operator()<false>();
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int64ToDateTimeConverter::init(const std::string& timezone, const tparquet::SchemaElement& schema_element) {
    DCHECK_EQ(schema_element.type, tparquet::Type::INT64);
    if (schema_element.__isset.logicalType) {
        if (!schema_element.logicalType.__isset.TIMESTAMP) {
            std::stringstream ss;
            schema_element.logicalType.printTo(ss);
            return Status::InternalError(
                    strings::Substitute("expect parquet logical type is TIMESTAMP, actual is $0", ss.str()));
        }

        _is_adjusted_to_utc = schema_element.logicalType.TIMESTAMP.isAdjustedToUTC;

        const auto& time_unit = schema_element.logicalType.TIMESTAMP.unit;

        if (time_unit.__isset.MILLIS) {
            _second_mask = 1000;
            _scale_to_nano_factor = 1000000;
        } else if (time_unit.__isset.MICROS) {
            _second_mask = 1000000;
            _scale_to_nano_factor = 1000;
        } else if (time_unit.__isset.NANOS) {
            _second_mask = 1000000000;
            _scale_to_nano_factor = 1;
        } else {
            std::stringstream ss;
            time_unit.printTo(ss);
            return Status::InternalError(strings::Substitute("unexpected time unit $0", ss.str()));
        }
    } else if (schema_element.__isset.converted_type) {
        _is_adjusted_to_utc = true;

        const auto& converted_type = schema_element.converted_type;
        if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
            _second_mask = 1000;
            _scale_to_nano_factor = 1000000;
        } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
            _second_mask = 1000000;
            _scale_to_nano_factor = 1000;
        } else {
            return Status::InternalError(
                    strings::Substitute("unexpected converted type $0", tparquet::to_string(converted_type)));
        }
    } else {
        return Status::InternalError(strings::Substitute("can not convert parquet type $0 to date time",
                                                         tparquet::to_string(schema_element.type)));
    }

    if (_is_adjusted_to_utc) {
        if (!TimezoneUtils::find_cctz_time_zone(timezone, _ctz)) {
            return Status::InternalError(strings::Substitute("can not find cctz time zone $0", timezone));
        }
        const auto tp = std::chrono::system_clock::now();
        const cctz::time_zone::absolute_lookup al = _ctz.lookup(tp);
        _offset = al.offset;
    }

    return Status::OK();
}

Status Int64ToDateTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<TimestampColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();
    auto fill_dst_fn = [&]<bool UTC_TO_TZ, bool FAST_TZ>() {
        for (size_t i = 0; i < size; i++) {
            dst_null_data[i] = src_null_data[i];
            if (!src_null_data[i]) {
                int64_t seconds = src_data[i] / _second_mask;
                int64_t nanoseconds = (src_data[i] % _second_mask) * _scale_to_nano_factor;
                // Truncating division leaves a negative sub-second remainder for a pre-1970 tick;
                // borrow a whole second so nanoseconds stays in [0, NANOSECS_PER_SEC), matching the
                // floor split the FE boundary computation uses. Without this, of_epoch_second packs
                // a negative microsecond into the timestamp and corrupts the value.
                if (nanoseconds < 0) {
                    seconds -= 1;
                    nanoseconds += NANOSECS_PER_SEC;
                }

                if constexpr (UTC_TO_TZ) {
                    int offset = _offset;
                    if constexpr (!FAST_TZ) {
                        offset = timestamp::get_timezone_offset_by_epoch_seconds(seconds, _ctz);
                    }
                    seconds += offset;
                }

                dst_data[i].set_timestamp(timestamp::of_epoch_second(seconds, nanoseconds));
            }
        }
    };

    if (_is_adjusted_to_utc) {
        // use fixed offset to adjust to local timezone(potentially could get wrong result but faster)
        // or when it's UTC timezone.
        if (config::parquet_fast_timezone_conversion || _offset == 0) {
            fill_dst_fn.operator()<true, true>();
        } else {
            fill_dst_fn.operator()<true, false>();
        }
    } else {
        fill_dst_fn.operator()<false, true>();
    }

    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

Status Int64ToTimeConverter::convert(const Column* src, Column* dst) {
    auto* src_nullable_column = ColumnHelper::as_raw_column<NullableColumn>(src);
    // hive only support null column
    // TODO: support not null
    auto* dst_nullable_column = down_cast<NullableColumn*>(dst);
    dst_nullable_column->resize_uninitialized(src_nullable_column->size());

    auto* src_column = ColumnHelper::as_raw_column<FixedLengthColumn<int64_t>>(src_nullable_column->data_column());
    auto* dst_column = ColumnHelper::as_raw_column<DoubleColumn>(dst_nullable_column->data_column_raw_ptr());

    auto& src_data = src_column->get_data();
    auto& dst_data = dst_column->get_data();
    auto& src_null_data = src_nullable_column->null_column()->get_data();
    auto& dst_null_data = dst_nullable_column->null_column_raw_ptr()->get_data();

    size_t size = dst_null_data.size();

    for (size_t i = 0; i < size; i++) {
        dst_null_data[i] = src_null_data[i];
        if (!src_null_data[i]) {
            dst_data.data()[i] = src_data.data()[i] / 1000000;
        }
    }
    dst_nullable_column->set_has_null(src_nullable_column->has_null());
    return Status::OK();
}

} // namespace starrocks::parquet
