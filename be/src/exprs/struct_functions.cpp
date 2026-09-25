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

#include "exprs/struct_functions.h"

#include <array>
// Boost 1.80 float128 limits conflict with GCC 14; this path uses integers only.
#define BOOST_CSTDFLOAT_NO_LIBQUADMATH_SUPPORT
#include <boost/multiprecision/cpp_int.hpp>

#include "column/column_helper.h"
#include "column/column_viewer.h"
#include "column/struct_column.h"

namespace starrocks {

StatusOr<ColumnPtr> StructFunctions::new_struct(FunctionContext* context, const Columns& columns) {
    MutableColumnPtr res = context->create_column(context->get_return_type(), false);

    StructColumn* st = down_cast<StructColumn*>(res.get());
    size_t fields_size = st->fields_size();
    DCHECK_EQ(fields_size, columns.size());

    for (int i = 0; i < fields_size; i++) {
        auto& column = columns[i];
        auto* field_column = st->field_column_raw_ptr(i);
        if (column->only_null()) {
            field_column->append_nulls(column->size());
        } else if (column->is_constant()) {
            auto* cc = ColumnHelper::get_data_column(column.get());
            field_column->append_value_multiple_times(*cc, 0, column->size());
        } else {
            field_column->append(*column, 0, column->size());
        }
    }

    return res;
}

StatusOr<ColumnPtr> StructFunctions::named_struct(FunctionContext* context, const Columns& columns) {
    Columns cols;
    for (int i = 1; i < columns.size(); i = i + 2) {
        cols.emplace_back(columns[i]);
    }

    return new_struct(context, cols);
}
namespace {
using UInt128 = unsigned __int128;

// Shared immutable powers: no per-row allocation or floating-point conversion.
const std::array<UInt128, 39> kDecimalPowers = [] {
    std::array<UInt128, 39> powers{};
    powers[0] = 1;
    for (size_t i = 1; i < powers.size(); ++i) powers[i] = powers[i - 1] * 10;
    return powers;
}();

StatusOr<int128_t> decode_debezium_decimal(Slice bytes, int32_t source_scale, int precision, int scale) {
    if (bytes.empty()) {
        return Status::InvalidArgument("debezium_decimal: empty value is not a signed integer");
    }
    const auto* data = reinterpret_cast<const uint8_t*>(bytes.data);
    size_t size = bytes.size;
    const bool negative = (data[0] & 0x80) != 0;
    // Accept Java BigInteger sign padding, including redundant sign-extension bytes.
    while (size > 1 && data[0] == (negative ? 0xff : 0) && ((data[1] & 0x80) != 0) == negative) {
        ++data;
        --size;
    }
    const int64_t delta = static_cast<int64_t>(scale) - source_scale;
    const UInt128 limit = kDecimalPowers[precision];
    UInt128 magnitude = 0;
    if (size <= 16) {
        if (size <= 8) {
            uint64_t small = 0;
            for (size_t i = 0; i < size; ++i) {
                small = (small << 8) | (negative ? static_cast<uint8_t>(~data[i]) : data[i]);
            }
            magnitude = static_cast<UInt128>(small) + negative;
        } else {
            for (size_t i = 0; i < size; ++i) {
                magnitude = (magnitude << 8) | (negative ? static_cast<uint8_t>(~data[i]) : data[i]);
            }
            magnitude += negative;
        }
        if (magnitude == 0) return int128_t{0};
        if (delta > 0) {
            if (delta >= precision || magnitude >= kDecimalPowers[precision - delta]) {
                return Status::InvalidArgument("debezium_decimal: value exceeds target precision");
            }
            magnitude *= kDecimalPowers[delta];
        } else if (delta < 0) {
            if (-delta > 38 || magnitude % kDecimalPowers[-delta] != 0) {
                return Status::InvalidArgument("debezium_decimal: target scale would lose fractional digits");
            }
            magnitude /= kDecimalPowers[-delta];
        }
    } else {
        // Unconstrained source NUMERIC can exceed 128 bits but still fit after exact rescaling.
        // Keep arbitrary precision off the ordinary (<=16 byte) path.
        boost::multiprecision::cpp_int wide = 0;
        for (size_t i = 0; i < size; ++i) {
            wide <<= 8;
            wide += negative ? static_cast<uint8_t>(~data[i]) : data[i];
        }
        wide += negative;
        if (delta >= 0) {
            // A non-sign-padded >16-byte value already exceeds any DECIMAL(38,s).
            return Status::InvalidArgument("debezium_decimal: value exceeds target precision");
        }
        int64_t remaining = -delta;
        while (remaining > 0) {
            int step = static_cast<int>(std::min<int64_t>(remaining, 18));
            uint64_t divisor = static_cast<uint64_t>(kDecimalPowers[step]);
            if (wide % divisor != 0) {
                return Status::InvalidArgument("debezium_decimal: target scale would lose fractional digits");
            }
            wide /= divisor;
            remaining -= step;
        }
        if (wide >= boost::multiprecision::cpp_int(limit)) {
            return Status::InvalidArgument("debezium_decimal: value exceeds target precision");
        }
        magnitude = wide.convert_to<UInt128>();
    }
    if (magnitude >= limit) {
        return Status::InvalidArgument("debezium_decimal: value exceeds target precision");
    }
    const auto result = static_cast<int128_t>(magnitude);
    return negative ? -result : result;
}
} // namespace

StatusOr<ColumnPtr> StructFunctions::debezium_decimal(FunctionContext* context, const Columns& columns) {
    const auto& type = context->get_return_type();
    const auto& input = columns[0];
    if (input->only_null()) return ColumnHelper::create_const_null_column(input->size());
    const auto* structure = down_cast<const StructColumn*>(ColumnHelper::get_data_column(input.get()));
    ASSIGN_OR_RETURN(const auto& scales, structure->field_column("scale"));
    ASSIGN_OR_RETURN(const auto& values, structure->field_column("value"));
    ColumnViewer<TYPE_INT> scale_view(scales);
    ColumnViewer<TYPE_VARBINARY> value_view(values);
    const bool constant = input->is_constant();
    const size_t count = constant && input->size() != 0 ? 1 : input->size();
    auto data_column = Decimal128Column::create(type.precision, type.scale);
    auto null_column = NullColumn::create();
    auto& data = data_column->get_data();
    auto& nulls = null_column->get_data();
    data.resize(count);
    nulls.resize(count, 0);
    const bool input_has_null = input->has_null();
    bool has_null = false;
    for (size_t row = 0; row < count; ++row) {
        if ((input_has_null && input->is_null(row)) || scale_view.is_null(row) || value_view.is_null(row)) {
            nulls[row] = 1;
            has_null = true;
        } else {
            ASSIGN_OR_RETURN(data[row], decode_debezium_decimal(value_view.value(row), scale_view.value(row),
                                                                type.precision, type.scale));
        }
    }
    if (constant) {
        if (has_null) return ColumnHelper::create_const_null_column(input->size());
        return ConstColumn::create(std::move(data_column), input->size());
    }
    if (has_null) return NullableColumn::create(std::move(data_column), std::move(null_column));
    return data_column;
}

} // namespace starrocks

#include "gen_cpp/opcode/StructFunctions.inc"
