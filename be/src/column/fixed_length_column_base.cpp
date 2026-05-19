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

#include "column/fixed_length_column_base.h"

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "base/hash/hash_util.hpp"
#include "base/simd/gather.h"
#include "base/simd/simd_utils.h"
#include "base/types/decimal12.h"
#include "base/types/int128.h"
#include "base/types/int256.h"
#include "column/column_filter_range.h"
#include "column/column_sorter_comparator.h"
#include "column/mysql_row_buffer.h"
#include "column/raw_data_visitor.h"
#include "column/runtime_type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/config_local_io_fwd.h"
#include "gutil/casts.h"
#include "gutil/strings/fastmem.h"
#include "gutil/strings/substitute.h"
#include "types/value_generator.h"

namespace starrocks {

template <typename T>
StatusOr<MutableColumnPtr> FixedLengthColumnBase<T>::upgrade_if_overflow() {
    RETURN_IF_ERROR(capacity_limit_reached());
    return nullptr;
}

template <typename T>
void FixedLengthColumnBase<T>::append(const Column& src, size_t offset, size_t count) {
    DCHECK(this != &src);

    auto& datas = get_data();
    const size_t orig_size = datas.size();
    raw::stl_vector_resize_uninitialized(&datas, orig_size + count);

    RawDataVisitor rv;
    CHECK(src.accept(&rv).ok());
    const T* src_data = reinterpret_cast<const T*>(rv.result());
    strings::memcpy_inlined(datas.data() + orig_size, src_data + offset, count * sizeof(T));
}

template <typename T>
void FixedLengthColumnBase<T>::append_selective(const Column& src, const uint32_t* indexes, uint32_t from,
                                                uint32_t size) {
    DCHECK(this != &src);

    indexes += from;
    auto& datas = get_data();
    const size_t orig_size = datas.size();
    raw::stl_vector_resize_uninitialized(&datas, orig_size + size);
    auto* dest_data = datas.data() + orig_size;

    RawDataVisitor rv;
    CHECK(src.accept(&rv).ok());
    const T* src_data = reinterpret_cast<const T*>(rv.result());
    SIMDGather::gather(dest_data, src_data, indexes, size);
}

template <typename T>
void FixedLengthColumnBase<T>::append_value_multiple_times(const Column& src, uint32_t index, uint32_t size) {
    DCHECK(this != &src);

    auto& datas = get_data();

    size_t orig_size = datas.size();
    datas.resize(orig_size + size);

    const auto& src_col = down_cast<const FixedLengthColumnBase<T>&>(src);
    const auto src_datas = src_col.immutable_data();
    const T* src_data = src_datas.data();

    for (size_t i = 0; i < size; ++i) {
        datas[orig_size + i] = src_data[index];
    }
}

template <typename T>
size_t FixedLengthColumnBase<T>::append_numbers(const ContainerResource& res) {
    bool could_apply_opt = config::enable_zero_copy_from_page_cache && res.owned() && res.is_aligned<T>();
    if (could_apply_opt && empty() && _resource.empty()) {
        DCHECK(res.length() % sizeof(ValueType) == 0);
        _resource.acquire(res);
        _resource.set_data(res.data());
        _resource.set_length(res.length() / sizeof(ValueType));
        return _resource.length();
    } else {
        return append_numbers(res.data(), res.length());
    }
}

template <typename T>
void FixedLengthColumnBase<T>::append_default() {
    auto& datas = get_data();
    datas.emplace_back(DefaultValueGenerator<ValueType>::next_value());
}

template <typename T>
void FixedLengthColumnBase<T>::append_default(size_t count) {
    auto& datas = get_data();
    datas.resize(datas.size() + count, DefaultValueGenerator<ValueType>::next_value());
}

// SIMD-optimized replicate: uses AVX2 broadcast+store for filling repeated values
template <typename T>
StatusOr<MutableColumnPtr> FixedLengthColumnBase<T>::replicate(const Buffer<uint32_t>& offsets) {
    auto dest = this->clone_empty();
    auto& dest_data = down_cast<FixedLengthColumnBase<T>&>(*dest);
    auto& dest_datas = dest_data.get_data();

    const auto datas = this->immutable_data();
    dest_datas.resize(offsets.back());
    size_t orig_size = offsets.size() - 1; // this->size() may be large than offsets->size() -1

    T* dest_ptr = dest_datas.data();
    for (size_t i = 0; i < orig_size; ++i) {
        size_t fill_count = offsets[i + 1] - offsets[i];
        SIMDUtils::simd_fill(dest_ptr + offsets[i], datas[i], fill_count);
    }
    return dest;
}

template <typename T>
void FixedLengthColumnBase<T>::fill_default(const Filter& filter) {
    auto& datas = get_data();
    T val = DefaultValueGenerator<T>::next_value();
    const size_t size = filter.size();
    const uint8_t* f = filter.data();
    T* data = datas.data();

    // On AVX-512 builds the compiler auto-vectorises the scalar fallback to
    // 16-lane AVX-512 blend, which beats the hand-written 8-lane AVX2 blend.
    // Only opt into the hand-written AVX2 path on AVX2-only builds.
#if defined(__AVX2__) && !defined(__AVX512F__)
    if constexpr (sizeof(T) == 4) {
        int32_t val_bits;
        memcpy(&val_bits, &val, sizeof(val_bits));
        const __m256i val_vec = _mm256_set1_epi32(val_bits);
        const __m256i zero = _mm256_setzero_si256();
        size_t i = 0;
        for (; i + 8 <= size; i += 8) {
            // Load 8 filter bytes, expand to 32-bit mask
            __m256i flt = _mm256_cvtepu8_epi32(_mm_loadl_epi64(reinterpret_cast<const __m128i*>(f + i)));
            __m256i mask = _mm256_cmpgt_epi32(flt, zero);
            __m256i cur = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i), _mm256_blendv_epi8(cur, val_vec, mask));
        }
        for (; i < size; i++) {
            if (f[i] == 1) data[i] = val;
        }
    } else if constexpr (sizeof(T) == 8) {
        int64_t val_bits;
        memcpy(&val_bits, &val, sizeof(val_bits));
        const __m256i val_vec = _mm256_set1_epi64x(val_bits);
        const __m256i zero = _mm256_setzero_si256();
        size_t i = 0;
        for (; i + 4 <= size; i += 4) {
            // AVX2: manually expand 4 bytes to 4 int64
            uint32_t f4;
            memcpy(&f4, f + i, sizeof(f4));
            __m256i flt = _mm256_set_epi64x((f4 >> 24) & 0xFF, (f4 >> 16) & 0xFF, (f4 >> 8) & 0xFF, f4 & 0xFF);
            __m256i mask = _mm256_cmpgt_epi64(flt, zero);
            __m256i cur = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
            _mm256_storeu_si256(reinterpret_cast<__m256i*>(data + i), _mm256_blendv_epi8(cur, val_vec, mask));
        }
        for (; i < size; i++) {
            if (f[i] == 1) data[i] = val;
        }
    } else {
        for (size_t i = 0; i < size; i++) {
            if (f[i] == 1) data[i] = val;
        }
    }
#elif defined(__ARM_NEON) && defined(__aarch64__)
    if constexpr (sizeof(T) == 4) {
        uint32_t val_bits;
        memcpy(&val_bits, &val, sizeof(val_bits));
        const uint32x4_t val_vec = vdupq_n_u32(val_bits);
        size_t i = 0;
        for (; i + 4 <= size; i += 4) {
            // Load exactly 4 filter bytes, expand to 32-bit
            uint32_t f4;
            memcpy(&f4, f + i, sizeof(f4));
            uint32x4_t flt32 = {f4 & 0xFF, (f4 >> 8) & 0xFF, (f4 >> 16) & 0xFF, (f4 >> 24) & 0xFF};
            uint32x4_t mask = vcgtq_u32(flt32, vdupq_n_u32(0));
            uint32x4_t cur = vld1q_u32(reinterpret_cast<const uint32_t*>(data + i));
            vst1q_u32(reinterpret_cast<uint32_t*>(data + i), vbslq_u32(mask, val_vec, cur));
        }
        for (; i < size; i++) {
            if (f[i] == 1) data[i] = val;
        }
    } else if constexpr (sizeof(T) == 8) {
        uint64_t val_bits;
        memcpy(&val_bits, &val, sizeof(val_bits));
        const uint64x2_t val_vec = vdupq_n_u64(val_bits);
        size_t i = 0;
        for (; i + 2 <= size; i += 2) {
            uint16_t f01;
            memcpy(&f01, f + i, sizeof(f01));
            uint64x2_t flt64 = {static_cast<uint64_t>(f01 & 0xFF), static_cast<uint64_t>((f01 >> 8) & 0xFF)};
            uint64x2_t mask = vcgtq_u64(flt64, vdupq_n_u64(0));
            uint64x2_t cur = vld1q_u64(reinterpret_cast<const uint64_t*>(data + i));
            vst1q_u64(reinterpret_cast<uint64_t*>(data + i), vbslq_u64(mask, val_vec, cur));
        }
        for (; i < size; i++) {
            if (f[i] == 1) data[i] = val;
        }
    } else {
        for (size_t i = 0; i < size; i++) {
            if (f[i] == 1) data[i] = val;
        }
    }
#else
    for (size_t i = 0; i < size; i++) {
        if (f[i] == 1) data[i] = val;
    }
#endif
}

template <typename T>
Status FixedLengthColumnBase<T>::fill_range(const std::vector<T>& ids, const Filter& filter) {
    auto& datas = get_data();

    DCHECK_EQ(filter.size(), datas.size());
    size_t j = 0;
    for (size_t i = 0; i < datas.size(); ++i) {
        if (filter[i] == 1) {
            datas[i] = ids[j];
            ++j;
        }
    }
    DCHECK_EQ(j, ids.size());

    return Status::OK();
}

template <typename T>
void FixedLengthColumnBase<T>::update_rows(const Column& src, const uint32_t* indexes) {
    auto& datas = get_data();

    const auto& src_col = down_cast<const FixedLengthColumnBase<T>&>(src);
    const auto src_datas = src_col.immutable_data();
    const T* src_data = src_datas.data();

    size_t replace_num = src.size();
    for (uint32_t i = 0; i < replace_num; ++i) {
        DCHECK_LT(indexes[i], _data.size());
        datas[indexes[i]] = src_data[i];
    }
}

template <typename T>
size_t FixedLengthColumnBase<T>::filter_range(const Filter& filter, size_t from, size_t to) {
    // TODO: FIXME
    const auto src = immutable_data();
    raw::stl_vector_resize_uninitialized(&_data, src.size());
    auto size = column_filter_range::filter_range<T>(filter, _data.data(), src.data(), from, to);
    _data.resize(size);
    _resource.reset();
    return size;
}

template <typename T>
int FixedLengthColumnBase<T>::compare_at(size_t left, size_t right, const Column& rhs, int nan_direction_hint) const {
    const auto lhs_datas = this->immutable_data();
    const auto rhs_datas = down_cast<const FixedLengthColumnBase<T>&>(rhs).immutable_data();
    DCHECK_LT(left, lhs_datas.size());
    DCHECK_LT(right, rhs_datas.size());
    T x = lhs_datas[left];
    T y = rhs_datas[right];
    return SorterComparator<T>::compare(x, y);
}

template <typename T>
uint32_t FixedLengthColumnBase<T>::serialize(size_t idx, uint8_t* pos) const {
    const auto datas = this->immutable_data();
    memcpy(pos, &datas[idx], sizeof(T));
    return sizeof(T);
}

template <typename T>
uint32_t FixedLengthColumnBase<T>::serialize_default(uint8_t* pos) const {
    ValueType value{};
    memcpy(pos, &value, sizeof(T));
    return sizeof(T);
}

template <typename T>
void FixedLengthColumnBase<T>::serialize_batch(uint8_t* __restrict__ dst, Buffer<uint32_t>& slice_sizes,
                                               size_t chunk_size, uint32_t max_one_row_size) const {
    uint32_t* sizes = slice_sizes.data();
    const T* __restrict__ src = this->immutable_data().data();

    for (size_t i = 0; i < chunk_size; ++i) {
        memcpy(dst + i * max_one_row_size + sizes[i], src + i, sizeof(T));
    }

    for (size_t i = 0; i < chunk_size; i++) {
        sizes[i] += sizeof(T);
    }
}

template <typename T>
void FixedLengthColumnBase<T>::serialize_batch_with_null_masks(uint8_t* __restrict__ dst, Buffer<uint32_t>& slice_sizes,
                                                               size_t chunk_size, uint32_t max_one_row_size,
                                                               const uint8_t* null_masks, bool has_null) const {
    uint32_t* sizes = slice_sizes.data();
    const T* __restrict__ src = this->immutable_data().data();

    if (!has_null) {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], &has_null, sizeof(bool));
            memcpy(dst + i * max_one_row_size + sizes[i] + sizeof(bool), src + i, sizeof(T));
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            sizes[i] += sizeof(bool) + sizeof(T);
        }
    } else {
        for (size_t i = 0; i < chunk_size; ++i) {
            memcpy(dst + i * max_one_row_size + sizes[i], null_masks + i, sizeof(bool));
            if (!null_masks[i]) {
                memcpy(dst + i * max_one_row_size + sizes[i] + sizeof(bool), src + i, sizeof(T));
            }
        }

        for (size_t i = 0; i < chunk_size; ++i) {
            sizes[i] += static_cast<uint32_t>(sizeof(bool) + (1 - null_masks[i]) * sizeof(T));
        }
    }
}

template <typename T>
size_t FixedLengthColumnBase<T>::serialize_batch_at_interval(uint8_t* dst, size_t byte_offset, size_t byte_interval,
                                                             uint32_t max_row_size, size_t start, size_t count) const {
    const size_t value_size = sizeof(T);
    DCHECK_EQ(max_row_size, value_size);
    const auto key_data = this->immutable_data();
    uint8_t* buf = dst + byte_offset;
    for (size_t i = start; i < start + count; ++i) {
        strings::memcpy_inlined(buf, &key_data[i], value_size);
        buf = buf + byte_interval;
    }
    return value_size;
}

template <typename T>
const uint8_t* FixedLengthColumnBase<T>::deserialize_and_append(const uint8_t* pos) {
    T value{};
    memcpy(&value, pos, sizeof(T));
    this->get_data().emplace_back(value);
    return pos + sizeof(T);
}

template <typename T>
void FixedLengthColumnBase<T>::deserialize_and_append_batch(Buffer<Slice>& srcs, size_t chunk_size) {
    auto& datas = this->get_data();
    raw::make_room(&datas, chunk_size);
    for (size_t i = 0; i < chunk_size; ++i) {
        memcpy(&datas[i], srcs[i].data, sizeof(T));
        srcs[i].data = srcs[i].data + sizeof(T);
    }
}

template <typename T>
int64_t FixedLengthColumnBase<T>::xor_checksum(uint32_t from, uint32_t to) const {
    const auto datas = this->immutable_data();

    int64_t xor_checksum = 0;
    if constexpr (IsDate<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= datas[i].to_date_literal();
        }
    } else if constexpr (IsTimestamp<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= datas[i].to_timestamp_literal();
        }
    } else if constexpr (IsDecimal<T>) {
        for (size_t i = from; i < to; ++i) {
            xor_checksum ^= datas[i].int_value();
            xor_checksum ^= datas[i].frac_value();
        }
    } else if constexpr (is_signed_integer<T>) {
        const T* src = reinterpret_cast<const T*>(datas.data());
        for (size_t i = from; i < to; ++i) {
            if constexpr (std::is_same_v<T, int128_t>) {
                xor_checksum ^= static_cast<int64_t>(src[i] >> 64);
                xor_checksum ^= static_cast<int64_t>(src[i] & ULLONG_MAX);
            } else if constexpr (std::is_same_v<T, int256_t>) {
                xor_checksum ^= static_cast<int64_t>(src[i].high >> 64);
                xor_checksum ^= static_cast<int64_t>(src[i].high & ULLONG_MAX);
                xor_checksum ^= static_cast<int64_t>(src[i].low >> 64);
                xor_checksum ^= static_cast<int64_t>(src[i].low & ULLONG_MAX);
            } else {
                xor_checksum ^= src[i];
            }
        }
    }

    return xor_checksum;
}

template <typename T>
void FixedLengthColumnBase<T>::put_mysql_row_buffer(MysqlRowBuffer* buf, size_t idx, bool is_binary_protocol) const {
    const auto datas = this->immutable_data();
    if constexpr (IsDecimal<T>) {
        buf->push_decimal(datas[idx].to_string());
    } else if constexpr (IsDate<T>) {
        buf->push_date(datas[idx], is_binary_protocol);
    } else if constexpr (IsTimestamp<T>) {
        buf->push_timestamp(datas[idx], is_binary_protocol);
    } else if constexpr (std::is_arithmetic_v<T>) {
        buf->push_number(datas[idx], is_binary_protocol);
    } else {
        std::string s = datas[idx].to_string();
        buf->push_string(s.data(), s.size());
    }
}

template <typename T>
void FixedLengthColumnBase<T>::remove_first_n_values(size_t count) {
    // TODO: avoid memcpy here
    auto& datas = this->get_data();
    size_t remain_size = datas.size() - count;
    memmove(_data.data(), _data.data() + count, remain_size * sizeof(T));
    _data.resize(remain_size);
}

template <typename T>
std::string FixedLengthColumnBase<T>::debug_item(size_t idx) const {
    const auto datas = this->immutable_data();
    std::stringstream ss;
    if constexpr (sizeof(T) == 1) {
        // for bool, int8_t
        ss << (int)datas[idx];
    } else {
        ss << datas[idx];
    }
    return ss.str();
}

template <>
std::string FixedLengthColumnBase<int128_t>::debug_item(size_t idx) const {
    const auto datas = this->immutable_data();
    std::stringstream ss;
    starrocks::operator<<(ss, datas[idx]);
    return ss.str();
}

template <typename T>
std::string FixedLengthColumnBase<T>::debug_string() const {
    std::stringstream ss;
    ss << "[";
    size_t size = this->size();
    for (size_t i = 0; i + 1 < size; ++i) {
        ss << debug_item(i) << ", ";
    }
    if (size > 0) {
        ss << debug_item(size - 1);
    }
    ss << "]";
    return ss.str();
}

template <typename T>
Status FixedLengthColumnBase<T>::capacity_limit_reached() const {
    if (_data.size() > Column::MAX_CAPACITY_LIMIT) {
        return Status::CapacityLimitExceed(strings::Substitute("row count of fixed length column exceend the limit: $0",
                                                               std::to_string(Column::MAX_CAPACITY_LIMIT)));
    }
    return Status::OK();
}

template <typename T>
std::string FixedLengthColumnBase<T>::get_name() const {
    if constexpr (IsDecimal<T>) {
        return "decimal";
    } else if constexpr (IsDate<T>) {
        return "date";
    } else if constexpr (IsTimestamp<T>) {
        return "timestamp";
    } else if constexpr (IsInt128<T>) {
        return "int128";
    } else if constexpr (IsInt256<T>) {
        return "int256";
    } else if constexpr (std::is_floating_point_v<T>) {
        return "float-" + std::to_string(sizeof(T));
    } else {
        return "integral-" + std::to_string(sizeof(T));
    }
}

template class FixedLengthColumnBase<uint8_t>;
template class FixedLengthColumnBase<uint16_t>;
template class FixedLengthColumnBase<uint32_t>;
template class FixedLengthColumnBase<uint64_t>;

template class FixedLengthColumnBase<int8_t>;
template class FixedLengthColumnBase<int16_t>;
template class FixedLengthColumnBase<int32_t>;
template class FixedLengthColumnBase<int64_t>;
template class FixedLengthColumnBase<int96_t>;
template class FixedLengthColumnBase<int128_t>;
template class FixedLengthColumnBase<int256_t>;

template class FixedLengthColumnBase<float>;
template class FixedLengthColumnBase<double>;

template class FixedLengthColumnBase<uint24_t>;
template class FixedLengthColumnBase<decimal12_t>;

template class FixedLengthColumnBase<DateValue>;
template class FixedLengthColumnBase<DecimalV2Value>;
template class FixedLengthColumnBase<TimestampValue>;

} // namespace starrocks
