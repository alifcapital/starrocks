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

#include <utility>

#ifdef __AVX2__
#include <immintrin.h>
#elif defined(__ARM_NEON) && defined(__aarch64__)
#include <arm_neon.h>
#endif

#include "column/column.h"
#include "column/nullable_column.h"
#include "gutil/casts.h"
#include "olap_type_infra.h"
#include "storage/column_predicate.h"
#include "storage/in_predicate_utils.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "util/string_parser.hpp"

namespace starrocks {

template <LogicalType field_type>
class ColumnNotInPredicate final : public ColumnPredicate {
    using ValueType = typename CppTypeTraits<field_type>::CppType;

public:
    ColumnNotInPredicate(const TypeInfoPtr& type_info, ColumnId id, const std::vector<std::string>& strs)
            : ColumnPredicate(type_info, id), _values(predicate_internal::strings_to_hashset<field_type>(strs)) {}

    ColumnNotInPredicate(const TypeInfoPtr& type_info, ColumnId id, ItemHashSet<ValueType>&& values)
            : ColumnPredicate(type_info, id), _values(std::move(values)) {}

    ~ColumnNotInPredicate() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());
        if (!column->has_null()) {
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(!_values.contains(v[i])));
            }
        } else {
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (size_t i = from; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(!null_data[i] && !_values.contains(v[i])));
            }
        }
    }

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAssignOp>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAndOp>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateOrOp>(column, selection, from, to);
        return Status::OK();
    }

    StatusOr<uint16_t> evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
        auto* v = reinterpret_cast<const ValueType*>(column->raw_data());

        uint16_t new_size = 0;
        if (!column->has_null()) {
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !(_values.contains(v[data_idx]));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !null_data[data_idx] && !(_values.contains(v[data_idx]));
            }
        }
        return new_size;
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override {
        if (detail.min_or_null_value() == detail.max_value()) {
            const auto type_info = this->type_info();
            for (const ValueType& v : _values) {
                if (type_info->cmp(Datum(v), detail.max_value()) == 0) {
                    return false;
                }
            }
        }
        return true;
    }

    bool support_bitmap_filter() const override { return false; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        return Status::Cancelled("not-equal predicate not support bitmap index");
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
        roaring::Roaring indices;
        for (auto value : _values) {
            roaring::Roaring index;
            RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &value, query_type, &index));
            indices |= index;
        }
        *row_bitmap -= indices;
        return Status::OK();
    }

    PredicateType type() const override { return PredicateType::kNotInList; }

    bool can_vectorized() const override { return false; }

    std::vector<Datum> values() const override {
        std::vector<Datum> ret;
        ret.reserve(_values.size());
        for (const ValueType& value : _values) {
            ret.emplace_back(value);
        }
        return ret;
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        const auto to_type = target_type_info->type();
        if (to_type == field_type) {
            *output = this;
            return Status::OK();
        }

        if (to_type == TYPE_DECIMAL128) {
            std::vector<std::string> strs;
            const auto type_info = this->type_info();
            for (ValueType value : _values) {
                strs.emplace_back(type_info->to_string(&value));
            }
            *output = obj_pool->add(new_column_not_in_predicate(target_type_info, _column_id, strs));
            return Status::OK();
        }
        if constexpr (field_type == TYPE_DECIMAL128) {
            std::vector<std::string> strs;
            for (ValueType value : _values) {
                strs.emplace_back(DecimalV3Cast::to_string<ValueType>(value, 27, 9));
            }
            *output = obj_pool->add(new_column_not_in_predicate(target_type_info, _column_id, strs));
            return Status::OK();
        }
        const auto type_info = this->type_info();
        std::vector<std::string> strs;
        for (ValueType value : _values) {
            strs.emplace_back(type_info->to_string(&value));
        }
        *output = obj_pool->add(new_column_not_in_predicate(target_type_info, _column_id, strs));
        return Status::OK();
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "((columnId=" << _column_id << ")NOT IN(";
        int i = 0;
        for (auto& item : _values) {
            if (i++ != 0) {
                ss << ",";
            }
            ss << this->type_info()->to_string(&item);
        }
        ss << "))";
        return ss.str();
    }

private:
    ItemHashSet<ValueType> _values;
};

// Template specialization for binary column
template <LogicalType field_type>
class BinaryColumnNotInPredicate final : public ColumnPredicate {
public:
    BinaryColumnNotInPredicate(const TypeInfoPtr& type_info, ColumnId id, std::vector<std::string> strings)
            : ColumnPredicate(type_info, id), _zero_padded_strs(std::move(strings)) {
        _min_len = UINT32_MAX;
        _max_len = 0;
        for (const std::string& s : _zero_padded_strs) {
            _slices.emplace(Slice(s));
            uint32_t len = static_cast<uint32_t>(s.size());
            _min_len = std::min(_min_len, len);
            _max_len = std::max(_max_len, len);
        }
        if (_min_len == UINT32_MAX) _min_len = 0;
    }

    ~BinaryColumnNotInPredicate() override = default;

    template <typename Op>
    inline void t_evaluate(const Column* column, uint8_t* sel, uint16_t from, uint16_t to) const {
        // Get BinaryColumn
        const BinaryColumn* binary_column;
        if (column->is_nullable()) {
            // This is NullableColumn, get its data_column
            binary_column =
                    down_cast<const BinaryColumn*>(down_cast<const NullableColumn*>(column)->data_column().get());
        } else {
            binary_column = down_cast<const BinaryColumn*>(column);
        }

        const auto& offsets = binary_column->get_offset();

        if (!column->has_null()) {
            size_t i = from;
#ifdef __AVX2__
            // SIMD batch length filtering: process 8 strings at a time
            // For NOT IN: if length is outside [min_len, max_len], result is 1 (not in set)
            const __m256i min_len_vec = _mm256_set1_epi32(_min_len);
            const __m256i max_len_vec = _mm256_set1_epi32(_max_len);

            for (; i + 8 <= to; i += 8) {
                // Load 9 offsets to compute 8 lengths
                __m256i off_curr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&offsets[i]));
                __m256i off_next = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&offsets[i + 1]));
                __m256i lengths = _mm256_sub_epi32(off_next, off_curr);

                // Check length in range [min_len, max_len]
                __m256i ge_min = _mm256_cmpgt_epi32(lengths, _mm256_sub_epi32(min_len_vec, _mm256_set1_epi32(1)));
                __m256i le_max = _mm256_cmpgt_epi32(_mm256_add_epi32(max_len_vec, _mm256_set1_epi32(1)), lengths);
                __m256i in_range = _mm256_and_si256(ge_min, le_max);
                int mask = _mm256_movemask_ps(_mm256_castsi256_ps(in_range));

                if (mask == 0) {
                    // All 8 lengths outside range - all are NOT IN set (result = 1)
                    for (size_t j = i; j < i + 8; j++) {
                        sel[j] = Op::apply(sel[j], 1);
                    }
                    continue;
                }

                // Compute masks before processing
                int in_range_mask = mask;
                int out_of_range_mask = (~mask) & 0xFF;

                // Strings with lengths in range need hash lookup
                while (in_range_mask) {
                    int bit = __builtin_ctz(in_range_mask);
                    size_t idx = i + bit;
                    sel[idx] = Op::apply(sel[idx], (uint8_t)(!(_slices.contains(binary_column->get_slice(idx)))));
                    in_range_mask &= (in_range_mask - 1);
                }
                // Strings with lengths outside range are NOT IN (result = 1)
                while (out_of_range_mask) {
                    int bit = __builtin_ctz(out_of_range_mask);
                    size_t idx = i + bit;
                    sel[idx] = Op::apply(sel[idx], 1);
                    out_of_range_mask &= (out_of_range_mask - 1);
                }
            }
#elif defined(__ARM_NEON) && defined(__aarch64__)
            // NEON batch length filtering: process 4 strings at a time
            const uint32x4_t min_len_vec = vdupq_n_u32(_min_len);
            const uint32x4_t max_len_vec = vdupq_n_u32(_max_len);

            for (; i + 4 <= to; i += 4) {
                uint32x4_t off_curr = vld1q_u32(&offsets[i]);
                uint32x4_t off_next = vld1q_u32(&offsets[i + 1]);
                uint32x4_t lengths = vsubq_u32(off_next, off_curr);

                // Check length in range
                uint32x4_t ge_min = vcgeq_u32(lengths, min_len_vec);
                uint32x4_t le_max = vcleq_u32(lengths, max_len_vec);
                uint32x4_t in_range = vandq_u32(ge_min, le_max);

                uint32_t mask_arr[4];
                vst1q_u32(mask_arr, in_range);

                bool all_out_of_range = (mask_arr[0] == 0 && mask_arr[1] == 0 && mask_arr[2] == 0 && mask_arr[3] == 0);
                if (all_out_of_range) {
                    for (size_t j = i; j < i + 4; j++) {
                        sel[j] = Op::apply(sel[j], 1);
                    }
                    continue;
                }

                for (int j = 0; j < 4; j++) {
                    if (mask_arr[j]) {
                        sel[i + j] = Op::apply(sel[i + j], (uint8_t)(!(_slices.contains(binary_column->get_slice(i + j)))));
                    } else {
                        sel[i + j] = Op::apply(sel[i + j], 1);
                    }
                }
            }
#endif
            // Scalar tail
            for (; i < to; i++) {
                sel[i] = Op::apply(sel[i], (uint8_t)(!(_slices.contains(binary_column->get_slice(i)))));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            size_t i = from;

#ifdef __AVX2__
            // Combined null check + length filtering
            const __m256i min_len_vec = _mm256_set1_epi32(_min_len);
            const __m256i max_len_vec = _mm256_set1_epi32(_max_len);

            for (; i + 8 <= to; i += 8) {
                // Build non-null mask from 8 null flag bytes
                int non_null_mask = 0;
                for (int j = 0; j < 8; j++) {
                    if (null_data[i + j] == 0) {
                        non_null_mask |= (1 << j);
                    }
                }

                if (non_null_mask == 0) {
                    // All 8 are NULL - result is 0 for NOT IN
                    for (size_t j = i; j < i + 8; j++) {
                        sel[j] = Op::apply(sel[j], 0);
                    }
                    continue;
                }

                // Compute lengths
                __m256i off_curr = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&offsets[i]));
                __m256i off_next = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&offsets[i + 1]));
                __m256i lengths = _mm256_sub_epi32(off_next, off_curr);

                // Check length in range
                __m256i ge_min = _mm256_cmpgt_epi32(lengths, _mm256_sub_epi32(min_len_vec, _mm256_set1_epi32(1)));
                __m256i le_max = _mm256_cmpgt_epi32(_mm256_add_epi32(max_len_vec, _mm256_set1_epi32(1)), lengths);
                __m256i in_range = _mm256_and_si256(ge_min, le_max);
                int len_mask = _mm256_movemask_ps(_mm256_castsi256_ps(in_range));

                // Non-null AND length in range -> need hash lookup
                int need_lookup_mask = len_mask & non_null_mask;
                // Non-null AND length out of range -> result is 1
                int out_of_range_mask = (~len_mask) & non_null_mask & 0xFF;
                // NULL -> result is 0
                int null_mask = (~non_null_mask) & 0xFF;

                // Process elements needing hash lookup
                while (need_lookup_mask) {
                    int bit = __builtin_ctz(need_lookup_mask);
                    size_t idx = i + bit;
                    sel[idx] = Op::apply(sel[idx], (uint8_t)(!(_slices.contains(binary_column->get_slice(idx)))));
                    need_lookup_mask &= (need_lookup_mask - 1);
                }
                // Elements with length out of range (and non-null) are NOT IN
                while (out_of_range_mask) {
                    int bit = __builtin_ctz(out_of_range_mask);
                    size_t idx = i + bit;
                    sel[idx] = Op::apply(sel[idx], 1);
                    out_of_range_mask &= (out_of_range_mask - 1);
                }
                // NULL elements get 0
                while (null_mask) {
                    int bit = __builtin_ctz(null_mask);
                    size_t idx = i + bit;
                    sel[idx] = Op::apply(sel[idx], 0);
                    null_mask &= (null_mask - 1);
                }
            }
#elif defined(__ARM_NEON) && defined(__aarch64__)
            const uint32x4_t min_len_vec = vdupq_n_u32(_min_len);
            const uint32x4_t max_len_vec = vdupq_n_u32(_max_len);

            for (; i + 4 <= to; i += 4) {
                // Check for all nulls
                bool all_null = true;
                for (int j = 0; j < 4; j++) {
                    if (null_data[i + j] == 0) all_null = false;
                }
                if (all_null) {
                    for (size_t j = i; j < i + 4; j++) {
                        sel[j] = Op::apply(sel[j], 0);
                    }
                    continue;
                }

                uint32x4_t off_curr = vld1q_u32(&offsets[i]);
                uint32x4_t off_next = vld1q_u32(&offsets[i + 1]);
                uint32x4_t lengths = vsubq_u32(off_next, off_curr);

                uint32x4_t ge_min = vcgeq_u32(lengths, min_len_vec);
                uint32x4_t le_max = vcleq_u32(lengths, max_len_vec);
                uint32x4_t in_range = vandq_u32(ge_min, le_max);

                uint32_t range_arr[4];
                vst1q_u32(range_arr, in_range);

                for (int j = 0; j < 4; j++) {
                    if (null_data[i + j]) {
                        sel[i + j] = Op::apply(sel[i + j], 0);
                    } else if (range_arr[j]) {
                        sel[i + j] = Op::apply(sel[i + j], (uint8_t)(!(_slices.contains(binary_column->get_slice(i + j)))));
                    } else {
                        sel[i + j] = Op::apply(sel[i + j], 1);
                    }
                }
            }
#endif
            // Scalar tail
            for (; i < to; i++) {
                sel[i] =
                        Op::apply(sel[i], (uint8_t)(!null_data[i] && !(_slices.contains(binary_column->get_slice(i)))));
            }
        }
    }

    Status evaluate(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAssignOp>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_and(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateAndOp>(column, selection, from, to);
        return Status::OK();
    }

    Status evaluate_or(const Column* column, uint8_t* selection, uint16_t from, uint16_t to) const override {
        t_evaluate<ColumnPredicateOrOp>(column, selection, from, to);
        return Status::OK();
    }

    StatusOr<uint16_t> evaluate_branchless(const Column* column, uint16_t* sel, uint16_t sel_size) const override {
        // Get BinaryColumn
        const BinaryColumn* binary_column;
        if (column->is_nullable()) {
            // This is NullableColumn, get its data_column
            binary_column =
                    down_cast<const BinaryColumn*>(down_cast<const NullableColumn*>(column)->data_column().get());
        } else {
            binary_column = down_cast<const BinaryColumn*>(column);
        }

        uint16_t new_size = 0;
        if (!column->has_null()) {
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !(_slices.contains(binary_column->get_slice(data_idx)));
            }
        } else {
            /* must use uint8_t* to make vectorized effect */
            const uint8_t* null_data = down_cast<const NullableColumn*>(column)->immutable_null_column_data().data();
            for (uint16_t i = 0; i < sel_size; ++i) {
                uint16_t data_idx = sel[i];
                sel[new_size] = data_idx;
                new_size += !null_data[data_idx] && !(_slices.contains(binary_column->get_slice(data_idx)));
            }
        }
        return new_size;
    }

    bool zone_map_filter(const ZoneMapDetail& detail) const override { return true; }

    bool support_bitmap_filter() const override { return false; }

    Status seek_bitmap_dictionary(BitmapIndexIterator* iter, SparseRange<>* range) const override {
        return Status::Cancelled("not-equal predicate not support bitmap index");
    }

    Status seek_inverted_index(const std::string& column_name, InvertedIndexIterator* iterator,
                               roaring::Roaring* row_bitmap) const override {
        InvertedIndexQueryType query_type = InvertedIndexQueryType::EQUAL_QUERY;
        roaring::Roaring indices;
        for (const std::string& s : _zero_padded_strs) {
            Slice padded_value(s);
            roaring::Roaring index;
            RETURN_IF_ERROR(iterator->read_from_inverted_index(column_name, &padded_value, query_type, &index));
            indices |= index;
        }
        *row_bitmap -= indices;
        return Status::OK();
    }

    bool can_vectorized() const override { return false; }

    PredicateType type() const override { return PredicateType::kNotInList; }

    std::vector<Datum> values() const override {
        std::vector<Datum> ret;
        ret.reserve(_slices.size());
        for (const std::string& s : _zero_padded_strs) {
            ret.emplace_back(Slice(s));
        }
        return ret;
    }

    Status convert_to(const ColumnPredicate** output, const TypeInfoPtr& target_type_info,
                      ObjectPool* obj_pool) const override {
        const auto to_type = target_type_info->type();
        if (to_type == field_type) {
            *output = this;
            return Status::OK();
        }

        CHECK(false) << "Not support, from_type=" << field_type << ", to_type=" << to_type;
        return Status::OK();
    }

    bool padding_zeros(size_t len) override {
        _slices.clear();
        _min_len = UINT32_MAX;
        _max_len = 0;
        for (auto& str : _zero_padded_strs) {
            size_t old_sz = str.size();
            str.append(len > old_sz ? len - old_sz : 0, '\0');
            _slices.emplace(str.data(), old_sz);
            uint32_t str_len = static_cast<uint32_t>(old_sz);
            _min_len = std::min(_min_len, str_len);
            _max_len = std::max(_max_len, str_len);
        }
        if (_min_len == UINT32_MAX) _min_len = 0;
        return true;
    }

private:
    std::vector<std::string> _zero_padded_strs;
    ItemHashSet<Slice> _slices;
    uint32_t _min_len{0};
    uint32_t _max_len{0};
};

ColumnPredicate* new_column_not_in_predicate(const TypeInfoPtr& type_info, ColumnId id,
                                             const std::vector<std::string>& strs) {
    auto type = type_info->type();
    switch (type) {
    case TYPE_BOOLEAN:
        return new ColumnNotInPredicate<TYPE_BOOLEAN>(type_info, id, strs);
    case TYPE_TINYINT:
        return new ColumnNotInPredicate<TYPE_TINYINT>(type_info, id, strs);
    case TYPE_SMALLINT:
        return new ColumnNotInPredicate<TYPE_SMALLINT>(type_info, id, strs);
    case TYPE_INT:
        return new ColumnNotInPredicate<TYPE_INT>(type_info, id, strs);
    case TYPE_BIGINT:
        return new ColumnNotInPredicate<TYPE_BIGINT>(type_info, id, strs);
    case TYPE_LARGEINT:
        return new ColumnNotInPredicate<TYPE_LARGEINT>(type_info, id, strs);
    case TYPE_DECIMAL:
        return new ColumnNotInPredicate<TYPE_DECIMAL>(type_info, id, strs);
    case TYPE_DECIMALV2:
        return new ColumnNotInPredicate<TYPE_DECIMALV2>(type_info, id, strs);
    case TYPE_DECIMAL32: {
        const auto scale = type_info->scale();
        using SetType = ItemHashSet<CppTypeTraits<TYPE_DECIMAL32>::CppType>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL32>(scale, strs);
        return new ColumnNotInPredicate<TYPE_DECIMAL32>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL64: {
        const auto scale = type_info->scale();
        using SetType = ItemHashSet<CppTypeTraits<TYPE_DECIMAL64>::CppType>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL64>(scale, strs);
        return new ColumnNotInPredicate<TYPE_DECIMAL64>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL128: {
        const auto scale = type_info->scale();
        using SetType = ItemHashSet<CppTypeTraits<TYPE_DECIMAL128>::CppType>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL128>(scale, strs);
        return new ColumnNotInPredicate<TYPE_DECIMAL128>(type_info, id, std::move(values));
    }
    case TYPE_DECIMAL256: {
        const auto scale = type_info->scale();
        using SetType = ItemHashSet<CppTypeTraits<TYPE_DECIMAL256>::CppType>;
        SetType values = predicate_internal::strings_to_decimal_set<TYPE_DECIMAL256>(scale, strs);
        return new ColumnNotInPredicate<TYPE_DECIMAL256>(type_info, id, std::move(values));
    }
    case TYPE_CHAR:
        return new BinaryColumnNotInPredicate<TYPE_CHAR>(type_info, id, strs);
    case TYPE_VARCHAR:
        return new BinaryColumnNotInPredicate<TYPE_VARCHAR>(type_info, id, strs);
    case TYPE_DATE_V1:
        return new ColumnNotInPredicate<TYPE_DATE_V1>(type_info, id, strs);
    case TYPE_DATE:
        return new ColumnNotInPredicate<TYPE_DATE>(type_info, id, strs);
    case TYPE_DATETIME_V1:
        return new ColumnNotInPredicate<TYPE_DATETIME_V1>(type_info, id, strs);
    case TYPE_DATETIME:
        return new ColumnNotInPredicate<TYPE_DATETIME>(type_info, id, strs);
    case TYPE_FLOAT:
        return new ColumnNotInPredicate<TYPE_FLOAT>(type_info, id, strs);
    case TYPE_DOUBLE:
        return new ColumnNotInPredicate<TYPE_DOUBLE>(type_info, id, strs);
    case TYPE_UNSIGNED_TINYINT:
    case TYPE_UNSIGNED_SMALLINT:
    case TYPE_UNSIGNED_INT:
    case TYPE_UNSIGNED_BIGINT:
    case TYPE_DISCRETE_DOUBLE:
    case TYPE_STRUCT:
    case TYPE_ARRAY:
    case TYPE_MAP:
    case TYPE_UNKNOWN:
    case TYPE_NONE:
    case TYPE_HLL:
    case TYPE_OBJECT:
    case TYPE_PERCENTILE:
    case TYPE_JSON:
    case TYPE_VARIANT:
    case TYPE_NULL:
    case TYPE_FUNCTION:
    case TYPE_TIME:
    case TYPE_BINARY:
    case TYPE_MAX_VALUE:
    case TYPE_VARBINARY:
    case TYPE_INT256:
        return nullptr;
        // No default to ensure newly added enumerator will be handled.
    }
    return nullptr;
}

ColumnPredicate* new_column_not_in_predicate_from_datum(const TypeInfoPtr& type_info, ColumnId id,
                                                        const std::vector<Datum>& operands) {
    const auto type = type_info->type();
    return field_type_dispatch_column_predicate(
            type, static_cast<ColumnPredicate*>(nullptr), [&]<LogicalType LT>() -> ColumnPredicate* {
                if constexpr (lt_is_string<LT>) {
                    std::vector<std::string> strings;
                    strings.reserve(operands.size());
                    for (const auto& v : operands) {
                        strings.emplace_back(v.get_slice().to_string());
                    }
                    return new BinaryColumnNotInPredicate<LT>(type_info, id, std::move(strings));
                } else {
                    using SetType = ItemHashSet<typename CppTypeTraits<LT>::CppType>;
                    SetType value_set = predicate_internal::datums_to_set<LT>(operands);
                    return new ColumnNotInPredicate<LT>(type_info, id, std::move(value_set));
                }
            });
}

} //namespace starrocks
