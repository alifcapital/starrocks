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

#include "formats/parquet/parquet_block_split_bloom_filter.h"

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "util/debug_util.h"

namespace starrocks {

void ParquetBlockSplitBloomFilter::add_hash(uint64_t hash) {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    DCHECK(_num_bytes >= BYTES_PER_BLOCK);
    uint32_t num_blocks = _num_bytes / BYTES_PER_BLOCK;
    uint32_t block_index = (uint32_t)(((hash >> 32) * num_blocks) >> 32);
    auto key = (uint32_t)hash;

    // Calculate masks for bucket.
    uint32_t masks[8];
    _set_masks(key, masks);
    auto* block_offset = (uint32_t*)(_data + BYTES_PER_BLOCK * block_index);

#ifdef __AVX2__
    // SIMD: load 8 uint32 block values and 8 masks, OR them together
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_offset));
    __m256i v_masks = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(masks));
    __m256i v_result = _mm256_or_si256(v_block, v_masks);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(block_offset), v_result);
#else
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        *(block_offset + i) |= masks[i];
    }
#endif
}

bool ParquetBlockSplitBloomFilter::test_hash(uint64_t hash) const {
    // most significant 32 bit mod block size as block index(BTW:block size is
    // power of 2)
    uint32_t num_blocks = _num_bytes / BYTES_PER_BLOCK;
    uint32_t block_index = (uint32_t)(((hash >> 32) * num_blocks) >> 32);
    auto key = (uint32_t)hash;
    // Calculate masks for bucket.
    uint32_t masks[BITS_SET_PER_BLOCK];
    _set_masks(key, masks);
    auto* block_offset = (uint32_t*)(_data + BYTES_PER_BLOCK * block_index);

#ifdef __AVX2__
    // SIMD: load 8 uint32 block values and 8 masks, AND them, check for zeros
    __m256i v_block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(block_offset));
    __m256i v_masks = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(masks));
    __m256i v_and = _mm256_and_si256(v_block, v_masks);
    __m256i v_zero = _mm256_cmpeq_epi32(v_and, _mm256_setzero_si256());
    // If any element is zero, the corresponding bit in mask will be set
    int zero_mask = _mm256_movemask_ps(_mm256_castsi256_ps(v_zero));
    return zero_mask == 0;  // All non-zero means hash exists
#else
    for (int i = 0; i < BITS_SET_PER_BLOCK; ++i) {
        if ((*(block_offset + i) & masks[i]) == 0) {
            return false;
        }
    }
    return true;
#endif
}

} // namespace starrocks
