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

#include "exprs/builtin_functions.h"
#include "exprs/function_helper.h"

namespace starrocks {

class EncryptionFunctions {
public:
    /**
     * @param: [data, key, iv, mode] or [data, key, iv, mode, aad]
     * @paramType: [BinaryColumn, BinaryColumn, BinaryColumn, BinaryColumn, BinaryColumn]
     * @return: BinaryColumn
     * 4/5-parameter version, supports IV, encryption mode, and AAD (for GCM mode)
     * Note: FE's ExpressionAnalyzer automatically converts 2/3 params to 4 params
     */
    DEFINE_VECTORIZED_FN(aes_encrypt_with_mode);
    static StatusOr<ColumnPtr> aes_encrypt_with_mode_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> aes_encrypt_with_mode_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [data, key, iv, mode] or [data, key, iv, mode, aad]
     * @paramType: [BinaryColumn, BinaryColumn, BinaryColumn, BinaryColumn, BinaryColumn]
     * @return: BinaryColumn
     * 4/5-parameter version, supports IV, encryption mode, and AAD (for GCM mode)
     * Note: FE's ExpressionAnalyzer automatically converts 2/3 params to 4 params
     */
    DEFINE_VECTORIZED_FN(aes_decrypt_with_mode);
    static StatusOr<ColumnPtr> aes_decrypt_with_mode_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> aes_decrypt_with_mode_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: BinaryColumn
     */
    DEFINE_VECTORIZED_FN(from_base64);
    static StatusOr<ColumnPtr> from_base64_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> from_base64_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(to_base64);
    static StatusOr<ColumnPtr> to_base64_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> to_base64_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(md5sum);
    static StatusOr<ColumnPtr> md5sum_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> md5sum_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(md5sum_numeric);
    static StatusOr<ColumnPtr> md5sum_numeric_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> md5sum_numeric_impl(FunctionContext*, const Inputs&);

    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(md5);
    static StatusOr<ColumnPtr> md5_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> md5_impl(FunctionContext*, const Inputs&);

    /*
     * Called by sha2 to the corresponding part
     */
    DEFINE_VECTORIZED_FN(sha224);
    static StatusOr<ColumnPtr> sha224_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> sha224_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(sha256);
    static StatusOr<ColumnPtr> sha256_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> sha256_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(sha384);
    static StatusOr<ColumnPtr> sha384_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> sha384_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(sha512);
    static StatusOr<ColumnPtr> sha512_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> sha512_impl(FunctionContext*, const Inputs&);
    DEFINE_VECTORIZED_FN(invalid_sha);
    static StatusOr<ColumnPtr> invalid_sha_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> invalid_sha_impl(FunctionContext*, const Inputs&);
    /**
     * @param: [json_string, tagged_value]
     * @paramType: [BinaryColumn, BinaryColumn]
     * @return: Int32Column
     */
    DEFINE_VECTORIZED_FN(sha2);
    static StatusOr<ColumnPtr> sha2_selected(FunctionContext*, const SelectedColumns&, size_t);
    template <typename Inputs>
    static StatusOr<ColumnPtr> sha2_impl(FunctionContext*, const Inputs&);
    static Status sha2_prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);
    static Status sha2_close(FunctionContext* context, FunctionContext::FunctionStateScope scope);

    DEFINE_VECTORIZED_FN(encode_fingerprint_sha256);

    // method for sha2
    struct SHA2Ctx {
        ScalarFunction function;
        SelectedScalarFunction selected_function;
    };
};

} // namespace starrocks
