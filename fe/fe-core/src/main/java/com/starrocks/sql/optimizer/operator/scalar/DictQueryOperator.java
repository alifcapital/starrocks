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


package com.starrocks.sql.optimizer.operator.scalar;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.thrift.TDictQueryExpr;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Objects;

public class DictQueryOperator extends CallOperator {

    private final TDictQueryExpr dictQueryExpr;
    private final Function fn;

    public DictQueryOperator(List<ScalarOperator> arguments, TDictQueryExpr dictQueryExpr, Function fn,
                             Type type) {
        super(FunctionSet.DICT_MAPPING, type, arguments);
        this.dictQueryExpr = dictQueryExpr;
        this.fn = fn;
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return  visitor.visitDictQueryOperator(this, context);
    }

    // The dictionary name argument may leave out the database, which the analyzer takes from the session, so equal
    // arguments can name different dictionaries. The partition versions only record the dictionary data at analysis
    // time and are left out: two analyses of one dictionary must stay equivalent.
    @Override
    public boolean equivalent(Object obj) {
        if (!super.equivalent(obj)) {
            return false;
        }
        TDictQueryExpr that = ((DictQueryOperator) obj).dictQueryExpr;
        if (dictQueryExpr == null || that == null) {
            return dictQueryExpr == that;
        }
        return Objects.equals(dictQueryExpr.getDb_name(), that.getDb_name()) &&
                Objects.equals(dictQueryExpr.getTbl_name(), that.getTbl_name()) &&
                Objects.equals(dictQueryExpr.getKey_fields(), that.getKey_fields()) &&
                Objects.equals(dictQueryExpr.getValue_field(), that.getValue_field()) &&
                dictQueryExpr.isStrict_mode() == that.isStrict_mode();
    }

    public TDictQueryExpr getDictQueryExpr() {
        return dictQueryExpr;
    }

    public Function getFn() {
        return fn;
    }
}
