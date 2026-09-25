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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/** Adds necessary bounds for positive scan conjuncts without changing their SQL NULL semantics. */
public final class MonotonicFilterDerivation {
    private MonotonicFilterDerivation() {
    }

    public static ScalarOperator addScanBounds(ScalarOperator predicate) {
        if (predicate == null) {
            return predicate;
        }
        List<ScalarOperator> originals = Utils.extractConjuncts(predicate);
        Set<ScalarOperator> conjuncts = new LinkedHashSet<>(originals);
        for (ScalarOperator original : originals) {
            // Do not descend into OR, NOT, IS NULL or other boolean/value expressions.
            ScalarOperator bound = derive(original);
            if (bound != original) {
                bound = new ScalarOperatorRewriter().rewrite(bound,
                        ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
                conjuncts.addAll(Utils.extractConjuncts(bound));
            }
        }
        return conjuncts.size() == new LinkedHashSet<>(originals).size()
                ? predicate : Utils.compoundAnd(conjuncts);
    }

    private static ScalarOperator derive(ScalarOperator predicate) {
        if (!(predicate instanceof BinaryPredicateOperator)
                || !(predicate.getChild(0) instanceof CallOperator)
                || !(predicate.getChild(1) instanceof ConstantOperator)
                || ((ConstantOperator) predicate.getChild(1)).isNull()) {
            return predicate;
        }
        if (predicate.getChild(0) instanceof CastOperator) {
            return StringDatePredicateDerivation.derive((BinaryPredicateOperator) predicate);
        }
        ConnectContext context = ConnectContext.get();
        if (context != null && !context.getSessionVariable().isEnableMonotonicPredicateRewrite()) {
            return predicate;
        }
        CallOperator call = (CallOperator) predicate.getChild(0);
        MonotonicFunctionRegistry.PredicateInverse inverse = MonotonicFunctionRegistry.filterInverse(call.getFnName());
        if (inverse == null) {
            inverse = MonotonicFunctionRegistry.exactInverse(call.getFnName());
        }
        ScalarOperator dataChild = MonotonicFunctionRegistry.dataChildOf(call);
        if (inverse == null || dataChild == null) {
            return predicate;
        }
        ScalarOperator bound = inverse.invert(call, dataChild,
                ((BinaryPredicateOperator) predicate).getBinaryType(), (ConstantOperator) predicate.getChild(1))
                .orElse(predicate);
        if (bound == predicate) {
            return predicate;
        }
        // Each inverse removes one function layer; recursively derive bounds through a chain.
        return Utils.compoundAnd(Utils.extractConjuncts(bound).stream()
                .map(MonotonicFilterDerivation::derive).toList());
    }
}
