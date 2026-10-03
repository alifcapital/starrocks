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

import java.util.List;
import java.util.Objects;

/**
 * An IN predicate whose constants are kept as values of the comparison type instead of operators. The children
 * are the compared expression and the first constant. LargeInPredicateToJoinRule turns it into a join with the
 * values.
 */
public class LargeInPredicateOperator extends InPredicateOperator {
    private final String rawText;
    private final LargeInConstants constants;

    public LargeInPredicateOperator(String rawText, LargeInConstants constants, boolean isNotIn,
                                    List<ScalarOperator> children) {
        super(isNotIn, children.toArray(new ScalarOperator[0]));
        this.rawText = rawText;
        this.constants = constants;
    }

    public String getRawText() {
        return rawText;
    }

    public LargeInConstants getConstants() {
        return constants;
    }

    public int getConstantCount() {
        return constants.getValues().size();
    }

    public ScalarOperator getCompareExpr() {
        return getChild(0);
    }

    @Override
    public String toString() {
        String inClause = isNotIn() ? " NOT IN " : " IN ";
        if (getConstantCount() > 100) {
            return getCompareExpr() + inClause + "(<" + getConstantCount() + " values>)";
        } else {
            return getCompareExpr() + inClause + "(" + rawText + ")";
        }
    }

    @Override
    public boolean equalsSelf(Object obj) {
        if (!super.equalsSelf(obj)) {
            return false;
        }
        LargeInPredicateOperator that = (LargeInPredicateOperator) obj;
        return Objects.equals(constants, that.constants);
    }

    @Override
    public boolean equivalent(Object obj) {
        if (!super.equivalent(obj)) {
            return false;
        }
        LargeInPredicateOperator that = (LargeInPredicateOperator) obj;
        return Objects.equals(constants, that.constants);
    }

    @Override
    public int hashCodeSelf() {
        return 31 * (31 + super.hashCodeSelf()) + Objects.hashCode(constants);
    }

    @Override
    public <R, C> R accept(ScalarOperatorVisitor<R, C> visitor, C context) {
        return visitor.visitLargeInPredicate(this, context);
    }

    @Override
    public boolean allValuesMatch(java.util.function.Predicate<? super ScalarOperator> lambda) {
        return false;
    }

    @Override
    public boolean hasAnyNullValues() {
        return constants.hasNull();
    }
}
