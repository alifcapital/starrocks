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

package com.starrocks.sql.optimizer;

import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.LogicalProperty;
import com.starrocks.sql.optimizer.operator.Operator;

/** Read-only inputs for logical property derivation; statistics are deliberately absent. */
public interface LogicalPropertyContext {
    Operator getOp();

    int arity();

    boolean isGroupExprContext();

    LogicalProperty getChildLogicalProperty(int index);

    Operator getChildOperator(int index);

    default ColumnRefSet getChildOutputColumns(int index) {
        return getChildLogicalProperty(index).getOutputColumns();
    }

    default LogicalProperty.OneTabletProperty oneTabletProperty(int index) {
        return getChildLogicalProperty(index).oneTabletProperty();
    }

    static LogicalPropertyContext of(OptExpression expression) {
        return new Snapshot(expression, null);
    }

    static LogicalPropertyContext of(GroupExpression expression) {
        return new Snapshot(null, expression);
    }

    /** Preserve child-property snapshot semantics without collecting unused statistics. */
    final class Snapshot implements LogicalPropertyContext {
        private static final LogicalProperty[] EMPTY_PROPERTIES = new LogicalProperty[0];
        private final OptExpression expression;
        private final GroupExpression groupExpression;
        private final LogicalProperty[] childProperties;

        private Snapshot(OptExpression expression, GroupExpression groupExpression) {
            this.expression = expression;
            this.groupExpression = groupExpression;
            int size = arity();
            childProperties = size == 0 ? EMPTY_PROPERTIES : new LogicalProperty[size];
            int index = 0;
            if (expression != null) {
                for (OptExpression child : expression.getInputs()) {
                    childProperties[index++] = child.getLogicalProperty();
                }
            } else {
                for (Group child : groupExpression.getInputs()) {
                    childProperties[index++] = child.getLogicalProperty();
                }
            }
        }

        @Override
        public Operator getOp() {
            return expression != null ? expression.getOp() : groupExpression.getOp();
        }

        @Override
        public int arity() {
            return expression != null ? expression.arity() : groupExpression.arity();
        }

        @Override
        public boolean isGroupExprContext() {
            return groupExpression != null;
        }

        @Override
        public LogicalProperty getChildLogicalProperty(int index) {
            return childProperties[index];
        }

        @Override
        public Operator getChildOperator(int index) {
            return expression != null ? expression.inputAt(index).getOp()
                    : groupExpression.inputAt(index).getFirstLogicalExpression().getOp();
        }
    }
}
