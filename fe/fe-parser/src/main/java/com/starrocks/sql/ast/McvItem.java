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

package com.starrocks.sql.ast;

import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.type.Type;

import java.util.List;

/**
 * One element of an MCV column group after analysis: a column of the table or a deterministic expression of its
 * columns. The element text names the element in the collected statistics and their metadata: the column name, or
 * for an expression a format prefix and its canonical SQL text. The text the user wrote is not kept.
 */
public class McvItem {
    private final Expr expr;
    private final String elementText;
    private final Type type;
    private final List<String> sourceColumns;
    private final boolean expression;

    public McvItem(Expr expr, String elementText, Type type, List<String> sourceColumns, boolean expression) {
        this.expr = expr;
        this.elementText = elementText;
        this.type = type;
        this.sourceColumns = List.copyOf(sourceColumns);
        this.expression = expression;
    }

    public Expr getExpr() {
        return expr;
    }

    public String getElementText() {
        return elementText;
    }

    public Type getType() {
        return type;
    }

    /** The columns the element reads: the column itself, or the columns of the expression. */
    public List<String> getSourceColumns() {
        return sourceColumns;
    }

    public boolean isExpression() {
        return expression;
    }
}
