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

package com.starrocks.sql.ast.expression;

import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.parser.NodePosition;

import java.util.List;
import java.util.Objects;

/**
 * LargeInPredicate is a special optimization for queries with extremely large IN constant lists.
 * 
 * <p>Problem: When a query contains a huge number of IN items (e.g., 100,000+ constants), 
 * parsing and storing each constant as an Expr object creates significant overhead in FE's 
 * parse/Analyzer/Planner/Deploy phases, leading to severe performance degradation.
 * 
 * <p>Solution: Instead of storing all children Expr objects, LargeInPredicate:
 * <ul>
 *   <li>Only keeps ONE child Expr (first element) in the in_list for type validation</li>
 *   <li>Stores the remaining constants as a raw text string (rawConstantList)</li>
 *   <li>Records the total constant count and type information</li>
 * </ul>
 * 
 * <p>This dramatically reduces memory usage and processing time during query compilation.
 * 
 * <p><b>Execution Strategy:</b>
 * LargeInPredicate is NOT directly executed by BE. It must be transformed to a Left semi/anti join
 * via {@link com.starrocks.sql.optimizer.rule.transformation.LargeInPredicateToJoinRule} before execution.
 * 
 * <p><b>Constants:</b>
 * The list keeps integer values as Long, string values as String and other number values as the literals the
 * parser built for them. The planner resolves their comparison type as for an InPredicate with the same list.
 */
public class LargeInPredicate extends InPredicate {
    private final String rawText;
    private final List<Object> rawConstantList;
    private final int constantCount;

    public LargeInPredicate(Expr compareExpr, String rawText, List<?> rawConstantList, int constantCount,
                           boolean isNotIn, List<Expr> inList, NodePosition pos) {
        super(compareExpr, inList, isNotIn, pos);
        this.rawText = trimParentheses(rawText);
        this.rawConstantList = (List<Object>) rawConstantList;
        this.constantCount = constantCount;
    }
    
    private static String trimParentheses(String rawText) {
        if (rawText == null) {
            return null;
        }
        
        String trimmed = rawText.trim();
        if (trimmed.startsWith("(") && trimmed.endsWith(")")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        
        return trimmed;
    }

    public String getRawText() {
        return rawText;
    }

    public Expr getCompareExpr() {
        return getChild(0);
    }

    public List<Object> getRawConstantList() {
        return rawConstantList;
    }

    public int getConstantCount() {
        return constantCount;
    }

    /**
     * The constant at the given position as the literal that the InPredicate form of this predicate holds.
     */
    public LiteralExpr getConstantLiteral(int index) {
        Object value = rawConstantList.get(index);
        if (value instanceof LiteralExpr literal) {
            return literal;
        } else if (value instanceof Long longValue) {
            return new IntLiteral(longValue);
        } else if (value instanceof String stringValue) {
            return new StringLiteral(stringValue);
        }
        throw new IllegalStateException("Unexpected LargeInPredicate constant: " + value);
    }

    @Override
    public int getInElementNum() {
        return constantCount;
    }

    @Override
    public boolean isConstantValues() {
        return true;
    }

    @Override
    public Expr clone() {
        return new LargeInPredicate(getChild(0).clone(), rawText, rawConstantList, constantCount,
                                   isNotIn(), getListChildren(), pos);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (!super.equals(obj)) {
            return false;
        }

        LargeInPredicate that = (LargeInPredicate) obj;
        return constantCount == that.constantCount &&
               rawConstantList.equals(that.rawConstantList);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), rawConstantList, constantCount);
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        LargeInPredicate that = (LargeInPredicate) obj;
        return constantCount == that.constantCount &&
               rawConstantList.equals(that.rawConstantList);
    }

    @Override
    public String toString() {
        return String.format("LargeInPredicate{compareExpr=%s, constantCount=%d, isNotIn=%s}",
                           getChild(0), constantCount, isNotIn());
    }


    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)  {
        return visitor.visitLargeInPredicate(this, context);
    }
}
