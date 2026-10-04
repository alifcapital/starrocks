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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Field {
    // The name here is a column name, not qualified name.
    private final String name;
    private Type type;
    // shadow column is not visible, e.g. schema change column and materialized column
    private final boolean visible;

    /**
     * TableName of field
     * relationAlias is origin table which table name is explicit, such as t0.a
     * Field come from scope is resolved by scope relation alias,
     * such as subquery alias and table relation name
     */
    private final TableName relationAlias;
    private final Expr originExpression;
    private boolean isNullable;

    // Record tmp match record. We expect most fields to never match into a struct, so the list is made on demand.
    private List<Integer> tmpUsedStructFieldPos;

    public Field(String name, Type type, TableName relationAlias, Expr originExpression) {
        this(name, type, relationAlias, originExpression, true);
    }

    public Field(String name, Type type, TableName relationAlias, Expr originExpression, boolean visible) {
        this.name = name;
        this.type = type;
        this.relationAlias = relationAlias;
        this.originExpression = originExpression;
        this.visible = visible;
        this.isNullable = true;
    }

    public Field(String name, Type type, TableName relationAlias, Expr originExpression, boolean visible, boolean isNullable) {
        this.name = name;
        this.type = type;
        this.relationAlias = relationAlias;
        this.originExpression = originExpression;
        this.visible = visible;
        this.isNullable = isNullable;
    }

    public Field(Field other) {
        this.name = other.name;
        this.type = other.type;
        this.relationAlias = other.relationAlias;
        this.originExpression = other.originExpression;
        this.visible = other.visible;
        this.isNullable = other.isNullable;
    }

    public String getName() {
        return name;
    }

    public TableName getRelationAlias() {
        return relationAlias;
    }

    public Expr getOriginExpression() {
        return originExpression;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public boolean isVisible() {
        return visible;
    }

    public boolean isNullable() {
        return isNullable;
    }

    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    public boolean canResolve(SlotRef expr) {
        return canResolve(expr, isRelationAliasCaseInsensitive());
    }

    /** As canResolve(expr), with the session flag for the case of relation aliases read once by the caller. */
    public boolean canResolve(SlotRef expr, boolean aliasCaseInsensitive) {
        if (type.isStructType()) {
            return tryToParseAsStructType(expr);
        }

        TableName tableName = expr.getTblNameWithoutAnalyzed();
        if (tableName != null) {
            if (relationAlias == null) {
                return false;
            }
            // Most fields fail on the name, which is cheaper to compare than the qualifier.
            return expr.getColumnName().equalsIgnoreCase(this.name) && matchesPrefix(tableName, aliasCaseInsensitive);
        } else {
            return expr.getColumnName().equalsIgnoreCase(this.name);
        }
    }

    private boolean tryToParseAsStructType(SlotRef slotRef) {
        QualifiedName qualifiedName = slotRef.getQualifiedName();
        if (tmpUsedStructFieldPos != null) {
            tmpUsedStructFieldPos.clear();
        }

        if (qualifiedName == null) {
            return slotRef.getColumnName().equalsIgnoreCase(this.name);
        }

        if (relationAlias == null) {
            return false;
        }

        // Generate current field's full qualified name.
        // fieldFullQualifiedName: [CatalogName, DatabaseName, TableName, ColumnName]
        String[] fieldFullQualifiedName = new String[] {
                relationAlias.getCatalog(),
                relationAlias.getDb(),
                relationAlias.getTbl(),
                name
        };
        // The matches from each start compare the same parts, so each part is turned to lower case once.
        List<String> slotRefParts = qualifiedName.getParts();
        String[] lowerSlotRefParts = new String[slotRefParts.size()];
        String[] lowerFieldParts = new String[fieldFullQualifiedName.length];

        // First start matching from CatalogName, if it fails, then start matching from DatabaseName, and so on.
        for (int i = 0; i < 4; i++) {
            if (tryToMatch(fieldFullQualifiedName, lowerFieldParts, i, slotRefParts, lowerSlotRefParts)) {
                return true;
            }
        }
        return false;
    }

    private boolean tryToMatch(String[] fieldFullQualifiedName, String[] lowerFieldParts, int index,
                               List<String> slotRefParts, String[] lowerSlotRefParts) {
        int matchIndex = 0;
        // i = 0 means match from catalog name,
        // i = 1, match from database name,
        // i = 2, match from table name, only table name is case-sensitive,
        // i = 3, match from column name.
        for (; index < 4 && matchIndex < slotRefParts.size(); index++) {
            if (fieldFullQualifiedName[index] == null) {
                return false;
            }

            String part;
            String comparedPart;
            // Only table name is case-sensitive, we will convert other parts to lower case.
            if (index != 2) {
                part = lowerSlotRefParts[matchIndex];
                if (part == null) {
                    part = slotRefParts.get(matchIndex).toLowerCase();
                    lowerSlotRefParts[matchIndex] = part;
                }
                comparedPart = lowerFieldParts[index];
                if (comparedPart == null) {
                    comparedPart = fieldFullQualifiedName[index].toLowerCase();
                    lowerFieldParts[index] = comparedPart;
                }
            } else {
                part = slotRefParts.get(matchIndex);
                comparedPart = fieldFullQualifiedName[index];
            }
            matchIndex++;
            boolean matches = !GlobalVariable.enableTableNameCaseInsensitive
                    ? part.equals(comparedPart)
                    : part.equalsIgnoreCase(comparedPart);

            if (!matches) {
                return false;
            }
        }

        if (index < 4) {
            // Not match to col name, return false directly.
            return false;
        }

        // matchIndex reach the end of the slot ref parts, means this SlotRef matched all.
        if (matchIndex == slotRefParts.size()) {
            return true;
        }

        // matchIndex not reach end of the slot ref parts, it must be StructType.
        Type tmpType = type;
        for (; matchIndex < slotRefParts.size(); matchIndex++) {
            if (!tmpType.isStructType()) {
                return false;
            }
            StructField structField = ((StructType) tmpType).getField(slotRefParts.get(matchIndex));
            if (structField == null) {
                return false;
            }
            // Record the struct field position that matches successfully.
            if (tmpUsedStructFieldPos == null) {
                tmpUsedStructFieldPos = new LinkedList<>();
            }
            tmpUsedStructFieldPos.add(structField.getPosition());
            tmpType = structField.getType();
        }
        return true;
    }

    public List<Integer> getTmpUsedStructFieldPos() {
        return tmpUsedStructFieldPos == null ? Collections.emptyList() : tmpUsedStructFieldPos;
    }

    public boolean matchesPrefix(TableName tableName) {
        return matchesPrefix(tableName, isRelationAliasCaseInsensitive());
    }

    private boolean matchesPrefix(TableName tableName, boolean aliasCaseInsensitive) {
        if (tableName.getCatalog() != null && relationAlias.getCatalog() != null &&
                !tableName.getCatalog().equals(relationAlias.getCatalog())) {
            return false;
        }

        if (tableName.getDb() != null && !tableName.getDb().equals(relationAlias.getDb())) {
            return false;
        }

        if (aliasCaseInsensitive) {
            return tableName.getTbl().equalsIgnoreCase(relationAlias.getTbl());
        } else {
            return tableName.getTbl().equals(relationAlias.getTbl());
        }
    }

    private static boolean isRelationAliasCaseInsensitive() {
        ConnectContext context = ConnectContext.get();
        return context != null && context.isRelationAliasCaseInsensitive();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        if (name == null) {
            result.append("<anonymous>");
        } else {
            result.append(name);
        }
        result.append(":").append(type);
        return result.toString();
    }
}