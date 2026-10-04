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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/analysis/SlotRef.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.sql.ast.expression;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.TableName;
import com.starrocks.planner.SlotDescriptor;
import com.starrocks.planner.SlotId;
import com.starrocks.server.CatalogMgr;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.AstVisitorExtendInterface;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.type.InvalidType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;

import java.util.List;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkArgument;

public class SlotRef extends Expr {
    // The name hash helpers keep a String.hashCode() state in the low 32 bits of a long, and return
    // NOT_ASCII once they see a non-ASCII char.
    private static final long NOT_ASCII = -1L;

    private TableName tblName;
    private String colName;
    private ColumnId columnId;
    //label/isBackQuoted used in toSql
    private String label;
    private boolean isBackQuoted = false;

    private QualifiedName qualifiedName;

    // results of analysis
    protected SlotDescriptor desc;

    // Only Struct Type need this field
    // Record access struct subfield path position
    // Example: struct type: col: STRUCT<c1: INT, c2: STRUCT<c1: INT, c2: DOUBLE>>,
    // We execute sql: `SELECT col FROM table`, the usedStructField value is [].
    // We execute sql: `SELECT col.c2 FROM table`, the usedStructFieldPos value is [1].
    // We execute sql: `SELECT col.c2.c1 FROM table`, the usedStructFieldPos value is [1, 0].
    private ImmutableList<Integer> usedStructFieldPos;

    // now it is used in Analyzer phase of creating mv to decide the field nullable of mv
    // can not use desc because the slotId is unknown in Analyzer phase
    private boolean nullable = true;

    // Only used write
    private SlotRef() {
        super();
    }

    public SlotRef(TableName tblName, String col) {
        super();
        this.tblName = tblName;
        this.colName = col;
        this.label = "`" + col + "`";
    }

    public SlotRef(TableName tblName, String col, String label) {
        super();
        this.tblName = tblName;
        this.colName = col;
        this.label = label;
    }

    public SlotRef(QualifiedName qualifiedName) {
        super(qualifiedName.getPos());
        List<String> parts = qualifiedName.getParts();
        // If parts.size() = 1, it must be a column name. Like `Select a FROM table`.
        // If parts.size() = [2, 3, 4], it maybe a column name or specific struct subfield name.
        checkArgument(parts.size() > 0);
        this.qualifiedName = QualifiedName.of(qualifiedName.getParts(), qualifiedName.getPos());
        if (parts.size() == 1) {
            this.colName = parts.get(0);
            this.label = parts.get(0);
        } else if (parts.size() == 2) {
            this.tblName = new TableName(null, null, parts.get(0), qualifiedName.getPos());
            this.colName = parts.get(1);
            this.label = parts.get(1);
        } else if (parts.size() == 3) {
            this.tblName = new TableName(null, parts.get(0), parts.get(1), qualifiedName.getPos());
            this.colName = parts.get(2);
            this.label = parts.get(2);
        } else if (parts.size() == 4) {
            this.tblName = new TableName(parts.get(0), parts.get(1), parts.get(2), qualifiedName.getPos());
            this.colName = parts.get(3);
            this.label = parts.get(3);
        } else {
            // If parts.size() > 4, it must refer to a struct subfield name, so we set SlotRef's TableName null value,
            // set col, label a qualified name here[Of course it's a wrong value].
            // Correct value will be parsed in Analyzer according context.
            this.tblName = null;
            this.colName = qualifiedName.toString();
            this.label = qualifiedName.toString();
        }
    }

    // C'tor for a "pre-analyzed" ref to slot that doesn't correspond to
    // a table's column.
    public SlotRef(SlotDescriptor desc) {
        super();
        this.tblName = null;
        this.colName = desc.getLabel();
        this.desc = desc;
        this.type = desc.getType();
        this.originType = desc.getOriginType();
        this.label = null;
        if (this.type.isChar()) {
            this.type = VarcharType.VARCHAR;
        }
        analysisDone();
    }

    protected SlotRef(SlotRef other) {
        super(other);
        tblName = other.tblName;
        colName = other.colName;
        columnId = other.columnId;
        label = other.label;
        desc = other.desc;
        qualifiedName = other.qualifiedName;
        usedStructFieldPos = other.usedStructFieldPos;
    }

    public SlotRef(String label, SlotDescriptor desc) {
        this(desc);
        this.label = label;
    }

    public SlotRef(SlotId slotId) {
        this(new SlotDescriptor(slotId, "", InvalidType.INVALID, false));
    }

    public void setBackQuoted(boolean isBackQuoted) {
        this.isBackQuoted = isBackQuoted;
    }

    public boolean isBackQuoted() {
        return isBackQuoted;
    }

    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    public void setQualifiedName(QualifiedName qualifiedName) {
        this.qualifiedName = qualifiedName;
    }

    public void setUsedStructFieldPos(List<Integer> usedStructFieldPos) {
        this.usedStructFieldPos = ImmutableList.copyOf(usedStructFieldPos);
    }

    public List<Integer> getUsedStructFieldPos() {
        return usedStructFieldPos;
    }

    // When SlotRef is accessing struct subfield, we need to reset SlotRef's type and col name
    // Do this is for compatible with origin SlotRef
    public void resetStructInfo() {
        checkArgument(type.isStructType());
        checkArgument(usedStructFieldPos.size() > 0);

        StringBuilder colStr = new StringBuilder();
        colStr.append(colName);

        setOriginType(type);
        Type tmpType = type;
        for (int pos : usedStructFieldPos) {
            StructField structField = ((StructType) tmpType).getField(pos);
            colStr.append(".");
            colStr.append(structField.getName());
            tmpType = structField.getType();
        }
        // Set type to subfield's type
        type = tmpType;
        // col name like a.b.c
        colName = colStr.toString();
    }

    @Override
    public Expr clone() {
        return new SlotRef(this);
    }

    public SlotDescriptor getDesc() {
        return desc;
    }

    public SlotId getSlotId() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        return desc.getId();
    }

    public Column getColumn() {
        if (desc == null) {
            return null;
        } else {
            return desc.getColumn();
        }
    }

    public boolean isFromLambda() {
        return tblName != null && tblName.getTbl().equalsIgnoreCase(TableName.LAMBDA_FUNC_TABLE);
    }

    public void setTblName(TableName name) {
        this.tblName = name;
    }

    public void setDesc(SlotDescriptor desc) {
        this.desc = desc;
    }

    public void setType(Type type) {
        super.setType(type);
        if (desc != null) {
            desc.setType(type);
        }
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public SlotDescriptor getSlotDescriptorWithoutCheck() {
        return desc;
    }

    public TableName getTblName() {
        return tblName;
    }

    public String getColName() {
        return colName;
    }

    @Override
    public String debugString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
        helper.add("slotDesc", desc != null ? desc.debugString() : "null");
        helper.add("col", colName);
        helper.add("label", label);
        helper.add("tblName", tblName != null ? tblName.toSql() : "null");
        return helper.toString();
    }

    public boolean isColumnRef() {
        return tblName != null && !isFromLambda();
    }

    public TableName getTableName() {
        Preconditions.checkState(isAnalyzed);
        Preconditions.checkNotNull(desc);
        if (tblName == null) {
            Preconditions.checkNotNull(desc.getParent());
            if (desc.getParent().getRef() == null) {
                return null;
            }
            return desc.getParent().getRef().getName();
        }
        return tblName;
    }

    @Override
    public int hashCode() {
        if (desc != null) {
            return desc.getId().hashCode();
        }
        // The value is Objects.hashCode(name) or Objects.hashCode(name, usedStructFieldPos), where name is
        // (tblName == null ? "" : tblName.toSql() + "." + label).toLowerCase().
        // The planner iterates hash maps keyed by expressions and that order decides column ref ids,
        // so the value must stay the same.
        int nameHash = lowerCaseNameHash();
        if (usedStructFieldPos != null) {
            // Means this SlotRef is going to access subfield in StructType
            return 31 * (31 + nameHash) + usedStructFieldPos.hashCode();
        } else {
            return 31 + nameHash;
        }
    }

    // Returns (tblName.toSql() + "." + label).toLowerCase().hashCode(). We expect this to run for every slot
    // in every expression hash, so for ASCII names we compute it without building the strings. For other
    // characters, and for locales where toLowerCase() maps ASCII letters in a special way, we build the
    // string, because then the lower case form can differ from a per-char mapping.
    private int lowerCaseNameHash() {
        TableName name = tblName;
        if (name == null) {
            return 0;
        }
        // Same characters as TableName.toSql() followed by "." and the label. StringBuilder.append and
        // string concatenation write a null String as "null".
        String catalog = name.getCatalog();
        String db = name.getDb();
        long h = isAsciiLowerCasePlain() ? 0 : NOT_ASCII;
        if (catalog != null && !CatalogMgr.isInternalCatalog(catalog)) {
            h = lowerCaseQuotedHash(h, catalog);
        }
        if (db != null) {
            h = lowerCaseQuotedHash(h, db);
        }
        h = lowerCaseQuotedHash(h, String.valueOf(name.getTbl()));
        h = lowerCaseHash(h, String.valueOf(label));
        if (h == NOT_ASCII) {
            return (name.toSql() + "." + label).toLowerCase().hashCode();
        }
        return (int) h;
    }

    // String.toLowerCase() uses the default locale. Only for these languages it may map ASCII letters
    // to something other than 'a'..'z'.
    private static boolean isAsciiLowerCasePlain() {
        String lang = Locale.getDefault().getLanguage();
        return !lang.equals("tr") && !lang.equals("az") && !lang.equals("lt");
    }

    // Continues the hash over "`" + s + "`." in lower case.
    private static long lowerCaseQuotedHash(long h, String s) {
        if (h == NOT_ASCII) {
            return h;
        }
        h = lowerCaseHash(Integer.toUnsignedLong(31 * (int) h + '`'), s);
        if (h == NOT_ASCII) {
            return h;
        }
        return Integer.toUnsignedLong(31 * (31 * (int) h + '`') + '.');
    }

    // Continues the hash over s in lower case.
    private static long lowerCaseHash(long h, String s) {
        if (h == NOT_ASCII) {
            return h;
        }
        int x = (int) h;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c >= 0x80) {
                return NOT_ASCII;
            }
            if (c >= 'A' && c <= 'Z') {
                c = (char) (c + ('a' - 'A'));
            }
            x = 31 * x + c;
        }
        return Integer.toUnsignedLong(x);
    }

    @Override
    public boolean equalsWithoutChild(Object obj) {
        if (!super.equalsWithoutChild(obj)) {
            return false;
        }
        SlotRef other = (SlotRef) obj;
        // check slot ids first; if they're both set we only need to compare those
        // (regardless of how the ref was constructed)
        if (desc != null && other.desc != null) {
            return desc.getId().equals(other.desc.getId());
        }
        if ((tblName == null) != (other.tblName == null)) {
            return false;
        }
        if (tblName != null && !tblName.equals(other.tblName)) {
            return false;
        }
        if ((colName == null) != (other.colName == null)) {
            return false;
        }
        if (colName != null && !colName.equalsIgnoreCase(other.colName)) {
            return false;
        }

        if (usedStructFieldPos != null && !usedStructFieldPos.equals(other.usedStructFieldPos)) {
            return false;
        }
        return true;
    }

    @Override
    protected boolean isConstantImpl() {
        return false;
    }

    public boolean isNullable() {
        if (desc != null) {
            return desc.getIsNullable();
        }
        return nullable;
    }

    public String getColumnName() {
        return colName;
    }

    public void setColumnName(String columnName) {
        this.colName = columnName;
    }

    public ColumnId getColumnId() {
        return columnId;
    }

    public void setColumnId(ColumnId columnId) {
        this.columnId = columnId;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public boolean supportSerializable() {
        return true;
    }

    /**
     * Below function is added by new analyzer
     */
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitSlot(this, context);
    }

    public TableName getTblNameWithoutAnalyzed() {
        return tblName;
    }

    @Override
    public boolean isSelfMonotonic() {
        return true;
    }
}
