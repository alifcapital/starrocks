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

package com.starrocks.sql.parser;

import com.starrocks.catalog.FunctionName;
import com.starrocks.common.util.Util;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.FunctionRef;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.UnitIdentifier;
import com.starrocks.sql.ast.expression.CastExpr;
import com.starrocks.sql.ast.expression.FunctionCallExpr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TimestampArithmeticExpr;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ResourceLock("sql-parser-global-settings")
class SqlParserLocaleNormalizationTest {
    private ConnectContext previousContext;

    @BeforeEach
    void installParserContext() {
        previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        // Parser access only: no FE initialization or external services are needed.
        context.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
        context.setThreadLocalInfo();
    }

    @AfterEach
    void restoreParserContext() {
        if (previousContext == null) {
            ConnectContext.remove();
        } else {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    void foldsSemanticNamesIndependentlyOfDefaultLocaleAndKeepsRawValues() {
        Locale previous = Locale.getDefault();
        boolean previousCaseInsensitive = GlobalVariable.enableTableNameCaseInsensitive;
        try {
            for (Locale locale : List.of(Locale.US, Locale.forLanguageTag("tr-TR"))) {
                Locale.setDefault(locale);
                SessionVariable session = new SessionVariable();
                session.setSqlMode(0);
                FunctionCallExpr parsed = (FunctionCallExpr) SqlParser.parseExpression("MIN(1)", session);
                assertEquals("min", parsed.getFunctionName());
                assertEquals("min", new FunctionName("DB", "MIN").getFunction());
                assertEquals("DB", new FunctionName("DB", "MIN").getDb());
                FunctionRef reference = new FunctionRef(QualifiedName.of(List.of("DB", "MIN")), null, NodePosition.ZERO);
                assertEquals("min", reference.getFunctionName());
                assertEquals("DB", reference.getDbName());
                assertEquals(List.of("DB", "MIN"), reference.getFnName().getParts());
                FunctionCallExpr constructed = new FunctionCallExpr("MIN", List.of(new IntLiteral(1)));
                assertEquals("min", constructed.getFunctionName());
                assertEquals("MICROSECOND", new UnitIdentifier("microsecond").getDescription());
                TimestampArithmeticExpr timestamp = new TimestampArithmeticExpr("TIMESTAMPDIFF",
                        new IntLiteral(1), new IntLiteral(2), "MICROSECOND");
                assertEquals("timestampdiff", timestamp.getFuncName());
                assertEquals("int", IntegerType.INT.toMysqlDataTypeString());

                StructField rawField = new StructField("ITEM", IntegerType.INT);
                StructType structure = new StructType(new ArrayList<>(List.of(rawField)));
                assertSame(rawField, structure.getField("item"));
                assertSame(rawField, structure.getField("ITEM"));
                assertEquals("ITEM", rawField.getName());
                CastExpr cast = (CastExpr) SqlParser.parseExpression("CAST(1 AS STRUCT<`ITEM` INT>)", session);
                assertTrue(((StructType) cast.getTargetTypeDef().getType()).containsField("item"));
                assertTrue(((StructType) cast.getTargetTypeDef().getType()).containsField("ITEM"));
                StringLiteral literal = (StringLiteral) SqlParser.parseExpression("'Iİıi'", session);
                assertEquals("Iİıi", literal.getStringValue());

                GlobalVariable.enableTableNameCaseInsensitive = true;
                assertEquals("item", Util.normalizeName("ITEM"));
                GlobalVariable.enableTableNameCaseInsensitive = false;
                assertEquals("ITEM", Util.normalizeName("ITEM"));
            }
        } finally {
            GlobalVariable.enableTableNameCaseInsensitive = previousCaseInsensitive;
            Locale.setDefault(previous);
        }
    }
}
