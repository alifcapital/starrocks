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

import com.starrocks.sql.parser.NodePosition;
import com.starrocks.statistic.JoinStatisticsDefinition;

import java.util.Locale;
import java.util.Map;

public class JoinStatisticsStmt extends StatementBase {
    public enum Action { CREATE, ANALYZE, DROP, SHOW }

    private final Action action;
    private final String name;
    private final QueryStatement query;
    private final boolean asynchronous;
    private final boolean ifExists;
    private final Map<String, String> properties;
    private JoinStatisticsDefinition definition;

    public JoinStatisticsStmt(Action action, String name, QueryStatement query, boolean asynchronous,
                              boolean ifExists, Map<String, String> properties, NodePosition pos) {
        super(pos);
        this.action = action;
        this.name = name == null ? null : name.toLowerCase(Locale.ROOT);
        this.query = query;
        this.asynchronous = asynchronous;
        this.ifExists = ifExists;
        this.properties = Map.copyOf(properties);
    }

    public Action getAction() {
        return action;
    }

    public String getName() {
        return name;
    }

    public QueryStatement getQuery() {
        return query;
    }

    public boolean isAsynchronous() {
        return asynchronous;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public JoinStatisticsDefinition getDefinition() {
        return definition;
    }

    public void setDefinition(JoinStatisticsDefinition definition) {
        this.definition = definition;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return ((AstVisitorExtendInterface<R, C>) visitor).visitJoinStatisticsStatement(this, context);
    }
}
