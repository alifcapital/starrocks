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

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;

// Most expression contexts have only one or two children.
public class CompactParserRuleContext extends ParserRuleContext {
    public CompactParserRuleContext() {
        super();
    }
    public CompactParserRuleContext(ParserRuleContext parent, int state) {
        super(parent, state);
    }
    @Override
    public <T extends ParseTree> T addAnyChild(T child) {
        if (children == null) {
            children = new ArrayList<>(2);
        }
        children.add(child);
        return child;
    }
    @Override
    public String getText() {
        // Unary grammar rules do not need concatenation or a temporary builder.
        if (getChildCount() == 1) {
            return getChild(0).getText();
        }
        return super.getText();
    }
}
