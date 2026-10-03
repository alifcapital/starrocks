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

import com.starrocks.sql.ast.expression.Parameter;
import org.antlr.v4.runtime.ParserRuleContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Assigns immutable slots by lexical position and shares the resulting nodes with the binding list.
 * Binding updates each node's expression without replacing its identity or slot.
 */
public final class LexicalParameterContext {
    private final int[] offsets;
    private final List<Parameter> parameters;

    private LexicalParameterContext(int[] offsets) {
        this.offsets = offsets.clone();
        Arrays.sort(this.offsets);
        ArrayList<Parameter> nodes = new ArrayList<>(offsets.length);
        for (int i = 0; i < this.offsets.length; i++) {
            if (i != 0 && this.offsets[i] == this.offsets[i - 1]) {
                throw new IllegalArgumentException("duplicate parameter offset");
            }
            nodes.add(new Parameter(i));
        }
        parameters = Collections.unmodifiableList(nodes);
    }

    static LexicalParameterContext fromOffsets(int[] offsets) {
        return offsets.length == 0 ? null : new LexicalParameterContext(offsets);
    }

    public Parameter parameterAt(int offset) {
        int index = Arrays.binarySearch(offsets, offset);
        if (index < 0) {
            throw new IllegalArgumentException("parameter outside lexical domain: " + offset);
        }
        return parameters.get(index);
    }

    public List<Parameter> parameters() {
        return parameters;
    }

    public static LexicalParameterContext forRule(StarRocksParser parser, ParserRuleContext rule) {
        for (var listener : parser.getParseListeners()) {
            if (listener instanceof PostProcessListener post) {
                return post.parameterContext(rule);
            }
        }
        throw new IllegalStateException("parameter listener not installed");
    }
}
