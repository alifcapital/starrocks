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

import org.antlr.v4.runtime.atn.ATN;

final class QueryIdentifiers {
    private QueryIdentifiers() {}

    /** Token types that the grammar rule identifier accepts, taken from the generated parser. */
    static boolean[] flags() {
        boolean[] flags = new boolean[FrozenTokenCatalog.MAX_LEXER_TOKEN_TYPE + 1];
        ATN atn = StarRocksParser._ATN;
        for (int type : atn.nextTokens(atn.ruleToStartState[StarRocksParser.RULE_identifier]).toArray()) {
            if (type >= 0 && type < flags.length) {
                flags[type] = true;
            }
        }
        return flags;
    }

    static String decodeBackQuoted(String token) {
        return decodeBackQuotedInterior(token.substring(1, token.length() - 1));
    }

    static String decodeBackQuotedInterior(String interior) {
        return interior.indexOf('`') < 0 ? interior : interior.replace("``", "`");
    }
}
