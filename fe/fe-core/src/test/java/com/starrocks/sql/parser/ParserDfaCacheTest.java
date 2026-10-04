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

import com.starrocks.common.Config;
import com.starrocks.qe.SessionVariable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParserDfaCacheTest {
    private final long maxStates = Config.parser_dfa_cache_max_states;
    private final boolean contextCache = Config.enable_parser_context_cache;

    @AfterEach
    void restore() {
        Config.parser_dfa_cache_max_states = maxStates;
        Config.enable_parser_context_cache = contextCache;
    }

    private static void parse() {
        SqlParser.parse("CREATE TABLE t (k int, v varchar(10)) DUPLICATE KEY(k) DISTRIBUTED BY HASH(k)",
                new SessionVariable());
        SqlParser.parse("SHOW TABLES FROM db LIKE 'a%'", new SessionVariable());
    }

    @Test
    void clearTheCacheOverTheLimit() {
        Config.parser_dfa_cache_max_states = 1;
        parse();
        assertTrue(ParserDfaCache.parserStates() > 1);
        long clears = ParserDfaCache.clears();
        ParserDfaCache.checkNow();
        assertEquals(clears + 1, ParserDfaCache.clears());
        assertEquals(0, ParserDfaCache.parserStates());
        assertEquals(0, ParserDfaCache.parserContexts());
        // The parser fills the new cache.
        parse();
        assertTrue(ParserDfaCache.parserStates() > 0);
    }

    @Test
    void keepTheCacheWithoutALimit() {
        Config.parser_dfa_cache_max_states = 0;
        parse();
        long states = ParserDfaCache.parserStates();
        long clears = ParserDfaCache.clears();
        ParserDfaCache.checkNow();
        assertEquals(clears, ParserDfaCache.clears());
        assertEquals(states, ParserDfaCache.parserStates());
    }

    @Test
    void privateCacheLeavesTheSharedCache() {
        Config.parser_dfa_cache_max_states = 1;
        parse();
        ParserDfaCache.checkNow();
        Config.enable_parser_context_cache = false;
        parse();
        assertEquals(0, ParserDfaCache.parserStates());
    }
}
