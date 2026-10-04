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

package com.starrocks.sql.optimizer.operator;

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OperatorBuilderFactory {
    // Planner threads of concurrent queries read and fill the cache at the same time, so it must be thread safe.
    private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_MAP = new ConcurrentHashMap<>();

    public static <T extends Operator.Builder> T build(Operator operator) {
        try {
            Constructor<?> c = CONSTRUCTOR_MAP.get(operator.getClass());
            if (c == null) {
                c = Class.forName(operator.getClass().getName() + "$Builder").getConstructor();
                CONSTRUCTOR_MAP.put(operator.getClass(), c);
            }
            return (T) c.newInstance();
        } catch (Exception e) {
            throw new StarRocksPlannerException("not implement builder: " + operator.getOpType(),
                    ErrorType.INTERNAL_ERROR, e);
        }
    }
}
