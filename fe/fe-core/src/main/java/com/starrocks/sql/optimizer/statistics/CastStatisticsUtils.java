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


package com.starrocks.sql.optimizer.statistics;

import com.starrocks.type.ScalarType;
import com.starrocks.type.Type;

final class CastStatisticsUtils {
    private CastStatisticsUtils() {
    }

    static boolean preservesValues(Type source, Type target) {
        if (source.isStringType() && source.getPrimitiveType() == target.getPrimitiveType()) {
            int fromLength = ((ScalarType) source).getLength();
            int toLength = ((ScalarType) target).getLength();
            return source.equals(target) || toLength < 0 || (fromLength > 0 && toLength >= fromLength);
        }
        if (source.isDecimalOfAnyVersion() && target.isDecimalOfAnyVersion()) {
            ScalarType from = (ScalarType) source;
            ScalarType to = (ScalarType) target;
            return from.getScalarPrecision() > 0 && to.getScalarPrecision() > 0
                    && to.getScalarScale() >= from.getScalarScale()
                    && to.getScalarPrecision() - to.getScalarScale()
                        >= from.getScalarPrecision() - from.getScalarScale();
        }
        return source.equals(target)
                || (source.isFixedPointType() && target.isFixedPointType()
                    && target.getTypeSize() >= source.getTypeSize())
                || (source.isFloat() && target.isDouble())
                || (source.isIntegerType() && source.getTypeSize() <= 4 && target.isDouble())
                || (source.isIntegerType() && source.getTypeSize() <= 2 && target.isFloat());
    }
}
