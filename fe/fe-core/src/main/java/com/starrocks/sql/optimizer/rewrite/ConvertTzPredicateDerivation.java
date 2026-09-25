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

package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.zone.ZoneOffsetTransition;
import java.util.Optional;

/** Necessary input bounds for comparisons over convert_tz; the original filter must remain. */
public final class ConvertTzPredicateDerivation {
    // Each offset is within +/-18 hours. Only input clocks within 36 hours of the
    // target clock can cross the comparison boundary. Their possible instants lie
    // within 72 hours of the boundary instant, including gap/overlap interpretations.
    private static final long TRANSITION_WINDOW_SECONDS = 4L * ZoneOffset.MAX.getTotalSeconds();

    private ConvertTzPredicateDerivation() {
    }

    public static Optional<ScalarOperator> invert(CallOperator call, ScalarOperator dataChild,
                                                  BinaryType comparison, ConstantOperator value) {
        if (call.getChildren().size() != 3 || !call.getType().isDatetime()
                || !dataChild.getType().isDatetime() || !value.getType().isDateType() || value.isNull()
                || comparison == BinaryType.NE) {
            return Optional.empty();
        }
        try {
            ZoneId from = constantZone(call.getChild(1));
            ZoneId to = constantZone(call.getChild(2));
            if (from == null || to == null) {
                return Optional.empty();
            }
            LocalDateTime target = value.getDatetime();
            if (to.getRules().getValidOffsets(target).size() != 1) {
                return Optional.empty();
            }
            Instant instant = target.atZone(to).toInstant();
            LocalDateTime source = LocalDateTime.ofInstant(instant, from);
            if (from.getRules().getValidOffsets(source).size() != 1
                    || !transitionFree(from, instant) || !transitionFree(to, instant)
                    || source.isBefore(ConstantOperator.MIN_DATETIME)
                    || source.isAfter(ConstantOperator.MAX_DATETIME)) {
                return Optional.empty();
            }
            // A far-away transition cannot cross this boundary. Around it both zones
            // have one constant offset, so reversing the shift preserves strictness
            // and microseconds. NULL/overflow behavior stays in the original filter.
            return Optional.of(new BinaryPredicateOperator(comparison, dataChild,
                    ConstantOperator.createDatetime(source)));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private static ZoneId constantZone(ScalarOperator argument) {
        if (!(argument instanceof ConstantOperator constant) || constant.isNull()
                || !constant.getType().isStringType()) {
            return null;
        }
        String name = constant.getVarchar();
        // BE interprets CST as a fixed UTC+8 offset, not America's Central time.
        if (name.equals("CST")) {
            return ZoneOffset.ofHours(8);
        }
        return ZoneId.of(name);
    }

    private static boolean transitionFree(ZoneId zone, Instant boundary) {
        Instant lower = boundary.minusSeconds(TRANSITION_WINDOW_SECONDS);
        Instant upper = boundary.plusSeconds(TRANSITION_WINDOW_SECONDS);
        ZoneOffsetTransition next = zone.getRules().nextTransition(lower.minusNanos(1));
        return next == null || next.getInstant().isAfter(upper);
    }
}
