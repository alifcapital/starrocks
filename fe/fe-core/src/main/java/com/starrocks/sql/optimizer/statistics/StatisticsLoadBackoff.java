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

import com.github.benmanes.caffeine.cache.Ticker;

import java.util.concurrent.TimeUnit;

/** Pauses query-driven retries during an outage without caching a failed read as absence. */
final class StatisticsLoadBackoff {
    private final Ticker ticker;
    private volatile Long failedAt;

    StatisticsLoadBackoff() {
        this(Ticker.systemTicker());
    }

    StatisticsLoadBackoff(Ticker ticker) {
        this.ticker = ticker;
    }

    void failed() {
        failedAt = ticker.read();
    }

    boolean active() {
        Long failure = failedAt;
        return failure != null && ticker.read() - failure < TimeUnit.SECONDS.toNanos(60);
    }
}
