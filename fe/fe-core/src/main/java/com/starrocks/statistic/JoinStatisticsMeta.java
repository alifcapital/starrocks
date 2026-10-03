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

package com.starrocks.statistic;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Writable;

/** Metadata publication is the commit point for all pieces of a collected generation. */
public final class JoinStatisticsMeta implements Writable {
    @SerializedName("id")
    private final long id;
    @SerializedName("definition")
    private final JoinStatisticsDefinition definition;
    @SerializedName("generation")
    private final long generation;
    @SerializedName("parts")
    private final int parts;
    @SerializedName("payloadBytes")
    private final long payloadBytes;
    @SerializedName("checksum")
    private final String checksum;
    @SerializedName("collectedAt")
    private final long collectedAt;

    public JoinStatisticsMeta(long id, JoinStatisticsDefinition definition) {
        this(id, definition, 0, 0, 0, "", 0);
    }

    public JoinStatisticsMeta(long id, JoinStatisticsDefinition definition, long generation, int parts,
                              long payloadBytes, String checksum, long collectedAt) {
        if (id <= 0 || definition == null || generation < 0 || parts < 0 || payloadBytes < 0 || collectedAt < 0
                || (generation == 0) != (parts == 0) || (parts == 0) != (payloadBytes == 0)
                || checksum == null) {
            throw new IllegalArgumentException("Invalid JOIN statistics metadata");
        }
        this.id = id;
        this.definition = definition;
        this.generation = generation;
        this.parts = parts;
        this.payloadBytes = payloadBytes;
        this.checksum = checksum;
        this.collectedAt = collectedAt;
    }

    public long getId() {
        return id;
    }

    public JoinStatisticsDefinition getDefinition() {
        return definition;
    }

    public long getGeneration() {
        return generation;
    }

    public int getParts() {
        return parts;
    }

    public long getPayloadBytes() {
        return payloadBytes;
    }

    public String getChecksum() {
        return checksum;
    }

    public long getCollectedAt() {
        return collectedAt;
    }
}
