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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/timezone_utils.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

#pragma once

#include <re2/re2.h>

#include <string_view>
#include <chrono>
#include <limits>
#include <cstdint>

#include "cctz/time_zone.h"
#include "util/timezone_hsscan.h"

namespace starrocks {

// Forward declarations - only use basic types to avoid conflicts
typedef int32_t JulianDate;
typedef int64_t Timestamp;

// Forward declare necessary classes without defining their contents
class date;
class timestamp;

// Constant needed for timestamp calculations
constexpr JulianDate UNIX_EPOCH_JULIAN = 2440588;

// Forward declare functions we need without using namespaces
// We'll implement these via external linkage
int64_t ts_to_julian(Timestamp ts);
int64_t ts_to_time(Timestamp ts);
Timestamp ts_add_seconds(Timestamp ts, int seconds);

/**
 * Utility class for timezone operations.
 *
 * Provides functionality for:
 * 1. Finding and loading timezone definitions
 * 2. Calculating timezone offsets for timestamps
 * 3. Finding timezone transitions (changes in offset)
 *
 * This implementation supports the full timeline, including dates before 1970 (Unix epoch)
 * and handles all timezone transitions defined in the IANA timezone database.
 */
class TimezoneUtils {
public:
    static bool find_cctz_time_zone(const TimezoneHsScan& timezone_hsscan, std::string_view timezone,
                                    cctz::time_zone& ctz);
    static void init_time_zones();
    static bool find_cctz_time_zone(std::string_view timezone, cctz::time_zone& ctz);
    static bool timezone_offsets(std::string_view src, std::string_view dst, int64_t* offset);
    static int64_t to_utc_offset(const cctz::time_zone& ctz); // timezone offset in seconds.
    static cctz::time_zone local_time_zone();

    // Get timezone offset at a specific epoch timestamp (supports both positive and negative timestamps)
    static int get_offset_at_timestamp(const cctz::time_zone& ctz, int64_t seconds_since_epoch);

    // Get timezone offset at a specific timestamp
    static int get_offset_for_timestamp(const cctz::time_zone& ctz, Timestamp timestamp);

    // Get timezone offset for date/time components
    static int get_offset_for_date_time(const cctz::time_zone& ctz, int year, int month, int day,
                                       int hour, int minute, int second);

    // Find the next timezone transition from a given epoch timestamp
    // Returns true if a transition was found, false otherwise
    // If a transition is found, next_transition_seconds and next_offset will be set
    static bool find_next_transition(const cctz::time_zone& ctz, int64_t seconds_since_epoch,
                                    int64_t* next_transition_seconds, int* next_offset);

    // Find the previous timezone transition from a given epoch timestamp
    // Returns true if a transition was found, false otherwise
    // If a transition is found, prev_transition_seconds and prev_offset will be set
    // Supports negative timestamps for dates before 1970
    static bool find_prev_transition(const cctz::time_zone& ctz, int64_t seconds_since_epoch,
                                    int64_t* prev_transition_seconds, int* prev_offset);

public:
    static const std::string default_time_zone;

private:
    static bool _match_cctz_time_zone(std::string_view timezone, cctz::time_zone& ctz);
};

/**
 * Helper class to cache timezone offset and transition information.
 *
 * This class reduces redundant timezone lookup operations by caching the most
 * recent timezone transition and offset information. It handles both pre-1970
 * and post-1970 timestamps efficiently.
 */
class TimezoneOffsetCache {
public:
    /**
     * Initialize a new timezone offset cache with the given timezone.
     *
     * @param timezone The cctz timezone to use for offset calculations
     */
    explicit TimezoneOffsetCache(const cctz::time_zone& timezone)
        : _ctz(timezone),
          _prev_transition_seconds(std::numeric_limits<int64_t>::min()),
          _next_transition_seconds(std::numeric_limits<int64_t>::max()),
          _current_offset(0) {}

    /**
     * Get the offset in seconds for the given timestamp.
     *
     * This method caches transition information to avoid redundant lookups.
     * It handles both pre-1970 and post-1970 timestamps correctly.
     *
     * @param seconds_since_epoch The time in seconds since the Unix epoch
     * @return The timezone offset in seconds
     */
    int get_offset_for_seconds(int64_t seconds_since_epoch);

    /**
     * Get the offset in seconds for the given timestamp.
     *
     * This is a convenience method that converts Timestamp to seconds.
     *
     * @param timestamp The timestamp to get the offset for
     * @return The timezone offset in seconds
     */
    int get_offset_for_timestamp(Timestamp timestamp);

    /**
     * Apply the timezone offset to convert a UTC timestamp to local time.
     *
     * @param timestamp The UTC timestamp to convert
     * @return The local timestamp with timezone offset applied
     */
    Timestamp utc_to_local(Timestamp timestamp);

private:
    cctz::time_zone _ctz;
    int64_t _prev_transition_seconds;  // Last transition before current cache window
    int64_t _next_transition_seconds;  // First transition after current cache window
    int _current_offset;               // Current timezone offset in seconds
};

} // namespace starrocks
