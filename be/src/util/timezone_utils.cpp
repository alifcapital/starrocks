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
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/timezone_utils.cpp

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

#include "util/timezone_utils.h"

#include <cctz/time_zone.h>

#include <charconv>
#include <string_view>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "runtime/time_types.h"
#include "util/phmap/phmap.h"

namespace starrocks {


const std::string TimezoneUtils::default_time_zone = "+08:00";

static phmap::flat_hash_map<std::string_view, cctz::time_zone> _s_cached_timezone;
static phmap::flat_hash_map<std::pair<std::string_view, std::string_view>, int64_t> _s_cached_offsets;

cctz::time_zone TimezoneUtils::local_time_zone() {
    return cctz::local_time_zone();
}

void TimezoneUtils::init_time_zones() {
    // timezone cache list
    // We cannot add a time zone to the cache that contains both daylight saving time and winter time
    static std::vector<std::string> timezones = {
#include "timezone.dat"
    };
    for (const auto& timezone : timezones) {
        cctz::time_zone ctz;
        if (cctz::load_time_zone(timezone, &ctz)) {
            _s_cached_timezone.emplace(timezone, ctz);
        } else {
            LOG(WARNING) << "not found timezone:" << timezone;
        }
    }
    auto civil = cctz::civil_second(2021, 12, 1, 8, 30, 1);
    for (const auto& [timezone1, _] : _s_cached_timezone) {
        for (const auto& [timezone2, _] : _s_cached_timezone) {
            const auto tp1 = cctz::convert(civil, _s_cached_timezone[timezone1]);
            const auto tp2 = cctz::convert(civil, _s_cached_timezone[timezone2]);
            std::pair<std::string_view, std::string_view> key = {timezone1, timezone2};
            _s_cached_offsets[key] = tp1.time_since_epoch().count() - tp2.time_since_epoch().count();
        }
    }

    // other cached timezone
    // only cache CCTZ, won't caculate offsets
    static std::vector<std::string> other_timezones = {
#include "othertimezone.dat"
    };
    for (const auto& timezone : other_timezones) {
        cctz::time_zone ctz;
        if (cctz::load_time_zone(timezone, &ctz)) {
            _s_cached_timezone.emplace(timezone, ctz);
        } else {
            LOG(WARNING) << "not found timezone:" << timezone;
        }
    }
}

// _match_cctz_time_zone use regular expressions to match timezone.
bool TimezoneUtils::_match_cctz_time_zone(std::string_view timezone, cctz::time_zone& ctz) {
    re2::StringPiece value;

    // RE2 obj is thread safe
    // +8:00
    static RE2 reg1(R"(^[+-]{1}\d{2}\:\d{2}$)", re2::RE2::Quiet);
    // GMT+8:00
    static RE2 reg2(R"(^GMT[+-]{1}\d{2}\:\d{2}$)", re2::RE2::Quiet);

    if (reg1.Match(re2::StringPiece(timezone.data(), timezone.size()), 0, timezone.size(), RE2::UNANCHORED, &value,
                   1)) {
        // Do nothing.
    } else if (reg2.Match(re2::StringPiece(timezone.data(), timezone.size()), 0, timezone.size(), RE2::UNANCHORED,
                          &value, 1)) {
        // remove GMT header.
        value = value.substr(3);
    } else {
        // all regular expressions return empty.
        return false;
    }

    bool positive = (value[0] != '-');

    //Regular expression guarantees hour and minute mush be int
    int hour = std::stoi(value.substr(1, 2).as_string());
    int minute = std::stoi(value.substr(4, 2).as_string());

    // timezone offsets around the world extended from -12:00 to +14:00
    if (!positive && hour > 12) {
        return false;
    } else if (positive && hour > 14) {
        return false;
    }
    int offset = hour * 60 * 60 + minute * 60;
    offset *= positive ? 1 : -1;
    ctz = cctz::fixed_time_zone(cctz::seconds(offset));
    return true;
}

bool TimezoneUtils::find_cctz_time_zone(std::string_view timezone, cctz::time_zone& ctz) {
    re2::StringPiece value;
    if (auto iter = _s_cached_timezone.find(timezone); iter != _s_cached_timezone.end()) {
        ctz = iter->second;
        return true;
    } else if (_match_cctz_time_zone(timezone, ctz)) {
        return true;
    } else if (timezone == "CST") {
        // Supports offset and region timezone type, "CST" use here is compatibility purposes.
        ctz = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));
        return true;
    } else {
        return cctz::load_time_zone(std::string(timezone), &ctz);
    }
}

bool TimezoneUtils::timezone_offsets(std::string_view src, std::string_view dst, int64_t* offset) {
    if (const auto iter = _s_cached_offsets.find(std::make_pair(src, dst)); iter != _s_cached_offsets.end()) {
        *offset = iter->second;
        return true;
    }
    return false;
}

bool TimezoneUtils::find_cctz_time_zone(const TimezoneHsScan& timezone_hsscan, std::string_view timezone,
                                        cctz::time_zone& ctz) {
    // find time_zone by cache
    if (auto iter = _s_cached_timezone.find(timezone); iter != _s_cached_timezone.end()) {
        ctz = iter->second;
        return true;
    }

    bool v = false;
    hs_scan(
            timezone_hsscan.database, timezone.data(), timezone.size(), 0, timezone_hsscan.scratch,
            [](unsigned int id, unsigned long long from, unsigned long long to, unsigned int flags, void* ctx) -> int {
                *((bool*)ctx) = true;
                return 1;
            },
            &v);

    if (v) {
        bool positive = (timezone.substr(0, 1) != "-");

        //Regular expression guarantees hour and minute mush be int
        int hour = 0;
        std::string_view hour_str = timezone.substr(1, 2);
        std::from_chars(hour_str.begin(), hour_str.end(), hour);
        int minute = 0;
        std::string_view minute_str = timezone.substr(4, 5);
        std::from_chars(minute_str.begin(), minute_str.end(), minute);

        // timezone offsets around the world extended from -12:00 to +14:00
        if (!positive && hour > 12) {
            return false;
        } else if (positive && hour > 14) {
            return false;
        }
        int offset = hour * 60 * 60 + minute * 60;
        offset *= positive ? 1 : -1;
        ctz = cctz::fixed_time_zone(cctz::seconds(offset));
        return true;
    }

    if (timezone == "CST") {
        // Supports offset and region timezone type, "CST" use here is compatibility purposes.
        ctz = cctz::fixed_time_zone(cctz::seconds(8 * 60 * 60));
        return true;
    } else {
        return cctz::load_time_zone(std::string(timezone), &ctz);
    }
}

int64_t TimezoneUtils::to_utc_offset(const cctz::time_zone& ctz) {
    cctz::time_zone utc = cctz::utc_time_zone();
    const std::chrono::time_point<std::chrono::system_clock> tp;
    const cctz::time_zone::absolute_lookup a = ctz.lookup(tp);
    const cctz::time_zone::absolute_lookup b = utc.lookup(tp);
    return a.offset - b.offset;
}

int TimezoneUtils::get_offset_at_timestamp(const cctz::time_zone& ctz, int64_t seconds_since_epoch) {
    // Handle both positive (post-1970) and negative (pre-1970) timestamps
    std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(seconds_since_epoch);
    return ctz.lookup(tp).offset;
}

int TimezoneUtils::get_offset_for_date_time(const cctz::time_zone& ctz, int year, int month, int day,
                                          int hour, int minute, int second) {
    cctz::civil_second cs(year, month, day, hour, minute, second);
    auto tp = cctz::convert(cs, ctz);
    return ctz.lookup(tp).offset;
}

int TimezoneOffsetCache::get_offset_for_seconds(int64_t seconds_since_epoch) {
    // 1. Check if the timestamp is within our cached range
    if (seconds_since_epoch >= _prev_transition_seconds && seconds_since_epoch < _next_transition_seconds) {
        return _current_offset;
    }

    // 2. Not in cache boundaries - need to update
    std::chrono::system_clock::time_point tp = std::chrono::system_clock::from_time_t(seconds_since_epoch);

    // Get current offset
    const auto lookup = _ctz.lookup(tp);
    _current_offset = lookup.offset;

    // Find previous transition
    cctz::time_zone::civil_transition prev_transition;
    bool has_prev_transition = _ctz.prev_transition(tp, &prev_transition);

    if (has_prev_transition) {
        // Convert the civil time to time_point to get the exact transition time
        auto transition_tp = cctz::convert(prev_transition.from, _ctz);
        // Get seconds since epoch
        _prev_transition_seconds = std::chrono::system_clock::to_time_t(transition_tp);
    } else {
        // No more previous transitions, use minimum value
        _prev_transition_seconds = std::numeric_limits<int64_t>::min();
    }

    // Find next transition
    cctz::time_zone::civil_transition next_transition;
    bool has_next_transition = _ctz.next_transition(tp, &next_transition);

    if (has_next_transition) {
        // Convert the civil time to time_point to get the exact transition time
        auto transition_tp = cctz::convert(next_transition.to, _ctz);
        // Get seconds since epoch
        _next_transition_seconds = std::chrono::system_clock::to_time_t(transition_tp);
    } else {
        // No more transitions, use maximum value
        _next_transition_seconds = std::numeric_limits<int64_t>::max();
    }

    return _current_offset;
}

int TimezoneOffsetCache::get_offset_for_timestamp(Timestamp timestamp) {
    int64_t days = timestamp::to_julian(timestamp);
    int64_t microseconds = timestamp::to_time(timestamp);
    int64_t seconds_since_epoch = (days - date::UNIX_EPOCH_JULIAN) * SECS_PER_DAY + microseconds / USECS_PER_SEC;

    return get_offset_for_seconds(seconds_since_epoch);
}

Timestamp TimezoneOffsetCache::utc_to_local(Timestamp timestamp) {
    int offset = get_offset_for_timestamp(timestamp);

    return timestamp::add<TimeUnit::SECOND>(timestamp, offset);
}

} // namespace starrocks
