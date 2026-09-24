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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdio>
#include <cstring>
#include <memory>

#define private public

#include "types/date_value.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"

namespace starrocks {

TEST(TimestampValueTest, normal) {
    LOG(INFO) << "MAX: " << timestamp::from_julian_and_time(date::MAX_DATE, 86400 * USECS_PER_SEC - 1);
    LOG(INFO) << "MIN: " << timestamp::from_julian_and_time(date::MIN_DATE, 0);

    {
        auto v = TimestampValue::create(2004, 1, 1, 18, 30, 30);

        LOG(INFO) << "UNIX SECONDS: " << v.to_unix_second();

        TimestampValue a;
        a.from_unix_second(v.to_unix_second());
        LOG(INFO) << "UNIX TIMESTAMP: " << a;
        ASSERT_EQ(20040101183030, v.to_timestamp_literal());
        ASSERT_EQ("2004-01-01 18:30:30", v.to_string());
    }

    {
        auto v = TimestampValue::create(1004, 1, 1, 18, 30, 30);

        LOG(INFO) << "UNIX SECONDS: " << v.to_unix_second();

        TimestampValue a;
        a.from_unix_second(v.to_unix_second());
        LOG(INFO) << "UNIX TIMESTAMP: " << a;
    }
}

TEST(TimestampValueTest, toString) {
    {
        auto v = TimestampValue::create(2004, 1, 1, 18, 30, 30);
        ASSERT_EQ("2004-01-01 18:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30);
        ASSERT_EQ("2004-02-29 23:30:30", v.to_string());
    }
}

TEST(TimestampValueTest, calculate) {
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::SECOND>(30);
        ASSERT_EQ("2004-02-29 23:31:00", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::MINUTE>(30);
        ASSERT_EQ("2004-03-01 00:00:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::HOUR>(1);
        ASSERT_EQ("2004-03-01 00:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::DAY>(30);
        ASSERT_EQ("2004-03-30 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::DAY>(365);
        ASSERT_EQ("2005-02-28 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 3, 29, 23, 30, 30).add<TimeUnit::DAY>(365);
        ASSERT_EQ("2005-03-29 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::DAY>(-365);
        ASSERT_EQ("2003-03-01 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::YEAR>(1);
        ASSERT_EQ("2005-02-28 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::YEAR>(8);
        ASSERT_EQ("2012-02-29 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::MONTH>(8);
        ASSERT_EQ("2004-10-29 23:30:30", v.to_string());
    }
    {
        auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30).add<TimeUnit::MONTH>(13);
        ASSERT_EQ("2005-03-29 23:30:30", v.to_string());
    }
}

TEST(TimestampValueTest, cast) {
    auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30);
    DateValue date = (DateValue)v;
    ASSERT_EQ("2004-02-29", date.to_string());
    ASSERT_EQ(1078012800000, date.to_unixtime());
}

TEST(TimestampValueTest, unixTime) {
    auto v = TimestampValue::create(2004, 2, 29, 23, 30, 30);
    ASSERT_EQ(1078097430000, v.to_unixtime());
    ASSERT_EQ(1078097430000, v.to_unixtime(cctz::utc_time_zone()));
}

TEST(TimestampValueTest, from_uncommon_format_str_microsecond) {
    // Test that from_uncommon_format_str preserves microseconds parsed from %f
    {
        TimestampValue ts;
        std::string format = "%Y-%m-%dT%H:%i:%s.%f";
        std::string value = "2026-02-09T00:15:01.535569";
        bool result = ts.from_uncommon_format_str(format.c_str(), format.size(), value.c_str(), value.size());
        ASSERT_TRUE(result);
        EXPECT_EQ("2026-02-09 00:15:01.535569", ts.to_string());
    }
    // Test with fewer than 6 fractional digits
    {
        TimestampValue ts;
        std::string format = "%Y-%m-%d %H:%i:%s.%f";
        std::string value = "2026-02-09 12:30:45.123";
        bool result = ts.from_uncommon_format_str(format.c_str(), format.size(), value.c_str(), value.size());
        ASSERT_TRUE(result);
        EXPECT_EQ("2026-02-09 12:30:45.123000", ts.to_string());
    }
    // Test with zero microseconds
    {
        TimestampValue ts;
        std::string format = "%Y-%m-%d %H:%i:%s.%f";
        std::string value = "2026-02-09 12:30:45.000000";
        bool result = ts.from_uncommon_format_str(format.c_str(), format.size(), value.c_str(), value.size());
        ASSERT_TRUE(result);
        EXPECT_EQ("2026-02-09 12:30:45", ts.to_string());
    }
}

TEST(TimestampValueTest, from_date_format_str_rejects_zero_day_or_month) {
    TimestampValue ts;
    const char* fmt = "%Y-%m-%d";
    ASSERT_FALSE(ts.from_date_format_str("0000-01-00", 10, fmt));
    ASSERT_FALSE(ts.from_date_format_str("0000-00-01", 10, fmt));
    ASSERT_TRUE(ts.from_date_format_str("2020-01-01", 10, fmt));

    TimestampValue ts2;
    const char* dt_fmt = "%Y-%m-%d %H:%i:%s";
    ASSERT_FALSE(ts2.from_datetime_format_str("0000-01-00 00:00:00", 19, dt_fmt));
    ASSERT_TRUE(ts2.from_datetime_format_str("2020-01-01 00:00:00", 19, dt_fmt));
}

TEST(TimestampValueTest, fixed_datetime_simd) {
    for (int i = 0; i < 1000; ++i) {
        const int year = 1900 + i % 200;
        const int month = 1 + i % 12;
        const int day = 1 + i % 28;
        const int hour = i % 24;
        const int minute = (i * 7) % 60;
        const int second = (i * 13) % 60;
        const int usec = (i * 971) % 1000000;
        char buffer[32];
        snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ", year, month, day, hour, minute, second,
                 usec);
        for (size_t length : {19, 26, 27}) {
            for (char separator : {'T', ' '}) {
                buffer[10] = separator;
                // An exact allocation makes SIMD overreads visible to ASAN.
                auto input = std::make_unique<char[]>(length);
                memcpy(input.get(), buffer, length);
                TimestampValue value;
                ASSERT_TRUE(value.from_string(input.get(), length));
                const auto expected =
                        TimestampValue::create(year, month, day, hour, minute, second, length == 19 ? 0 : usec);
                ASSERT_EQ(expected.timestamp(), value.timestamp());
            }
        }
    }
}

TEST(TimestampValueTest, fixed_datetime_compatibility) {
    const std::pair<std::string, std::string> cases[] = {
            {"2026-09-22T17:26:26.679658Z", "2026-09-22 17:26:26.679658"},
            {"2023-12-25 12", "2023-12-25 12:00:00"},
            {"2023-12-25 12:34", "2023-12-25 12:34:00"},
            {"2026-09-22T17:26:26.1Z", "2026-09-22 17:26:26.100000"},
            {"2026-09-22T17:26:26.123Z", "2026-09-22 17:26:26.123000"},
            {"2026-09-22T17:26:26.1234567Z", "2026-09-22 17:26:26.123456"},
            {" 2026-09-22T17:26:26.679658Z\t", "2026-09-22 17:26:26.679658"},
            {"2026/09/22 17:26:26", "2026-09-22 17:26:26"},
            {"2000-02-29T23:59:59.999999Z", "2000-02-29 23:59:59.999999"},
    };
    for (const auto& [input, expected] : cases) {
        TimestampValue value;
        ASSERT_TRUE(value.from_string(input.data(), input.size())) << input;
        ASSERT_EQ(expected, value.to_string()) << input;
    }
    for (std::string input :
         {"2024-01-01 01:61:00", "2024-01-01 24:00:00", "2024-01-01 00:00:60", "1900-02-29T00:00:00.000000Z",
          "2026-00-22T17:26:26.679658Z", "2026-09-00T17:26:26.679658Z", "2026-09-31T17:26:26.679658Z"}) {
        TimestampValue value;
        ASSERT_FALSE(value.from_string(input.data(), input.size())) << input;
    }
}

TEST(TimestampValueTest, fixed_datetime_date_cache_boundaries) {
    for (int year : {0, 1, 1582, 1900, 1989, 1990, 2000, 2049, 2050, 9999}) {
        for (int month = 1; month <= 12; ++month) {
            for (int day : {1, static_cast<int>(DAYS_IN_MONTH[date::is_leap(year)][month])}) {
                char buffer[32];
                snprintf(buffer, sizeof(buffer), "%04d-%02d-%02dT23:59:59.999999Z", year, month, day);
                for (size_t length : {19, 26, 27}) {
                    TimestampValue value;
                    ASSERT_TRUE(value.from_string(buffer, length));
                    const auto expected =
                            TimestampValue::create(year, month, day, 23, 59, 59, length == 19 ? 0 : 999999);
                    ASSERT_EQ(expected.timestamp(), value.timestamp()) << std::string(buffer, length);
                }
            }
        }
    }
}

} // namespace starrocks
