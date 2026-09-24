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

#include <benchmark/benchmark.h>

#include <cstdio>

#include "column/binary_column.h"
#include "column/column_helper.h"
#include "types/time_types.h"
#include "types/timestamp_value.h"

namespace starrocks {

// Benchmark for TimestampValue::from_string with date only
static void BM_TimestampValue_FromString_Date(benchmark::State& state) {
    std::vector<std::string> date_strings = {
            "2024-01-15", "2023-12-31", "2024-02-29", "1990-06-15", "2025-08-20",
            "2024-03-10", "2023-11-25", "2024-07-04", "1988-04-08", "2026-05-17",
    };

    size_t idx = 0;
    size_t items_processed = 0;

    for (auto _ : state) {
        TimestampValue ts;
        const std::string& date_str = date_strings[idx % date_strings.size()];
        bool success = ts.from_string(date_str.c_str(), date_str.size());
        benchmark::DoNotOptimize(success);
        benchmark::DoNotOptimize(ts);
        idx++;
        items_processed++;
    }

    state.SetItemsProcessed(items_processed);
}

// Benchmark for TimestampValue::from_string with datetime (space separator)
static void BM_TimestampValue_FromString_Datetime_Space(benchmark::State& state) {
    std::vector<std::string> datetime_strings = {
            "2024-01-15 10:30:45", "2023-12-31 23:59:59", "2024-02-29 00:00:00", "1990-06-15 12:15:30",
            "2025-08-20 18:45:22", "2024-03-10 06:00:15", "2023-11-25 14:30:00", "2024-07-04 09:15:45",
            "1988-04-08 20:20:20", "2026-05-17 11:11:11",
    };

    size_t idx = 0;
    size_t items_processed = 0;

    for (auto _ : state) {
        TimestampValue ts;
        const std::string& datetime_str = datetime_strings[idx % datetime_strings.size()];
        bool success = ts.from_string(datetime_str.c_str(), datetime_str.size());
        benchmark::DoNotOptimize(success);
        benchmark::DoNotOptimize(ts);
        idx++;
        items_processed++;
    }

    state.SetItemsProcessed(items_processed);
}

// Benchmark for TimestampValue::from_string with datetime (T separator, ISO 8601)
static void BM_TimestampValue_FromString_Datetime_ISO8601(benchmark::State& state) {
    std::vector<std::string> datetime_strings = {
            "2024-01-15T10:30:45", "2023-12-31T23:59:59", "2024-02-29T00:00:00", "1990-06-15T12:15:30",
            "2025-08-20T18:45:22", "2024-03-10T06:00:15", "2023-11-25T14:30:00", "2024-07-04T09:15:45",
            "1988-04-08T20:20:20", "2026-05-17T11:11:11",
    };

    size_t idx = 0;
    size_t items_processed = 0;

    for (auto _ : state) {
        TimestampValue ts;
        const std::string& datetime_str = datetime_strings[idx % datetime_strings.size()];
        bool success = ts.from_string(datetime_str.c_str(), datetime_str.size());
        benchmark::DoNotOptimize(success);
        benchmark::DoNotOptimize(ts);
        idx++;
        items_processed++;
    }

    state.SetItemsProcessed(items_processed);
}

// Benchmark for batch processing (more realistic workload)
static void BM_TimestampValue_FromString_Batch(benchmark::State& state) {
    int batch_size = state.range(0);

    std::vector<std::string> datetime_strings;
    datetime_strings.reserve(batch_size);
    for (int i = 0; i < batch_size; i++) {
        // Mix of date and datetime formats
        if (i % 3 == 0) {
            datetime_strings.push_back("2024-01-15");
        } else if (i % 3 == 1) {
            datetime_strings.push_back("2024-01-15 10:30:45");
        } else {
            datetime_strings.push_back("2024-01-15T10:30:45");
        }
    }

    size_t items_processed = 0;

    for (auto _ : state) {
        for (const auto& datetime_str : datetime_strings) {
            TimestampValue ts;
            bool success = ts.from_string(datetime_str.c_str(), datetime_str.size());
            benchmark::DoNotOptimize(success);
            benchmark::DoNotOptimize(ts);
            items_processed++;
        }
    }

    state.SetItemsProcessed(items_processed);
}

static void BM_TimestampValue_FromString_Fraction(benchmark::State& state) {
    std::vector<std::string> rows;
    for (int i = 0; i < 1024; ++i) {
        char value[32];
        snprintf(value, sizeof(value), "2026-%02d-%02dT%02d:%02d:%02d.%06dZ", 1 + i % 12, 1 + i % 28, i % 24, i % 60,
                 (i * 7) % 60, (i * 971) % 1000000);
        std::string input(value, state.range(0));
        if (state.range(0) == 23) {
            input += 'Z';
        }
        rows.emplace_back(std::move(input));
    }
    for (auto _ : state) {
        for (const auto& input : rows) {
            TimestampValue value;
            bool success = value.from_string(input.data(), input.size());
            benchmark::DoNotOptimize(success);
            benchmark::DoNotOptimize(value);
        }
    }
    state.SetItemsProcessed(state.iterations() * rows.size());
}

BENCHMARK(BM_TimestampValue_FromString_Fraction)->Arg(19)->Arg(23)->Arg(26)->Arg(27);

BENCHMARK(BM_TimestampValue_FromString_Date);
BENCHMARK(BM_TimestampValue_FromString_Datetime_Space);
BENCHMARK(BM_TimestampValue_FromString_Datetime_ISO8601);
BENCHMARK(BM_TimestampValue_FromString_Batch)->Arg(100)->Arg(1000)->Arg(10000);

} // namespace starrocks

BENCHMARK_MAIN();
