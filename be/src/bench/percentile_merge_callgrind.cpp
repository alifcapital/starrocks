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

// Deterministic callgrind target for dissecting per-row percentile merge.
// No benchmark harness -- just N varied partials merged into a target, repeated
// `passes` times. Run under callgrind (no kernel perf perms needed):
//   valgrind --tool=callgrind ./percentile_merge_callgrind <compression> <passes> <mode>
//   callgrind_annotate callgrind.out.* | head -40
// mode: 0 = per-row merge() (current), 1 = batch add(), 2 = per-row merge_one().

#include <cstdlib>
#include <memory>
#include <random>
#include <vector>

#include "types/tdigest.h"

using namespace starrocks;

int main(int argc, char** argv) {
    const double c = (argc > 1) ? atof(argv[1]) : 1000.0;
    const int passes = (argc > 2) ? atoi(argv[2]) : 20;
    const int mode = (argc > 3) ? atoi(argv[3]) : 0;
    const int N = 4096;

    std::mt19937_64 rng(0x9E3779B97F4A7C15ull);
    std::normal_distribution<double> dist(100.0, 30.0);
    std::vector<std::unique_ptr<TDigest>> parts;
    std::vector<const TDigest*> ptrs;
    for (int i = 0; i < N; ++i) {
        auto t = std::make_unique<TDigest>(c);
        t->add(static_cast<float>(dist(rng)));
        ptrs.push_back(t.get());
        parts.push_back(std::move(t));
    }

    volatile double sink = 0;
    for (int r = 0; r < passes; ++r) {
        TDigest target(c);
        if (mode == 1) {
            target.add(ptrs.cbegin(), ptrs.cend());
        } else if (mode == 2) {
            for (auto* p : ptrs) target.merge_one(p);
        } else {
            for (auto* p : ptrs) target.merge(p);
        }
        sink += target.quantile(0.5);
    }
    return static_cast<int>(sink) & 1;
}
