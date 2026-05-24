#!/usr/bin/env bash
# Build + run percentile_merge_scratch_bench standalone (no full BE / no JNI).
# Uses the build-only shim under percentile_merge_scratch_shim/ to compile
# types/tdigest.cpp without glog/gutil/Base.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

TP="${STARROCKS_THIRDPARTY:-/var/local/thirdparty/installed}"
CXX="${CXX:-g++}"
SHIM=be/src/bench/percentile_merge_scratch_shim
OUT=/tmp/percentile_merge_scratch_bench

"$CXX" -O3 -std=c++17 -mavx2 -DNDEBUG -static-libstdc++ -static-libgcc \
    -I "$SHIM" -I be/src \
    be/src/bench/percentile_merge_scratch_bench.cpp be/src/types/tdigest.cpp \
    -I"$TP/include" -L"$TP/lib" -L"$TP/lib64" -lbenchmark -lpthread -lrt \
    -o "$OUT"

"$OUT" --benchmark_repetitions=10 --benchmark_report_aggregates_only=true
