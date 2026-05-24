#!/usr/bin/env bash
# Build + run percentile_merge_scratch_bench standalone (no full BE / no JNI).
# Uses the build-only shim under percentile_merge_scratch_shim/ to compile
# types/tdigest.cpp without glog/gutil/Base.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

CXX="${CXX:-g++}"
SHIM=be/src/bench/percentile_merge_scratch_shim
OUT=/tmp/percentile_merge_scratch_bench

# Locate google benchmark: try thirdparty install roots, picking the one that
# actually has benchmark/benchmark.h (STARROCKS_THIRDPARTY may or may not already
# include the trailing "installed").
TPI=""
for c in "${STARROCKS_THIRDPARTY:-}/installed" "${STARROCKS_THIRDPARTY:-}" \
         /var/local/thirdparty/installed /var/local/thirdparty \
         "${STARROCKS_HOME:-}/thirdparty/installed"; do
    if [ -n "$c" ] && [ -f "$c/include/benchmark/benchmark.h" ]; then
        TPI="$c"
        break
    fi
done
if [ -z "$TPI" ]; then
    echo "ERROR: benchmark/benchmark.h not found. Set STARROCKS_THIRDPARTY to the thirdparty install dir." >&2
    exit 1
fi
echo "Using thirdparty: $TPI"

"$CXX" -O3 -std=c++17 -mavx2 -DNDEBUG -static-libstdc++ -static-libgcc \
    -I "$SHIM" -I be/src -I"$TPI/include" \
    be/src/bench/percentile_merge_scratch_bench.cpp be/src/types/tdigest.cpp \
    -L"$TPI/lib" -L"$TPI/lib64" -lbenchmark -lpthread -lrt \
    -o "$OUT"

"$OUT" --benchmark_repetitions=10 --benchmark_report_aggregates_only=true
