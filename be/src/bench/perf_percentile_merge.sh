#!/usr/bin/env bash
# perf-profile the per-row percentile merge to see which TDigest function eats
# the time (updateCumulative / mergeProcessed / process / pq setup ...).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

CXX="${CXX:-g++}"
SHIM=be/src/bench/percentile_merge_scratch_shim
OUT=/tmp/percentile_merge_profile_g
DATA=/tmp/perf_pmm.data

TPI=""
for c in "${STARROCKS_THIRDPARTY:-}/installed" "${STARROCKS_THIRDPARTY:-}" \
         /var/local/thirdparty/installed /var/local/thirdparty \
         "${STARROCKS_HOME:-}/thirdparty/installed"; do
    if [ -n "$c" ] && [ -f "$c/include/benchmark/benchmark.h" ]; then TPI="$c"; break; fi
done
[ -n "$TPI" ] || { echo "ERROR: benchmark/benchmark.h not found." >&2; exit 1; }

# -O2 -g -fno-omit-frame-pointer: keep release-ish codegen but with symbols +
# dwarf inline info so perf can attribute inlined TDigest helpers.
"$CXX" -O2 -g -fno-omit-frame-pointer -std=c++17 -mavx2 -DNDEBUG -static-libstdc++ -static-libgcc \
    -I "$SHIM" -I be/src -I"$TPI/include" \
    be/src/bench/percentile_merge_profile_bench.cpp be/src/types/tdigest.cpp \
    -L"$TPI/lib" -L"$TPI/lib64" -lbenchmark -lpthread -lrt -o "$OUT"

# Profile only the per-row merge at compression 1000, run it long enough for
# plenty of samples (30 repetitions).
perf record -F 4000 --call-graph dwarf -o "$DATA" -- \
    "$OUT" --benchmark_filter='^BM_PerMerge/1000$' --benchmark_repetitions=30

echo
echo "================= FLAT self-time (who burns cycles) ================="
perf report -i "$DATA" --stdio --inline -g none 2>/dev/null | grep -v '^#' | head -35
echo
echo "================= call graph (top) ================="
perf report -i "$DATA" --stdio --inline 2>/dev/null | grep -v '^#' | head -50
