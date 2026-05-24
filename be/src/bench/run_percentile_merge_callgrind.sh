#!/usr/bin/env bash
# Build + callgrind the per-row percentile merge to get an exact per-function
# instruction breakdown (no kernel perf perms needed -- works in a container).
# Needs valgrind: apt-get install -y valgrind
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

CXX="${CXX:-g++}"
SHIM=be/src/bench/percentile_merge_scratch_shim
OUT=/tmp/percentile_merge_callgrind
COMPRESSION="${1:-1000}"
PASSES="${2:-20}"
MODE="${3:-0}" # 0=merge(), 1=batch add(), 2=merge_one()

# No benchmark dependency here; pure tdigest. Just need the shim + be/src.
"$CXX" -O2 -g -fno-omit-frame-pointer -std=c++17 -mavx2 -DNDEBUG -static-libstdc++ -static-libgcc \
    -I "$SHIM" -I be/src \
    be/src/bench/percentile_merge_callgrind.cpp be/src/types/tdigest.cpp \
    -o "$OUT"

OUTFILE=/tmp/callgrind.pmm.out
rm -f "$OUTFILE"
valgrind --tool=callgrind --callgrind-out-file="$OUTFILE" --dump-instr=yes \
    "$OUT" "$COMPRESSION" "$PASSES" "$MODE"

echo
echo "===== callgrind_annotate: self instructions per function (mode=$MODE compression=$COMPRESSION) ====="
callgrind_annotate --threshold=98 "$OUTFILE" 2>/dev/null | sed -n '1,45p'
