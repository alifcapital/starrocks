#!/usr/bin/env bash
# Run one stage on the builder; each stage preserves its log and exit status.
set -euo pipefail
stage=${1:?Usage: build-stage.sh release|asan|ut|fe-tests|java-tests}
container=${SR_BUILD_CONTAINER:-starrocks-4.1-ubuntu24}
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
mkdir -p "$HOME/build-logs"
case "$stage" in
    release) command='./build.sh --be --fe --enable-shared-data -j$(nproc)' ;;
    asan) command='BUILD_TYPE=ASAN ./build.sh --be --enable-shared-data -j$(nproc)' ;;
    ut) command='./run-be-ut.sh --enable-shared-data --without-java-ext -j$(nproc) --gtest_filter="AggregateTest.*:HashMapTest.*:ChunkTest.*:StarCacheEngineTest.*:BlockCacheTest.*:NewFsStarletTest.*"' ;;
    fe-tests) command='CUSTOM_MVN="mvn -Dmaven.clean.skip=true" ./run-fe-ut.sh -j4 --test "com.starrocks.proc.FrontendsProcNodeTest,com.starrocks.service.FrontendOptionsTest,com.starrocks.system.SystemInfoServiceTest,com.starrocks.service.GroovyUDSServerTest,com.starrocks.sql.analyzer.CreateFunctionStmtAnalyzerTest,com.starrocks.sql.plan.UDFTest"' ;;
    java-tests) command='cd java-extensions && mvn -B -pl udf-extensions -am test -DfailIfNoTests=false -Dsurefire.failIfNoSpecifiedTests=false' ;;
    *) echo "Unknown stage: $stage" >&2; exit 2 ;;
esac
echo "Running $stage; log: $HOME/build-logs/$stage.log"
rm -f "$HOME/build-logs/$stage.rc"
set +e
docker exec "$container" bash -c "ulimit -n 524288; $command" >"$HOME/build-logs/$stage.log" 2>&1
rc=$?
set -e
printf '\nFINAL_RC=%s\n' "$rc" | tee -a "$HOME/build-logs/$stage.log"
printf '%s\n' "$rc" >"$HOME/build-logs/$stage.rc"
exit "$rc"
