#!/usr/bin/env bash
# Start a builder with persistent source, ccache and Maven mounts. The host user database is mounted
# read-only: we fear that Hadoop and the JVM reject a uid without a passwd entry, which fails HDFS, Iceberg
# and JDBC unit tests.
set -euo pipefail
image=${1:?Usage: container.sh IMAGE [CONTAINER_NAME]}
name=${2:-starrocks-4.1-ubuntu24}
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
mkdir -p "$HOME/ccache" "$HOME/.m2"
docker run -d --name "$name" --restart unless-stopped \
    --user "$(id -u):$(id -g)" --workdir /workspace \
    --ulimit nofile=524288:524288 \
    -e HOME=/tmp -e CCACHE_DIR=/ccache -e MAVEN_OPTS=-Dmaven.repo.local=/tmp/.m2/repository \
    -e 'CMAKE_CXX_LINKER_LAUNCHER=flock;/workspace/.golden-link.lock' \
    -v "$repo:/workspace" -v "$HOME/ccache:/ccache" -v "$HOME/.m2:/tmp/.m2" \
    -v /etc/passwd:/etc/passwd:ro -v /etc/group:/etc/group:ro \
    "$image" sleep infinity
