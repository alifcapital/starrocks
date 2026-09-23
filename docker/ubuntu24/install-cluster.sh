#!/usr/bin/env bash
set -euo pipefail
repo=$(git -C "$(dirname "$0")" rev-parse --show-toplevel)
region=${AWS_REGION:-eu-central-1}
s3_path=${SR_TEST_S3_PATH:?Set SR_TEST_S3_PATH to a test bucket/prefix}
network=${SR_TEST_NETWORK:-100.96.128.0/20}
mkdir -p "$HOME/bin" "$HOME/cluster-conf"
cp "$repo/conf/fe.conf" "$HOME/cluster-conf/fe.conf"
cp "$repo/conf/cn.conf" "$HOME/cluster-conf/cn.conf"
cat >>"$HOME/cluster-conf/fe.conf" <<EOF

run_mode = shared_data
priority_networks = $network
cloud_native_storage_type = S3
aws_s3_path = $s3_path
aws_s3_region = $region
aws_s3_use_instance_profile = true
enable_load_volume_from_conf = true
enable_udf = true
EOF
printf '\npriority_networks = %s\n' "$network" >>"$HOME/cluster-conf/cn.conf"
cat >>"$HOME/cluster-conf/cn.conf" <<'EOF'
JAVA_OPTS="--add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
EOF
install -m 755 "$repo/docker/ubuntu24/sr-cluster-start" "$HOME/bin/"
install -m 755 "$repo/docker/ubuntu24/sr-cluster-stop" "$HOME/bin/"
