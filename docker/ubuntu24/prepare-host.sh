#!/usr/bin/env bash
# Prepare an Ubuntu 24.04 EC2 builder. Run as root.
set -euo pipefail
source /etc/os-release
[[ $ID == ubuntu && $VERSION_ID == 24.04 && $(id -u) == 0 ]]
export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y docker.io docker-buildx git rsync curl unzip jq ca-certificates \
    openjdk-21-jdk mysql-client libssl-dev python3-venv python3-pip \
    gdb linux-tools-common linux-tools-generic ccache time locales
groupadd -f docker
if ! id eshishkin >/dev/null 2>&1; then
    useradd --create-home --uid 1001 --shell /bin/bash --groups docker eshishkin
fi
usermod -aG docker eshishkin
install -d -m 700 -o eshishkin -g eshishkin /home/eshishkin/.ssh
if [[ ! -f /home/eshishkin/.ssh/authorized_keys ]]; then
    install -m 600 -o eshishkin -g eshishkin /home/ubuntu/.ssh/authorized_keys \
        /home/eshishkin/.ssh/authorized_keys
fi
printf 'eshishkin ALL=(ALL) NOPASSWD:ALL\n' >/etc/sudoers.d/90-eshishkin
chmod 440 /etc/sudoers.d/90-eshishkin
install -d -o eshishkin -g eshishkin /home/eshishkin/{starrocks,ccache,.m2,bin,cluster-conf}
if [[ ! -f /home/eshishkin/ccache/ccache.conf ]]; then
    printf 'max_size = 80G\nsloppiness = pch_defines,time_macros\n' >/home/eshishkin/ccache/ccache.conf
    chown eshishkin:eshishkin /home/eshishkin/ccache/ccache.conf
fi
printf 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64\n' >/etc/profile.d/starrocks-java.sh
printf '* soft nofile 524288\n* hard nofile 524288\n' >/etc/security/limits.d/90-starrocks.conf
printf 'vm.max_map_count=262144\nkernel.perf_event_paranoid=1\n' >/etc/sysctl.d/90-starrocks.conf
sysctl --system
if [[ ! -f /swapfile ]]; then
    fallocate -l 16G /swapfile
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    printf '/swapfile none swap sw 0 0\n' >>/etc/fstab
fi
locale-gen en_US.UTF-8
systemctl enable --now docker
touch /var/lib/starrocks-host-ready
