# syntax=docker/dockerfile:1
ARG UPSTREAM_IMAGE
ARG TOOLCHAIN_IMAGE
FROM ${UPSTREAM_IMAGE} AS dependencies
FROM ${TOOLCHAIN_IMAGE}
ARG UPSTREAM_IMAGE
ARG TOOLCHAIN_IMAGE
ARG SOURCE_REVISION
LABEL org.opencontainers.image.source="https://github.com/alifcapital/starrocks" \
      org.opencontainers.image.revision="${SOURCE_REVISION}" \
      com.alif.starrocks.upstream-image="${UPSTREAM_IMAGE}" \
      com.alif.starrocks.toolchain-image="${TOOLCHAIN_IMAGE}"
ENV STARROCKS_THIRDPARTY=/var/local/thirdparty \
    STARLET_INSTALL_DIR=/var/local/thirdparty/installed/starlet
COPY --from=dependencies /var/local/thirdparty /var/local/thirdparty
COPY --from=dependencies /root/.m2 /root/.m2
COPY --from=dependencies /root/.mvn /root/.mvn
# The 4.1 BE link still uses -liberty; the main toolchain does not ship it.
RUN apt-get update && \
    apt-get install -y --no-install-recommends libiberty-dev=20240117-1build1 && \
    rm -rf /var/lib/apt/lists/*
RUN . /etc/os-release && test "$VERSION_ID" = 24.04 && \
    javac -version 2>&1 | grep -E '^javac 21\.' && \
    test -f "$STARROCKS_THIRDPARTY/installed/starcache/lib/libstarcache.a" && \
    test -d "$STARLET_INSTALL_DIR/starlet_install"
WORKDIR /workspace
