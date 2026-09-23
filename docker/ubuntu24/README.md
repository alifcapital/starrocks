# StarRocks 4.1 on Ubuntu 24.04 and JDK 21

The development image uses the official Ubuntu 24.04/GCC 14.3/JDK 21 toolchain and
copies the complete thirdparty installation from the official **4.1** development
image. This includes the matching Starlet and StarCache binaries. Ubuntu 22.04
dependencies are usable with the newer glibc on Ubuntu 24.04.

`images.lock.json` pins both input images by their multiarchitecture manifest
digests. Updating `4.1-latest` upstream has no effect until this lock is refreshed.
The scripts default to amd64, which is the architecture of the build AMI.

## Refresh and build the development image

Run `refresh` on a workstation when upstream publishes a new 4.1 dev-env image:

```sh
python3 docker/ubuntu24/image.py refresh
git diff -- docker/ubuntu24/images.lock.json
```

Review the corresponding upstream 4.1 dependency changes and update the source
branch to match. In particular, compare `thirdparty/starlet-artifacts-version.sh`
and `thirdparty/vars-ubuntu22-*.sh`. Commit the lock update with the source changes.
Do not select `starrocks/dev-env-ubuntu:latest`: it contains main's dependencies.

Build on an Ubuntu builder with Docker Buildx:

```sh
python3 docker/ubuntu24/image.py build --tag alif/starrocks-dev-env-ubuntu24:4.1-build
bash docker/ubuntu24/container.sh alif/starrocks-dev-env-ubuntu24:4.1-build
```

For publication use a unique tag under the private ECR repository
`211125404854.dkr.ecr.eu-central-1.amazonaws.com/starrocks/dev-env-ubuntu24`.
ECR tags are immutable. Authenticate using the workstation's AWS credentials:

```sh
aws ecr get-login-password --region eu-central-1 | ssh BUILDER_IP \
  'docker login --username AWS --password-stdin 211125404854.dkr.ecr.eu-central-1.amazonaws.com'
```

`image.py build --tag ECR_REPOSITORY:UNIQUE_TAG --push` publishes from the builder.
Record the resulting digest in the AMI receipt. Use that digest when creating its
build container. A refreshed development image needs the validation below before
it replaces a previously verified AMI.

## Prepare a fresh builder

```sh
bash docker/ubuntu24/launch-builder.sh > /tmp/starrocks-builder.json
```

The launcher uses Canonical's Ubuntu 24.04 AMI, a 300 GiB gp3 root volume,
`starrocks-role`, and termination protection. Override `SR_INSTANCE_TYPE` or
`SR_INSTANCE_NAME` as needed. `prepare-host.sh` runs through EC2 user data;
`/var/lib/starrocks-host-ready` marks completion. The user is `eshishkin` (uid 1001).
Initial setup failures are visible in `/var/log/cloud-init-output.log` as `ubuntu`.

Clone the source branch to `/home/eshishkin/starrocks`, then build the image and
create the container as above. The source, ccache, and Maven repository are separate
host mounts. The container restarts automatically after boot.

`bash docker/ubuntu24/prepare-tests.sh` installs the SQL harness into
`~/.venvs/starrocks`. Activate it before running `test/run.py`; Ubuntu 24.04's
system Python packages are managed by apt.

For the test cluster, run on the builder:

```sh
SR_TEST_S3_PATH=alif-data-warehouse/warehouse/startest \
  bash docker/ubuntu24/install-cluster.sh
```

This creates shared-data FE/CN configs using the EC2 instance role. No static AWS
credentials are needed. The cluster runs on the Ubuntu 24.04 **host** with JDK 21.
The container is used for compilation. Keep `~/cluster-conf` as the config source;
builds replace the configs in `output/`.

## Build and validate

Run one stage at a time. Logs and actual exit codes are in `~/build-logs`:

```sh
bash docker/ubuntu24/build-stage.sh release
bash docker/ubuntu24/build-stage.sh java-tests
bash docker/ubuntu24/build-stage.sh fe-tests
bash docker/ubuntu24/build-stage.sh asan
bash docker/ubuntu24/build-stage.sh ut
```

The UT stage builds all native test targets and runs selected suites with ASAN.
Check nonzero test counts in the log as well as `FINAL_RC=0`.
Compilation uses all cores; the container's `CMAKE_CXX_LINKER_LAUNCHER` serializes
native links with `flock`. Running Release and UT generators simultaneously in one
checkout can overwrite the shared version source; keep stages sequential.

After ASAN, force the shared output executable to be relinked as Release:

```sh
rm -f be/output/lib/starrocks_be
bash docker/ubuntu24/build-stage.sh release
~/bin/sr-cluster-start
python3 docker/ubuntu24/smoke.py
```

Check FE and CN versions, run shared-data SQL and a Java UDF, restart the cluster,
and verify persisted data. Keep the SQL smoke running for at least six minutes and
check ports 9060/9070 throughout. Always test the final Release output.

### Changing the development image

Stop the test cluster first. Remove and recreate its build container with the new
image, keeping the mounts. Delete the generated C++ build trees
`be/build_Release`, `be/build_ASAN`, `be/ut_build_ASAN`, and generated native outputs
before rebuilding. Run `mvn clean` inside the new container for `fe/` and
`java-extensions/`. Keep `~/ccache` and `~/.m2`.

An incremental build is insufficient: upstream headers can retain old mtimes,
leaving objects compiled for the old Starlet ABI. Successful linking does not
prove ABI compatibility.

## Bake and verify an AMI

After validation, preserve logs and a receipt listing the source commit, input and
output image digests, OS/JDK/compiler versions, and test results. Stop the cluster
and clear only this test cluster's local runtime state (FE metadata, CN storage,
cache, spill, logs). Keep source, build trees, artifacts, ccache and Maven cache.
Do not clean S3 or shell history.

`bash docker/ubuntu24/bake-ami.sh INSTANCE_ID AMI_NAME` stops the donor, creates the
image and waits for `available`. It prints the AMI ID to stdout, so save that output.
Launch a verification instance using
`SR_AMI_ID=NEW_AMI_ID bash docker/ubuntu24/launch-builder.sh`, then repeat startup
and SQL/UDF smoke checks from that instance.

Leave the donor and verification instances stopped. Preserve existing AMIs and
snapshots. Termination and deletion require the owner's explicit instruction.

## Backports

- #74787 / #75854: Ubuntu 24.04 toolchain and JDK 21.
- #76505: use Ubuntu 22 thirdparty overrides on Ubuntu 24.
- #76058: JDK 21 runtime images and version guidance.
- #79460: allow the UDF security manager on JDK 18–23.
- #79519: JDK 21 FE test infrastructure and affected tests.

The source baseline already includes #75666 (DirectByteBuffer on JDK 21) and
#79583 (the JDK 21 aggregate cast test fix).
