# Pre-live profile-guided BE builds

This workflow prepares an experiment; it does not deploy binaries or run queries.
AutoFDO is deliberately not supported. PGO, LTO and BOLT preparation default OFF.

Use the final integration source revision, a Linux build container and fixed
third-party libraries. GCC and Clang need separate build trees/containers and
separate training profiles. Set CC and CXX explicitly for Clang: this build's
CMake otherwise prefers STARROCKS_GCC_HOME over STARROCKS_LLVM_HOME.
Keep the container source/build paths, compiler version, CPU target and build
options identical between training and optimized builds. Do not use fast-math.

## Options

- `build.sh --be --pgo-generate /absolute/path`: instrument BE; atomic counters.
- `build.sh --be --pgo-use /absolute/path`: use GCC .gcda files or Clang
  `merged.profdata` in that directory.
- `--with-lto`: GCC LTO (bfd linker) or Clang ThinLTO (LLD, llvm-ar/ranlib).
- `--with-bolt`: retain relocations for a subsequent BOLT experiment. This does
  not execute BOLT or collect its profile.

PGO requires Release without coverage. Profile paths must contain only letters,
digits, `_`, `.`, `/`, `:`, `+`, `-`. PGO disables ccache. The wrapper below also
cleans BE build targets before profile-use, because changing profile contents
alone would not make the build system rebuild existing objects. It does not
rebuild third-party dependencies. Save each binary before building the next.

## Training and rebuilding

Use `python3 build-support/pgo.py`. Run these commands **inside the build
container**. The source checkout must be clean, and experiment directories must
be outside the checkout. Adjust paths and job count to the machine.

```sh
# Save an ordinary Release binary as the baseline first.
./build.sh --be -j 16
cp be/output/lib/starrocks_be /profiles/baseline-be

# GCC example. For Clang set CC/CXX to the chosen LLVM installation instead.
python3 build-support/pgo.py build generate /profiles/gcc-training --jobs 16
cp be/output/lib/starrocks_be /profiles/training-be
```

The wrapper saves training.json with the revision, compiler, build arguments,
environment, source location and binary SHA256. Install the training binary on
pre-live BE/CN nodes using the normal deployment process. Preserve that receipt
with every node's profile. Mount `/profiles/gcc-training/raw` writable at the
same absolute path inside every runtime container, backed by **separate local
storage on each node**. LLVM_PROFILE_FILE and GCOV_PREFIX overrides should be
unset for this workflow.

Replay a representative mixture of scans, joins, aggregation, JSON and spill
with representative concurrency. Counters add overhead: training timings are
not performance results. Include native and Iceberg paths you intend to use.

After training, stop sending queries, drain the node, and shut down BE gracefully
with SIGTERM; wait for exit. Do not use SIGKILL or the quick-stop HTTP endpoint.
The normal exit path writes profiles, and the resulting .gcda/.profraw files
must actually exist before continuing. Background/detached threads and shutdown
behavior still need validation on the complete training BE; do not assume a
successful workload alone produced a usable profile.

Copy each stopped node's `raw/` and `training.json` into a separate directory on
the build machine, e.g. `/profiles/node1` and `/profiles/node2`. Avoid copying a
profile while the process is writing it. Merge only identical training binaries.

```sh
# GCC: use gcov-tool from the same compiler installation.
python3 build-support/pgo.py merge --gcov-tool /opt/gcc-toolset-14/bin/gcov-tool \
  --output /profiles/gcc-merged /profiles/node1 /profiles/node2

# Clang alternative: merge raw profiles using the matching LLVM installation.
python3 build-support/pgo.py merge --llvm-profdata /opt/llvm/bin/llvm-profdata \
  --output /profiles/clang-merged /profiles/node1 /profiles/node2

# In the original build checkout/container, using the same compiler/options:
python3 build-support/pgo.py build use /profiles/gcc-merged --jobs 16
cp be/output/lib/starrocks_be /profiles/pgo-be
```

One node also works; pass a single input to merge. Clang always needs this merge
step. GCC profile names contain build paths: do not move the build checkout.
The wrapper rejects differing training receipts and source/build settings.
Keep compiler warnings: an uncovered translation unit can lack a profile, but
profile mismatches must be investigated rather than suppressed. Save build logs.

Extra ordinary build.sh options follow `--`. The same options must be supplied
for generate/use. For PGO+LTO repeat the complete training/use cycle with `--lto`
on both wrapper calls and a new experiment directory. An existing explicit
STARROCKS_LINKER must match bfd for GCC LTO or lld for Clang ThinLTO. Third-party
LTO is not required for the first experiment; keep the ordinary dependency cache.

## Comparison matrix

For each compiler compare Release, Release+PGO, Release+PGO+LTO. Measure wall
latency, CPU time, peak memory, binary size, build/link time and result equality.
Alternate measured runs after warmup; use both training and held-out queries,
including different selectivities. Fix source revision, data snapshots, query
settings, hardware and cache policy. Do not compare an instrumented binary's
latency with an optimized binary and call that PGO speedup.

BOLT is a separate follow-up: build the selected candidate with relocations,
record a BOLT-compatible profile of that exact binary (or use BOLT instrumentation),
then retain both original and rewritten binaries for comparison. A PGO profile
is not a BOLT profile; this patch does not automate BOLT rewriting/deployment.
