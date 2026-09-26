# JOIN statistics development controls

These tools are validation tools, not production dependencies. Collection, persistence, loading and
estimation are implemented in StarRocks FE/BE.

- `sql_validation.py` requires a writable **development** Iceberg catalog/database. It creates only
  `join_stats_validation_*` tables and JOIN-statistics objects there. It checks duplicate keys, NULL,
  Unicode, full and partial predicate combinations, an extra predicate, SEMI JOIN, a chain, empty
  sources, unchanged results with the estimator disabled/enabled, and generation refresh. It writes
  EXPLAIN plans and results to an explicit output path. Fixtures are retained for debugging.
- `PreparedStatisticsBench.java` decodes actual collector binary payloads (`name.bin`) and manifests
  (`name.meta.json`, Gson representation of `JoinStatisticsMeta`). It measures the actual object graph
  and Caffeine increment with JOL, decoding allocation/GC, and full/partial entropy solves. Compile
  against the matching FE runtime libraries and JOL 0.16; provide an instrumentation agent for JOL.
  The five-second per-solve allowance exposes estimator cost; these scalar timings are **not**
  measurements of the ordinary 60 ms query-wide planner budget or total planning time.

When measuring collection, wait for previous query/load activity and spill cleanup before starting
another object. `SHOW ... READY` reports publication and can precede final cleanup. Sample the sum of
BE query and load trackers, not just a single operator peak. Record disk spill, FE heap, source
snapshots, session settings, and actual memory/disk Data Cache quotas. An enabled cache with zero disk
quota is not a warm native-table cache. Never mix a mid-run cache repair with a timing comparison.

The prepared representation has one basis per equality-key domain: one coordinated 16K head and
three 256-bucket tail layouts per predicate slice. Fixed tail moments support the required arities
and powers; table subsets and RF projections are derived during planning, not stored as separate
tails. Membership unions are bounded by summed supports and are not assumed disjoint.
Sparse tails share one short bucket-offset array across all stored moment rows, using the sparse
representation only when it saves retained memory. Populated tails remain dense. Projected norms
are immutable views over the prepared rows, so planning does not allocate/copy per-role bucket arrays.

A compact pair matrix stores frequency/frequency, support/frequency, frequency/support and
support/support inner products for covered slices. These are computed from the same exact spilled
degree tables. Frequency unions are linear; merging membership slices gives an upper bound if
keys overlap. Pair values strengthen the power-one LP constraints without a separate head or tail
for every projection. Matrix allocations are admitted against the object memory limit before
collection/allocation. Native collection and the prepared Java cache have no Python dependency.

The collector writes payload version 5, including compact head-key labels for skew planning.
The reader also accepts version 4; older development generations must be recollected.
When comparing estimates, use the same source snapshots and slice dictionary. A smaller shared tail
alone can lose precision because it no longer pre-intersects supports for every subset. Test the
complete representation, including pair values, and report partial predicate coverage separately
from the normal planner's handling of predicates outside the collected dictionary.
