# Aggregate TopN boundary peers

Runtime validation uses an isolated native cluster. Do not run concurrent builds
or other benchmarks on the measurement host. See BENCHMARK.md for measured results.

## Correctness

`GROUP BY a,b ORDER BY a LIMIT K` cannot use local ROW_NUMBER TopN before the
final aggregation. Different drivers can retain different groups at the same
`a`, producing incomplete COUNT/SUM states in surviving groups.

The local sort now uses RANK <= K (keeps all boundary peers), while the original
TopN still implements the user's LIMIT. All grouping keys in ORDER BY retain the
existing bounded ROW_NUMBER path. ASC/DESC and NULL ordering are passed through.
This rule still excludes HAVING, OFFSET and aggregate-valued ordering.

Once a group has more than K-1 strictly better local groups, it also has more
than K-1 strictly better global groups and cannot win. Retaining boundary peers
therefore preserves every contribution of every possible winning group. This
proof does not use statistics. The RF uses an inclusive boundary, including NULLs.

The aggregate RF's candidate heap excludes NULL. Do not publish a numeric bound
until the heap contains K non-NULL candidates, even if the group hash table
already contains K groups. Otherwise NULLS LAST could lose later non-NULL keys.

The spillable sort factory now forwards the rank type to its in-memory sorter.
Previously it constructed a ROW_NUMBER sorter regardless of the requested type,
which would lose peers even before spilling. Once spilled, retaining a superset
is safe: the original final TopN still performs the user-visible truncation.

Dynamic min/max filters carry an always-true membership flag, but their range
can still narrow. The normal probe collector now evaluates these stream-built
pure range filters without requiring TopN backpressure. Inactive membership
filters and the separate WITHOUT_TOPN pass retain their previous behavior.
Existing runtime-filter input/output and timing counters measure this path.

## Cost choice (main statistics only)

`TopNAggregationCost` compares two correct paths:

* partial aggregation -> peer-preserving TopN -> final aggregation;
* partial aggregation with its existing TopN RF -> final aggregation, omitting
  the local sort when its buffer/comparison cost outweighs estimated shuffle savings.

Joint NDV uses `Statistics.getLargestSubsetMCStats`, including an exact key-set
match when available; remaining independent column NDVs are capped by input rows.
The ORDER BY tuple is estimated separately from the complete grouping tuple.
No external catalog calls or statistics collection are added by the rule.

For a single ORDER BY column, a known leading MCV/bucket endpoint or NULL fraction
can reveal a large leading peer group. Group frequency is estimated from row
frequency: main has no conditional distinct histogram. A univariate histogram
cannot describe a multi-column ordering, so it is not used for that case.
Unknown statistics retain the peer-preserving path. Stale or inaccurate estimates
can change performance, never which groups contribute to the result.

Incremental costs use the existing CPU/memory/network weights from CostModel:
comparison bytes, retained partial-row bytes, and estimated shuffle bytes avoided.
These are planner estimates, not measured latency or I/O estimates. In particular,
physical file layout and conditional NDV remain unknown; overlap between drivers
is modeled by an occupancy estimate, not measured.
No workload-specific thresholds have been tuned yet. The no-sort path does not
promise bounded aggregation memory and may provide little RF pruning on all-equal
keys. With aggregate RF disabled, an expensive extra sort is simply not inserted.

The estimate participates in the ordinary optimizer competition against an unchanged
aggregation plan. StatisticsCalculator estimates RANK output as K plus boundary
peers (capped by input groups), never unconditionally K. Unknown statistics keep
the input estimate. CostModel charges the RANK comparator/buffer instead of giving
it the bounded TopN's zero cost. Sort CPU includes materializing partial rows
and logarithmic comparisons of ordering keys. Forced preaggregation retains full
hash-table memory; it cannot use the streaming aggregation memory discount.
Global group NDV is expanded to the expected number of independent driver-local
states, bounded by input rows and group NDV times the number of drivers. The
local sort is charged for these copies too, rather than sorting each global
group only once.

Aggregate RF CPU savings are estimated for its first ordering key only, always
including NULL rows. A conservative startup allowance uses 32 chunks per driver:
the normal BE probe path checks for newly available filters at that cadence.
This is based on RuntimeFilterProbeCollector::do_evaluate, not a fitted row-count
threshold. Warehouse node count, DOP and chunk size determine startup rows. Passing rows
also incur payload compaction after RF evaluation. A
backpressure-enabled scan may activate earlier; input ordering and overlapping
scan/aggregation can also differ. It remains a cost heuristic, not a latency
prediction or a proof of how many rows an actual scan will skip. The ordinary
partial-aggregation CPU weighting remains unchanged for strong reduction; forced
preaggregation with weak reduction uses the existing high-cardinality criterion
(estimated groups >= input rows / 4) even below projects or external scans.

Histogram row frequency is distinct from peer-buffer cardinality. A leading value
can contain many distinct groups but only a small fraction of input rows. When
estimated leading groups can fill K candidates, RF CPU selectivity uses that
value's histogram frequency; the local sort still keeps its conservative peer
estimate. This can choose RF without a large local sort. Main lacks conditional
joint NDV, so the number of groups within that value remains an estimate.

## Validation commands

FE: `TopNAggregationCostTest`, `PushDownTopNToPreAggRuleTest`,
`IcebergTopNRuntimeFilterTest`, existing aggregate/TopN plan tests.

BE: `AggTopNRuntimeFilterTest.*`, existing `ChunksSorterTest.rank_topn` and sorter
runtime-filter tests. SQL regression: `test_sort/test_agg_topn_boundary_peers`.
The paired SQL T/R results follow analytically: each complete group has 1000 rows.

## Benchmarks

On an isolated native test cluster with automatic statistics disabled:

```sh
python3 benchmark.py --host 127.0.0.1 --out /tmp/topn-agg-benchmark
```

The script creates a uniquely named database and retains it for inspection. It
never accesses production or external catalogs. Password comes from MYSQL_PWD.
Dependencies: pymysql. Default: 524288 rows/case, DOP 8, LIMIT 10, one warmup and
seven measured rounds, randomized mode order.

Cases: rare peers, many peers, small heavily repeated group sets, all equal ordering keys, hot first/last key,
NULL-heavy prefix, correlated keys, and wide group keys. Cumulative statistics
phases: none, basic, joint NDV, histogram plus joint NDV. Modes: original rule
disabled (-1), no aggregate RF (0), aggregate RF enabled (1). Inspect EXPLAIN for
actual path selection; statistics caches may populate asynchronously. Compare
corrected builds with mode -1; the old build is expected to fail result checks.

Repeat a timing run on an existing fixture without changing its statistics:

```sh
python3 benchmark.py --host 127.0.0.1 --reuse-database topn_bench_EXAMPLE \
  --stats existing --rows 524288 --rounds 21 --out /tmp/topn-agg-repeat
```

Additional boundary checks, separate from the default timing matrix:

```sh
python3 benchmark.py --host 127.0.0.1 --out /tmp/topn-agg-boundaries \
  --cases many_peers null_prefix correlated --stats none histogram \
  --orders asc desc null_last tuple full --limits 1 129 --dops 1 8 --rounds 1
```

Every returned group's COUNT/SUM/MIN/MAX is compared with the complete unoptimized
aggregation. ORDER BY key sequences are compared with an unoptimized TopN; ties
may select different groups, but incomplete aggregates are never accepted.
Plans, raw timings and one untimed query profile per configuration are saved. Report median runtime,
peak memory, sort time, RF input/output, scan bytes and shuffle bytes, including
regressions. A native benchmark tests the general rule, not Iceberg I/O savings;
Iceberg has separate FE plan coverage and needs a pinned read-only dataset for
an external runtime benchmark.

No PR has been published. Integration into the statistics branch should extend
this estimator, leaving the peer-preservation correctness contract unchanged.
