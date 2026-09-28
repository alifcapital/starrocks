# Aggregate TopN: correctness and cost validation

Validated 2026-09-28. The unsafe original rule is not a performance baseline: it
returns incomplete aggregates. Compare the corrected build with
`topn_push_down_agg_mode=-1` (ordinary aggregation) against mode 1 (cost-selected
aggregate TopN/RF). Mode 0 timings are retained in the raw results.

## Method

One isolated native BE, 32 vCPU / 128 GiB host, 16 hash(id) buckets, DOP 8,
LIMIT 10, `GROUP BY a,b,c,s`, ordering only by `a`. One warmup followed by 21
interleaved measured executions per mode. Timings are client-observed query
latency including FE planning. Query cache, adaptive DOP, automatic/query-triggered
statistics and spill are disabled for timings. No builds run during measurement.

Small fixture: 524,288 rows per case. Large fixture: 8,388,608 rows per case.
The complete generation expressions are in benchmark.py. Every row contains
three grouping numbers, a 32-byte grouping string (1,024 bytes in wide_groups)
and a signed aggregate input. Full basic statistics, joint NDV and a full-sample
64-bucket histogram on a are present, except the explicit histogram ablation.

The result oracle computes every group's complete COUNT/SUM/MIN/MAX with the
optimization disabled, then checks all returned groups and the ORDER BY key
sequence. Different groups tied at the LIMIT boundary are legal; missing aggregate
contributions are not. Profiles are captured separately from timing runs.

## Measured latency

Negative time change is less time; positive is more time. ORDINARY means the CBO
rejected the extra local aggregation/sort alternative, not that a configuration
switch disabled the rule. Small differences between identical physical plans
include planning overhead and run-to-run variability; these runs do not isolate
those components.

| Rows | Case | Chosen route | Ordinary, ms | Patched, ms | Time change |
|---:|---|---|---:|---:|---:|
| 524,288 | rare_peers | ORDINARY | 24.58 | 25.09 | +2.1% |
| 524,288 | many_peers | ORDINARY | 24.86 | 25.74 | +3.5% |
| 524,288 | few_groups_rare_peers | ORDINARY | 13.39 | 13.63 | +1.8% |
| 524,288 | few_groups_null_peers | ORDINARY | 11.45 | 11.67 | +1.9% |
| 524,288 | few_groups_many_peers | ORDINARY | 10.26 | 10.52 | +2.6% |
| 524,288 | all_equal | ORDINARY | 21.04 | 21.99 | +4.5% |
| 524,288 | rare_rows_many_peers | ORDINARY | 12.61 | 12.86 | +2.0% |
| 524,288 | hot_first | ORDINARY | 20.96 | 22.07 | +5.3% |
| 524,288 | hot_last | ORDINARY | 20.75 | 21.00 | +1.2% |
| 524,288 | null_prefix | ORDINARY | 22.11 | 23.24 | +5.1% |
| 524,288 | correlated | ORDINARY | 19.36 | 19.44 | +0.4% |
| 524,288 | wide_groups | ORDINARY | 132.84 | 134.13 | +1.0% |
| 8,388,608 | few_groups_rare_peers | RANK + RF | 35.79 | 19.87 | -44.5% |
| 8,388,608 | few_groups_many_peers | ORDINARY | 32.12 | 32.57 | +1.4% |
| 8,388,608 | rare_rows_many_peers | RF only | 70.92 | 25.30 | -64.3% |
| 8,388,608 | few_groups_null_peers | RANK + RF | 35.30 | 19.34 | -45.2% |
| 8,388,608 | hot_last | ORDINARY | 224.87 | 221.35 | -1.6% |

The former 50% regression on small hot_last is gone: charging each driver's
copies in both the hash table and local sorter changes the chosen plan to ordinary
aggregation (20.75 -> 21.00 ms). This is not a blanket disable: the large useful
cases still select peer-preserving sort or RF alone.

## What the profiles confirm

| Large case | RF input rows | RF output rows | Baseline peak memory | Patched peak memory |
|---|---:|---:|---:|---:|
| 2,000 groups, few peers per ordering value | 8,388,608 | 1,085,267 | 212.911 MB | 139.172 MB |
| Many peers in a value containing only 1% of input rows | 8,388,608 | 1,121,980 | 451.362 MB | 264.188 MB |
| 2,000 groups, including NULL ordering keys | 8,388,608 | 1,092,607 | See raw profile | See raw profile |

Memory values are individual untimed profile samples, not medians. RF output
includes startup rows admitted before the boundary is available. This establishes
row-level filtering before aggregation, not proportional disk/S3 I/O savings.
Existing JoinRuntimeFilterInputRows/OutputRows, QueryPeakMemoryUsage, sort timers,
spill counters and EXPLAIN were preserved and inspected; no new counters required.

## Histogram ablation

On the same 8,388,608-row rare_rows_many_peers fixture, retain basic/joint NDV and
remove only the histogram on a. The CBO chooses ORDINARY (baseline 67.83 ms,
mode 1 64.52 ms). With the histogram it chooses RF only (baseline 70.92 ms,
mode 1 25.30 ms). These are separate interleaved series; the useful comparison is
within each series. The histogram is restored afterward.

Joint NDV alone knows that many groups tie on a; it does not know that the first
value contains only 1% of rows. The histogram supplies that row-frequency estimate.
It still does not supply conditional group NDV. That approximation affects only
cost choice, never the correctness of peer retention.

## Validation

- Main FE: 56 selected tests pass, including cost, statistics, transformation,
  existing aggregate/TopN plans and Iceberg plan coverage.
- 4.1 FE: 45 corresponding tests pass (main has additional statistics tests).
- 4.1 BE ASAN: 19 aggregate RF, rank sorter and sorter RF tests pass. The new
  probe test first failed against the old probe path and passes with the fix.
- Final runtime: 1,188 checked executions across 18 timing configurations,
  including histogram ablation; 520 additional boundary configurations pass.
- Boundary coverage includes ASC/DESC, NULLS FIRST/LAST, tuple/full ordering,
  K=1/129, DOP=1/8, forced spill and adaptive DOP with event scheduling.
  EXPLAIN confirms 105 RANK configurations. Profiles confirm 120 executions with
  nonzero spill, including 12 RANK cases; this is not just a spill flag check.
- Main BE module-boundary and generated-guide checks pass. The runtime binary
  and ASAN binary are the 4.1 backport; a separate main BE binary was not built.
- SHA-256 of all six changed production/test source files matches the tested
  4.1 checkout. No production data or production cluster was accessed.

## Limits and artifacts

This is a single-node native runtime benchmark. Iceberg has FE plan coverage,
not an external runtime/I/O benchmark. Parallel-driver group overlap, input order,
RF startup and histogram-to-group frequency are estimates. Small queries do not
uniformly improve: the final small-case medians range from +0.08 to +1.29 ms
relative to the baseline. Cost selection is not a latency guarantee.

Harness: benchmark.py, including --reuse-database / --stats existing. No SQL or
configuration change is required to gain the corrected mode-1 choice. No new
setting or workload-specific cutoff was introduced.

Remote artifacts: 100.96.143.81:/home/eshishkin/adaptive-dop-byte-limit/
`topn-cost-{matrix,large,no-histogram}-validated/`,
`topn-boundaries-{matrix,large}-validated/`, `topn-validated-evidence.tar.gz`.
The archive contains plans, timings, profiles, test logs, boundary summaries,
validation runners and source checksums. A local copy is kept in
handbook/plans/local/pr-72332-review/validated in the integration checkout.

The measurements above were made on the standalone 4.1 fix branch. Its TopN changes
were subsequently integrated into integration/statistics-4.1 with conditional MCV
and JOIN-NDV support described in README.md. Those additions preserve the same
peer-preserving correctness contract; the runtime matrix above has not been rerun
for the statistics adaptation. No PR has been published.
