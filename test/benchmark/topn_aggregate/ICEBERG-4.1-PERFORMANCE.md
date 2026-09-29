# Aggregate TopN: production Iceberg validation on our 4.1

Measured 2026-09-28 on the previously validated 4.1 FE/BE, whose six changed
source hashes match `08870d2b73f`. This is not the main benchmark and does not
include main #75444 or the independent scan IO fix. No engine source was changed
for these measurements. The statistics integration/MCV adaptation f01cb67cd78
is not included.

## Method

Own isolated cluster on 100.96.143.81, container adaptive-dop-byte-limit, SQL9030.
Seven production Glue/Iceberg tables read from the same pinned snapshots as the
main matrix. No production writes. One BE, one client, DOP8, chunk4096, forced
two-stage aggregation. Query cache, adaptive DOP, spill, query-triggered ANALYZE,
plan advisor and scan datacache disabled in each timed session. FE automatic
statistics and process profiling are disabled. Timing excludes profile collection.

Compare ordinary aggregation (topn_push_down_agg_mode=-1) with the cost-selected
corrected alternative (mode1). Do not use the original wrong-result implementation
as a performance baseline. Warm metadata, three warmups per mode. Two independent
series with reversed case order: 25 paired rounds for each transactions query,
15 for each other query. The table below pools raw timings across the two series.

Each timed result is checked against complete ordinary aggregates: row count,
unique group keys, ordering-prefix sequence, and COUNT/SUM/MIN/MAX per returned
group. Different peers at the LIMIT boundary are legal. High-group reference
queries retrieve all groups through the reference boundary rather than exporting
millions of identifiers. All main-matrix 740 timed executions pass.

## Results

| Table / grouping | Input rows | Ordinary ms | Optimized ms | Elapsed time |
|---|---:|---:|---:|---|
| accounts / user_id,type | 5,153,822 | 331.85 | 121.45 | 63.4% faster |
| analytica_transactions / user_id,analytica_id | 2,444,033 | 259.47 | 238.99 | 7.9% faster |
| accounts_history / account_id,operation_type | 72,267 | 122.19 | 116.69 | 4.5% faster |
| transactions / operation_type,provider_id | 4,896,582 | 148.46 | 147.16 | 0.9% faster |
| transactions / source_acc_type,dest_acc_gate | 4,896,582 | 135.19 | 135.46 | 0.2% slower |
| accounts / position_order,is_main | 5,153,822 | 185.75 | 191.42 | 3.1% slower |
| accounts_history / operation_type,account_type | 72,267 | 117.92 | 116.78 | 1.0% faster |
| analytica_transactions / analytica_id,currency | 2,444,033 | 247.73 | 248.91 | 0.5% slower |
| autopayments / operation_type,status | 18,445 | 32.27 | 33.08 | 2.5% slower |
| visa_histories / operation_type,source_acc_type | 912,987 | 234.42 | 234.68 | 0.1% slower |
| users / limits_profile_id,region_id | 2,576,911 | 194.41 | 220.40 | **13.4% slower** |

The matrix starts without collected statistics for these grouping columns. All
mode1 plans select RANK+RF. Small timing differences should not be treated as
established gains or regressions from medians alone. The users regression repeats
in both independent series (12.3%,13.7%). Accounts high-group gains repeat too
(62.0%,62.4%); the pooled ratio is63.4% because each mode's pooled median is
computed separately, rather than averaging the per-series percentages.

Representative untimed profiles: accounts/user_id,type CPU2.625s->492ms, peak
memory808.9->35.7MiB, filesystem read bytes about12.3->2.6MiB. Analytica high-group
CPU552->193ms and memory129.0->23.9MiB, with roughly25.2MiB read in either plan.
These are individual samples, not statistical summaries. They substantiate
reduced work; they do not promise proportional wall-time improvement.

## Users regression: controlled diagnosis

First collect ordinary basic statistics for only limits_profile_id,region_id via
ANALYZE FULL TABLE on the isolated FE. Statistics are stored locally; the external
source is only read. This is current-table statistics used for cost estimates;
the measured queries retain their pinned snapshot. CBO switches RANK+RF to
RF_ONLY. A separate15-pair series is202.11->221.78ms, still9.7% slower. Unlike main,
basic statistics alone do not remove the regression here.

Controls, each25 paired rounds, preserving result checks:

| Control | Ordinary ms | Optimized ms | Observation |
|---|---:|---:|---|
| Disable TopN RF in both modes | 198.48 | 193.58 | Both plans ordinary; no regression, but this also removes forced preaggregation, so it does not isolate RF by itself |
| Force preaggregation in both modes, RF enabled | 192.48 | 216.97 | Regression remains12.7%; forced preaggregation alone is not its explanation |
| Disable only Parquet page index in both modes | 192.05 | 189.51 | RF_ONLY remains in optimized plan; regression disappears |

With page index enabled, a representative forced-preaggregation control profile
shows56 filesystem reads for ordinary vs86 for RF, and about17.8vs18.2MiB read.
The RF evaluates2,576,911rows and retains every row. PageIndexTriedCounter=60,
PageIndexSuccessCounter=30 in the RF profile. Success here is not evidence of
useful pruning. With page index disabled, both profiles have56 filesystem reads
and zero page-index attempts; the RF remains enabled.

Thus this regression is additional Parquet page-index IO prompted by an
unproductive runtime filter, not the main IO-cap defect, local sort alone, or
CPU spent evaluating rows. The forced-preaggregation control reports only about
0.245ms cumulative JoinRuntimeFilterTime. The remaining work is to avoid paying
this index-read cost when it cannot prune, and/or choose ordinary aggregation
when statistics establish that the RF is unhelpful. This report does not claim
that issue is fixed. Page-index disabling was a diagnostic session control, not
a recommended global setting or a committed workaround.

## Artifacts and scope

740 matrix timed runs +30 with basic stats +150 diagnostic runs =920 checked
timed executions, all pass. Warmups/profiles also check results. The initial
100timed executions with scan datacache allowed are retained separately and
excluded from the table. No new full UT run: engine binaries are unchanged.

Remote evidence under /home/eshishkin/adaptive-dop-byte-limit/main-runtime/evidence:
iceberg-41-nocache-{transactions,multi,high}, iceberg-41-repeat-{transactions,multi,high},
iceberg-41-users-basic-stats, iceberg-41-users-no-rf,
iceberg-41-users-force-preagg, iceberg-41-users-no-page-index.
Scripts topn41-*.py and the evidence archive preserve settings, queries, pinned
snapshots, plans, profiles, individual timings and validation assertions.

Single-node/single-client measurements, not concurrency or multi-node validation.
The box remains running; no integration branch was merged or modified.
