# #75444 + scan fix: controlled comparison on our 4.1

Measured 2026-09-28. The backport compiles and passes the checks below, but this
Iceberg workload does not demonstrate a compelling general performance benefit.
The large main-versus-our-4.1 difference cannot be attributed to #75444 from these
results. Both builds here include our inline aggregation and the same aggregate
TopN ties/cost implementation. Inline aggregation itself was not ablated.

Branch `exp/topn-backpressure-4.1`, base `my41_tmp_0725` at `58b91f39734`.
Backport `beb9223fded`, scan fix `78d10f01e07`; subsequent commits add tests and the
benchmark harness. Tested source manifest ends at `46d321b81ad`. No merge into
my41 or the statistics integration branch was performed.

## What was ported

Upstream #75444 (3d4823f03c0): scan self-enabled TopN RF backpressure, bounded
waits/event wakeups, pending-RF IO cap, row evaluation of storage-pushed TopN
filters, and suppression across selected non-aggregation blocking nodes.
Our fix makes scan readiness honor the same IO cap as task submission, preventing
an empty runnable scan from spinning while submission cannot start more IO.
Normal IO startup remains allowed until the first nonempty chunk has been pulled.

4.1 adaptations preserve its timer ownership/cancellation API and existing
dynamic min/max RF fix. Upstream query-option field219 conflicts with our
`enable_agg_inline_accumulator`; it remains219 and the new options use245–250.
A dedicated FE test checks wire compatibility. The RF collector change belongs
in `runtime_filter_bank.cpp` on4.1, rather than main's relocated implementation.

## Method and controls

One isolated FE/BE on100.96.143.81, SQL9030, container adaptive-dop-byte-limit.
Seven production Iceberg tables,11 saved SELECTs with explicit snapshot IDs from
the preceding4.1 benchmark. Production sources are read-only. One client, DOP8,
chunk4096, forced two-stage aggregation, inline aggregation=true, event scheduler=true.
Query cache, scan datacache, adaptive DOP, spill, plan advisor and query-triggered
ANALYZE are disabled in timed sessions. No compilation runs concurrently.
No new ANALYZE was run: only users already has basic statistics for its two grouping
columns. No MCV/statistics-integration changes are included.

A1(old) → B1(patched) → A2(old):21 rounds per query/variant, alternating variant
order and reversing query order. B2(patched) → A3(old):31 rounds on users and the
high-group accounts/analytica queries, with reference-plan/statistics assertions.
Three warmups per variant; profiles outside timing. Each result checks cardinality,
unique groups, ordering prefix and complete COUNT/SUM/MIN/MAX against ordinary
aggregation. Arbitrary boundary peers are legal; truncated aggregate states are not.

Variants: ordinary aggregation(mode-1), cost-selected ties/RF(mode1), and patched
mode1 with `enable_topn_filter_back_pressure=false`. The last control disables
self-enabled throttling/IO cap; it does NOT undo the other #75444 changes.
`topn_filter_back_pressure_mode=0` is explicit throughout so the older FE-driven
path cannot override this control.

After the first patched restart, users had `stats source: NONE` and RANK+RF,
whereas the old build had ANALYZE and RF_ONLY. Those63 B1 users timings are excluded
from performance comparisons. They still passed result validation. After another
restart, EXPLAIN first reported NONE and then ANALYZE; B2 explicitly verified the
same statistics source and plan route as A2 before and after measurement. No
statistics data was recollected or modified. Startup cache availability is an
observed confounder, not evidence that #75444 changes the cost model. Its exact
startup race was not independently fixed here. FE statistics classes are byte-identical.
All other10 queries retain the same statistics-source marker and RANK/RF route
across A1/B1/A2. The harness now supports `--reference-plans` to reject mismatches.

## Absolute elapsed times

Milliseconds, pooled medians of all matching-plan runs. Negative delta means
less elapsed time. These are descriptive measurements, not established causal
speedups: per-series controls below show drift comparable to many differences.
No percentage from main is used in this comparison.

| Table / grouping | Old4.1 ms | +75444 and fix ms | Observed elapsed delta | Patched, backpressure off ms |
|---|---:|---:|---:|---:|
| transactions / operation_type, provider_id | 157.00 | 147.86 | -5.8% | 149.65 |
| transactions / source_acc_type, dest_acc_gate | 139.23 | 136.67 | -1.8% | 134.10 |
| accounts / position_order, is_main | 189.46 | 183.81 | -3.0% | 183.41 |
| accounts_history / operation_type, account_type | 118.32 | 118.13 | -0.2% | 117.83 |
| analytica_transactions / analytica_id, currency | 250.75 | 244.02 | -2.7% | 245.74 |
| autopayments / operation_type, status | 34.60 | 35.47 | +2.5% | 33.24 |
| users / limits_profile_id, region_id | 222.84 | 228.28 | +2.4% | 228.41 |
| visa_histories / operation_type, source_acc_type | 234.21 | 233.74 | -0.2% | 236.75 |
| accounts / user_id, type | 123.42 | 124.86 | +1.2% | 124.92 |
| accounts_history / account_id, operation_type | 121.73 | 119.29 | -2.0% | 123.87 |
| analytica_transactions / user_id, analytica_id | 243.61 | 241.02 | -1.1% | 244.81 |

The paired ordinary controls also vary. For transactions/operation_type,provider_id,
ordinary medians were166.78ms in A1,149.22ms in B1 and147.58ms in A2; optimized
medians159.47,147.86,152.58ms. Calling the pooled5.8% difference a demonstrated
backpressure gain would ignore that control drift.

Repeated important cases, each cell a separate series median:

| Query | A1 old | B1 patched | A2 old | B2 patched | A3 old |
|---|---:|---:|---:|---:|---:|
| accounts / user_id,type |118.78|127.40|128.06|123.16|125.22|
| analytica_transactions / user_id,analytica_id |234.81|242.33|248.81|239.23|242.99|
| users / limits_profile_id,region_id |212.61|excluded: different statistics/plan|227.69|228.28|225.08|

Thus the apparent accounts slowdown from A1→B1 is not stable on reversal/repetition.
The useful ties/RF accounts plan remains about125ms; #75444 does not supply a
new large gain. Users is still slower with RF than ordinary aggregation:
patched228.28vs200.04ms, latest old225.08vs194.59ms. Disabling backpressure on
the patched build gives228.41ms, so it does not remove that existing issue.

Representative A3/B2 optimized profiles have the same filesystem read counts:
accounts8+17, analytica15+5, users86+33 (ordinary scan + MOR/delete counters).
These are single profile samples, not averages. They show no IO reduction in
those samples. The previous page-index diagnosis for users remains open; this
backport is not its fix. Timing differences also include FE/client elapsed time
and remote storage variation; no CPU-only or multiclient performance claim is made.

## Validation and artifacts

- FE compilation plus53 targeted tests passed.
- BE Release build passed;3 Release-linked targeted BE tests passed, including
  cap/readiness/startup and storage-pushed dynamic RF evaluation. No new ASAN run.
- 2082 timed SELECT results checked, including63 excluded from performance analysis.
- 520 native boundary configurations with event scheduling passed, covering NULLs,
  peers, ordering directions, DOP1/8, forced spill and adaptive combinations.
- Additional104 native configurations with polling passed. Profiles/results also
  collected outside timed sessions; no crashes or wrong results in these runs.
- Source hashes for all20 changed files match the tested branch manifest.

Old FE SHA256: `2e8fbc6f418cfeb1d5fd17f8f6170302e6bd93cabb5fde763b63055f177faf07`.
Old BE: `b11a95224ed0b094753fbd363042a1357f99aacb0aee985297616b3f82035618`.
Patched FE: `80138d12f795ec9a7419c868fe35cca299894e1fe2c08ab1e307285779fbbe1f`.
Patched BE: `4574f200654665b9d05ec58653355605898c8d44ed8134f184be702e18a46634`.
Both distributions remain under `/workspace/bp41/dist-{baseline,patched}`.
The running9030 cluster was restored to the exact old binaries; box left running.

Archive `bp41-evidence-20260928.tar.gz`, SHA256
`88d37598d96f9a2b28b384bdcc9320fec7698dcd8f3e93f8c4cff5681017ab20`:
remote `/home/eshishkin/adaptive-dop-byte-limit/`, local
`handbook/plans/local/pr-72332-review/backpressure-41/` in the primary checkout.
Contains raw timings, all plans/profiles, phase settings, scripts, build/test logs,
source manifest and baseline source backups. Startup harness failures (low file
limit and already-removed FE PID file) are retained separately; fixed before
continuing the affected stage, without rerunning completed timings.

Recommendation: keep the validated backport isolated for now. Correctness checks
pass, but these workload measurements alone do not justify it as a performance
improvement for our4.1. They also do not establish the earlier cross-version gap's
cause; attribution to inline aggregation would need its own controlled ablation.
