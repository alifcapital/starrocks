---
display_name: JOIN statistics
---

# JOIN statistics

JOIN statistics describe how predicate values and equality keys are related across a declared set of Iceberg or native tables. They supplement basic statistics and local MCV statistics for INNER JOIN cardinality, SEMI JOIN cardinality, and runtime-filter selectivity. They do not change query results, runtime-filter construction, or the existing local MCV collection commands.

## Create and collect

```sql
CREATE JOIN STATISTICS transactions_users
WITH ASYNC MODE AS
SELECT t.status, t.dest_acc_gate, t.dest_acc_type, u.country, u.status
FROM iceberg.landing_mobi_tj.transactions t
JOIN iceberg.landing_mobi_tj.users u ON t.user_id = u.id;
```

The ON clause declares the equality relationship. SELECT lists the predicate columns whose joint values should be collected **within each source table**. Column aliases in this definition do not define query aliases. A query can use different table aliases.

CREATE persists the definition and starts collection immediately when the FE configuration
`enable_trigger_analyze_job_immediate` is `true` (the default). When it is `false`, CREATE only
saves the definition; run `ANALYZE JOIN STATISTICS` explicitly to collect it. This configuration
does not suppress an explicit ANALYZE. Collection is synchronous unless WITH ASYNC MODE is specified. Both modes publish only a complete generation. An unsuccessful CREATE collection leaves the definition available for a retry:

```sql
-- Start collection in the background (also works when immediate collection is disabled).
ANALYZE JOIN STATISTICS transactions_users WITH ASYNC MODE;
-- Or wait for collection to finish.
ANALYZE JOIN STATISTICS transactions_users WITH SYNC MODE;
SHOW JOIN STATISTICS transactions_users;
DROP JOIN STATISTICS IF EXISTS transactions_users;
```

ANALYZE refreshes every source in the object. A failed refresh preserves the previous generation. DROP cancels pending collection and removes the definition and its stored data. A cancelled or old leader's collector cannot republish a dropped object. SHOW reports the current collection state, published generation, collection time, compressed data size, and last error. Transient job state is not restored after an FE restart; the last published generation is restored.

The definition may contain two to four Iceberg or native table roles and one to three equality-key domains. A domain may be shared by several tables. For example, `t.user_id = u.id AND t.user_id = a.user_id` defines one domain, whereas `t.user_id = u.id AND t.provider_id = p.provider_id` defines two. Equalities connecting the same set of tables form one compound domain: `a.x=b.x AND a.y=b.y AND a.z=b.z` is one three-component key. Components retain their types and boundaries; NULL in any component does not match ordinary equality. Compatible integer widths are supported. A four-table star can have three independent domains, such as user, provider and account. Repeated occurrences of a physical table are separate roles, for example sender and recipient from the same users table. Each role keeps its own predicate dictionary; role matching reuses immutable arrays without copying the stored distributions. Expressions as keys, outer joins, and filtered or aggregated collection definitions are rejected. No PK/FK declaration or uniqueness assumption is required.

The maximum number of selected predicate combinations per source follows `statistic_mcv_size`. It may be overridden for an object:

```sql
CREATE JOIN STATISTICS transactions_providers
PROPERTIES ('mcv_size' = '200') WITH ASYNC MODE AS
SELECT t.status, p.active
FROM iceberg.landing_mobi_tj.transactions t
JOIN iceberg.landing_mobi_tj.providers p ON t.provider_id = p.provider_id;
```

Increasing this limit widens predicate coverage and increases collection work and retained size. It does not change the 16,384-key head budget. The accepted range is 1–4,096 predicate combinations, with at most 32 predicate columns per source. Creating, collecting and dropping an object require the same source-table privileges as ANALYZE. SHOW lists only objects for which the caller has SELECT on every source table.

## Inspect collected distributions

```sql
SHOW VERBOSE JOIN STATISTICS transactions_users LIMIT 100;
SHOW VERBOSE JOIN STATISTICS transactions_users LIMIT 100 OFFSET 100;
```

This command decodes the saved generation on FE using the existing binary codec. It does not scan the source tables or collect new statistics. It requires SELECT on **every** source table, including sources absent from the caller's other queries. Ordinary `SHOW JOIN STATISTICS` remains a short status listing.

The default page is 100 rows; LIMIT accepts 0–1,000. OFFSET and the returned `Row` are zero-based. Output columns are `Generation`, `Row`, `Section`, `Source`, `Domain`, `Slice`, and `Details`. `Details` is readable JSON with named fields. Source, domain and slice identifiers are zero-based; an empty identifier means it is not applicable to that row.

| Section | Contents |
| --- | --- |
| OBJECT | Object name, collection time, stored bytes and prepared memory estimate |
| SOURCE | Source table/role, snapshot or version, row count, predicate columns and types |
| SLICE | Predicate tuple and its row count; JSON null denotes SQL NULL |
| DEGREE | Rows, NULL rows, NDV, maximum key frequency and frequency moments indexed by power |
| DOMAIN | Equality-key columns by source and their types |
| BASIS | Participating sources, number of head positions and tail dimensions |
| HEAD_KEY | Shared head index and retained key value; `label_retained=false` means the key text was not retained, not that it is SQL NULL |
| HEAD | Nonzero frequency for a head index in a source slice; zero entries are omitted |
| TAIL | Occupied bucket in a tail layout and its stored Lp norms; empty buckets are omitted |
| PAIR | Correlations for a pair of source slices, including JOIN and membership products |
| INTRA | Support and moments for two key domains within one source slice |

For DEGREE, moment `p` is the sum of key frequencies raised to `p`. TAIL exposes the prepared stored norms: order `0` is the bucket's key count, and positive order `p` is the p-th root of the corresponding frequency moment. The three layouts represent the same tail hashed differently; do not sum their totals together. A unit-frequency tail stores only its key count. PAIR's four `products_by_presence_mask` entries use frequency/frequency, presence/frequency, frequency/presence, and presence/presence, respectively.

Pages are stable within one generation. Check `Generation` when fetching the next page; if ANALYZE published a new generation, restart at OFFSET 0. No historical generation is pinned across commands. Output is capped at 32 MiB per page; an oversized page reports an error asking for a smaller LIMIT instead of silently truncating values.

A cached generation is reused. Otherwise this explicit inspection command waits for the shared statistics loader, bounded by the session `query_timeout`; failure, timeout or a concurrent generation change reports an error and can be retried. This does not change asynchronous statistics loading during ordinary query planning.

## What is collected

Collection pins one snapshot of each Iceberg source before scanning. These are independent snapshots, not a transaction shared across tables. Native collection uses normal ANALYZE reads and does not block concurrent writes. A version fingerprint detects changes across collection passes; such an object remains usable on its own but does not claim a stable native version for composition with separately collected objects.

1. A bounded frequent-items sketch selects predicate combinations, using the local MCV candidate policy.
2. The collector gives these combinations compact local IDs and computes exact JOIN-key frequencies for the selected combinations. Hash aggregation and large intermediate joins may spill. There is no unbounded in-memory grouping of all predicate combinations.
3. Each selected combination receives a row count, NULL count, distinct-key count, maximum key frequency, and ten frequency moments. For degree counts `3, 2, 1`, the first three moments are `6, 14, 36`.
4. One coordinated head retains up to 16,384 keys per equality-key domain. Each source combination has three shared layouts of 256 tail buckets. Fixed moment summaries allow the planner to derive bounds for smaller subgraphs, powers and RF projections without storing a separate tail for each of them.
5. Compact pair matrices retain inner products of key frequencies and key membership for selected combinations from two sources. Build duplicates multiply JOIN cardinality but do not multiply membership. Combining frequency slices is exact; combining membership slices remains an upper bound when their key sets overlap. Pair-matrix space grows with pairs of selected predicate combinations, not their three-way or four-way Cartesian product, and is checked against the object memory limit before allocation.
6. The object also records within-source information for each pair of different JOIN keys. With three keys, this describes the three pairwise relationships; it does not claim to recover arbitrary three-way dependence. Prefix summaries are not collected.

The collector executes native StarRocks SQL and aggregates. Production collection and planning have no Python dependency. Scratch tables are private to an object generation, cleaned after success or failure, and reclaimed after an interrupted FE process.

## Query coverage

A query does not have to specify every predicate column. For `status = 'approved' AND dest_acc_gate = 0`, the planner selects and combines the stored full combinations matching those two conditions. It does not require separate grouping-set statistics for every subset.

Missing predicate combinations and the tail of JOIN keys are different things. The collector's key-tail buckets describe keys within selected combinations. They do not describe unselected predicate combinations. The latter retain a residual estimate based on ordinary local statistics. An additional condition on another column also uses the ordinary local row estimate while preserving constraints from the covered conditions.

Supported predicate matching includes scalar comparisons, IN and NULL tests. Unsupported operators, lossy key casts, incompatible schema changes, limits and incremental snapshot ranges end the applicable subgraph. Ordinary estimates remain available for unsupported cases. An object can estimate a smaller matching subgraph. With `cbo_enable_join_statistics_composition=true`, multiple overlapping objects can also contribute to one estimate: the planner maps their constraints onto shared table-row and equality-key identities and solves them together. It does not multiply independent pair estimates or create a new stored tensor. Source snapshot IDs must agree wherever objects overlap. Different predicate dictionaries remain local to each object.

Composition requires coverage of every table and equality in the estimated subgraph, at most 16 contributing objects, and at most seven entropy attributes (one per source plus one per distinct key domain). A larger query may still use composed estimates for smaller subplans. Missing coverage, incompatible snapshots or an exhausted budget retain ordinary estimation using the available child estimates. For example, five sources with three independent domains exceed the full-subgraph limit; a five-source common-key star fits. Partial objects do not contain correlations that were never collected, so their combined estimate can be looser than an ordinary estimate.

For a base table grouped by the complete equality key, a subsequent INNER/SEMI JOIN can use key membership instead of the original duplicate counts. Grouping by additional columns, arbitrary aggregate results, HAVING on aggregates, and LIMIT do not inherit the original distribution. Supported subgraphs below these boundaries remain useful.

LEFT/RIGHT JOINs whose optional side has at most one matching row preserve the retained-side provenance. For two base inputs joined by a complete equality key, fully covered predicates also allow estimation with duplicates: inner matches plus retained-side rows minus known matched retained rows. The subtraction uses exact membership from a single selected pair-matrix slice, or known head matches when several optional-side slices overlap. It never subtracts a SEMI upper bound. A duplicating outer JOIN still ends provenance for subsequent joins; NULL-extended rows are not treated as original base rows. Other outer-join cases use ordinary estimation.

Iceberg equality-delete rewrites retain the original source at the complete union after applying deletes. Individual data-file subsets and delete-file scans do not acquire the whole-table distribution.

For a supported subgraph, the planner solves a bounded entropy problem using row counts, degree moments, functional dependencies and available correlations. RF estimates project onto probe rows rather than using the bag cardinality of the JOIN. Estimates are transferred to the query's table sizes on a best-effort basis, assuming the relative key frequencies and key overlap have stayed the same. INNER JOIN cardinality is multiplied by the size ratio of each contributing output table; SEMI JOIN and RF membership scale only the retained/probe multiplicities. Composition counts a shared source once; separate aliases still contribute separate multiplicities. This transfer is an estimate, not a guaranteed upper bound on changed data.

Size ratios use full-table metadata, never a filtered scan estimate. Iceberg uses `total-records` from the snapshot read by the query, without enumerating files; snapshots with delete records do not supply a live-row count. Native tables use FE tablet row counters, which may lag and are treated as unknown until reported. When a current size is unavailable, the corresponding ratio stays one. Statistics collected from an empty table cannot be extrapolated to a newly populated table, so that object falls back to ordinary estimation. Outer-join row-preservation and matched-row subtraction require matching source versions and fall back otherwise. ANALYZE remains necessary after changes to key distributions or predicate correlations; multiplying row counts cannot recover those changes.

For a star sharing one key, the solver uses an exact smaller formulation in terms of key entropy and each source's conditional row entropy. It has the same optimum as the full Shannon formulation. Graphs with different keys use the general formulation.

## Storage, loading and controls

The definition and published-generation manifest are journaled and included in FE images. Distribution data is encoded as a versioned, checksummed binary payload, compressed with Zstandard, and stored in bounded parts in `default_catalog._statistics_.join_statistics`. The statistics worker creates this table. The planner never parses JSON distribution objects.

The FE cache stores prepared immutable generations. Heads use compact integer, presence-bitset or sparse representations. Sparse tail rows share a compact bucket index across their moments; populated tails keep direct dense rows. Subgraphs and projections read the same stored tail basis without allocating projected bucket arrays. Pair matrices are loaded directly into immutable primitive arrays. Loading checks the generation and checksum; a failed load is retryable and is not cached as an empty statistic. The existing `enable_sync_statistics_load` FE option also controls JOIN-statistics loading. With asynchronous loading, the first plan may use ordinary statistics while the generation is being loaded. A planner invocation retains one consistent generation per object.

| Control | Default | Purpose |
| --- | --- | --- |
| `statistic_join_collect_memory_limit` | 2 GiB | Per-BE query budget for internal collection statements; the load budget is capped at 512 MiB. Spill is enabled. |
| `statistic_join_object_max_bytes` | 256 MiB | Maximum serialized or prepared object size. |
| `statistic_join_cache_max_bytes` | 512 MiB | Mutable cache weight budget for prepared generations; positive bytes. |
| `statistic_join_optimizer_budget_ms` | 60 ms | Total query-local budget for matching and entropy solves, excluding statistics I/O. |
| `enable_statistics_collect_profile` | false | Profiles for internal collection queries. |
| `cbo_enable_join_statistics` | true | Session control for applying JOIN statistics. |
| `cbo_enable_join_statistics_composition` | true | Combine compatible partial objects within the query-local work and memory budgets. |

Collection uses compact exact integer states for one or two scalar integer domains. Compound keys and three-domain definitions use typed exact aggregation with spill under the same memory budget; their intermediate size depends on distinct tuples, not the head budget.

Collection runs one object at a time; cache loading allows two simultaneous objects. Solves and local result caching are bounded. A timeout or unavailable generation falls back to ordinary estimates. For an A/B plan comparison, toggle `cbo_enable_join_statistics` while keeping local MCV and all other settings unchanged. DROP is the persistent way to retire an unhelpful statistics object.

`TRACE VALUES OPTIMIZER SELECT ...` reports `JoinStatistics.Estimates`, `MemoHits`, `LoadMisses`, `BudgetFallbacks`, `EstimationMicros`, and `LoadMicros` when those events occur. `RfEstimates` and `RfFallbacks` identify membership requests during RF planning. `TRACE LOGS OPTIMIZER SELECT ...` identifies the applied object, generation, source/output masks and row estimate. `Compositions`, `CompositionSnapshotConflicts`, and `CompositionSizeFallbacks` report partial-object composition. Composition logs list the contributing definitions. These traces distinguish applying an estimate from falling back while an object is still loading or after the planning budget is exhausted.

Non-anonymized query dumps include the JOIN-statistics generations consulted by the planner only when the exporting user has SELECT on every source of the definition, including sources outside the query. Permissions are checked at export time. If access is denied or cannot be verified, the entire object is omitted with a generic notice; the query and the rest of the dump remain available. Replay remaps recreated table identities and uses only captured generations, without reading the live JOIN-statistics registry. Anonymized dumps omit these predicate values and report this omission explicitly. JOIN payloads remain excluded even if anonymization fails and the serializer falls back to ordinary content.


The JOIN cache budget can be changed without restarting FE:

```sql
ADMIN SET FRONTEND CONFIG ("statistic_join_cache_max_bytes" = "1073741824");
```

The existing cache applies the new limit on the next configuration refresh, normally within 10 seconds. Increasing it preserves warm generations; decreasing it evicts excess generations without dropping definitions or persisted statistics. Loads already in progress use the new retention budget when they complete. The limit covers estimated retained cache memory, not temporary loading or optimizer allocations.

### Predicates missing from collected slices

JOIN statistics use best-effort growth scaling from available full-table metadata; a metadata
refresh or file enumeration is not required. A predicate value absent from the stored dictionary
is not treated as proof of an empty current table or JOIN.

When the ordinary scan estimator (including available basic, histogram and local MCV statistics)
expects more rows than the selected known slices cover, and a requested value or combination is
missing, the planner keeps the known slices and estimates the missing mass from the other observed
slices. The latter are weighted to that missing row count. This assumes that the new values have
similar JOIN-key frequencies to the remaining observed distribution. It does not recover new keys
or changed correlations from old statistics. If the stored source was empty, no distribution is
available and the ordinary estimator remains in use.

The mixed distribution is temporary and is not stored in the statistics cache. INNER JOIN uses
frequency products; SEMI JOIN and runtime filters use membership probabilities, so duplicate build
rows do not multiply the number of passing probe rows. Only first-order correlations of an
extrapolated distribution are used: expected support and frequency products are not presented as
simultaneous hard constraints or higher moments of an exact distribution. Unchanged covered slices
continue to use the full collected information. Multiple compatible partial objects can still be
combined by the existing bounded estimator. A zero estimate from old support is ignored when all
current scan estimates are positive.

### Skew key labels

The numeric head retains up to 16,384 positions per equality domain independently of label storage.
Scalar integer labels use compact numeric arrays and do not consume the text budget. Textual and
compound labels of at most 256 UTF-8 bytes are always retained; remaining labels share the unused
portion of a 4 MiB text budget across the entire object. If the guaranteed short labels alone exceed
4 MiB, they are preserved and no longer labels are added. Three full short-label dictionaries can
therefore retain up to 12 MiB of text. UUID strings fit the short-label guarantee. No value is truncated.
Long labels are considered in head importance order, with equal initial shares across textual domains
and redistribution of unused shares. Only lengths are read before selecting labels, so oversized
values are not transferred to FE merely to discard them. Refresh the object to populate longer labels.

Automatic skew V2 rejects a partial candidate if an omitted label has a known frequency at least as
large as the largest selected key and meets the existing single-key skew threshold. This conservative
guard also skips histogram fallback for that candidate side. It does not change manual skew hints or
numeric JOIN/RF estimates. A less frequent omitted key does not by itself disable a useful rewrite.
