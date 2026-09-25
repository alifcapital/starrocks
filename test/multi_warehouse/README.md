# Multi-warehouse isolation integration checks

Run only on a disposable cluster: the script creates users and tables and truncates
`mw_isolation.t` and its writer tables. It temporarily changes global queue variables
and restores them when the queue check finishes.

Requirements:

- A shared-data FE built from this branch with `enable_multi_warehouse=true` and
  `enable_query_queue_v2=true`.
- Warehouses `default_warehouse`, `mw_etl`, and `mw_interactive`, each with its own CN.
- Both `lake_background_warehouse` and `lake_compaction_warehouse` set to `default_warehouse`.
- Additional warehouses may be present.
- Root SQL access without a password on this disposable fixture, Python with `pymysql`,
  and `curl`. Run from a host that can reach the CN addresses returned by the FE.

The default fixture uses FE SQL/HTTP ports 9030/8030 and CN BE service ports
9160 (ETL) and 9260 (interactive), and interactive CN HTTP port 8240. Override these
with the script arguments if needed. Target the current leader with `--host`; if the
CNs use a different address, pass it with `--cn-host`.

```sh
python3 test/multi_warehouse/run_isolation.py --phase verify
```

This checks shared table visibility, writes from both warehouses, user defaults,
USAGE enforcement through SET, hints, FE HTTP and direct CN HTTP, revocation in an existing SQL session,
an invalid user default after dropping its warehouse,
SELECT and INSERT execution profiles, and continued interactive execution while ETL
has a running query and another query waiting in its queue.

Stop only the ETL CN and wait for its heartbeat to become dead, then run:

```sh
python3 test/multi_warehouse/run_isolation.py --phase unavailable
```

Restart the ETL CN, restart the FE using the same metadata directory, wait for the
CNs to become alive, then run:

```sh
python3 test/multi_warehouse/run_isolation.py --phase restart
```

The restart check allows up to 60 seconds for StarMgr to register a live worker after
the FE heartbeat becomes alive. It only retries the specific missing-replica error.

Profiles and the node inventory are saved under `/tmp/multi-warehouse-profiles`.
Distinct CN processes on a single host verify routing and queue accounting; they do
not measure the performance isolation provided by separate physical machines.

## Statistics collection routing

`run_statistics.py` uses a separate disposable fixture with a populated
`default_warehouse`, `mw_etl`, and `mw_stats` (BE ports 9060, 9160, and 9360).
Background work and compaction use `default_warehouse`. The statistics tables must
have been created before running the script. The script creates tables in
`mw_statistics`, creates and removes a test user and ANALYZE jobs, and temporarily
changes FE statistics settings. It restores those settings when finished; run it
against a single-FE test cluster without other statistics jobs.

```sh
python3 test/multi_warehouse/run_statistics.py
```

The checks cover manual sync/async ANALYZE in the session warehouse, USAGE revoked
in an existing session, a scheduled job using the statistics warehouse, the same
job following a changed configuration, immediate job execution, first-load
collection, and manual collection with an invalid automatic-collection warehouse.
The profiles must contain only the expected warehouse's backend. Profiles are
saved under `/tmp/multi-warehouse-statistics`. A deterministic unit test holds an
async task until after the submitting session changes warehouse, covering the race
that a live test cannot reliably force.

## Query and warehouse observability

`run_observability.py` requires a disposable cluster with one CN in each of
`default_warehouse`, `mw_etl`, `mw_interactive`, and `mw_stats`. The ETL and
interactive CNs use BRPC ports 8160 and 8260; the ETL BE service uses 9160.
The script creates a table in `mw_observability`, creates and removes a shared
resource group, temporarily changes queue settings, and restores them afterward.

```sh
python3 test/multi_warehouse/run_observability.py --observer-host 172.17.0.3
```

The optional observer must have a different IP and the same SQL port as the leader.
The script checks local and global current queries, full processlist, actual query
hosts, running and pending counts, queue SQL with history disabled, Prometheus
warehouse labels, resource group usage across warehouses, and asynchronous FULL
ANALYZE status and scan profiles. It also checks task runs, INSERT load history,
MV refresh history, and compaction history. Both FE and CN binaries must include this patch.
Results are saved under `/tmp/multi-warehouse-observability`.

## Dictionary refresh

`run_dictionary.py` uses the same four-warehouse fixture and must connect to its leader FE.
It checks manual refresh and CREATE warmup on the submitting session's CN, warehouse USAGE,
session changes after submission, current_queries and actual fragment hosts, waiting in
the ETL queue while interactive stays available, and local dictionary_get() in every warehouse.
An automatic refresh after a manual run must return to the background warehouse (default).
All CNs must be reachable: the resulting cache is broadcast to all warehouses even when
the source scan runs on ETL. Temporary dictionaries, tables and users are removed afterward.

```sh
python3 test/multi_warehouse/run_dictionary.py --host LEADER_IP
```

## Task assignment and disabling multi-warehouse

`run_lifecycle.py` uses the four-warehouse fixture above. It creates persistent fixtures
in `mw_lifecycle` and a user named `mw_lifecycle_user`. Keep these between phases:

```sh
python3 test/multi_warehouse/run_lifecycle.py --phase prepare
python3 test/multi_warehouse/run_lifecycle.py --phase enabled
```

The enabled phase checks explicit SUBMIT TASK placement, ALTER TASK placement,
USAGE, scheduled execution, MV refresh, statistics and stream load. It changes the
scheduled task to `mw_interactive` and leaves it suspended between checks.

Set `enable_multi_warehouse=false` in every FE configuration, restart all FEs with
the same metadata directories, and stop the ETL, interactive and statistics CNs.
Keep the default CN running, then run:

```sh
python3 test/multi_warehouse/run_lifecycle.py --phase disabled
```

The disabled phase checks actual backend ports in profiles, successful execution of
saved task/MV/statistics assignments, retained task/MV definitions, and stream load.
It also checks automatic collection with an unavailable configured warehouse name.

Start the three CNs, restore `enable_multi_warehouse=true` on all FEs, restart them,
and run `--phase restored`. Use `--host` to target the current leader. Output goes to
`/tmp/multi-warehouse-lifecycle`. The flag requires a restart; these tests do not
assert that an older binary without the patch can read the persisted metadata.

## Queue counters and leader failover

`run_queue_counters.py --observer-host 172.17.0.3` verifies exact total/timeout counter
deltas across the leader and observer. Counters belong to the FE receiving the query;
sum their rates across FEs. Queue gauges belong to the leader.

`run_failover.py` requires three live electable FEs, an observer, and the lifecycle
fixtures. It stops the current leader while queries submitted through the observer
are running and queued. Run it on a disposable host only. Supply either the local
leader PID file or the leader container, and an available follower distinct from that
leader:

```sh
python3 test/multi_warehouse/run_failover.py --host 127.0.0.1 \
  --observer-host 172.17.0.3 --follower-host 172.17.0.4 \
  --leader-pid-file /path/to/test-leader/bin/fe.pid
```

The test checks metadata, grants, query completion and actual CNs. It separately records
whether a queued query resumed before the previously running query finished; this
reveals whether running slots survive the leadership change. Restart the stopped FE
after the check. Evidence goes to `/tmp/multi-warehouse-failover`.

Known limitation: allocated slots are not reconstructed on the new leader. A queued
query can start while a query admitted by the previous leader is still running,
temporarily exceeding the concurrency limit within that warehouse. The script reports
this as `LIMITATION` separately from metadata, grant and execution-routing checks.
