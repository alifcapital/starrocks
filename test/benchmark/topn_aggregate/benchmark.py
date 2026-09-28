#!/usr/bin/env python3
"""Synthetic correctness/performance matrix. Run only on an isolated test cluster.

Creates a fresh database; never connects to an external catalog. Install pymysql.
The unpatched baseline is expected to FAIL correctness on incomplete ORDER BY keys.
"""
import argparse
import collections
import datetime
import json
import os
from pathlib import Path
import random
import re
import statistics
import time
import uuid

import pymysql


CASES = {
    # Group repetition crosses hash(id) buckets and pipeline drivers.
    "rare_peers": ("(id DIV 8) % 65536", "id % 2", "id % 3"),
    "many_peers": ("id % 2", "(id DIV 2) % 32768", "id % 3"),
    "few_groups_rare_peers": ("id % 1000", "(id DIV 1000) % 2", "0"),
    "few_groups_null_peers": ("IF(id % 1000 = 0, NULL, id % 1000)", "(id DIV 1000) % 2", "0"),
    "few_groups_many_peers": ("id % 2", "id % 97", "0"),
    "all_equal": ("0", "id % 65536", "id % 3"),
    "rare_rows_many_peers": ("IF(id % 100 = 0, 0, 1)",
                             "IF(id % 100 = 0, (id DIV 100) % 1000, id % 1000)", "0"),
    "hot_first": ("IF(id % 10 < 9, 0, 1 + id % 10000)", "id % 65536", "id % 3"),
    "hot_last": ("IF(id % 10 < 9, 10001, id % 10000)", "id % 65536", "id % 3"),
    "null_prefix": ("IF(id % 10 < 9, NULL, id % 10000)", "id % 65536", "id % 3"),
    "correlated": ("id % 10000", "id % 10000", "(id DIV 10000) % 3"),
    "wide_groups": ("id % 2", "id % 32768", "id % 3"),
}
ORDERS = {
    "asc": "a ASC NULLS FIRST",
    "desc": "a DESC NULLS LAST",
    "null_last": "a ASC NULLS LAST",
    "tuple": "a ASC NULLS FIRST, b DESC NULLS LAST",
    "full": "a ASC NULLS FIRST, b DESC NULLS LAST, c ASC, s ASC",
}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, default=9030)
    ap.add_argument("--user", default="root")
    ap.add_argument("--reuse-database", help="Reuse this fixture and its statistics; requires --stats existing")
    ap.add_argument("--rows", type=int, default=524288)
    ap.add_argument("--rounds", type=int, default=7)
    ap.add_argument("--cases", nargs="+", choices=CASES, default=list(CASES))
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--orders", nargs="+", choices=ORDERS, default=["asc"])
    ap.add_argument("--limits", nargs="+", type=int, default=[10])
    ap.add_argument("--dops", nargs="+", type=int, default=[8])
    ap.add_argument("--stats", nargs="+", choices=["none", "basic", "multi", "histogram", "existing"],
                    default=["none", "basic", "multi", "histogram"])
    args = ap.parse_args()
    if args.reuse_database:
        if not re.fullmatch(r"topn_bench_[a-zA-Z0-9_]+", args.reuse_database) or args.stats != ["existing"]:
            ap.error("Reuse requires a topn_bench_* database and --stats existing")
    elif "existing" in args.stats:
        ap.error("--stats existing requires --reuse-database")
    args.out.mkdir(parents=True, exist_ok=True)
    db = args.reuse_database or "topn_bench_" + uuid.uuid4().hex[:12]
    conn = pymysql.connect(host=args.host, port=args.port, user=args.user,
                           password=os.environ.get("MYSQL_PWD", ""), autocommit=True,
                           read_timeout=900, write_timeout=900)
    cur = conn.cursor()

    def sql(q):
        cur.execute(q)
        result = cur.fetchall()
        if q.startswith("ANALYZE"):
            with (args.out / "analyze.jsonl").open("a") as out:
                out.write(json.dumps({"sql": q, "result": result}, default=str) + "\n")
            if any(str(cell).lower() in ("error", "failed") for row in result for cell in row):
                raise RuntimeError((q, result))
        return result

    def set_mode(mode, dop):
        sql(f"SET topn_push_down_agg_mode={mode}")
        sql(f"SET pipeline_dop={dop}")

    if not args.reuse_database:
        sql(f"CREATE DATABASE {db}")
    sql(f"USE {db}")
    metadata = {"database": db, "args": vars(args) | {"out": str(args.out)},
                "version": sql("SELECT current_version()"), "started": datetime.datetime.now().isoformat()}
    (args.out / "metadata.json").write_text(json.dumps(metadata, indent=2, default=str))
    print(f"Created {db}; tables are retained for inspection", flush=True)
    for setting in ("query_timeout=600", "enable_query_cache=false", "enable_plan_advisor=false",
                    "enable_runtime_adaptive_dop=false", "new_planner_agg_stage=2",
                    "enable_profile=false", "enable_async_profile=false", "pipeline_profile_level=2",
                    "enable_query_trigger_analyze=false", "enable_spill=false", "spill_mode=auto"):
        sql("SET " + setting)
    records = []
    for case in args.cases:
        a, b, c = CASES[case]
        if not args.reuse_database:
            sql(f"""CREATE TABLE {case}(id BIGINT, a INT, b BIGINT, c INT, s VARCHAR(1024), v BIGINT)
                DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 16 PROPERTIES('replication_num'='1')""")
            width = 32 if case == "wide_groups" else 1
            sql(f"""INSERT INTO {case}
                SELECT id, {a}, {b}, {c}, repeat(md5(cast(({b}) AS varchar)), {width}), id % 17 - 8
                FROM (SELECT generate_series AS id FROM TABLE(generate_series(0, {args.rows - 1}))) n""")
        else:
            assert sql(f"SELECT count(*) FROM {case}")[0][0] == args.rows, (case, "fixture row count")
        base = f"SELECT a,b,c,s,count(*) AS n,sum(v) AS total,min(v) AS lo,max(v) AS hi FROM {case} GROUP BY a,b,c,s"
        set_mode(-1, 8)
        truth = {tuple(r[:4]): tuple(r[4:]) for r in sql(base)}
        for phase in args.stats:
            # Phase order is cumulative; use a fresh invocation for individual phases.
            if phase not in ("none", "existing"):
                sql(f"ANALYZE FULL TABLE {case}")
            if phase in ("multi", "histogram"):
                for columns in ("a,b,c,s", "a,b"):
                    sql(f"ANALYZE FULL TABLE {case} MULTIPLE COLUMNS ({columns})")
            if phase == "histogram":
                sql(f"ANALYZE TABLE {case} UPDATE HISTOGRAM ON a WITH 64 BUCKETS "
                    "PROPERTIES('histogram_sample_ratio'='1')")
            # Verify actual plans: stats cache population is asynchronous, so preserve both
            # SHOW outputs and the plan used, rather than assuming ANALYZE chose a route.
            jobs = [(order, limit, dop, mode) for order in args.orders for limit in args.limits
                    for dop in args.dops for mode in (-1, 0, 1)]
            oracles = {}
            for order, limit, dop, mode in jobs:
                set_mode(mode, dop)
                query = base + f" ORDER BY {ORDERS[order]} LIMIT {limit}"
                tag = f"{case}.{phase}.{order}.k{limit}.dop{dop}.mode{mode}"
                if mode == -1:
                    oracles[(order, limit, dop)] = sql(query)
                plan = "\n".join(str(r[0]) for r in sql("EXPLAIN VERBOSE " + query))
                (args.out / (tag + ".plan.txt")).write_text(plan)
            for iteration in range(args.rounds + 1):
                random.Random(iteration).shuffle(jobs)
                for order, limit, dop, mode in jobs:
                    set_mode(mode, dop)
                    query = base + f" ORDER BY {ORDERS[order]} LIMIT {limit}"
                    begin = time.monotonic()
                    result = sql(query)
                    elapsed = time.monotonic() - begin
                    tag = f"{case}.{phase}.{order}.k{limit}.dop{dop}.mode{mode}.r{iteration}"
                    assert len(result) == min(limit, len(truth)), (tag, "incorrect row count")
                    assert len({tuple(r[:4]) for r in result}) == len(result), (tag, "duplicate group")
                    for row in result:
                        assert tuple(row[4:]) == truth[tuple(row[:4])], (tag, row, truth[tuple(row[:4])])
                    # Compare ORDER BY keys against unoptimized top-N, allowing only valid ties.
                    oracle = oracles[(order, limit, dop)]
                    def key(row):
                        return row[:4] if order == "full" else row[:2] if order == "tuple" else row[:1]
                    assert [key(r) for r in result] == [key(r) for r in oracle], (tag, "wrong top groups")
                    record = dict(case=case, stats=phase, order=order, limit=limit, dop=dop,
                                  mode=mode, iteration=iteration, seconds=elapsed)
                    records.append(record)
                    with (args.out / "runs.jsonl").open("a") as out:
                        out.write(json.dumps(record) + "\n")
            # Profiles are collected outside the timed series: profiling itself changes short-query latency.
            sql("SET enable_profile=true")
            for order, limit, dop, mode in jobs:
                set_mode(mode, dop)
                query = base + f" ORDER BY {ORDERS[order]} LIMIT {limit}"
                sql(query)
                query_id = sql("SELECT last_query_id()")[0][0]
                profile = sql("SELECT get_query_profile('%s')" % query_id)[0][0]
                tag = f"{case}.{phase}.{order}.k{limit}.dop{dop}.mode{mode}"
                (args.out / (tag + ".profile.txt")).write_text(profile or "PROFILE UNAVAILABLE")
            sql("SET enable_profile=false")
            print(case, phase, "correct", flush=True)
    grouped = collections.defaultdict(list)
    for r in records:
        if r["iteration"]:
            grouped[tuple(r[k] for k in ("case", "stats", "order", "limit", "dop", "mode"))].append(r["seconds"])
    summary = [{"case": k[0], "stats": k[1], "order": k[2], "limit": k[3], "dop": k[4], "mode": k[5],
                "median_seconds": statistics.median(v), "min_seconds": min(v), "max_seconds": max(v)}
               for k, v in grouped.items()]
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2))
    print("PASS", db, flush=True)


if __name__ == "__main__":
    main()
