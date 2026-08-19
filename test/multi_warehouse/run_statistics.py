#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Statistics routing checks for a disposable shared-data cluster."""

import argparse
import json
from pathlib import Path
import time
import uuid

import pymysql


def execute(conn, sql):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def records(conn, sql):
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def set_config(conn, name, value):
    with conn.cursor() as cursor:
        cursor.execute(f'ADMIN SET FRONTEND CONFIG ("{name}" = %s)', (str(value),))


def expect_error(conn, sql, message):
    try:
        execute(conn, sql)
    except pymysql.MySQLError as error:
        assert message.lower() in str(error).lower(), str(error)
        return
    raise AssertionError(f"Statement unexpectedly succeeded: {sql}")


def profile_ids(conn):
    return {row[0] for row in execute(conn, "SHOW PROFILELIST LIMIT 1000")}


def verify_collection(conn, before, table, warehouse, port, all_ports, output, label):
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        profiles = records(conn, "SHOW PROFILELIST LIMIT 1000")
        found = []
        for row in profiles:
            values = list(row.values())
            query_id = values[0]
            # Collection SQL names the source table and inserts statistics into _statistics_.
            if query_id in before:
                continue
            profile = execute(conn, f"SELECT get_query_profile('{query_id}')")[0][0]
            if not profile or table not in profile or "_statistics_" not in profile:
                continue
            assert f":{port}" in profile, (label, "expected backend absent", profile)
            assert all(f":{other}" not in profile for other in all_ports if other != port), profile
            assert f"Warehouse: {warehouse}\n" in profile, profile
            Path(output, f"{label}-{query_id}.profile").write_text(profile)
            found.append(query_id)
        statuses = records(conn, "SHOW ANALYZE STATUS")
        finished = any(table in str(row) and "SUCCESS" in str(row) and row.get("Warehouse") == warehouse
                       for row in statuses)
        if found and finished:
            print(json.dumps({"check": label, "result": "PASS", "warehouse": warehouse,
                              "backend_port": port, "profiles": found}), flush=True)
            return
        time.sleep(0.5)
    raise AssertionError((label, "collection profile/status not found", profiles, statuses))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9030)
    parser.add_argument("--etl-warehouse", default="mw_etl")
    parser.add_argument("--stats-warehouse", default="mw_stats")
    parser.add_argument("--etl-be-port", type=int, default=9160)
    parser.add_argument("--stats-be-port", type=int, default=9360)
    parser.add_argument("--default-be-port", type=int, default=9060)
    parser.add_argument("--output", default="/tmp/multi-warehouse-statistics")
    args = parser.parse_args()
    Path(args.output).mkdir(parents=True, exist_ok=True)
    root = pymysql.connect(host=args.host, port=args.port, user="root", password="", autocommit=True,
                           read_timeout=120)
    suffix = uuid.uuid4().hex[:10]
    db, user = "mw_statistics", "mw_stats_" + suffix
    manual, scheduled, immediate, firstload = [name + suffix for name in
                                               ("manual_", "scheduled_", "immediate_", "firstload_")]
    ports = [args.etl_be_port, args.stats_be_port, args.default_be_port, 9260]
    settings = {
        "enable_statistics_collect_profile": "true",
        "enable_auto_collect_statistics": "false",
        "enable_statistic_collect_on_first_load": "false",
        "enable_trigger_analyze_job_immediate": "false",
        # Route checks should not wait for the periodic lake tablet row-count refresh.
        "statistic_partition_healthy_v2": "false",
        "statistic_collect_interval_sec": "5",
        "statistic_collect_warehouse": args.stats_warehouse,
    }
    previous = {key: execute(root, f'ADMIN SHOW FRONTEND CONFIG LIKE "{key}"')[0][2] for key in settings}
    jobs = []

    def verify(before, table, warehouse, port, label):
        verify_collection(root, before, table, warehouse, port, ports, args.output, label)

    def create_table(table, load=True):
        execute(root, f"CREATE TABLE {db}.{table} (v INT) DUPLICATE KEY(v) "
                      'DISTRIBUTED BY HASH(v) BUCKETS 2 PROPERTIES ("replication_num"="1")')
        if load:
            execute(root, f"INSERT INTO {db}.{table} VALUES (10), (20)")

    def create_job(table):
        execute(root, f'CREATE ANALYZE FULL TABLE {db}.{table} PROPERTIES '
                      '("statistic_auto_collect_ratio"="1", "statistic_auto_collect_interval"="1")')
        job = next(row[0] for row in execute(root, "SHOW ANALYZE JOB") if table in row)
        jobs.append(job)
        return job

    try:
        for key, value in settings.items():
            set_config(root, key, value)
        execute(root, f"CREATE DATABASE IF NOT EXISTS {db}")
        execute(root, f"SET WAREHOUSE {args.etl_warehouse}")
        create_table(manual)
        create_table(scheduled)
        create_table(immediate)
        execute(root, f"CREATE USER {user} IDENTIFIED BY ''")
        execute(root, f"GRANT SELECT, INSERT ON ALL TABLES IN DATABASE {db} TO USER {user}")
        execute(root, f"GRANT USAGE ON WAREHOUSE {args.etl_warehouse} TO USER {user}")
        with pymysql.connect(host=args.host, port=args.port, user=user, password="", autocommit=True,
                             read_timeout=120) as caller:
            execute(caller, f"SET WAREHOUSE {args.etl_warehouse}")
            for mode in ("sync", "async"):
                before = profile_ids(root)
                execute(caller, f"ANALYZE FULL TABLE {db}.{manual} WITH {mode} MODE")
                if mode == "async":
                    execute(caller, "SET WAREHOUSE default_warehouse")
                verify(before, manual, args.etl_warehouse, args.etl_be_port, f"manual_{mode}")
            execute(caller, f"SET WAREHOUSE {args.etl_warehouse}")
            execute(root, f"REVOKE USAGE ON WAREHOUSE {args.etl_warehouse} FROM USER {user}")
            for mode in ("sync", "async"):
                expect_error(caller, f"ANALYZE TABLE {db}.{manual} WITH {mode} MODE", "USAGE")
            print(json.dumps({"check": "manual_usage_revoked_in_existing_session", "result": "PASS"}), flush=True)

        before = profile_ids(root)
        job = create_job(scheduled)
        verify(before, scheduled, args.stats_warehouse, args.stats_be_port, "scheduled_job")
        set_config(root, "statistic_collect_warehouse", "")
        before = profile_ids(root)
        execute(root, f"INSERT INTO {db}.{scheduled} VALUES (30), (40), (50)")
        verify(before, scheduled, "default_warehouse", args.default_be_port, "existing_job_inherits_background")
        execute(root, f"DROP ANALYZE {job}")
        jobs.remove(job)

        set_config(root, "statistic_collect_warehouse", args.stats_warehouse)
        set_config(root, "enable_trigger_analyze_job_immediate", "true")
        before = profile_ids(root)
        job = create_job(immediate)
        verify(before, immediate, args.stats_warehouse, args.stats_be_port, "immediate_job")
        execute(root, f"DROP ANALYZE {job}")
        jobs.remove(job)

        set_config(root, "enable_statistic_collect_on_first_load", "true")
        create_table(firstload, load=False)
        before = profile_ids(root)
        execute(root, f"INSERT INTO {db}.{firstload} VALUES (60), (70)")
        verify(before, firstload, args.stats_warehouse, args.stats_be_port, "first_load")
        expect_error(root, f"DROP WAREHOUSE {args.stats_warehouse}", "configured")

        set_config(root, "statistic_collect_warehouse", "missing_stats_warehouse")
        before = profile_ids(root)
        execute(root, f"ANALYZE FULL TABLE {db}.{manual} WITH SYNC MODE")
        verify(before, manual, args.etl_warehouse, args.etl_be_port, "manual_independent_of_auto_config")
        assert execute(root, f"SELECT SUM(v) FROM {db}.{manual}") == ((30,),)
    finally:
        for job in jobs:
            execute(root, f"DROP ANALYZE {job}")
        for key, value in previous.items():
            set_config(root, key, value)
        execute(root, f"DROP USER IF EXISTS {user}")
        root.close()


if __name__ == "__main__":
    main()
