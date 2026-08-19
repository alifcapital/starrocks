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

"""Integration checks for an isolated, disposable three-warehouse cluster."""

import argparse
import concurrent.futures
import json
from pathlib import Path
import subprocess
import time
import uuid

import pymysql


ARGS = None
DB = "mw_isolation"
WAREHOUSES = ("default_warehouse", "mw_etl", "mw_interactive")


def connect(user="root", warehouse="default_warehouse"):
    conn = pymysql.connect(host=ARGS.host, port=ARGS.port, user=user, password="",
                           autocommit=True, read_timeout=90)
    if warehouse:
        execute(conn, f"SET warehouse = '{warehouse}'")
    return conn


def execute(conn, sql):
    with conn.cursor() as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def expect_error(conn, sql, text):
    try:
        execute(conn, sql)
    except pymysql.MySQLError as error:
        assert text.lower() in str(error).lower(), (sql, str(error))
        return
    raise AssertionError(f"Statement unexpectedly succeeded: {sql}")


def stream_load(user, warehouse=None, value=31, http_port=None):
    command = ["curl", "--silent", "--show-error", "--noproxy", "*", "--location-trusted",
               "--max-time", "30", "-u", user + ":", "-X", "PUT",
               "-H", "Expect: 100-continue", "-H", "format: csv",
               "-H", "label: mw_" + uuid.uuid4().hex]
    if warehouse:
        command.extend(["-H", "warehouse: " + warehouse])
    host = (ARGS.cn_host or ARGS.host) if http_port else ARGS.host
    command.extend(["--data-binary", str(value) + "\n",
                    f"http://{host}:{http_port or ARGS.http_port}/api/{DB}/t/_stream_load"])
    response = subprocess.check_output(command, text=True)
    result = json.loads(response)
    return result


def report(name, **details):
    print(json.dumps({"check": name, "result": "PASS", **details}), flush=True)


def verify_shared_tables(root):
    execute(root, f"CREATE DATABASE IF NOT EXISTS {DB}")
    execute(root, "SET warehouse = 'mw_etl'")
    execute(root, f"CREATE TABLE IF NOT EXISTS {DB}.t (v INT) DUPLICATE KEY(v) "
                  'DISTRIBUTED BY HASH(v) BUCKETS 2 PROPERTIES ("replication_num"="1")')
    execute(root, f"TRUNCATE TABLE {DB}.t")
    execute(root, f"INSERT INTO {DB}.t VALUES (10), (20)")
    for wh in WAREHOUSES:
        execute(root, f"SET warehouse = '{wh}'")
        assert execute(root, f"SELECT sum(v) FROM {DB}.t") == ((30,),)
    execute(root, f"INSERT INTO {DB}.t VALUES (3)")
    execute(root, "SET warehouse = 'mw_etl'")
    assert execute(root, f"SELECT sum(v) FROM {DB}.t") == ((33,),)
    report("shared_table_read_write")


def verify_authorization(root):
    for user, wh in (("mw_etl_user", "mw_etl"), ("mw_interactive_user", "mw_interactive")):
        execute(root, f"CREATE USER IF NOT EXISTS {user} IDENTIFIED BY ''")
        execute(root, f"GRANT SELECT, INSERT ON ALL TABLES IN DATABASE {DB} TO USER {user}")
        execute(root, f"GRANT USAGE ON WAREHOUSE {wh} TO USER {user}")
        execute(root, f"GRANT CREATE TABLE ON DATABASE {DB} TO USER {user}")
        execute(root, f"ALTER USER '{user}' SET PROPERTIES ('session.warehouse' = '{wh}')")
    with connect("mw_etl_user", None) as conn:
        assert execute(conn, "SELECT @@warehouse")[0][0] == "mw_etl"
        assert execute(conn, f"SELECT sum(v) FROM {DB}.t") == ((33,),)
        expect_error(conn, "SET WAREHOUSE mw_interactive", "USAGE")
        expect_error(conn, "SET warehouse = 'mw_interactive'", "USAGE")
        expect_error(conn, "SET @@session.warehouse = 'mw_interactive'", "USAGE")
        expect_error(conn, f"SELECT /*+ SET_VAR(warehouse='mw_interactive') */ sum(v) FROM {DB}.t", "USAGE")
        execute(root, "REVOKE USAGE ON WAREHOUSE mw_etl FROM USER mw_etl_user")
        try:
            expect_error(conn, f"SELECT sum(v) FROM {DB}.t", "USAGE")
            expect_error(conn, f"CREATE TABLE {DB}.denied LIKE {DB}.t", "USAGE")
        finally:
            execute(root, "GRANT USAGE ON WAREHOUSE mw_etl TO USER mw_etl_user")
    loaded = stream_load("mw_etl_user")
    assert loaded.get("Status") == "Success", loaded
    denied = stream_load("mw_etl_user", "mw_interactive", 999)
    assert denied.get("Status") != "Success" and "USAGE" in json.dumps(denied), denied
    denied = stream_load("mw_etl_user", value=999, http_port=ARGS.interactive_http_port)
    assert denied.get("Status") != "Success" and "USAGE" in json.dumps(denied), denied
    report("warehouse_usage_sql_and_http")


def verify_deleted_user_default(root):
    execute(root, "CREATE WAREHOUSE mw_removed")
    execute(root, "CREATE USER IF NOT EXISTS mw_removed_user IDENTIFIED BY ''")
    execute(root, f"GRANT SELECT, INSERT ON ALL TABLES IN DATABASE {DB} TO USER mw_removed_user")
    execute(root, "ALTER USER mw_removed_user SET PROPERTIES ('session.warehouse'='mw_removed')")
    execute(root, "DROP WAREHOUSE mw_removed")
    with connect("mw_removed_user", None) as conn:
        expect_error(conn, f"SELECT sum(v) FROM {DB}.t", "warehouse")
        # A valid explicit switch remains possible without reconnecting.
        execute(root, "GRANT USAGE ON WAREHOUSE mw_interactive TO USER mw_removed_user")
        execute(conn, "SET warehouse='mw_interactive'")
        assert execute(conn, f"SELECT sum(v) FROM {DB}.t") == ((64,),)
    denied = stream_load("mw_removed_user", value=999)
    assert denied.get("Status") != "Success" and "mw_removed" in json.dumps(denied), denied
    report("deleted_user_default_does_not_fall_back")


def verify_profiles(root):
    nodes = execute(root, "SHOW COMPUTE NODES")
    Path(ARGS.output).mkdir(parents=True, exist_ok=True)
    Path(ARGS.output, "compute-nodes.json").write_text(json.dumps(nodes, default=str, indent=2))
    for wh in ("mw_etl", "mw_interactive"):
        with connect(warehouse=wh) as conn:
            execute(conn, "SET enable_profile=true")
            execute(conn, "SET enable_async_profile=false")
            execute(conn, "SET pipeline_profile_level=2")
            table = DB + ".writer_" + wh
            execute(conn, f"CREATE TABLE IF NOT EXISTS {table} LIKE {DB}.t")
            execute(conn, f"TRUNCATE TABLE {table}")
            for kind, sql in (("select", f"SELECT sum(v) FROM {DB}.t"),
                              ("insert", f"INSERT INTO {table} SELECT * FROM {DB}.t")):
                execute(conn, sql)
                query_id = execute(conn, "SELECT last_query_id()")[0][0]
                profile = execute(conn, f"SELECT get_query_profile('{query_id}')")[0][0]
                assert profile, (wh, kind, query_id)
                Path(ARGS.output, wh + "-" + kind + ".profile").write_text(profile)
                # Each process has a distinct BE service port, supplied by the fixture.
                expected_port = ARGS.etl_be_port if wh == "mw_etl" else ARGS.interactive_be_port
                foreign_port = ARGS.interactive_be_port if wh == "mw_etl" else ARGS.etl_be_port
                assert f":{expected_port}" in profile, (wh, kind, "own backend absent from profile")
                assert f":{foreign_port}" not in profile, (wh, kind, "foreign backend in profile")
    report("read_and_write_profiles_use_own_nodes")


def verify_queue(root):
    settings = {"enable_query_queue_select": "true", "query_queue_concurrency_limit": "1"}
    previous = {name: execute(root, f"SHOW GLOBAL VARIABLES LIKE '{name}'")[0][1] for name in settings}
    try:
        for name, value in settings.items():
            execute(root, f"SET GLOBAL {name}={value}")
        execute(root, "SET warehouse='default_warehouse'")
        token = "mw_queue_" + uuid.uuid4().hex

        def query(sql):
            with connect(warehouse="mw_etl") as conn:
                return execute(conn, sql)

        def wait_state(state):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                rows = execute(root, "SELECT STATE FROM information_schema.warehouse_queries "
                               "WHERE WAREHOUSE_NAME='mw_etl'")
                names = {"RUNNING": {"RUNNING", "ALLOCATED"}, "PENDING": {"PENDING", "REQUIRING"}}[state]
                if any(row[0] in names for row in rows):
                    return
                time.sleep(0.2)
            raise AssertionError(("queue state not observed", state, rows))

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            running = executor.submit(query, f"SELECT /* {token} */ sleep(15) FROM {DB}.t LIMIT 1")
            wait_state("RUNNING")
            pending = executor.submit(query, f"SELECT /* {token} */ sum(v) FROM {DB}.t")
            wait_state("PENDING")
            started = time.monotonic()
            with connect(warehouse="mw_interactive") as interactive:
                execute(interactive, f"SELECT sum(v) FROM {DB}.t")
            elapsed = time.monotonic() - started
            assert elapsed < 5, elapsed
            assert not running.done(), "ETL finished before isolation was observed"
            running.result(timeout=30)
            pending.result(timeout=30)
        report("etl_queue_does_not_block_interactive", interactive_seconds=round(elapsed, 3))
    finally:
        for name, value in previous.items():
            execute(root, f"SET GLOBAL {name}='{value}'")


def verify_unavailable(root):
    with connect(warehouse=None) as conn:
        try:
            execute(conn, "SET warehouse='mw_etl'")
            execute(conn, f"SELECT sum(v) FROM {DB}.t")
        except pymysql.MySQLError as error:
            assert "warehouse" in str(error).lower(), str(error)
        else:
            raise AssertionError("Query succeeded while the ETL warehouse was unavailable")
    with connect(warehouse="mw_interactive") as conn:
        execute(conn, f"SELECT sum(v) FROM {DB}.t")
    result = stream_load("mw_etl_user")
    assert result.get("Status") != "Success", result
    report("unavailable_etl_has_no_foreign_fallback")


def main():
    global ARGS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9030)
    parser.add_argument("--http-port", type=int, default=8030)
    parser.add_argument("--cn-host", help="CN address when it differs from the FE address")
    parser.add_argument("--etl-be-port", type=int, default=9160)
    parser.add_argument("--interactive-be-port", type=int, default=9260)
    parser.add_argument("--interactive-http-port", type=int, default=8240)
    parser.add_argument("--output", default="/tmp/multi-warehouse-profiles")
    parser.add_argument("--phase", choices=("verify", "queue", "unavailable", "restart"), default="verify")
    ARGS = parser.parse_args()
    with connect() as root:
        if ARGS.phase == "verify":
            verify_shared_tables(root)
            verify_authorization(root)
            verify_deleted_user_default(root)
            verify_profiles(root)
            verify_queue(root)
        elif ARGS.phase == "queue":
            verify_queue(root)
        elif ARGS.phase == "unavailable":
            verify_unavailable(root)
        else:
            assert set(WAREHOUSES).issubset({row[1] for row in execute(root, "SHOW WAREHOUSES")})
            with connect("mw_etl_user", None) as conn:
                assert execute(conn, "SELECT @@warehouse")[0][0] == "mw_etl"
                deadline = time.monotonic() + 60
                while True:
                    try:
                        assert execute(conn, f"SELECT sum(v) FROM {DB}.t") == ((64,),)
                        break
                    except pymysql.MySQLError as error:
                        # StarMgr can learn that a worker is alive after the FE heartbeat does.
                        if "no queryable replica" not in str(error) or time.monotonic() >= deadline:
                            raise
                        time.sleep(1)
            verify_profiles(root)
            report("fe_restart_preserves_warehouses_nodes_grants_and_tables")


if __name__ == "__main__":
    main()
