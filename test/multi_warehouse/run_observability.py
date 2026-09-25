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

"""Warehouse observability checks on a disposable shared-data cluster."""

import argparse
import concurrent.futures
import json
from pathlib import Path
import time
import urllib.request
import uuid

import pymysql


def rows(conn, sql):
    with conn.cursor(pymysql.cursors.DictCursor) as cursor:
        cursor.execute(sql)
        return cursor.fetchall()


def scalar(conn, sql):
    return next(iter(rows(conn, sql)[0].values()))


def wait_for(fetch, accept, description, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = fetch()
        if accept(value):
            return value
        time.sleep(0.2)
    raise AssertionError((description, value))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9030)
    parser.add_argument('--http-port', type=int, default=8030)
    parser.add_argument('--observer-host')
    parser.add_argument('--observer-port', type=int, default=9030)
    parser.add_argument('--output', default='/tmp/multi-warehouse-observability')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    def connect(warehouse='default_warehouse', observer=False):
        conn = pymysql.connect(host=args.observer_host if observer else args.host,
                               port=args.observer_port if observer else args.port,
                               user='root', autocommit=True, read_timeout=90)
        rows(conn, f"SET warehouse='{warehouse}'")
        return conn

    def save(name, data):
        (output / (name + '.json')).write_text(json.dumps(data, indent=2, default=str))

    def report(check):
        print(json.dumps({'check': check, 'result': 'PASS'}), flush=True)

    root = connect()
    settings = {'enable_query_queue_select': 'true', 'enable_query_queue_statistic': 'true',
                'query_queue_concurrency_limit': '1'}
    previous = {key: list(rows(root, f"SHOW GLOBAL VARIABLES LIKE '{key}'")[0].values())[1]
                for key in settings}
    history = list(rows(root, 'ADMIN SHOW FRONTEND CONFIG LIKE "max_query_queue_history_slots_number"')[0].values())[2]
    profile = list(rows(root, 'ADMIN SHOW FRONTEND CONFIG LIKE "enable_statistics_collect_profile"')[0].values())[2]
    mv_profile = list(rows(root, 'ADMIN SHOW FRONTEND CONFIG LIKE "enable_mv_refresh_collect_profile"')[0].values())[2]
    auto_configs = {name: list(rows(root, f'ADMIN SHOW FRONTEND CONFIG LIKE "{name}"')[0].values())[2]
                    for name in ('statistic_collect_warehouse', 'enable_trigger_analyze_job_immediate',
                                 'statistic_partition_healthy_v2', 'enable_statistic_collect')}
    analyze_jobs = []
    workers = []
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    token = 'obs_' + uuid.uuid4().hex[:12]
    group = 'mw_obs_' + uuid.uuid4().hex[:8]

    def start(warehouse, tag, observer=False):
        conn = connect(warehouse, observer)
        rows(conn, f"SET resource_group='{group}'")
        query = f'SELECT /* {token}_{tag} */ sleep(45) FROM mw_observability.t LIMIT 1'
        cid = scalar(conn, 'SELECT connection_id()')
        future = pool.submit(rows, conn, query)
        workers.append((conn, cid, observer, future))
        return cid

    def stop_workers():
        for conn, cid, observer, future in workers:
            if not future.done():
                with connect(observer=observer) as control:
                    rows(control, f'KILL QUERY {cid}')
            try:
                future.result(timeout=15)
            except pymysql.MySQLError as error:
                assert 'cancel' in str(error).lower() or 'killed' in str(error).lower(), str(error)
            conn.close()
        workers.clear()

    def current(conn=root, global_view=False):
        name = 'global_current_queries' if global_view else 'current_queries'
        return rows(conn, f"SHOW PROC '/{name}'")

    def wait_allocated(tag):
        return wait_for(lambda: rows(root, 'SELECT * FROM information_schema.warehouse_queries'),
                        lambda data: any(f'{token}_{tag}' in r['QUERY'] and r['STATE'] in ('ALLOCATED', 'RUNNING')
                                         for r in data), 'allocated slot for ' + tag)

    try:
        rows(root, 'CREATE DATABASE IF NOT EXISTS mw_observability')
        rows(root, 'CREATE TABLE IF NOT EXISTS mw_observability.t(v INT) DUPLICATE KEY(v) '
                   'DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES ("replication_num"="1")')
        if not scalar(root, 'SELECT count(*) FROM mw_observability.t'):
            rows(root, 'INSERT INTO mw_observability.t VALUES (1)')
        rows(root, f'CREATE RESOURCE GROUP {group} TO (user="root") '
                   'WITH ("cpu_weight"="1", "mem_limit"="20%")')
        for key, value in settings.items():
            rows(root, f'SET GLOBAL {key}={value}')
        rows(root, 'ADMIN SET FRONTEND CONFIG ("max_query_queue_history_slots_number"="0")')
        rows(root, 'ADMIN SET FRONTEND CONFIG ("enable_statistics_collect_profile"="true")')
        if args.observer_host:
            with connect(observer=True) as observer:
                wait_for(lambda: rows(observer, f'SHOW RESOURCE GROUP {group}'), bool, 'resource group replication')
                wait_for(lambda: scalar(observer, "SHOW GLOBAL VARIABLES LIKE 'enable_query_queue_select'"),
                         lambda value: value is not None, 'observer ready')

        running = start('mw_etl', 'running')
        wait_for(current, lambda data: any(r['ConnectionId'] == str(running) and r['ExecState'] == 'RUNNING'
                                          for r in data), 'ETL running')
        pending = start('mw_etl', 'pending', bool(args.observer_host))
        interactive = start('mw_interactive', 'interactive', bool(args.observer_host))
        snapshot = wait_for(lambda: current(global_view=True),
                            lambda data: sum(r['ResourceGroup'] == group for r in data) == 3
                            and any(r['ResourceGroup'] == group and r['ExecState'] == 'PENDING' for r in data),
                            'three queries across warehouses and FEs')
        mine = [r for r in snapshot if r['ResourceGroup'] == group]
        assert sorted((r['Warehouse'], r['ExecState']) for r in mine) == [
            ('mw_etl', 'PENDING'), ('mw_etl', 'RUNNING'), ('mw_interactive', 'RUNNING')], mine
        save('global_current_queries', snapshot)
        save('current_queries', current())
        if args.observer_host:
            assert sum(r['ResourceGroup'] == group for r in current()) == 1
            with connect(observer=True) as observer:
                remote = current(observer)
                assert sum(r['ResourceGroup'] == group for r in remote) == 2, remote
                save('observer_current_queries', remote)
                save('observer_global_current_queries', current(observer, True))
        processlist = rows(root, 'SHOW FULL PROCESSLIST')
        save('processlist', processlist)
        clients = [r for r in processlist if token in (r['Info'] or '')]
        assert len(clients) == 3, clients
        assert sum(r['IsPending'] == 'true' for r in clients) == 1, clients
        for query in mine:
            # The local PROC tree belongs to the FE shown in the row.
            host = query['feIp']
            with pymysql.connect(host=host, port=args.port, user='root', autocommit=True) as owner:
                details = rows(owner, f"SHOW PROC '/current_queries/{query['QueryId']}'")
                hosts = rows(owner, f"SHOW PROC '/current_queries/{query['QueryId']}/hosts'")
            save('query_' + query['QueryId'], {'details': details, 'hosts': hosts})
            assert token in str(details), details
            if query['ExecState'] == 'RUNNING':
                expected_port = 8160 if query['Warehouse'] == 'mw_etl' else 8260
                assert f':{expected_port}' in str(hosts), hosts
                assert f':{8260 if expected_port == 8160 else 8160}' not in str(hosts), hosts
        report('current_queries_processlist_and_actual_hosts')

        for sql in ['SHOW WAREHOUSES', "SHOW PROC '/warehouses'"]:
            warehouses = rows(root, sql)
            save('warehouse_counts_' + str(len(sql)), warehouses)
            by_name = {r['Name']: r for r in warehouses}
            assert int(by_name['default_warehouse']['NodeCount']) == 1, by_name
            assert (int(by_name['mw_etl']['RunningSql']), int(by_name['mw_etl']['QueuedSql'])) == (1, 1), by_name
            assert (int(by_name['mw_interactive']['RunningSql']), int(by_name['mw_interactive']['QueuedSql'])) == (1, 0)
        queue = rows(root, 'SELECT * FROM information_schema.warehouse_queries')
        save('warehouse_queries_history_disabled', queue)
        tagged = [r for r in queue if token in r['QUERY']]
        assert len(tagged) == 3, queue
        running_rows = rows(root, 'SHOW RUNNING QUERIES')
        save('running_queries', running_rows)
        assert {'mw_etl', 'mw_interactive'} <= {r['Warehouse'] for r in running_rows}, running_rows
        metrics = urllib.request.urlopen(f'http://{args.host}:{args.http_port}/metrics', timeout=10).read().decode()
        (output / 'metrics.txt').write_text(metrics)
        assert any('warehouse_name="mw_etl"' in line and 'field="query_pending_length"' in line
                   and line.endswith(' 1') for line in metrics.splitlines()), 'warehouse pending metric missing'
        usage = wait_for(lambda: rows(root, 'SHOW USAGE RESOURCE GROUPS'),
                         lambda data: {'mw_etl', 'mw_interactive'} <=
                         {r['Warehouse'] for r in data if r['Name'] == group}, 'resource group usage')
        save('resource_group_usage', usage)
        assert {'mw_etl', 'mw_interactive'} <= {r['Warehouse'] for r in usage if r['Name'] == group}, usage
        assert all(r['BackendId'] for r in usage), usage
        report('warehouse_counts_queue_sql_prometheus_and_shared_resource_group')
        stop_workers()
        wait_for(lambda: current(global_view=True), lambda data: not any(r['ResourceGroup'] == group for r in data),
                 'finished queries disappear')

        start('default_warehouse', 'default')
        default_slots = wait_for(lambda: rows(root, 'SHOW RUNNING QUERIES'),
                                 lambda data: any(r['Warehouse'] == 'default_warehouse' for r in data),
                                 'default warehouse slot')
        assert all(str(r['WarehouseId']) == '0' for r in default_slots
                   if r['Warehouse'] == 'default_warehouse'), default_slots
        save('default_running_queries', default_slots)
        stop_workers()

        rows(root, 'SET GLOBAL enable_query_queue_select=false')
        start('mw_etl', 'unqueued_one')
        start('mw_etl', 'unqueued_two')
        wait_for(current, lambda data: sum(r['ResourceGroup'] == group and r['ExecState'] == 'RUNNING'
                                           for r in data) == 2, 'queries bypassing the queue')
        warehouses = rows(root, 'SHOW WAREHOUSES')
        etl = next(r for r in warehouses if r['Name'] == 'mw_etl')
        assert (int(etl['RunningSql']), int(etl['QueuedSql'])) == (2, 0), etl
        save('warehouse_counts_queue_disabled', warehouses)
        stop_workers()
        rows(root, 'SET GLOBAL enable_query_queue_select=true')
        report('default_warehouse_id_and_counts_with_queue_disabled')

        start('mw_etl', 'stats_blocker')
        wait_allocated('stats_blocker')
        before = {r['QueryId'] for r in rows(root, 'SHOW PROFILELIST LIMIT 1000')}
        before_status = {r['Id'] for r in rows(root, 'SHOW ANALYZE STATUS')}
        with connect('mw_etl') as caller:
            rows(caller, 'ANALYZE FULL TABLE mw_observability.t WITH ASYNC MODE')
            rows(caller, 'SET warehouse=default_warehouse')
        stats = wait_for(current, lambda data: any(r['QueryType'] == 'Statistics' and r['ExecState'] == 'PENDING'
                                                  for r in data), 'internal statistics waiting')
        stats = [r for r in stats if r['QueryType'] == 'Statistics']
        assert all(r['Warehouse'] == 'mw_etl' for r in stats), stats
        save('pending_statistics', stats)
        etl = next(r for r in rows(root, 'SHOW WAREHOUSES') if r['Name'] == 'mw_etl')
        assert (int(etl['RunningSql']), int(etl['QueuedSql'])) == (1, 1), etl
        stop_workers()
        status = wait_for(lambda: rows(root, 'SHOW ANALYZE STATUS'),
                          lambda data: any(r['Database'].endswith('.mw_observability') and r['Table'] == 't'
                                           and r['Id'] not in before_status and r['Status'] == 'SUCCESS' for r in data),
                          'statistics completion')
        status = [r for r in status if r['Id'] not in before_status
                  and r['Database'].endswith('.mw_observability') and r['Table'] == 't']
        assert status and all(r['Warehouse'] == 'mw_etl' for r in status), status
        save('analyze_status', status)
        table_status = rows(root, "SELECT * FROM information_schema.analyze_status WHERE `DATABASE`='mw_observability'")
        table_status = [r for r in table_status if r['Id'] not in before_status]
        assert table_status and all(r['Warehouse'] == 'mw_etl' for r in table_status), table_status
        save('analyze_status_table', table_status)
        profiles = wait_for(lambda: rows(root, 'SHOW PROFILELIST LIMIT 1000'),
                            lambda data: any(r['QueryId'] not in before and r['Statement'].lower().startswith('select')
                                             and r['Warehouse'] == 'mw_etl' for r in data), 'FULL scan profile')
        scans = []
        for row in profiles:
            if row['QueryId'] in before or row['Warehouse'] != 'mw_etl':
                continue
            text = scalar(root, f"SELECT get_query_profile('{row['QueryId']}')")
            (output / (row['QueryId'] + '.profile')).write_text(text)
            if 'mw_observability' in text and row['Statement'].lower().startswith('select'):
                assert ':9160' in text and ':9060' not in text and ':9360' not in text, text
                scans.append(row['QueryId'])
        assert scans, profiles
        report('async_analyze_current_queries_status_and_full_scan_profile')

        auto_table = token + '_auto'
        rows(root, f'CREATE TABLE mw_observability.{auto_table} LIKE mw_observability.t')
        rows(root, f'INSERT INTO mw_observability.{auto_table} VALUES (2)')
        start('mw_stats', 'automatic_stats_blocker')
        wait_allocated('automatic_stats_blocker')
        rows(root, 'ADMIN SET FRONTEND CONFIG ("statistic_collect_warehouse"="mw_stats")')
        rows(root, 'ADMIN SET FRONTEND CONFIG ("enable_statistic_collect"="false")')
        rows(root, 'ADMIN SET FRONTEND CONFIG ("statistic_partition_healthy_v2"="false")')
        rows(root, 'ADMIN SET FRONTEND CONFIG ("enable_trigger_analyze_job_immediate"="true")')
        before_jobs = {r['Id'] for r in rows(root, 'SHOW ANALYZE JOB')}
        before_status = {r['Id'] for r in rows(root, 'SHOW ANALYZE STATUS')}
        rows(root, f'CREATE ANALYZE FULL TABLE mw_observability.{auto_table} '
                   'PROPERTIES ("statistic_auto_collect_ratio"="1", "statistic_auto_collect_interval"="1")')
        jobs = [r for r in rows(root, 'SHOW ANALYZE JOB') if r['Id'] not in before_jobs]
        analyze_jobs.extend(r['Id'] for r in jobs)
        assert jobs and all(r['CollectionWarehouse'] == 'mw_stats' for r in jobs), jobs
        stats = wait_for(current, lambda data: any(r['QueryType'] == 'Statistics' and r['Warehouse'] == 'mw_stats'
                                                  and r['ExecState'] == 'PENDING' for r in data), 'automatic collection queue')
        save('automatic_statistics_current_queries', stats)
        rows(root, 'ADMIN SET FRONTEND CONFIG ("statistic_collect_warehouse"="default_warehouse")')
        jobs = [r for r in rows(root, 'SHOW ANALYZE JOB') if r['Id'] in analyze_jobs]
        assert all(r['CollectionWarehouse'] == 'default_warehouse' for r in jobs), jobs
        save('analyze_job_next_warehouse', jobs)
        stop_workers()
        status = wait_for(lambda: rows(root, 'SHOW ANALYZE STATUS'),
                          lambda data: any(r['Id'] not in before_status and r['Warehouse'] == 'mw_stats'
                                           and r['Status'] == 'SUCCESS' for r in data), 'automatic collection completion')
        status = [r for r in status if r['Id'] not in before_status]
        assert status and all(r['Warehouse'] == 'mw_stats' for r in status), status
        save('automatic_statistics_actual_warehouse', status)
        for job in analyze_jobs:
            rows(root, f'DROP ANALYZE {job}')
        analyze_jobs.clear()
        report('automatic_statistics_current_queries_and_history_survive_config_change')

        task_name = token + '_task'
        rows(root, 'CREATE TABLE IF NOT EXISTS mw_observability.target LIKE mw_observability.t')
        with connect('mw_etl') as caller:
            rows(caller, 'USE mw_observability')
            rows(caller, f'SUBMIT TASK {task_name} PROPERTIES ("enable_profile"="true") '
                         'AS INSERT INTO target SELECT v+sleep(5) FROM t LIMIT 1')
        task_runs = wait_for(lambda: rows(root, 'SELECT QUERY_ID,TASK_NAME,STATE,WAREHOUSE '
                                         f"FROM information_schema.task_runs WHERE TASK_NAME='{task_name}'"),
                             bool, 'task run registration')
        task_id = task_runs[0]['QUERY_ID']
        task_query = wait_for(current, lambda data: any(r['QueryId'] == task_id for r in data), 'task current query')
        task_query = next(r for r in task_query if r['QueryId'] == task_id)
        assert (task_query['Warehouse'], task_query['QueryType']) == ('mw_etl', 'Task'), task_query
        save('task_current_query', task_query)
        task_runs = wait_for(lambda: rows(root, 'SELECT QUERY_ID,TASK_NAME,STATE,WAREHOUSE '
                                         f"FROM information_schema.task_runs WHERE TASK_NAME='{task_name}'"),
                             lambda data: data and data[0]['STATE'] == 'SUCCESS', 'task completion')
        assert task_runs[0]['WAREHOUSE'] == 'mw_etl', task_runs
        save('task_runs', task_runs)
        loads = rows(root, 'SHOW LOAD FROM mw_observability')
        task_load = next(r for r in loads if task_id in r['Label'])
        assert task_load['Warehouse'] == 'mw_etl', task_load
        save('task_load', task_load)

        rows(root, 'ADMIN SET FRONTEND CONFIG ("enable_mv_refresh_collect_profile"="true")')
        mv_name = token + '_mv'
        try:
            rows(root, f'CREATE MATERIALIZED VIEW mw_observability.{mv_name} DISTRIBUTED BY HASH(v) BUCKETS 1 '
                       'REFRESH DEFERRED MANUAL PROPERTIES ("replication_num"="1", "warehouse"="mw_interactive") '
                       'AS SELECT v,count(*) n FROM mw_observability.t GROUP BY v')
            rows(root, f'REFRESH MATERIALIZED VIEW mw_observability.{mv_name} WITH SYNC MODE')
            jobs = rows(root, 'SELECT * FROM information_schema.materialized_view_refresh_jobs '
                        f"WHERE TABLE_SCHEMA='mw_observability' AND TABLE_NAME='{mv_name}'")
            assert jobs and all(r['WAREHOUSE'] == 'mw_interactive' for r in jobs), jobs
            assert any(r['REFRESH_STATE'] == 'SUCCESS' for r in jobs), jobs
            save('mv_refresh_jobs', jobs)
        finally:
            rows(root, f'DROP MATERIALIZED VIEW IF EXISTS mw_observability.{mv_name}')
        report('task_current_queries_history_load_and_mv_refresh_history')

        rows(root, 'ALTER TABLE mw_observability.t COMPACT')
        compactions = wait_for(lambda: rows(root, "SHOW PROC '/compactions'"),
                               lambda data: any(r['Partition'].startswith('mw_observability.t.') for r in data),
                               'manual compaction history', timeout=90)
        compaction = next(r for r in compactions if r['Partition'].startswith('mw_observability.t.'))
        assert compaction['Warehouse'] == 'default_warehouse', compaction
        save('compactions', compactions)
        report('compaction_history_warehouse')
    finally:
        stop_workers()
        pool.shutdown(wait=True)
        for key, value in previous.items():
            rows(root, f"SET GLOBAL {key}='{value}'")
        rows(root, f'ADMIN SET FRONTEND CONFIG ("max_query_queue_history_slots_number"="{history}")')
        rows(root, f'ADMIN SET FRONTEND CONFIG ("enable_statistics_collect_profile"="{profile}")')
        rows(root, f'ADMIN SET FRONTEND CONFIG ("enable_mv_refresh_collect_profile"="{mv_profile}")')
        for job in analyze_jobs:
            rows(root, f'DROP ANALYZE {job}')
        for key, value in auto_configs.items():
            rows(root, f'ADMIN SET FRONTEND CONFIG ("{key}"="{value}")')
        rows(root, f'DROP RESOURCE GROUP IF EXISTS {group}')
        root.close()


if __name__ == '__main__':
    main()
