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

"""Stop a disposable cluster's leader while an observer has running and queued queries."""

import argparse
import concurrent.futures
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

import pymysql

from run_observability import rows, scalar, wait_for
from run_statistics import expect_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--observer-host', default='172.17.0.3')
    parser.add_argument('--follower-host', default='172.17.0.4')
    stop = parser.add_mutually_exclusive_group(required=True)
    stop.add_argument('--leader-pid-file')
    stop.add_argument('--leader-container')
    parser.add_argument('--output', default='/tmp/multi-warehouse-failover')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    def connect(host=args.host, user='root', warehouse=None):
        conn = pymysql.connect(host=host, port=9030, user=user, autocommit=True, read_timeout=100)
        if warehouse:
            rows(conn, f"SET warehouse='{warehouse}'")
        return conn

    def save(name, data):
        (output / (name + '.json')).write_text(json.dumps(data, indent=2, default=str))

    root = connect()
    frontends = rows(root, 'SHOW FRONTENDS')
    old_leader = next(r['IP'] for r in frontends if r['Role'] == 'LEADER')
    assert sum(r['Role'] in ('LEADER', 'FOLLOWER') and str(r['Alive']).lower() == 'true'
               for r in frontends) >= 3, frontends
    save('frontends_before', frontends)
    warehouses_before = rows(root, 'SHOW WAREHOUSES')
    nodes_before = rows(root, 'SHOW COMPUTE NODES')
    settings = {'enable_query_queue_select': 'true', 'query_queue_concurrency_limit': '1',
                'query_queue_pending_timeout_second': '90'}
    previous = {key: list(rows(root, f"SHOW GLOBAL VARIABLES LIKE '{key}'")[0].values())[1] for key in settings}
    for key, value in settings.items():
        rows(root, f'SET GLOBAL {key}={value}')
    observer = connect(args.observer_host)
    wait_for(lambda: list(rows(observer, "SHOW GLOBAL VARIABLES LIKE 'query_queue_concurrency_limit'")[0].values())[1],
             lambda value: value == '1', 'queue settings on observer')
    token = 'failover_' + uuid.uuid4().hex[:10]
    workers = []
    connection_ids = set()
    finished_at = {}
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=3)

    def start(warehouse, tag, query):
        conn = connect(args.observer_host, 'mw_lifecycle_user', warehouse)
        rows(conn, 'SET enable_profile=true')
        rows(conn, 'SET enable_async_profile=false')
        workers.append(conn)
        connection_ids.add(str(scalar(conn, "SELECT connection_id()")))
        def execute():
            try:
                return rows(conn, f'SELECT /* {token}_{tag} */ {query} FROM mw_lifecycle.source LIMIT 1')
            finally:
                finished_at[tag] = time.monotonic()
        return pool.submit(execute)

    def current():
        return [r for r in rows(observer, "SHOW PROC '/current_queries'") if r['ConnectionId'] in connection_ids]

    try:
        running = start('mw_etl', 'running', 'sleep(40)')
        wait_for(current, lambda data: any(r['ExecState'] == 'RUNNING' for r in data), 'running ETL query')
        pending = start('mw_etl', 'pending', 'sum(v)')
        wait_for(current, lambda data: any(r['ExecState'] == 'PENDING' for r in data), 'queued ETL query')
        interactive = start('mw_interactive', 'interactive', 'sum(v)')
        assert interactive.result(timeout=10)[0]['sum(v)'] == 30
        save('current_before', current())
        if args.leader_pid_file:
            pid = int(Path(args.leader_pid_file).read_text().strip())
            command = subprocess.check_output(['ps', '-p', str(pid), '-o', 'args='], text=True)
            assert 'com.starrocks.StarRocksFE' in command, command
            os.kill(pid, signal.SIGKILL)
        else:
            subprocess.run(['docker', 'kill', args.leader_container], check=True)

        control = connect(args.follower_host)

        def leader_ready():
            try:
                data = rows(control, 'SHOW FRONTENDS')
                leader = next((r['IP'] for r in data if r['Role'] == 'LEADER' and r['IP'] != old_leader), None)
                if leader:
                    with connect(leader) as candidate:
                        rows(candidate, 'SELECT 1')
                    return leader
            except pymysql.MySQLError:
                return None
            return None

        new_leader = wait_for(leader_ready, bool, 'new FE leader', timeout=70)
        result = pending.result(timeout=65)
        assert result[0]['sum(v)'] == 30, result
        overlap = finished_at['pending'] < finished_at.get('running', float('inf'))
        save('queue_recovery', {'new_leader': new_leader, 'pending_resumed_before_running_finished': overlap})
        with connect(new_leader) as leader:
            wait_for(lambda: rows(leader, 'SHOW FRONTENDS'),
                     lambda data: any(r['IP'] == old_leader and str(r['Alive']).lower() == 'false' for r in data),
                     'failed leader heartbeat', timeout=40)
            started = time.monotonic()
            after = rows(leader, 'SHOW WAREHOUSES')
            elapsed = time.monotonic() - started
            assert elapsed < 5, ('SHOW WAREHOUSES waited for an unavailable FE', elapsed)
            save('diagnostics_with_dead_fe', {'show_warehouses_seconds': elapsed})
            assert {(r['Id'], r['Name']) for r in after} == {(r['Id'], r['Name']) for r in warehouses_before}
            nodes_after = rows(leader, 'SHOW COMPUTE NODES')
            assert {(r['ComputeNodeId'], r['WarehouseName']) for r in nodes_after} == {
                (r['ComputeNodeId'], r['WarehouseName']) for r in nodes_before}
            save('frontends_after', rows(leader, 'SHOW FRONTENDS'))
            save('warehouses_after', after)
            save('nodes_after', nodes_after)
            save('current_after_election', rows(leader, "SHOW PROC '/global_current_queries'"))
            save('slots_after_election', rows(leader, 'SELECT * FROM information_schema.warehouse_queries'))
        running.result(timeout=50)
        with connect(new_leader, 'mw_lifecycle_user') as user:
            assert scalar(user, 'SELECT @@warehouse') == 'mw_etl'
            assert scalar(user, 'SELECT sum(v) FROM mw_lifecycle.source') == 30
            expect_error(user, 'SET warehouse=mw_stats', 'USAGE')
        for conn, warehouse in zip(workers, ('mw_etl', 'mw_etl', 'mw_interactive')):
            query_id = scalar(conn, 'SELECT last_query_id()')
            value = scalar(conn, f"SELECT get_query_profile('{query_id}')")
            assert f'Warehouse: {warehouse}\n' in value, value
            expected_port = 9160 if warehouse == 'mw_etl' else 9260
            assert f':{expected_port}' in value, value
            assert all(f':{port}' not in value for port in (9060, 9160, 9260, 9360) if port != expected_port), value
            (output / (query_id + '.profile')).write_text(value)
        print(json.dumps({'check': 'leader_failover_metadata_grants_pending_queries_and_routing', 'result': 'PASS',
                          'new_leader': new_leader, 'pending_resumed_before_running_finished': overlap}), flush=True)
        print(json.dumps({'check': 'concurrency_limit_during_leader_change',
                          'result': 'LIMITATION' if overlap else 'PASS'}), flush=True)
    finally:
        pool.shutdown(wait=True)
        for conn in workers:
            conn.close()
        for key, value in previous.items():
            rows(observer, f"SET GLOBAL {key}='{value}'")
        observer.close()
        root.close()


if __name__ == '__main__':
    main()
