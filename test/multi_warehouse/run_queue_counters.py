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

"""Check queue total/timeout counters across two FEs and two warehouses."""

import argparse
import concurrent.futures
import json
from pathlib import Path
import re
import urllib.request

import pymysql

from run_observability import rows, scalar, wait_for
from run_statistics import expect_error


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--observer-host', default='172.17.0.3')
    parser.add_argument('--output', default='/tmp/multi-warehouse-queue-counters')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def connect(host, warehouse=None):
        conn = pymysql.connect(host=host, port=9030, user='root', autocommit=True, read_timeout=50)
        if warehouse:
            rows(conn, f"SET warehouse='{warehouse}'")
        return conn

    def counters(label):
        result = {}
        for host in (args.host, args.observer_host):
            text = opener.open(f'http://{host}:8030/metrics', timeout=10).read().decode()
            (output / (label + '-' + host + '.txt')).write_text(text)
            for line in text.splitlines():
                match = re.match(r'starrocks_fe_warehouse_query_queue_(total|timeout)\{([^}]+)\} (\d+)', line)
                if not match:
                    continue
                warehouse = re.search(r'warehouse_name="([^"]+)"', match[2])[1]
                key = warehouse + '/' + match[1]
                result[key] = result.get(key, 0) + int(match[3])
        return result

    root = connect(args.host)
    observer = connect(args.observer_host)
    settings = {'enable_query_queue_select': 'true', 'query_queue_concurrency_limit': '1',
                'query_queue_pending_timeout_second': '2'}
    previous = {key: list(rows(root, f"SHOW GLOBAL VARIABLES LIKE '{key}'")[0].values())[1] for key in settings}
    running_conn = None
    try:
        for key, value in settings.items():
            rows(root, f'SET GLOBAL {key}={value}')
        wait_for(lambda: list(rows(observer, "SHOW GLOBAL VARIABLES LIKE 'query_queue_pending_timeout_second'")[0].values())[1],
                 lambda value: value == '2', 'queue settings replicated')
        before = counters('before')
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            running_conn = connect(args.host, 'mw_etl')
            connection_id = str(scalar(running_conn, 'SELECT connection_id()'))
            running = pool.submit(rows, running_conn, 'SELECT sleep(12) FROM mw_lifecycle.source LIMIT 1')
            wait_for(lambda: rows(root, "SHOW PROC '/current_queries'"),
                     lambda data: any(r['ConnectionId'] == connection_id and r['ExecState'] == 'RUNNING' for r in data),
                     'running ETL query')
            with connect(args.observer_host, 'mw_etl') as pending:
                expect_error(pending, 'SELECT sum(v) FROM mw_lifecycle.source', 'pending timeout')
            with connect(args.observer_host, 'mw_interactive') as interactive:
                assert scalar(interactive, 'SELECT sum(v) FROM mw_lifecycle.source') == 30
            assert not running.done(), 'running query finished before isolation was checked'
            running.result(timeout=30)
        after = counters('after')
        expected = {'mw_etl/total': 2, 'mw_etl/timeout': 1, 'mw_interactive/total': 1, 'mw_interactive/timeout': 0}
        actual = {key: after.get(key, 0) - before.get(key, 0) for key in expected}
        assert actual == expected, (before, after, actual)
        (output / 'deltas.json').write_text(json.dumps(actual, indent=2))
        print(json.dumps({'check': 'warehouse_queue_counter_deltas_across_fes', 'result': 'PASS', 'deltas': actual}), flush=True)
    finally:
        for key, value in previous.items():
            rows(root, f"SET GLOBAL {key}='{value}'")
        if running_conn:
            running_conn.close()
        observer.close()
        root.close()


if __name__ == '__main__':
    main()
