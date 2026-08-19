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

"""Check dictionary refresh routing and shared caches on a disposable cluster.

Requires mw_etl, mw_interactive and mw_stats with one live CN each, plus default.
Pass the leader FE address: dictionary refresh workers execute on the leader.
The background warehouse must be default_warehouse. All CNs must be reachable.
"""

import argparse
import concurrent.futures
import json
from pathlib import Path
import uuid

import pymysql

from run_observability import rows, scalar, wait_for


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9030)
    parser.add_argument('--output', default='/tmp/multi-warehouse-dictionary')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    token = 'mw_dict_' + uuid.uuid4().hex[:10]
    database, user, dictionary = token, token + '_user', token + '_cache'

    def connect(warehouse='default_warehouse', username='root'):
        conn = pymysql.connect(host=args.host, port=args.port, user=username,
                               autocommit=True, read_timeout=60)
        rows(conn, f"SET warehouse='{warehouse}'")
        return conn

    root = connect()
    created = []

    def save(name, value):
        (output / (name + '.json')).write_text(json.dumps(value, indent=2, default=str))

    def report(check):
        print(json.dumps({'check': check, 'result': 'PASS'}), flush=True)

    def observe(warehouse, label):
        def current():
            return [r for r in rows(root, "SHOW PROC '/current_queries'")
                    if r['Database'] == database and r['ExecState'] == 'RUNNING']
        query = wait_for(current, bool, label + ' running', timeout=60)[0]
        assert query['Warehouse'] == warehouse, query
        query_id = query['QueryId']
        details = rows(root, f"SHOW PROC '/current_queries/{query_id}'")
        assert 'slow_source' in str(details), details
        hosts = wait_for(lambda: rows(root, f"SHOW PROC '/current_queries/{query_id}/hosts'"),
                         bool, label + ' hosts', timeout=10)
        port = {'mw_etl': 8160, 'default_warehouse': 8060}[warehouse]
        assert f':{port}' in str(hosts), hosts
        for other in {8060, 8160, 8260, 8360} - {port}:
            assert f':{other}' not in str(hosts), hosts
        save(label, {'query': query, 'sql': details, 'hosts': hosts})

    def finished(name=dictionary):
        def state():
            data = rows(root, f'SHOW DICTIONARY {name}')
            assert data and not data[0]['ErrorMessage'], data
            return data
        data = wait_for(state, lambda data: data[0]['status'] == 'FINISHED',
                        name + ' finished', timeout=60)
        save(name + '_finished', data)

    def denied(conn, sql):
        try:
            rows(conn, sql)
        except pymysql.MySQLError as error:
            assert 'USAGE' in str(error) and 'mw_etl' in str(error), str(error)
        else:
            raise AssertionError('Expected warehouse USAGE denial: ' + sql)

    def check_queue():
        settings = {'enable_query_queue_select': 'true', 'query_queue_concurrency_limit': '1'}
        previous = {key: list(rows(root, f"SHOW GLOBAL VARIABLES LIKE '{key}'")[0].values())[1]
                    for key in settings}
        blocker = connect('mw_etl')
        rows(blocker, f'USE {database}')
        connection_id = scalar(blocker, 'SELECT connection_id()')
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = None
        try:
            for key, value in settings.items():
                rows(root, f'SET GLOBAL {key}={value}')
            future = pool.submit(rows, blocker, 'SELECT sleep(45) FROM source LIMIT 1')
            wait_for(lambda: rows(root, "SHOW PROC '/current_queries'"),
                     lambda data: any(r['ConnectionId'] == str(connection_id) and r['ExecState'] == 'RUNNING'
                                      for r in data), 'ETL blocker')
            with connect('mw_etl', user) as caller:
                rows(caller, f'REFRESH DICTIONARY {dictionary}')
                rows(caller, 'SET warehouse=default_warehouse')
            pending = wait_for(lambda: [r for r in rows(root, "SHOW PROC '/current_queries'")
                                        if r['Database'] == database and r['ExecState'] == 'PENDING'],
                               bool, 'dictionary waiting in ETL queue')
            assert len(pending) == 1 and pending[0]['Warehouse'] == 'mw_etl', pending
            save('manual_refresh_queue', pending)
            with connect('mw_interactive') as other:
                assert scalar(other, f'SELECT count(*) FROM {database}.source') == 1
            rows(root, f'KILL QUERY {connection_id}')
            try:
                future.result(timeout=10)
            except pymysql.MySQLError as error:
                assert 'cancel' in str(error).lower() or 'killed' in str(error).lower(), str(error)
            observe('mw_etl', 'manual_refresh_after_queue')
            finished()
            report('manual_refresh_uses_etl_queue_and_interactive_remains_available')
        finally:
            if future is not None and not future.done():
                rows(root, f'KILL QUERY {connection_id}')
            pool.shutdown(wait=True)
            blocker.close()
            for key, value in previous.items():
                rows(root, f'SET GLOBAL {key}={value}')

    try:
        rows(root, f'CREATE DATABASE {database}')
        rows(root, f'USE {database}')
        rows(root, 'CREATE TABLE source(k BIGINT NOT NULL, v VARCHAR(100) NOT NULL) '
                   'DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 PROPERTIES ("replication_num"="1")')
        rows(root, "INSERT INTO source VALUES (1, 'one')")
        # Keep the worker visible long enough to inspect its actual fragment hosts.
        rows(root, "CREATE VIEW slow_source AS SELECT k, concat(v, IF(sleep(8), '', '')) AS v FROM source")
        rows(root, f"CREATE USER '{user}'")
        rows(root, f"GRANT SELECT ON ALL TABLES IN DATABASE {database} TO '{user}'")
        rows(root, f"GRANT SELECT ON ALL VIEWS IN DATABASE {database} TO '{user}'")
        rows(root, f"GRANT USAGE ON WAREHOUSE mw_etl TO '{user}'")
        with connect('mw_etl', user) as caller:
            rows(caller, f'USE {database}')
            rows(caller, f'CREATE DICTIONARY {dictionary} USING slow_source (k KEY, v VALUE)')
            created.append(dictionary)
            rows(caller, 'SET warehouse=default_warehouse')
            observe('mw_etl', 'create_warmup_session_snapshot')
            finished()
            report('create_warmup_uses_session_warehouse')

            rows(caller, 'SET warehouse=mw_etl')
            rows(caller, f'REFRESH DICTIONARY {dictionary}')
            rows(caller, 'SET warehouse=default_warehouse')
            observe('mw_etl', 'manual_refresh_session_snapshot')
            finished()
            report('manual_refresh_keeps_submission_warehouse')

            rows(caller, 'SET warehouse=mw_etl')
            rows(root, f"REVOKE USAGE ON WAREHOUSE mw_etl FROM '{user}'")
            denied(caller, f'REFRESH DICTIONARY {dictionary}')
            denied(caller, f'CREATE DICTIONARY {dictionary}_denied USING slow_source (k KEY, v VALUE)')
            rows(root, f"GRANT USAGE ON WAREHOUSE mw_etl TO '{user}'")
            report('create_and_refresh_check_warehouse_usage')

        check_queue()

        lookups = {}
        for warehouse in ('default_warehouse', 'mw_etl', 'mw_interactive', 'mw_stats'):
            with connect(warehouse) as reader:
                data = rows(reader, f"SELECT dictionary_get('{dictionary}', k) AS value FROM {database}.source")
                assert 'one' in str(data), data
                lookups[warehouse] = data
        save('local_cache_lookup_all_warehouses', lookups)
        report('cache_available_in_all_warehouses')

        automatic = dictionary + '_auto'
        with connect('mw_etl', user) as caller:
            rows(caller, f'USE {database}')
            rows(caller, f'CREATE DICTIONARY {automatic} USING slow_source (k KEY, v VALUE) '
                         'PROPERTIES ("dictionary_warm_up"="false", "dictionary_refresh_interval"="30")')
            created.append(automatic)
            rows(caller, f'REFRESH DICTIONARY {automatic}')
        observe('mw_etl', 'scheduled_dictionary_manual_refresh')
        finished(automatic)
        observe('default_warehouse', 'automatic_refresh_background')
        finished(automatic)
        report('automatic_refresh_returns_to_background_after_manual_refresh')
    finally:
        for name in reversed(created):
            rows(root, f'DROP DICTIONARY {name}')
        rows(root, f"DROP USER IF EXISTS '{user}'")
        rows(root, f'DROP DATABASE IF EXISTS {database}')
        root.close()


if __name__ == '__main__':
    main()
