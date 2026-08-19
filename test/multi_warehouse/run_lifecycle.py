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

"""Task routing and flag rollback checks on a retained disposable cluster."""

import argparse
import json
from pathlib import Path
import subprocess
import time
import uuid

import pymysql

from run_observability import rows, scalar, wait_for
from run_statistics import expect_error, profile_ids, set_config, verify_collection


DB = 'mw_lifecycle'
USER = 'mw_lifecycle_user'
TASK = 'mw_lifecycle_scheduled'
PORTS = {'default_warehouse': 9060, 'mw_etl': 9160, 'mw_interactive': 9260, 'mw_stats': 9360}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9030)
    parser.add_argument('--http-port', type=int, default=8030)
    parser.add_argument('--phase', choices=('prepare', 'enabled', 'disabled', 'restored'), required=True)
    parser.add_argument('--output', default='/tmp/multi-warehouse-lifecycle')
    args = parser.parse_args()
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)

    def connect(user='root'):
        return pymysql.connect(host=args.host, port=args.port, user=user, autocommit=True, read_timeout=120)

    def save(name, value):
        (output / f'{args.phase}-{name}.json').write_text(json.dumps(value, default=str, indent=2))

    def report(name):
        print(json.dumps({'phase': args.phase, 'check': name, 'result': 'PASS'}), flush=True)

    def task_runs(name):
        return rows(root, 'SELECT QUERY_ID,TASK_NAME,STATE,WAREHOUSE,ERROR_MESSAGE '
                         f"FROM information_schema.task_runs WHERE TASK_NAME='{name}'")

    def successful_task(name, before, warehouse):
        data = wait_for(lambda: task_runs(name),
                        lambda data: any(r['QUERY_ID'] not in before and r['STATE'] in ('SUCCESS', 'FAILED')
                                         for r in data), 'task ' + name, timeout=100)
        fresh = [r for r in data if r['QUERY_ID'] not in before and r['STATE'] in ('SUCCESS', 'FAILED')]
        assert fresh and all(r['STATE'] == 'SUCCESS' and r['WAREHOUSE'] == warehouse for r in fresh), fresh
        save(name, fresh)
        return fresh[0]['QUERY_ID']

    def profile(query_id, warehouse):
        value = wait_for(lambda: scalar(root, f"SELECT get_query_profile('{query_id}')"),
                         lambda value: bool(value) and f':{PORTS[warehouse]}' in value, 'execution profile')
        assert f'Warehouse: {warehouse}\n' in value, value
        assert all(f':{port}' not in value for name, port in PORTS.items() if name != warehouse), value
        (output / f'{args.phase}-{query_id}.profile').write_text(value)

    root = connect()
    if args.phase == 'prepare':
        rows(root, f'CREATE DATABASE IF NOT EXISTS {DB}')
        rows(root, f'USE {DB}')
        for table in ('source', 'manual_stats', 'auto_stats', 'sink'):
            rows(root, f'CREATE TABLE IF NOT EXISTS {table}(v INT) DUPLICATE KEY(v) '
                       'DISTRIBUTED BY HASH(v) BUCKETS 1 PROPERTIES ("replication_num"="1")')
            if table != 'sink' and not scalar(root, f'SELECT count(*) FROM {table}'):
                rows(root, f'INSERT INTO {table} VALUES (10),(20)')
        rows(root, f"CREATE USER IF NOT EXISTS {USER} IDENTIFIED BY ''")
        rows(root, f'GRANT SELECT,INSERT ON ALL TABLES IN DATABASE {DB} TO USER {USER}')
        for warehouse in ('mw_etl', 'mw_interactive'):
            rows(root, f'GRANT USAGE ON WAREHOUSE {warehouse} TO USER {USER}')
        rows(root, f"ALTER USER '{USER}' SET PROPERTIES ('session.warehouse'='mw_etl')")
        rows(root, 'SET warehouse=mw_etl')
        rows(root, f'DROP TASK IF EXISTS {TASK}')
        rows(root, f'SUBMIT TASK {TASK} SCHEDULE EVERY(INTERVAL 10 SECOND) '
                   'PROPERTIES ("enable_profile"="true", "enable_async_profile"="false") '
                   f'AS INSERT INTO {DB}.sink SELECT sum(v) FROM {DB}.source')
        rows(root, f'ALTER TASK {TASK} SUSPEND')
        rows(root, f'CREATE MATERIALIZED VIEW IF NOT EXISTS {DB}.mv DISTRIBUTED BY HASH(v) BUCKETS 1 '
                   'REFRESH DEFERRED MANUAL PROPERTIES ("replication_num"="1", "warehouse"="mw_interactive") '
                   f'AS SELECT v,count(*) n FROM {DB}.source GROUP BY v')
        existing = [r for r in rows(root, 'SHOW ANALYZE JOB') if 'auto_stats' in str(r) and DB in str(r)]
        if not existing:
            rows(root, f'CREATE ANALYZE FULL TABLE {DB}.auto_stats '
                       'PROPERTIES ("statistic_auto_collect_ratio"="1", "statistic_auto_collect_interval"="1")')
        save('warehouses', rows(root, 'SHOW WAREHOUSES'))
        save('nodes', rows(root, 'SHOW COMPUTE NODES'))
        report('persistent_fixtures_created')
        return

    disabled = args.phase == 'disabled'
    config = rows(root, 'ADMIN SHOW FRONTEND CONFIG LIKE "enable_multi_warehouse"')[0]
    assert str(list(config.values())[2]).lower() == str(not disabled).lower(), config
    expected = 'default_warehouse' if disabled else 'mw_etl'
    with connect(USER) as caller:
        assert scalar(caller, 'SELECT @@warehouse') == expected
        rows(caller, 'SET enable_profile=true')
        rows(caller, 'SET enable_async_profile=false')
        assert scalar(caller, f'SELECT sum(v) FROM {DB}.source') == 30
        profile(scalar(caller, 'SELECT last_query_id()'), expected)
        rows(caller, f"SELECT /*+ SET_VAR(warehouse='mw_etl') */ sum(v) FROM {DB}.source")
        profile(scalar(caller, 'SELECT last_query_id()'), expected)
        if not disabled:
            expect_error(caller, 'SET warehouse=mw_stats', 'USAGE')
            rows(caller, f'USE {DB}')
            expect_error(caller, 'SUBMIT TASK denied PROPERTIES ("warehouse"="mw_stats") '
                                'AS INSERT INTO sink SELECT * FROM source', 'USAGE')
            expect_error(caller, f'ALTER TASK {TASK} SET ("warehouse"="mw_stats")', 'USAGE')
    report('user_assignment_hints_and_usage')

    rows(root, f'USE {DB}')
    if args.phase == 'enabled':
        with connect(USER) as caller:
            rows(caller, f'USE {DB}')
            task = 'explicit_' + uuid.uuid4().hex[:10]
            rows(caller, f'SUBMIT TASK {task} PROPERTIES ("warehouse"="mw_interactive", '
                         '"enable_profile"="true", "enable_async_profile"="false") '
                         f'AS INSERT INTO {DB}.sink SELECT sum(v) FROM {DB}.source')
            profile(successful_task(task, set(), 'mw_interactive'), 'mw_interactive')
            rows(caller, f'ALTER TASK {TASK} SET ("WAREHOUSE"="mw_interactive")')
        report('explicit_submit_and_alter_warehouse')

    task_wh = 'default_warehouse' if disabled else 'mw_interactive'
    before = {r['QUERY_ID'] for r in task_runs(TASK)}
    rows(root, f'ALTER TASK {TASK} RESUME')
    try:
        profile(successful_task(TASK, before, task_wh), task_wh)
    finally:
        rows(root, f'ALTER TASK {TASK} SUSPEND')
    task = rows(root, f"SELECT * FROM information_schema.tasks WHERE TASK_NAME='{TASK}'")
    assert task and 'mw_interactive' in str(task), task
    save('task_definition', task)
    report('scheduled_task_assignment_preserved')

    settings = {'enable_statistics_collect_profile': 'true', 'enable_mv_refresh_collect_profile': 'true',
                'statistic_collect_warehouse': 'missing_stats' if disabled else 'mw_stats',
                'enable_auto_collect_statistics': 'false',
                'statistic_partition_healthy_v2': 'false', 'statistic_collect_interval_sec': '5',
                'enable_statistic_collect': 'true'}
    previous = {name: list(rows(root, f'ADMIN SHOW FRONTEND CONFIG LIKE "{name}"')[0].values())[2]
                for name in settings}
    try:
        for name, value in settings.items():
            set_config(root, name, value)
        mv_jobs_sql = ('SELECT * FROM information_schema.materialized_view_refresh_jobs '
                       f"WHERE TABLE_SCHEMA='{DB}' AND TABLE_NAME='mv'")
        before_jobs = {r['JOB_ID'] for r in rows(root, mv_jobs_sql)}
        rows(root, f'REFRESH MATERIALIZED VIEW {DB}.mv FORCE WITH SYNC MODE')
        jobs = [r for r in rows(root, mv_jobs_sql) if r['JOB_ID'] not in before_jobs]
        assert jobs and all(r['REFRESH_STATE'] == 'SUCCESS' and r['WAREHOUSE'] == task_wh for r in jobs), jobs
        save('mv_refresh', jobs)
        for job in jobs:
            runs = rows(root, 'SELECT QUERY_ID FROM information_schema.task_runs '
                             f"WHERE JOB_ID='{job['JOB_ID']}'")
            assert runs, job
            for run in runs:
                profile(run['QUERY_ID'], task_wh)
        mv_definition = rows(root, f'SHOW CREATE MATERIALIZED VIEW {DB}.mv')
        assert 'mw_interactive' in str(mv_definition), mv_definition
        save('mv_definition', mv_definition)
        report('mv_refresh_assignment_preserved')

        before = profile_ids(root)
        with connect(USER) as caller:
            rows(caller, f'ANALYZE FULL TABLE {DB}.manual_stats WITH SYNC MODE')
        verify_collection(root, before, 'manual_stats', expected, PORTS[expected], list(PORTS.values()),
                          output, args.phase + '-manual')
        before = profile_ids(root)
        rows(root, f'INSERT INTO {DB}.auto_stats VALUES ({int(time.time()) % 100000})')
        stats_wh = 'default_warehouse' if disabled else 'mw_stats'
        verify_collection(root, before, 'auto_stats', stats_wh, PORTS[stats_wh], list(PORTS.values()),
                          output, args.phase + '-automatic')
        jobs = [r for r in rows(root, 'SHOW ANALYZE JOB') if 'auto_stats' in str(r) and DB in str(r)]
        assert jobs and all(r['CollectionWarehouse'] == stats_wh for r in jobs), jobs
        save('analyze_job', jobs)
        report('existing_automatic_analyze_job')
    finally:
        for name, value in previous.items():
            set_config(root, name, value)

    command = ['curl', '--silent', '--show-error', '--noproxy', '*', '--location-trusted', '--max-time', '60',
               '-u', USER + ':', '-X', 'PUT', '-H', 'Expect: 100-continue', '-H', 'format: csv',
               '-H', 'warehouse: mw_etl', '-H', 'label: lifecycle_' + uuid.uuid4().hex,
               '--data-binary', '7\n', f'http://{args.host}:{args.http_port}/api/{DB}/sink/_stream_load']
    loaded = json.loads(subprocess.check_output(command, text=True))
    assert loaded['Status'] == 'Success', loaded
    save('stream_load', loaded)
    report('stream_load_with_saved_warehouse')
    save('warehouses', rows(root, 'SHOW WAREHOUSES'))
    save('nodes', rows(root, 'SHOW COMPUTE NODES'))
    root.close()


if __name__ == '__main__':
    main()
