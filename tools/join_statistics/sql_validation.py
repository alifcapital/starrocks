#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Development-cluster integration control. Creates only explicitly prefixed Iceberg fixtures.

Run on the development host. This is a test harness, not a collector dependency.
An existing writable Iceberg database must be supplied explicitly.
"""
import argparse
import json
from pathlib import Path
import re
import time

import pymysql


def run(args):
    if not re.fullmatch(r'[a-zA-Z_][a-zA-Z_0-9]*\.[a-zA-Z_][a-zA-Z_0-9]*', args.database):
        raise ValueError('Use an explicit catalog.database for the development fixtures')
    connection = pymysql.connect(host=args.host, port=args.port, user=args.user, autocommit=True, read_timeout=180)

    def sql(statement):
        with connection.cursor() as cursor:
            cursor.execute(statement)
            return cursor.fetchall()

    tables = [args.database + '.join_stats_validation_' + name for name in ['a', 'b', 'c', 'empty']]
    a, b, c, empty = tables
    names = ['join_stats_validation_pair', 'join_stats_validation_chain', 'join_stats_validation_empty']
    result = {'database': args.database, 'commands': [], 'queries': {}}
    try:
        for name in names:
            sql('DROP JOIN STATISTICS IF EXISTS ' + name)
        for table in tables:
            sql('DROP TABLE IF EXISTS ' + table)
            sql('CREATE TABLE ' + table + ' (k BIGINT, j BIGINT, p VARCHAR(100), g INT)')
        sql(f"INSERT INTO {a} VALUES (1,10,'approved',0),(1,10,'approved',0),(2,20,'approved',0),"
            "(2,30,'approved',1),(3,30,'failed',0),(NULL,40,'approved',0),(4,NULL,NULL,0),(7,80,'😀',1)")
        sql(f"INSERT INTO {b} VALUES (1,10,'TJ',1),(1,20,'TJ',1),(2,20,'TJ',1),(3,30,'DE',0),"
            "(4,40,NULL,0),(NULL,40,'TJ',1),(7,80,'😀',1)")
        sql(f"INSERT INTO {c} VALUES (10,1,'active',0),(10,2,'active',0),(20,2,'active',0),"
            "(30,3,'inactive',0),(NULL,4,'active',0)")
        for name, definition in [
                (names[0], f'SELECT a.p,a.g,b.p,b.g FROM {a} a JOIN {b} b ON a.k=b.k'),
                (names[1], f'SELECT a.p,a.g,b.p,b.g,c.p FROM {a} a JOIN {b} b ON a.k=b.k JOIN {c} c ON a.j=c.k'),
                (names[2], f'SELECT a.p,b.p FROM {a} a JOIN {empty} b ON a.k=b.k')]:
            start = time.monotonic()
            sql('CREATE JOIN STATISTICS ' + name + ' AS ' + definition)
            state = sql('SHOW JOIN STATISTICS ' + name)
            assert state[0][1] == 'READY', state
            result['commands'].append({'name': name, 'seconds': time.monotonic() - start, 'state': state})
        cases = {
            'full': (f"FROM {a} a JOIN {b} b ON a.k=b.k WHERE a.p='approved' AND a.g=0 AND b.p='TJ' AND b.g=1", 5),
            'partial': (f"FROM {a} a JOIN {b} b ON a.k=b.k WHERE a.p='approved' AND b.p='TJ'", 6),
            'extra': (f"FROM {a} a JOIN {b} b ON a.k=b.k WHERE a.p='approved' AND b.p='TJ' AND a.j=20", 1),
            'null_predicate': (f'FROM {a} a JOIN {b} b ON a.k=b.k WHERE a.p IS NULL AND b.p IS NULL', 1),
            'unicode': (f"FROM {a} a JOIN {b} b ON a.k=b.k WHERE a.p='😀' AND b.p='😀'", 1),
            'semi': (f"FROM {a} a LEFT SEMI JOIN {b} b ON a.k=b.k WHERE a.p='approved'", 4),
            'chain': (f"FROM {a} a JOIN {b} b ON a.k=b.k JOIN {c} c ON a.j=c.k WHERE a.p='approved' AND b.p='TJ' AND c.p='active'", 9),
            'empty': (f'FROM {a} a JOIN {empty} b ON a.k=b.k', 0),
        }
        for label, (body, expected) in cases.items():
            observations = []
            for enabled in [False, True, False, True]:
                sql('SET cbo_enable_join_statistics=' + str(enabled).lower())
                query = 'SELECT count(*) ' + body
                actual = sql(query)[0][0]
                assert actual == expected, (label, enabled, actual, expected)
                plan = '\n'.join(row[0] for row in sql('EXPLAIN COSTS ' + query))
                observations.append({'enabled': enabled, 'actual': actual, 'plan': plan})
            result['queries'][label] = observations
        old = sql('SHOW JOIN STATISTICS ' + names[0])[0][2]
        sql('ANALYZE JOIN STATISTICS ' + names[0])
        new = sql('SHOW JOIN STATISTICS ' + names[0])[0][2]
        assert int(new) > int(old), (old, new)
        result['refresh'] = {'old_generation': old, 'new_generation': new}
        result['ok'] = True
    finally:
        args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False))
        connection.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', default=9030, type=int)
    parser.add_argument('--user', default='root')
    parser.add_argument('--database', required=True)
    parser.add_argument('--output', required=True, type=Path)
    run(parser.parse_args())
