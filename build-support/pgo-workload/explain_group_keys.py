#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""EXPLAIN VERBOSE every corpus query and classify its GROUP BY keys the way the BE picks the aggregation hash map.

Output: {query id: ["aggkey:<class>", ...]}. One key: its type and nullability (phase1_<type> / phase1_null_<type>).
Several keys: "fixed" when all are fixed width (the fx4/fx8/fx16 or compressed variants), "serialized" otherwise.
EXPLAIN only plans; it reads metadata, not data.

Usage: explain_group_keys.py --queries queries.jsonl --out explain-keys.json [--host 127.0.0.1 --port 9030] [--workers 1]
"""
import argparse
import concurrent.futures
import json
import re

import pymysql

GROUP_RE = re.compile(r'group by: (.*)$')
KEY_RE = re.compile(r'\[\d+: [^,\]]*, ([A-Z_0-9]+(?:\([^)]*\))?), (true|false)\]')
WIDTH = {'BOOLEAN': 1, 'TINYINT': 1, 'SMALLINT': 2, 'INT': 4, 'BIGINT': 8, 'LARGEINT': 16, 'DATE': 4, 'DATETIME': 8,
         'FLOAT': 4, 'DOUBLE': 8, 'DECIMAL32': 4, 'DECIMAL64': 8, 'DECIMAL128': 16, 'DECIMAL256': 32}


def classify(keys):
    if len(keys) == 1:
        t, nullable = keys[0]
        base = re.sub(r'\(.*', '', t)
        if base in ('VARCHAR', 'CHAR', 'STRING', 'VARBINARY'):
            base = 'string'
        return 'one:%s:%s' % (base.lower(), 'null' if nullable == 'true' else 'notnull')
    widths = [WIDTH.get(re.sub(r'\(.*', '', t)) for t, _ in keys]
    if all(w is not None for w in widths):
        total = sum(widths) + sum(1 for _, n in keys if n == 'true')
        return 'multi:fixed%s' % (4 if total <= 4 else 8 if total <= 8 else 16 if total <= 16 else 'big')
    return 'multi:serialized'


def explain(args, q):
    conn = pymysql.connect(host=args.host, port=args.port, user='root', password='', autocommit=True,
                           read_timeout=300, client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS)
    try:
        with conn.cursor() as cur:
            cur.execute('SET CATALOG `%s`' % (q.get('catalog') or 'default_catalog'))
            if q.get('db'):
                cur.execute('USE `%s`' % q['db'])
            cur.execute('EXPLAIN VERBOSE ' + q['sql'])
            text = '\n'.join(r[0] for r in cur.fetchall())
    finally:
        conn.close()
    feats = set()
    for line in text.splitlines():
        m = GROUP_RE.search(line)
        if m:
            keys = KEY_RE.findall(m.group(1))
            if keys:
                feats.add('aggkey:' + classify(keys))
    return sorted(feats)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--queries', required=True)
    p.add_argument('--out', required=True)
    p.add_argument('--host', default='127.0.0.1')
    p.add_argument('--port', type=int, default=9030)
    # Several EXPLAINs of very large queries at once can push the memo phase of one of them past the planner timeout on
    # a freshly started FE, so we plan one query at a time unless asked otherwise.
    p.add_argument('--workers', type=int, default=1)
    a = p.parse_args()
    qs = [json.loads(l) for l in open(a.queries)]
    out, errors = {}, {}
    with concurrent.futures.ThreadPoolExecutor(a.workers) as ex:
        futs = {ex.submit(explain, a, q): q['id'] for q in qs}
        for f in concurrent.futures.as_completed(futs):
            try:
                out[futs[f]] = f.result()
            except Exception as e:  # keep going; a query that does not plan has no key features
                errors[futs[f]] = str(e)[:300]
    json.dump(out, open(a.out, 'w'))
    json.dump(errors, open(a.out + '.errors.json', 'w'), indent=1)
    from collections import Counter
    print('planned', len(out), 'errors', len(errors))
    print(Counter(f for v in out.values() for f in v).most_common())


if __name__ == '__main__':
    main()
