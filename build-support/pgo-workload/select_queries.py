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

"""Pick corpus queries for PGO/BOLT training and measurement.

We want every operator, every join hash map specialization, every aggregation mode and inline-agg kind, the
aggregate and window functions and the SQL function families of the corpus to run at least once, within a time
budget on the current stand. Features come from the profiles of a past full run (operators and the info strings
the BE writes), from queries.jsonl (SQL and plan features) and, when given, from EXPLAIN VERBOSE dumps (types of
the GROUP BY keys, which pick the aggregation hash map variant).

Usage: select_queries.py --queries queries.jsonl --run RUN_DIR [--run1cn RUN_1CN --run4cn RUN_4CN]
                     [--explain-dir DIR] --budget-min 45 --out DIR
"""
import argparse
import collections
import gzip
import json
import os
import re

OP_RE = re.compile(r'^\s*([A-Z_]+) \(plan_node_id=-?\d+\):')
INFO_RE = re.compile(r'^\s*- (HashMapType|JoinType|DistributionMode|InlineAggOptimization|ProcessMode|SortType|PartType|'
                     r'MorselQueueType|Type|IsSpill|AggregateFunctions|GroupingKeys|DataSourceType): (.*)$')
SPILL_RE = re.compile(r'^\s*- SpillBytes: ([0-9.]+) ([KMGT]?B)')
FN_RE = re.compile(r'([a-z_0-9]+)\(')


def profile_features(path):
    feats = set()
    op = None
    with gzip.open(path, 'rt', errors='replace') as f:
        for line in f:
            m = OP_RE.match(line)
            if m:
                op = m.group(1)
                base = op[len('SPILLABLE_'):] if op.startswith('SPILLABLE_') else op
                feats.add('op:' + base)
                continue
            if op is None:
                continue
            m = INFO_RE.match(line)
            if m:
                key, value = m.group(1), m.group(2).strip()
                base = op[len('SPILLABLE_'):] if op.startswith('SPILLABLE_') else op
                if key == 'HashMapType':
                    for part in value.split(','):
                        part = re.sub(r':\d+$', '', part.strip())
                        if part:
                            feats.add('joinmap:' + part)
                elif key in ('JoinType', 'DistributionMode') and 'JOIN' in base:
                    feats.add('%s:%s:%s' % (key.lower(), 'nlj' if 'NESTLOOP' in base else 'hash', value))
                elif key == 'InlineAggOptimization':
                    feats.add('inlineagg:%s:%s' % (base, value))
                elif key == 'AggregateFunctions':
                    kind = 'window' if 'ANALYTIC' in base else 'agg'
                    for fn in set(FN_RE.findall(value.lower())):
                        feats.add('%sfn:%s' % (kind, fn))
                elif key == 'GroupingKeys':
                    feats.add('aggkeys:%s:%s' % (base, 'multi' if ',' in value else 'one'))
                elif key == 'IsSpill' and value == 'true':
                    feats.add('spill:' + base)
                elif key in ('ProcessMode', 'SortType', 'PartType', 'MorselQueueType', 'DataSourceType'):
                    feats.add('%s:%s' % (key.lower(), value))
                elif key == 'Type' and 'LOCAL_EXCHANGE' in base:
                    feats.add('localexchange:' + re.sub(r'\(.*', '', value))
                continue
            m = SPILL_RE.match(line)
            if m and float(m.group(1)) > 0:
                base = op[len('SPILLABLE_'):] if op.startswith('SPILLABLE_') else op
                feats.add('spill:' + base)
    return feats


def load_results(run):
    res = {}
    with open(os.path.join(run, 'results.jsonl')) as f:
        for line in f:
            r = json.loads(line)
            if r.get('status') == 'ok' or r.get('status') == 'OK' or r.get('latency_ms') is not None and not r.get('error'):
                res[r['id']] = r
    return res


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--queries', required=True)
    p.add_argument('--run', required=True)
    p.add_argument('--run1cn')
    p.add_argument('--run4cn')
    p.add_argument('--explain-features', help='json {id: [features]} from EXPLAIN VERBOSE')
    p.add_argument('--budget-min', type=float, required=True)
    p.add_argument('--max-query-min', type=float, default=6.0)
    p.add_argument('--out', required=True)
    a = p.parse_args()

    queries = {}
    with open(a.queries) as f:
        for line in f:
            q = json.loads(line)
            queries[q['id']] = q
    results = load_results(a.run)
    statuses = collections.Counter(r.get('status') for r in results.values())

    # Latency on the current stand: scale the 4 CN run by the median ratio of the 1 CN rerun.
    scale = 1.0
    if a.run1cn and a.run4cn:
        r1, r4 = load_results(a.run1cn), load_results(a.run4cn)
        ratios = sorted(r1[i]['latency_ms'] / r4[i]['latency_ms'] for i in r1
                        if i in r4 and r4[i].get('latency_ms') and r1[i].get('latency_ms'))
        if ratios:
            scale = ratios[len(ratios) // 2]

    explain = {}
    if a.explain_features:
        explain = json.load(open(a.explain_features))

    feats = {}
    cost = {}
    for qid, r in results.items():
        if qid not in queries or not r.get('latency_ms'):
            continue
        prof = os.path.join(a.run, 'profiles', '%s.profile.gz' % r.get('query_id'))
        fs = set()
        if os.path.exists(prof):
            fs |= profile_features(prof)
        q = queries[qid]
        for f_ in q.get('features') or []:
            if f_.startswith(('fn.', 'agg.', 'expr.', 'sql.', 'in_list.', 'cast.', 'json', 'regexp', 'lambda', 'array',
                              'map', 'window')):
                fs.add('sql:' + f_)
        fs |= set(explain.get(qid, []))
        flags = q.get('flags') or {}
        est = r['latency_ms'] * scale / 60000.0
        if est > a.max_query_min:
            continue
        feats[qid] = fs
        cost[qid] = max(est, 0.02)

    universe = collections.Counter(f_ for fs in feats.values() for f_ in fs)
    chosen, covered, total = [], set(), 0.0
    remaining = set(feats)
    while True:
        best, best_score = None, 0.0
        for qid in remaining:
            gain = len(feats[qid] - covered)
            if gain == 0:
                continue
            score = gain / cost[qid]
            if score > best_score:
                best, best_score = qid, score
        if best is None:
            break
        chosen.append(best)
        covered |= feats[best]
        total += cost[best]
        remaining.discard(best)

    cover_min = total
    cover_n = len(chosen)
    # Fill the rest of the budget with frequent production work, so the profile also weighs what runs most.
    def weight(qid):
        c = queries[qid].get('corpus') or {}
        return float(c.get('calls') or c.get('total_calls') or 1)

    for qid in sorted(remaining, key=lambda i: (-weight(i) / cost[i])):
        if total + cost[qid] > a.budget_min:
            continue
        chosen.append(qid)
        total += cost[qid]

    os.makedirs(a.out, exist_ok=True)
    with open(os.path.join(a.out, 'ids.txt'), 'w') as f:
        f.write('\n'.join(chosen) + '\n')
    with open(os.path.join(a.out, 'queries.jsonl'), 'w') as f:
        for qid in chosen:
            f.write(json.dumps(queries[qid]) + '\n')
    rare = sorted(f_ for f_, n in universe.items() if n <= 2)
    report = {
        'scale_1cn_vs_4cn': scale, 'result_statuses': dict(statuses), 'candidates': len(feats),
        'features_total': len(universe), 'features_covered': len(covered), 'cover_queries': cover_n, 'cover_minutes': round(cover_min, 1), 'total_queries': len(chosen),
        'total_minutes_est': round(total, 1), 'rare_features': rare,
        'feature_counts': dict(sorted(universe.items())),
    }
    json.dump(report, open(os.path.join(a.out, 'report.json'), 'w'), indent=1)
    print(json.dumps({k: v for k, v in report.items() if k not in ('feature_counts', 'rare_features')}, indent=1))
    print('rare features:', len(rare))


if __name__ == '__main__':
    main()
