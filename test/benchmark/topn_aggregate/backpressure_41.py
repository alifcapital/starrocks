#!/usr/bin/env python3
"""Compare preserved 4.1 binaries with #75444 + IO-readiness fix on pinned reads.

Requires PyMySQL and the saved query files from ICEBERG-4.1-PERFORMANCE.md.
Runs only SELECT, EXPLAIN, SHOW and session SET. No ANALYZE or global changes.
Run phases sequentially, with no concurrent builds or benchmarks.
"""
import argparse
import json
import statistics
import time
from pathlib import Path

import pymysql


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=9030)
    parser.add_argument('--query-root', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--rounds', type=int, default=21)
    parser.add_argument('--reverse', action='store_true')
    parser.add_argument('--patched', action='store_true')
    parser.add_argument('--cases', nargs='*')
    parser.add_argument('--reference-plans', type=Path,
                        help='Require the same RANK/RF choice and statistics source as this baseline')
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    conn = pymysql.connect(host='127.0.0.1', port=args.port, user='root',
                           autocommit=True, read_timeout=660)

    def sql(statement, values=None):
        assert statement.lstrip().upper().startswith(('SELECT ', 'EXPLAIN ', 'SHOW ', 'SET '))
        with conn.cursor() as cursor:
            cursor.execute(statement, values)
            return cursor.fetchall()

    settings = [
        'query_timeout=600', 'enable_query_trigger_analyze=false',
        'enable_query_cache=false', 'enable_plan_advisor=false',
        'enable_runtime_adaptive_dop=false', 'new_planner_agg_stage=2',
        'enable_spill=false', "spill_mode='auto'", 'enable_scan_datacache=false',
        'enable_profile=false', 'enable_async_profile=false',
        'pipeline_profile_level=2', 'pipeline_dop=8', 'chunk_size=4096',
        "streaming_preaggregation_mode='auto'", 'enable_topn_runtime_filter=true',
        'enable_parquet_reader_page_index=true', 'enable_agg_inline_accumulator=true',
        'enable_pipeline_event_scheduler=true', 'topn_filter_back_pressure_mode=0',
    ]
    for setting in settings:
        sql('SET ' + setting)
    variants = [('ordinary', -1, True), ('optimized', 1, True)]
    if args.patched:
        variants.append(('no_backpressure', 1, False))
        sql('SET topn_filter_back_pressure_io_tasks=1')

    def select_variant(variant):
        _, mode, backpressure = variant
        sql(f'SET topn_push_down_agg_mode={mode}')
        if args.patched:
            sql('SET enable_topn_filter_back_pressure=' + str(backpressure).lower())

    env = {'settings': settings, 'variants': variants, 'rounds': args.rounds,
           'version': sql('SELECT current_version()'), 'backends': sql('SHOW BACKENDS')}
    env['topn_variables'] = sql("SHOW VARIABLES LIKE 'topn%'")
    (args.out / 'environment.json').write_text(json.dumps(env, indent=2, default=str))
    cases = []
    for kind in ['transactions', 'multi', 'high']:
        for path in sorted((args.query_root / ('iceberg-41-nocache-' + kind)).glob('*.sql')):
            name = kind + '_' + path.stem
            if not args.cases or name in args.cases:
                cases.append((name, path.read_text().strip().rstrip(';'), kind))
    assert cases, 'No saved pinned queries found'
    if args.reverse:
        cases.reverse()
    summaries = []
    for name, query, kind in cases:
        assert 'FOR VERSION AS OF' in query
        select_variant(variants[0])
        oracle = sql(query)
        assert oracle, name + ': empty fixture'
        base = query.rsplit(' ORDER BY ', 1)[0]
        if kind == 'high':
            key = query.rsplit(' ORDER BY ', 1)[1].split()[0]
            edge = oracle[-1][0]
            predicate = f'{key} IS NULL'
            if edge is not None:
                assert isinstance(edge, int), 'Expected integer ordering key'
                predicate += f' OR {key} <= {edge}'
            complete = sql(f'SELECT * FROM ({base}) g WHERE {predicate}')
        else:
            complete = sql(base)
        truth = {row[:2]: row[2:] for row in complete}

        def check(rows):
            assert len(rows) == len(oracle), name + ': cardinality mismatch'
            assert len({row[:2] for row in rows}) == len(rows), name + ': duplicate groups'
            assert [row[0] for row in rows] == [row[0] for row in oracle], name + ': ordering mismatch'
            assert all(row[2:] == truth.get(row[:2]) for row in rows), name + ': incomplete aggregate'

        (args.out / (name + '.sql')).write_text(query + ';\n')
        routes = {}
        stats_sources = {}
        def statistics_sources(plan):
            return [line.strip() for line in plan.splitlines() if 'stats source:' in line]

        for variant in variants:
            select_variant(variant)
            for _ in range(3):
                check(sql(query))
            plan = '\n'.join(row[0] for row in sql('EXPLAIN VERBOSE ' + query))
            routes[variant[0]] = ('type: RANK' in plan, 'build runtime filters:' in plan)
            stats_sources[variant[0]] = statistics_sources(plan)
            if args.reference_plans:
                reference_variant = 'optimized' if variant[0] == 'no_backpressure' else variant[0]
                reference = (args.reference_plans / (name + '.' + reference_variant + '.plan')).read_text()
                assert routes[variant[0]] == ('type: RANK' in reference, 'build runtime filters:' in reference), \
                    name + ': RANK/RF differs from reference'
                assert stats_sources[variant[0]] == statistics_sources(reference), \
                    name + ': statistics source differs from reference'
            (args.out / (name + '.' + variant[0] + '.plan')).write_text(plan)
        print('BEGIN', name, flush=True)
        records = []
        for iteration in range(args.rounds):
            offset = iteration % len(variants)
            for variant in variants[offset:] + variants[:offset]:
                select_variant(variant)
                start = time.perf_counter()
                rows = sql(query)
                elapsed = (time.perf_counter() - start) * 1000
                check(rows)
                record = {'case': name, 'round': iteration, 'variant': variant[0], 'ms': elapsed}
                records.append(record)
                with (args.out / 'timings.jsonl').open('a') as stream:
                    stream.write(json.dumps(record) + '\n')
        result = {'case': name, 'executions': len(records), 'routes': routes,
                  'stats_sources': stats_sources, 'variants': {}}
        for variant in variants:
            values = [r['ms'] for r in records if r['variant'] == variant[0]]
            q = statistics.quantiles(values, n=4)
            result['variants'][variant[0]] = {'median_ms': statistics.median(values),
                                            'p25_ms': q[0], 'p75_ms': q[2]}
            select_variant(variant)
            final_plan = '\n'.join(row[0] for row in sql('EXPLAIN VERBOSE ' + query))
            assert routes[variant[0]] == ('type: RANK' in final_plan, 'build runtime filters:' in final_plan), \
                name + ': plan choice changed during timing'
            assert stats_sources[variant[0]] == statistics_sources(final_plan), \
                name + ': statistics source changed during timing'
            sql('SET enable_profile=true')
            check(sql(query))
            qid = sql('SELECT last_query_id()')[0][0]
            profile = sql('SELECT get_query_profile(%s)', (qid,))[0][0]
            (args.out / (name + '.' + variant[0] + '.profile')).write_text(profile or '')
            sql('SET enable_profile=false')
        summaries.append(result)
        (args.out / 'summary.json').write_text(json.dumps(summaries, indent=2))
        print('RESULT', json.dumps(result), flush=True)
    print('DONE', sum(s['executions'] for s in summaries), 'checked timed queries', flush=True)


if __name__ == '__main__':
    main()
