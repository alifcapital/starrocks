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

"""Build GCC/Clang training/PGO binaries and merge stopped pre-live nodes' profiles."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def output(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def profiles(directory, pattern='*.gcda'):
    files = sorted(directory.rglob(pattern))
    if not files or any(p.stat().st_size < 16 for p in files):
        raise ValueError(f'No complete profiles in {directory}; drain and gracefully stop training BE first')
    return files


def write_json(path, data):
    path.write_text(json.dumps(data, indent=2) + '\n')


def compiler():
    cache = ROOT / 'be/build_Release/CMakeCache.txt'
    for line in cache.read_text().splitlines():
        if line.startswith(('CMAKE_CXX_COMPILER:FILEPATH=', 'CMAKE_CXX_COMPILER:STRING=')):
            executable = line.split('=', 1)[1]
            return {'path': executable, 'version': output(executable, '--version')}
    raise ValueError('Cannot find C++ compiler in Release CMake cache')


def build(args):
    if args.jobs < 1:
        raise ValueError('--jobs must be positive')
    # Exact source, build location and build flags must match between training and use.
    if output('git', 'status', '--porcelain', '--untracked-files=normal'):
        raise ValueError('Commit tracked changes before building a reproducible training binary')
    extra = args.build_args
    if extra[:1] == ['--']:
        extra = extra[1:]
    if any(a.startswith(('--pgo', '--with-lto', '--with-bolt', '--clean', '--output')) for a in extra):
        raise ValueError('Use the wrapper options; do not override PGO/LTO/output or clean the build')
    run = args.run.resolve()
    stamp = {'revision': output('git', 'rev-parse', 'HEAD'), 'source_root': str(ROOT),
             'lto': args.lto, 'bolt': args.bolt, 'build_args': extra,
             'environment': {k: os.environ.get(k, '') for k in
                             ['CC', 'CXX', 'STARROCKS_GCC_HOME', 'STARROCKS_LLVM_HOME',
                              'STARROCKS_LINKER', 'STARROCKS_CXX_COMMON_FLAGS',
                              'STARROCKS_CXX_LINKER_FLAGS', 'USE_STAROS']}}
    if args.mode == 'generate':
        # Never mix a fresh training build with data from an older build.
        run.mkdir(parents=True, exist_ok=False)
        (run / 'raw').mkdir()
    else:
        old = json.loads((run / 'training.json').read_text())
        for key, value in stamp.items():
            if old[key] != value:
                raise ValueError(f'Training mismatch: {key}')
        if old['compiler'] != compiler():
            raise ValueError('Training compiler changed')
        profiles(run / 'raw', 'merged.profdata' if 'clang' in old['compiler']['version'].lower() else '*.gcda')
        # Profile content changes do not change compiler flags. Force recompilation,
        # preserving third-party libraries and avoiding stale PGO object files.
        subprocess.run(['cmake', '--build', str(ROOT / 'be/build_Release'), '--target', 'clean'], check=True)
    env = dict(os.environ, BUILD_TYPE='Release', CCACHE_DISABLE='1')
    cmd = ['bash', './build.sh', '--be', '-j', str(args.jobs), '--pgo-' + args.mode, str(run / 'raw')]
    if args.lto:
        cmd.append('--with-lto')
    if args.bolt:
        cmd.append('--with-bolt')
    subprocess.run(cmd + extra, cwd=ROOT, env=env, check=True)
    stamp['compiler'] = compiler()
    binary = ROOT / 'be/output/lib/starrocks_be'
    if not binary.is_file():
        raise ValueError(f'Build finished but binary missing: {binary}')
    stamp['binary_sha256'] = digest(binary)
    write_json(run / ('training.json' if args.mode == 'generate' else 'optimized.json'), stamp)
    if args.mode == 'use':
        write_json(run / 'profile-sha256.json', {str(p.relative_to(run)): digest(p) for p in profiles(run / 'raw', 'merged.profdata' if 'clang' in stamp['compiler']['version'].lower() else '*.gcda')})
    print(f'{args.mode} complete: {binary}; receipt: {run}')


def merge(args):
    sources = [p.resolve() for p in args.inputs]
    receipts = [json.loads((p / 'training.json').read_text()) for p in sources]
    if any(r != receipts[0] for r in receipts[1:]):
        raise ValueError('Only profiles from the identical training build may be merged')
    clang = 'clang' in receipts[0]['compiler']['version'].lower()
    for p in sources:
        profiles(p / 'raw', '*.profraw' if clang else '*.gcda')
    if clang and not args.llvm_profdata or not clang and not args.gcov_tool:
        raise ValueError('Specify --llvm-profdata for Clang or --gcov-tool for GCC')
    target = args.output.resolve()
    if target.exists():
        raise ValueError('Output must be a new directory')
    target.parent.mkdir(parents=True, exist_ok=True)
    # Use the gcov-tool from the same GCC installation that built the BE.
    expected = receipts[0]['compiler']['version'].splitlines()[0]
    print('Training compiler:', expected)
    with tempfile.TemporaryDirectory(dir=target.parent) as tmp:
        tmp = Path(tmp)
        result = tmp / 'result'
        result.mkdir()
        if clang:
            (result / 'raw').mkdir()
            files = [str(p) for source in sources for p in profiles(source / 'raw', '*.profraw')]
            # Avoid command-line length limits for profiles from many processes.
            listing = tmp / 'profiles.txt'
            listing.write_text('\n'.join(files) + '\n')
            subprocess.run([args.llvm_profdata, 'merge', '--failure-mode=any',
                            '-f', str(listing), '-o', str(result / 'raw/merged.profdata')], check=True)
            profiles(result / 'raw', 'merged.profdata')
        else:
            current = tmp / 'accumulated'
            shutil.copytree(sources[0] / 'raw', current)
            for i, source in enumerate(sources[1:]):
                next_dir = tmp / f'merge-{i}'
                subprocess.run([args.gcov_tool, 'merge', str(current), str(source / 'raw'),
                                '-o', str(next_dir)], check=True)
                current = next_dir
            shutil.copytree(current, result / 'raw')
            profiles(result / 'raw')
        write_json(result / 'training.json', receipts[0])
        write_json(result / 'inputs.json', [str(p) for p in sources])
        result.rename(target)
    print(f'Merged {len(sources)} profile directories into {target}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    b = sub.add_parser('build')
    b.add_argument('mode', choices=['generate', 'use'])
    b.add_argument('run', type=Path)
    b.add_argument('--jobs', type=int, required=True)
    b.add_argument('--lto', action='store_true')
    b.add_argument('--bolt', action='store_true')
    m = sub.add_parser('merge')
    m.add_argument('--gcov-tool')
    m.add_argument('--llvm-profdata')
    m.add_argument('--output', type=Path, required=True)
    m.add_argument('inputs', type=Path, nargs='+')
    # Extra build.sh flags follow --, without argparse swallowing wrapper options.
    import sys
    argv = sys.argv[1:]
    extra = []
    if '--' in argv:
        split = argv.index('--')
        argv, extra = argv[:split], argv[split + 1:]
    args = parser.parse_args(argv)
    if args.command == 'merge' and extra:
        parser.error('merge does not accept extra build flags')
    args.build_args = extra
    try:
        (build if args.command == 'build' else merge)(args)
    except (ValueError, OSError, subprocess.CalledProcessError) as exc:
        parser.exit(1, f'PGO: {exc}\n')


if __name__ == '__main__':
    main()
