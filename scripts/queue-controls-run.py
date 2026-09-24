#!/usr/bin/env python3
"""Run the fixed open-loop queue-control comparison in an isolated Redis container."""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import random
import subprocess
import tarfile
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
STUDY = ROOT / 'research/queue-controls'
SEEDS = [20260925, 20260926]
CASES = {
    'fairness-pressure': ['fifo-static', 'fairness-only'],
    'admission-pressure': ['fifo-static', 'admission-only'],
    'dependency-pressure': ['fifo-static', 'budget-only', 'static-capacity'],
    'adaptive-pressure': ['fifo-static', 'adaptive-only', 'static-capacity'],
    'stable-light': ['fifo-static', 'fairness-only', 'admission-only', 'budget-only', 'adaptive-only'],
    'fragile-light': ['fifo-static', 'budget-only', 'adaptive-only', 'static-capacity'],
}

def sha(data):
    return hashlib.sha256(data).hexdigest()

def run(*args, **kwargs):
    return subprocess.check_output(args, cwd=kwargs.pop('cwd', ROOT), text=True, **kwargs).strip()

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    for name in ['raw', 'traces', 'source-overlays']:
        (output / name).mkdir()
    metadata = {'schema': 'taskforge-queue-controls/v1', 'status': 'running',
                'source_parent_commit': run('git', 'rev-parse', 'HEAD'),
                'seeds': SEEDS, 'matrix': CASES, 'cells': [], 'traces': [],
                'host': {'os': platform.system(), 'architecture': platform.machine(), 'logical_cpus': os.cpu_count()},
                'go_version': run('go', 'version'), 'inputs': {}, 'source_files': {}}
    def save():
        (output / 'metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    for path in [STUDY / 'plan.md', Path(__file__).resolve(), *sorted((STUDY / 'profiles').glob('*.json'))]:
        metadata['inputs'][str(path.relative_to(ROOT))] = sha(path.read_bytes())
    archive = subprocess.check_output(['git', 'archive', 'HEAD'], cwd=ROOT)
    metadata['parent_archive_sha256'] = sha(archive)
    files = run('git', 'ls-files').splitlines()
    files += ['research/cmd/experiment-neutral/main_test.go']
    files = sorted(set(p for p in files if p.endswith('.go') or Path(p).name in ('go.mod', 'go.sum')))
    container = 'taskforge-queue-controls-' + uuid.uuid4().hex[:10]
    env = dict(os.environ, GOCACHE='/tmp/taskforge-queue-controls-cache', CGO_ENABLED='0', GOMAXPROCS='8')
    metadata['gomaxprocs'] = 8
    with tempfile.TemporaryDirectory(prefix='taskforge-queue-controls-') as tmp:
        source = Path(tmp) / 'source'
        source.mkdir()
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            tar.extractall(source, filter='data')
        for name in files:
            data = (ROOT / name).read_bytes()
            metadata['source_files'][name] = sha(data)
            target = source / name
            if not target.exists() or target.read_bytes() != data:
                overlay = output / 'source-overlays' / name
                overlay.parent.mkdir(parents=True, exist_ok=True)
                overlay.write_bytes(data)
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        binary = Path(tmp) / 'neutral'
        trace_binary = Path(tmp) / 'trace'
        for cmd, target in [('experiment-neutral', binary), ('experiment-trace', trace_binary)]:
            run('go', '-C', 'research', 'build', '-trimpath', '-buildvcs=false', '-o', str(target), './cmd/' + cmd, cwd=source, env=env)
        metadata['binary_sha256'] = sha(binary.read_bytes())
        metadata['trace_binary_sha256'] = sha(trace_binary.read_bytes())
        # Freeze every input trace before observing any retained cell.
        for profile in CASES:
            for seed in SEEDS:
                path = output / 'traces' / f'{profile}-{seed}.json'
                run(str(trace_binary), '-profile', str(STUDY / 'profiles' / f'{profile}.json'), '-seed', str(seed), '-output', str(path), env=env)
                data = path.read_bytes()
                compressed = gzip.compress(data, mtime=0)
                path.with_suffix('.json.gz').write_bytes(compressed)
                metadata['traces'].append({'file': path.name + '.gz', 'sha256': sha(compressed), 'trace_sha256': json.loads(data)['sha256']})
        save()
        try:
            run('docker', 'run', '--detach', '--name', container, '-p', '127.0.0.1::6379', 'redis:7.4', 'redis-server', '--save', '', '--appendonly', 'no')
            metadata['redis_image'] = run('docker', 'inspect', '--format', '{{.Image}}', container)
            addr = run('docker', 'port', container, '6379/tcp')
            for _ in range(50):
                try:
                    if run('docker', 'exec', container, 'redis-cli', 'ping') == 'PONG':
                        break
                except subprocess.CalledProcessError:
                    pass
                time.sleep(.1)
            else:
                raise RuntimeError('dedicated Redis did not become ready')
            metadata['redis'] = {'topology': 'dedicated Redis 7.4 container on loopback', 'db': 14,
                                 'config': run('docker', 'exec', container, 'redis-cli', '--json', 'CONFIG', 'GET', 'appendonly', 'save', 'maxmemory-policy')}
            for profile, arms in CASES.items():
                for seed in SEEDS:
                    order = list(arms)
                    random.Random(seed).shuffle(order)
                    for position, arm in enumerate(order):
                        system = 'taskforge-' + arm
                        print(f'{profile} seed={seed} {system}', flush=True)
                        trace = output / 'traces' / f'{profile}-{seed}.json'
                        cell_dir = Path(tmp) / f'{profile}-{seed}-{arm}'
                        command = [str(binary), '-trace', str(trace), '-output', str(cell_dir), '-systems', system,
                                   '-redis-addr', addr, '-redis-db', '14', '-concurrency', '8',
                                   '-taskforge-admission-pending', '32', '-snapshot-period', '100ms', '-drain-timeout', '15s']
                        cell = {'profile': profile, 'seed': seed, 'system': system, 'position': position,
                                'arguments': command[1:], 'status': 'running'}
                        # Paths are task-generated, with no user environment values.
                        cell['arguments'] = [arg.replace(str(output), '<output>').replace(str(tmp), '<temporary>') for arg in cell['arguments']]
                        metadata['cells'].append(cell)
                        save()
                        try:
                            result = subprocess.run(command, cwd=source, env=env, capture_output=True, text=True, timeout=90)
                            if result.returncode:
                                raise RuntimeError(result.stderr[-2000:])
                            raw_path = cell_dir / f'{profile}-{seed}--{system}--r0.json'
                            raw = json.loads(raw_path.read_bytes())
                            assert not raw['excluded'] and raw['system'] == system
                            assert len(raw['enqueues']) == len(json.loads(trace.read_bytes())['arrivals'])
                            compressed = gzip.compress(raw_path.read_bytes(), mtime=0)
                            name = raw_path.name + '.gz'
                            (output / 'raw' / name).write_bytes(compressed)
                            cell.update(status='ok', file=name, sha256=sha(compressed))
                        except Exception as error:
                            cell.update(status='failed', failure=str(error))
                        save()
            metadata['status'] = 'completed' if all(c['status'] == 'ok' for c in metadata['cells']) else 'failed'
            save()
        finally:
            subprocess.run(['docker', 'rm', '-f', container], capture_output=True, check=False)
            for path in (output / 'traces').glob('*.json'):
                path.unlink()
    if metadata['status'] != 'completed':
        raise SystemExit('one or more retained cells failed; inspect metadata.json')
    print(f"retained {len(metadata['cells'])} cells in {output}", flush=True)

if __name__ == '__main__':
    main()
