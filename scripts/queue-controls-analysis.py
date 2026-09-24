#!/usr/bin/env python3
"""Validate and summarize the retained fixed-arrival queue-control study."""
import argparse, gzip, hashlib, json, math, statistics
from pathlib import Path

CASES = {
    'fairness-pressure': ['taskforge-fifo-static', 'taskforge-fairness-only'],
    'admission-pressure': ['taskforge-fifo-static', 'taskforge-admission-only'],
    'dependency-pressure': ['taskforge-fifo-static', 'taskforge-budget-only', 'taskforge-static-capacity'],
    'adaptive-pressure': ['taskforge-fifo-static', 'taskforge-adaptive-only', 'taskforge-static-capacity'],
    'stable-light': ['taskforge-fifo-static', 'taskforge-fairness-only', 'taskforge-admission-only', 'taskforge-budget-only', 'taskforge-adaptive-only'],
    'fragile-light': ['taskforge-fifo-static', 'taskforge-budget-only', 'taskforge-adaptive-only', 'taskforge-static-capacity'],
}
METRICS = ('slo_success', 'accepted_fraction', 'conditional_p99_ms', 'downstream_failure_rate', 'downstream_over_capacity_rate', 'ready_backlog_max', 'deferred_backlog_max', 'total_backlog_max', 'controller_min', 'controller_max', 'redis_cpu_seconds', 'dispatch_p99_ms')

def percentile(values, p):
    if not values: return None
    values = sorted(values)
    return values[max(0, math.ceil(p * len(values)) - 1)]

def load_result(path):
    with gzip.open(path, 'rt') as f: return json.load(f)

def summarize(result, trace):
    warm, steady = trace['profile']['warmup'], trace['profile']['steady_state']
    start = warm; end = warm + steady
    arrivals = {x['id']: x for x in trace['arrivals'] if start <= nanos(x['at'], trace['start_at']) < end}
    tenants = {x['tenant']: x for x in result.get('tenants', [])}
    offered = sum(x['offered'] for x in tenants.values())
    accepted = sum(x['accepted'] for x in tenants.values())
    compliant = sum(x['slo_compliant'] for x in tenants.values())
    completed = sum(x['completed'] for x in tenants.values())
    durations = []
    by_task = {}
    for task in result.get('tasks', []):
        if task['task_id'] not in arrivals or task.get('outcome') != 'completed': continue
        old = by_task.get(task['task_id'])
        if old is None or task['attempt'] > old['attempt']: by_task[task['task_id']] = task
    for task_id, task in by_task.items():
        if task['completed_at']:
            durations.append((parse_time(task['completed_at']) - (parse_time(result['run_epoch']) + nanos(arrivals[task_id]['at'], trace['start_at']) - nanos(trace['start_at'], trace['start_at'])))/1e6)
    telemetry = result.get('telemetry', [])
    def max_backlog(key): return max((p['backlog'][key] for p in telemetry), default=0)
    controllers = [p['controller']['effective_concurrency'] for p in telemetry]
    downstream = result.get('downstream', [])
    return {
        'offered': offered, 'accepted': accepted, 'completed': completed, 'slo_compliant': compliant,
        'slo_success': compliant / offered if offered else 0,
        'accepted_fraction': accepted / offered if offered else 0,
        'unresolved': offered - completed,
        'conditional_p99_ms': percentile(durations, .99),
        'downstream_failure_rate': sum(x['failed'] for x in downstream) / len(downstream) if downstream else 0,
        'downstream_over_capacity_rate': sum(x['overlap'] > x['capacity'] for x in downstream) / len(downstream) if downstream else 0,
        'ready_backlog_max': max_backlog('ready'), 'deferred_backlog_max': max_backlog('deferred'),
        'total_backlog_max': max((sum(p['backlog'][key] for key in ('ready', 'deferred', 'retry', 'dlq')) for p in telemetry), default=0),
        'controller_min': min(controllers, default=0), 'controller_max': max(controllers, default=0),
        'redis_cpu_seconds': (telemetry[-1]['redis']['cpu_seconds'] - telemetry[0]['redis']['cpu_seconds']) if len(telemetry) > 1 else 0,
        'dispatch_p99_ms': result['harness']['dispatch_lag']['p99'] / 1e6,
    }

def parse_time(value):
    from datetime import datetime
    return datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() * 1e9

def nanos(value, anchor):
    return parse_time(value) - parse_time(anchor)

def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--output', required=True); args = ap.parse_args()
    data = Path(args.data); meta = json.loads((data/'metadata.json').read_text()); rows = []; seen = set()
    for cell in meta['cells']:
        key = (cell['profile'], cell['seed'], cell['system'])
        if key in seen: raise SystemExit(f'duplicate cell {key}')
        seen.add(key)
        if cell['status'] != 'ok': raise SystemExit(f"cell {key} is {cell['status']}")
        raw_path = data/'raw'/cell['file']
        compressed = raw_path.read_bytes()
        if hashlib.sha256(compressed).hexdigest() != cell['sha256']: raise SystemExit(f'digest mismatch {key}')
        result = load_result(raw_path)
        trace = json.loads(gzip.open(data/'traces'/f"{cell['profile']}-{cell['seed']}.json.gz", 'rt').read())
        if result['trace_id'] != trace['id'] or result['system'] != cell['system']: raise SystemExit(f'identity mismatch {key}')
        values = summarize(result, trace); rows.append({'profile':cell['profile'],'seed':cell['seed'],'system':cell['system'],**values})
    expected = {(p,s,a) for p, arms in CASES.items() for s in meta['seeds'] for a in arms}
    if seen != expected: raise SystemExit(f'grid mismatch: got {len(seen)}, want {len(expected)}')
    output = Path(args.output); output.mkdir(parents=True, exist_ok=True)
    result = {'schema':'taskforge-queue-controls-analysis/v1','method':'fixed immutable open-loop traces; per-cell metrics and paired seed differences','rows':rows,'contrasts':[]}
    by = {(r['profile'],r['seed'],r['system']):r for r in rows}
    for profile, arms in CASES.items():
        base = 'taskforge-fifo-static'
        for arm in arms[1:]:
            for metric in METRICS:
                diffs = [by[(profile,s,arm)][metric] - by[(profile,s,base)][metric] for s in meta['seeds']]
                result['contrasts'].append({'profile':profile,'contrast':f'{arm} minus {base}','metric':metric,'median_difference':statistics.median(diffs),'values':diffs})
    (output/'analysis.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    lines=['# Corrected queue-control comparison','', 'All cells use immutable open-loop arrivals. Metrics use all offered measurement-window tasks as the success denominator; conditional p99 includes only successful completions.', '', '| Profile | System | Offered | Accepted | Completed | SLO success | p99 ms | downstream failures | over-capacity | ready max | deferred max | concurrency range |', '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |']
    for r in rows:
        lines.append(f"| {r['profile']} | {r['system']} | {r['offered']} | {r['accepted']} | {r['completed']} | {r['slo_success']:.3f} | {r['conditional_p99_ms'] if r['conditional_p99_ms'] is not None else 'n/a'} | {r['downstream_failure_rate']:.3f} | {r['downstream_over_capacity_rate']:.3f} | {r['ready_backlog_max']} | {r['deferred_backlog_max']} | {r['controller_min']:.0f}–{r['controller_max']:.0f} |")
    lines += ['', '## Paired contrasts', '', '| Profile | Contrast | Metric | Median difference | Per-seed values |', '| --- | --- | --- | ---: | --- |']
    for c in result['contrasts']:
        lines.append(f"| {c['profile']} | {c['contrast']} | {c['metric']} | {c['median_difference']:.4g} | {', '.join(f'{x:.4g}' for x in c['values'])} |")
    lines += ['', 'These are descriptive runs on one host. They test the controls under fixed offered load and modeled downstream behavior; they do not establish production SLOs or general effects.']
    (output/'analysis.md').write_text('\n'.join(lines)+'\n')
    print(output/'analysis.md')
if __name__ == '__main__': main()
