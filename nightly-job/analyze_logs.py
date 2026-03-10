"""Analyze sanitation job log files.

Parses JSONL log output from the sanitation job and prints:
  - A checkpoint timing table (count, total, avg, p50/p90/p95/p99, min, max, stddev)
    sorted by total time descending.
  - Overall throughput (total rows / wall-clock time) derived from
    page_size fields and the first/last log timestamps.

Usage:
    python analyze_logs.py <logfile.jsonl>
"""

import json
import math
import sys
from datetime import datetime, timezone

if len(sys.argv) < 2:
    raise ValueError("A log filename argument is required")
filename = sys.argv[1]

checkpoints = {}
total_rows = 0
first_timestamp = None
last_timestamp = None

for line in open(filename):
    raw = line.strip()
    # Find the JSON portion
    brace = raw.find('{')
    if brace == -1:
        continue
    try:
        obj = json.loads(raw[brace:])
    except Exception:
        continue

    # Parse nanosecond epoch timestamp from JSON
    ts_ns = obj.get('Timestamp')
    if ts_ns:
        ts = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
        if first_timestamp is None:
            first_timestamp = ts
        last_timestamp = ts

    fields = obj.get('Fields', {})
    msg = fields.get('msg', '')

    if 'page_size' in fields:
        total_rows += fields['page_size']

    if msg.startswith('checkpoint_'):
        name = msg.split(':')[0].strip()
        delta = fields.get('checkpoint_delta_seconds', 0)
        if name not in checkpoints:
            checkpoints[name] = {'count': 0, 'total_seconds': 0, 'min': float('inf'), 'max': 0, 'deltas': []}
        checkpoints[name]['count'] += 1
        checkpoints[name]['total_seconds'] += delta
        checkpoints[name]['min'] = min(checkpoints[name]['min'], delta)
        checkpoints[name]['max'] = max(checkpoints[name]['max'], delta)
        checkpoints[name]['deltas'].append(delta)

# Sort by total time descending
sorted_cp = sorted(checkpoints.items(), key=lambda x: x[1]['total_seconds'], reverse=True)

def percentile(sorted_vals, p):
    """Return the p-th percentile (0-100) from a pre-sorted list."""
    if not sorted_vals:
        return 0
    k = (len(sorted_vals) - 1) * p / 100
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] * (c - k) + sorted_vals[c] * (k - f)

print(f"{'Checkpoint':<40} {'Count':>6} {'Total(s)':>10} {'Avg(s)':>10} {'P50(s)':>10} {'P90(s)':>10} {'P95(s)':>10} {'P99(s)':>10} {'Min(s)':>10} {'Max(s)':>10} {'StdDev(s)':>10}")
print('-' * 146)
grand_total = 0
for name, stats in sorted_cp:
    avg = stats['total_seconds'] / stats['count'] if stats['count'] > 0 else 0
    variance = sum((d - avg) ** 2 for d in stats['deltas']) / stats['count'] if stats['count'] > 0 else 0
    stddev = variance ** 0.5
    sorted_deltas = sorted(stats['deltas'])
    p50 = percentile(sorted_deltas, 50)
    p90 = percentile(sorted_deltas, 90)
    p95 = percentile(sorted_deltas, 95)
    p99 = percentile(sorted_deltas, 99)
    grand_total += stats['total_seconds']
    print(f"{name:<40} {stats['count']:>6} {stats['total_seconds']:>10.2f} {avg:>10.2f} {p50:>10.4f} {p90:>10.4f} {p95:>10.4f} {p99:>10.4f} {stats['min']:>10.4f} {stats['max']:>10.4f} {stddev:>10.4f}")

print('-' * 146)
grand_count = sum(s['count'] for _, s in sorted_cp)
grand_avg = grand_total / grand_count if grand_count > 0 else 0
grand_min = min((s['min'] for _, s in sorted_cp), default=0)
grand_max = max((s['max'] for _, s in sorted_cp), default=0)
print(f"{'TOTAL':<40} {grand_count:>6} {grand_total:>10.2f}")
print(f"\nGrand total: {grand_total:.2f}s ({grand_total/60:.1f}min)")

# Throughput
if first_timestamp and last_timestamp and total_rows > 0:
    wall_seconds = (last_timestamp - first_timestamp).total_seconds()
    print(f"\n--- Throughput ---")
    print(f"Total rows:    {total_rows:,}")
    print(f"Wall time:     {wall_seconds:.1f}s ({wall_seconds/60:.1f}min)")
    if wall_seconds > 0:
        print(f"Throughput:    {total_rows / wall_seconds:,.1f} rows/s ({total_rows / wall_seconds * 60:,.0f} rows/min)")