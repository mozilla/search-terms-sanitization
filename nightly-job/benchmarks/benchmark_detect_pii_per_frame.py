"""
Benchmark: measure time-per-frame and peak memory for detect_pii.

A "frame" mirrors one page from BigQuery's to_dataframe_iterable().
We simulate it by tiling the NER integration test data up to FRAME_SIZE rows.

Each configuration runs in a fresh subprocess so peak RSS reflects only that
configuration's footprint. resource.getrusage() reports the OS-tracked peak,
which is reliable regardless of Python's allocator behaviour.

Most recently used to benchmark several config options for spaCy NER:
  d   = disable tok2vec + tagger + parser + attribute_ruler + lemmatizer
  d+e = disable tok2vec, exclude tagger + parser + attribute_ruler + lemmatizer
  e   = exclude tok2vec + tagger + parser + attribute_ruler + lemmatizer

Results are written to benchmark_detect_pii_per_frame.csv.

Usage:

1. Update FRAME_SIZE for the size of dataframe you want to test on
2. Update RUNS for the number of runs you want to do (to confirm consistent results)
3. Update CONFIGS to whatever different versions of detect_pii you want to compare
4. $python benchmarks/benchmark_detect_pii_per_frame.py
"""

import csv
import json
import resource
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd

FRAME_SIZE = 10_000  # rows per simulated BigQuery page
RUNS = 10
TEST_DATA_PATH = Path(__file__).parent.parent / "tests" / "test_data" / "ner_integration_test_data.csv"
CENSUS_PATH = Path(__file__).parent.parent / "Names_2010Census.csv"

CONFIGS = [
    (
        "disable_all",
        {"disable": ["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"]},
    ),
    (
        "disable_tok2vec_exclude_rest",
        {
            "disable": ["tok2vec"],
            "exclude": ["tagger", "parser", "attribute_ruler", "lemmatizer"],
        },
    ),
    (
        "exclude_all",
        {"exclude": ["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"]},
    ),
]
OUTPUT_CSV = Path(__file__).parent / "benchmark_detect_pii_per_frame.csv"

_WORKER_FLAG = "--_worker"

def build_frame(size: int) -> pd.Series:
    """Tile the integration test queries to reach `size` rows."""
    base = pd.read_csv(TEST_DATA_PATH)["query"]
    reps = -(-size // len(base))  # ceiling division
    return pd.concat([base] * reps, ignore_index=True).iloc[:size]

def _worker(config_json: str) -> None:
    """Entry point for subprocess workers. Prints a JSON result line."""
    import logging
    import spacy
    import spacy_fastlang  # noqa: F401
    from query_sanitization import detect_pii

    _STANDARD_LOG_ATTRS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__)

    class _ExtraFormatter(logging.Formatter):
        def format(self, record):
            base = super().format(record)
            extras = {k: v for k, v in record.__dict__.items() if k not in _STANDARD_LOG_ATTRS}
            if extras:
                return base + " | " + " ".join(f"{k}={v}" for k, v in extras.items())
            return base

    handler = logging.StreamHandler()
    handler.setFormatter(_ExtraFormatter())
    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging.INFO)

    config = json.loads(config_json)
    census_df = pd.read_csv(CENSUS_PATH)
    census_surnames = set(str(n).lower() for n in census_df.name)

    t0 = time.perf_counter()
    nlp = spacy.load("en_core_web_lg", **config)
    nlp.add_pipe("language_detector")
    load_s = time.perf_counter() - t0

    frame = build_frame(FRAME_SIZE)

    t1 = time.perf_counter()
    pii_risk, _, _ = detect_pii(frame.copy(), census_surnames, nlp)
    elapsed_s = time.perf_counter() - t1

    # OS-tracked peak RSS: bytes on macOS, kilobytes on Linux
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform != "darwin":
        peak *= 1024

    print(json.dumps({
        "load_s": load_s,
        "elapsed_s": elapsed_s,
        "flagged": int(sum(pii_risk)),
        "peak_rss_mib": peak / (1024 * 1024),
    }))


def _run_config(label: str, kwargs: dict) -> dict:
    """Spawn a fresh subprocess for one config and return its JSON result."""
    proc = subprocess.run(
        [sys.executable, __file__, _WORKER_FLAG, json.dumps(kwargs)],
        stdout=subprocess.PIPE,
        text=True,
    )
    if proc.returncode != 0:
        print(f"\nERROR in config '{label}' (see stderr above)", file=sys.stderr)
        sys.exit(1)
    lines = [l for l in proc.stdout.splitlines() if l.strip()]
    r = json.loads(lines[-1])
    r["total_s"] = r["load_s"] + r["elapsed_s"]
    r["queries_per_sec"] = FRAME_SIZE / r["elapsed_s"]
    r["flagged_as_pii"] = r["flagged"]
    return r


def main() -> None:
    labels = [label for label, _ in CONFIGS]
    metrics = ["peak_rss_mib", "model_load_s", "inference_s", "total_s", "queries_per_sec", "flagged_as_pii"]

    fieldnames = ["run"] + [f"{lbl}:{m}" for m in metrics for lbl in labels]

    rows = []
    for run in range(1, RUNS + 1):
        print(f"  run {run}/{RUNS}…", end="\r", flush=True)
        run_results = {}
        for lbl, kwargs in CONFIGS:
            r = _run_config(lbl, kwargs)
            r["inference_s"] = r.pop("elapsed_s")
            r["model_load_s"] = r.pop("load_s")
            run_results[lbl] = r

        row = {"run": run}
        for m in metrics:
            for lbl in labels:
                row[f"{lbl}:{m}"] = round(run_results[lbl][m], 3)
        rows.append(row)

    print(f"\nWriting {OUTPUT_CSV}…")
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done. {RUNS} runs × {len(CONFIGS)} configs written to {OUTPUT_CSV.name}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == _WORKER_FLAG:
        _worker(sys.argv[2])
    else:
        main()
