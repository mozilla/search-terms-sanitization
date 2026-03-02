"""
Benchmark: measure time-per-frame for detect_pii.

A "frame" mirrors one page from BigQuery's to_dataframe_iterable().
We simulate it by tiling the NER integration test data up to FRAME_SIZE rows.

Usage (from nightly-job/):
    python benchmark_frame.py
"""

import time
from pathlib import Path

import pandas as pd

from query_sanitization import detect_pii, load_nlp_model
import spacy_fastlang  # noqa: F401 — registers the language_detector pipe

FRAME_SIZE = 10_000  # rows per simulated BigQuery page

TEST_DATA_PATH = Path(__file__).parent / "test_data" / "ner_integration_test_data.csv"
CENSUS_PATH = Path(__file__).parent / "Names_2010Census.csv"


def build_frame(size: int) -> pd.Series:
    """Tile the integration test queries to reach `size` rows."""
    base = pd.read_csv(TEST_DATA_PATH)["query"]
    reps = -(-size // len(base))  # ceiling division
    return pd.concat([base] * reps, ignore_index=True).iloc[:size]


def main():
    print(f"Frame size: {FRAME_SIZE:,} queries")

    census_df = pd.read_csv(CENSUS_PATH)
    census_surnames = set(str(n).lower() for n in census_df.name)

    print("Loading NLP model…")
    t0 = time.perf_counter()
    nlp = load_nlp_model()
    nlp.add_pipe("language_detector")
    model_load_s = time.perf_counter() - t0
    print(f"  model load time : {model_load_s:.2f}s")
    print(f"  pipeline        : {nlp.pipe_names}")

    frame = build_frame(FRAME_SIZE)

    print("Running detect_pii…")
    t1 = time.perf_counter()
    pii_risk, run_data, _ = detect_pii(frame.copy(), census_surnames, nlp)
    elapsed = time.perf_counter() - t1

    flagged = sum(pii_risk)
    print(f"\n--- Results ---")
    print(f"  frame size      : {FRAME_SIZE:,} queries")
    print(f"  flagged as PII  : {flagged:,} ({flagged/FRAME_SIZE*100:.1f}%)")
    print(f"  elapsed         : {elapsed:.3f}s")
    print(f"  ms / query      : {elapsed/FRAME_SIZE*1000:.3f} ms")
    print(f"  queries / sec   : {FRAME_SIZE/elapsed:,.0f}")
    print(f"  run_data        : {run_data}")


if __name__ == "__main__":
    main()
