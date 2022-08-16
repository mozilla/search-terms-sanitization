"""
Utilities to load datasets and assets.
"""

from pathlib import Path

import pandas as pd
from google.cloud import bigquery

ASSET_DIR = Path(__file__).parent / ".." / "assets"

SUGGEST_QUERIES_SQL = """
SELECT
    {ts_expr} AS merino_timestamp,
    jsonPayload.fields.query AS query,
    jsonPayload.fields.session_id AS session_id,
    jsonPayload.fields.sequence_no AS sequence_no,
FROM `suggest-searches-prod-a30f.logs.stdout`
WHERE
    jsonPayload.type = "web.suggest.request"
"""

TERMINAL_QUERIES_SQL = """
with query_logs as (
    SELECT
        {ts_expr} AS merino_timestamp,
        jsonPayload.fields.*
    FROM `suggest-searches-prod-a30f.logs.stdout`
    WHERE
        jsonPayload.type = "web.suggest.request"
        and jsonPayload.fields.session_id is not null
), terminal_queries as (
    select 
        --If the duplicate sequence_no's do not get resolved, then we also need to
        --choose the longest query for a given sequence_no.
        --See: https://bugzilla.mozilla.org/show_bug.cgi?id=1779267
        array_agg(ql order by sequence_no desc limit 1)[offset(0)].*
    from query_logs ql
    group by ql.session_id
)
select
    merino_timestamp,
    query,
    session_id,
    sequence_no,
from terminal_queries
"""

def get_scrabble():
    """Load the Scrabble dictionary."""
    with open(f"{ASSET_DIR}/word.list") as f:
        scrabble = [x.strip() for x in f.readlines()]
    return sorted(scrabble)


def get_surnames():
    """Load the dataset of common surnames from the US Census."""
    # The surname "Null" is getting parsed as NaN.
    # On loading from CSV, ignore default missing value identifiers
    # that are all upper-case alphabetical.
    na_vals = [
        w for w in pd._libs.parsers.STR_NA_VALUES
        if not (w.isupper() and w.isalpha())
    ]
    df = pd.read_csv(f"{ASSET_DIR}/Names_2010Census.csv", na_values=na_vals, keep_default_na=False)
    return df.name.str.lower().tolist()[:-1]


def get_queries(truncate_ts=True, terminal_only=False):
    """Returns a 2-day dataset of unsanitized search queries.

    truncate_ts: should timestamps be truncated to the hour?
    terminal_only: return all queries, or only terminal ones?
    """
    if truncate_ts:
        ts_expr = "TIMESTAMP_TRUNC(timestamp, HOUR)"
    else:
        ts_expr = "timestamp"

    client = bigquery.Client()
    if terminal_only:
        query = TERMINAL_QUERIES_SQL
    else:
        query = SUGGEST_QUERIES_SQL
    query_job = client.query(query.format(ts_expr=ts_expr))
    df = query_job.to_dataframe()
    print(f"Fetched {len(df)} queries between {df.merino_timestamp.min()} and {df.merino_timestamp.max()}.")
    return df
