import math
import multiprocessing
import queue
import threading
from datetime import datetime, timezone
import argparse
import logging
import os
import tempfile
from contextlib import ExitStack

import pyarrow as pa
import pyarrow.parquet as pq

from query_sanitization import get_initial_term_stats, parse_run_date, stream_search_terms, detect_pii, export_search_queries_to_bigquery, export_sample_to_bigquery, record_job_metadata, load_nlp_model, resolve_nlp_n_process, filter_queries_for_sanitization, load_english_detection_model

import logging_config
import numpy
import pandas as pd
import spacy_fastlang
from google.cloud.bigquery_storage import BigQueryReadClient


import collections
import functools
import operator

UTC = timezone.utc
logging_config.configure_logging()
logger = logging.getLogger("sanitation_job")

pd.set_option("mode.copy_on_write", True)

parser = argparse.ArgumentParser(description="Sanitize Search Terms",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--run_date", help="Date to run sanitization over. Defaults to the current date - 1 day.", default="latest")
parser.add_argument("--sanitized_term_destination", help="Destination table for sanitary search terms")
parser.add_argument("--job_reporting_destination", help="Destination table for sanitation job metadata")
parser.add_argument("--unsanitized_term_sample_destination", help="Destination table for a sample of unsanitized search terms")
parser.add_argument("--nlp_n_process", type=int, default=None, help="Number of processes for spaCy NLP pipeline")
args = parser.parse_args()

df = pd.read_csv('Names_2010Census.csv')
census_surnames = set(str(name).lower() for name in df.name)

_worker_nlp = None
_worker_census_surnames = None


def _init_worker(census_surnames):
    """Each worker loads the model after forking instead of trying to load once and pass without copying."""
    global _worker_nlp, _worker_census_surnames
    _worker_nlp = load_nlp_model()
    _worker_nlp.add_pipe("language_detector")
    _worker_census_surnames = census_surnames


def _process_chunk(chunk_series):
    """
    Worker function: calls detect_pii on a chunk of texts.
    Accesses nlp and census_surnames from forked process globals.
    Returns (pii_risk, run_data, language_data) — all simple types.
    """
    return detect_pii(chunk_series, _worker_census_surnames, _worker_nlp)


def _pooled_detect_pii(pool, n_process, series):
    """Dispatch detect_pii across pool workers in chunks and merge results."""
    chunk_size = math.ceil(len(series) / n_process)
    chunks = [
        series.iloc[i:i + chunk_size]
        for i in range(0, len(series), chunk_size)
    ]
    results = pool.map(_process_chunk, chunks)

    pii_in_query_mask = []
    run_data = {key: 0 for key in results[0][1]}
    language_data = {}
    for chunk_pii, chunk_run_data, chunk_language_data in results:
        pii_in_query_mask.extend(chunk_pii)
        for key in chunk_run_data:
            run_data[key] += chunk_run_data[key]
        for lang, count in chunk_language_data.items():
            language_data[lang] = language_data.get(lang, 0) + count
    return pii_in_query_mask, run_data, language_data


_SENTINEL = object()


def _prefetch_iterator(iterable, batch_size=100):
    """Wrap an iterator to prefetch the next item in a background thread."""

    # give enough space in the queue that we can build up a few batches
    buf = queue.Queue(maxsize=3 * batch_size)

    def _producer():
        try:
            for item in iterable:
                buf.put(item)
                logger.info("producer inserted", extra={"queue_size": buf.qsize()})
        except Exception as e:
            buf.put(e)
        finally:
            buf.put(_SENTINEL)

    thread = threading.Thread(target=_producer, daemon=True)
    thread.start()

    while True:
        batch = []
        item = None
        # for whatever reason big query gives us back 2432 rows at a time
        # so we batch them up to make it worth passing it off to background processes
        while len(batch) < batch_size:
            item = buf.get()
            if item is _SENTINEL:
                break
            if isinstance(item, Exception):
                raise item
            batch.append(item)
        yield pa.concat_batches(batch)
        if item is _SENTINEL:
            break


def run_sanitation(args):
    start_time = datetime.now(UTC)
    last_checkpoint = start_time

    # stats before analysis
    total_terms = 0
    total_blank = 0

    # analyzed term stats
    total_run = 0
    total_allow_listed = 0
    total_cleared_in_sanitation = 0
    summary_run_data = {}
    summary_language_data = {}
    start_date, end_date = parse_run_date(args.run_date)
    n_process = resolve_nlp_n_process(args.nlp_n_process)
    logger.info("Starting sanitation job", extra={
        "start_date": start_date,
        "end_date": end_date,
        "n_nlp_processes": n_process,
    })
    logger.info("checkpoint_0: Job initialized", extra={
        "checkpoint_delta_seconds": 0,
    })

    data_validation_sample_list = []
    # use exit stack to avoid extra nesting from with blocks
    cleanup = ExitStack()
    # init as None so we can check for none in the main finally of the job
    parquet_writer = None


    try:
        initial_stats = get_initial_term_stats(start_date=start_date, end_date=end_date)
        total_terms = initial_stats.loc[0].total_term_count
        total_blank = initial_stats.loc[0].total_blank_count
        now = datetime.now(UTC)
        logger.info("checkpoint_1: Initial stats query completed", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

        result_row_iter = stream_search_terms(start_date=start_date, end_date=end_date) # load unsanitized search terms
        logger.info("Fetched rows from bigquery", extra={
            "total_rows": result_row_iter.total_rows,
        })
        now = datetime.now(UTC)
        logger.info("checkpoint_2: Stream search terms query completed", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

        bqstorage_client = BigQueryReadClient()
        unsanitized_search_term_stream = result_row_iter.to_arrow_iterable(bqstorage_client=bqstorage_client, max_stream_count=1000)
        now = datetime.now(UTC)
        logger.info("checkpoint_3: Dataframe iterable created", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

        english_nlp = load_english_detection_model()

        now = datetime.now(UTC)
        logger.info("checkpoint_3a: spaCy model loaded", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

        if n_process > 1:
            pool = multiprocessing.Pool(
                processes=n_process,
                initializer=_init_worker,
                initargs=(census_surnames,),
            )
            cleanup.enter_context(pool)
            nlp = None
        else:
            pool = None
            nlp = load_nlp_model()
            nlp.add_pipe("language_detector")

        sanitized_terms_tmp = cleanup.enter_context(
            tempfile.NamedTemporaryFile(suffix=f"_sanitized_terms_{start_date}.parquet")
        )
        # needs to match https://github.com/mozilla/bigquery-etl/blob/main/sql/moz-fx-data-shared-prod/search_terms_derived/merino_log_sanitized_v3/schema.yaml
        sanitized_terms_schema = pa.schema([
            ("timestamp", pa.timestamp("us", tz="UTC")),
            ("request_id", pa.string()),
            ("session_id", pa.string()),
            ("sequence_no", pa.int64()),
            ("query", pa.string()),
            ("country", pa.string()),
            ("region", pa.string()),
            ("dma", pa.string()),
            ("form_factor", pa.string()),
            ("browser", pa.string()),
            ("os_family", pa.string()),
        ])
        parquet_writer = pq.ParquetWriter(sanitized_terms_tmp.name, sanitized_terms_schema, compression='zstd')

        for idx, arrow_page in enumerate(_prefetch_iterator(unsanitized_search_term_stream)):
            raw_page = arrow_page.to_pandas()
            page_start = datetime.now(UTC)
            logger.info("Sanitizing dataframe of search terms", extra={
                "page_num": idx,
                "page_size": raw_page.shape[0],
            })
            logger.info("checkpoint_4: Page received from iterator", extra={
                "checkpoint_delta_seconds": (page_start - last_checkpoint).total_seconds(),
            })
            last_checkpoint = page_start

            total_run += raw_page.shape[0]

            one_percent_sample = raw_page.sample(frac = 0.01)
            data_validation_sample_list.append(one_percent_sample)

            allow_listed_terms_page = raw_page.loc[raw_page.present_in_allow_list]

            terms_to_sanitize = filter_queries_for_sanitization(english_nlp, raw_page.loc[~raw_page.present_in_allow_list])

            now = datetime.now(UTC)
            logger.info("checkpoint_5: Dataframe filtering completed", extra={
                "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
            })
            last_checkpoint = now

            if pool is not None:
                pii_in_query_mask, run_data, language_data = _pooled_detect_pii(
                    pool, n_process, terms_to_sanitize['query']
                )
            else:
                pii_in_query_mask, run_data, language_data = detect_pii(
                    terms_to_sanitize['query'], census_surnames, nlp
                )

            now = datetime.now(UTC)
            logger.info("checkpoint_6: PII detection completed", extra={
                "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
            })
            last_checkpoint = now
            # ~ reverses the mask so we get the queries WITHOUT PII in them
            sanitized_page = terms_to_sanitize.loc[~numpy.array(pii_in_query_mask)]

            total_allow_listed += allow_listed_terms_page.shape[0]
            total_cleared_in_sanitation += sanitized_page.shape[0]

            summary_language_data = dict(functools.reduce(operator.add,
                            map(collections.Counter, [summary_language_data, language_data])))
            summary_run_data = dict(functools.reduce(operator.add,
                            map(collections.Counter, [summary_run_data, run_data])))

            all_terms_to_keep = pd.concat([allow_listed_terms_page, sanitized_page])
            all_terms_to_keep = all_terms_to_keep.drop(columns=['present_in_allow_list'])

            # Cast columns that may arrive as non-string dtypes (e.g. float64 due to nulls) so that pyarrow knows how to
            # handle them
            all_terms_to_keep = all_terms_to_keep.astype({
                "request_id": "string", "session_id": "string", "sequence_no": "Int64",
                "query": "string", "country": "string", "region": "string",
                "dma": "string", "form_factor": "string", "browser": "string",
                "os_family": "string",
            })

            now = datetime.now(UTC)
            logger.info("checkpoint_7: Data preparation for write done", extra={
                "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
            })
            last_checkpoint = now

            parquet_writer.write_table(pa.Table.from_pandas(all_terms_to_keep, schema=sanitized_terms_schema))

            now = datetime.now(UTC)
            logger.info("checkpoint_8: Page written to local parquet", extra={
                "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
                "parquet_file_size_mb": round(os.path.getsize(sanitized_terms_tmp.name) / (1024 * 1024), 2),
            })
            last_checkpoint = now

        parquet_writer.close()

        now = datetime.now(UTC)
        logger.info("checkpoint_9: All pages processed, starting BigQuery export", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

        export_search_queries_to_bigquery(
            parquet_file_path=sanitized_terms_tmp.name,
            destination_table_id=args.sanitized_term_destination,
            date=start_date,
        )

        now = datetime.now(UTC)
        logger.info("checkpoint_9a: BigQuery export completed", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

        record_job_metadata(
            status='SUCCESS',
            started_at=start_time,
            ended_at=datetime.now(UTC),
            destination_table_id=args.job_reporting_destination,
            total_run=total_run,
            total_allow_listed=total_allow_listed,
            total_rejected=total_run - (total_allow_listed + total_cleared_in_sanitation),
            run_data=summary_run_data,
            language_data=summary_language_data,
            implementation_notes="Run with a page_size of UNLIMITED from script",
            total_terms_inclusive=total_terms,
            total_blank=total_blank
        )

        now = datetime.now(UTC)
        logger.info("checkpoint_10: Job metadata recorded", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

    except Exception as e:
        record_job_metadata(
            status='FAILURE',
            started_at=start_time,
            ended_at=datetime.now(UTC),
            destination_table_id=args.job_reporting_destination,
            failure_reason=str(e)
        )
        raise e
    finally:
        if parquet_writer:
            parquet_writer.close()
        cleanup.close()

    data_validation_sample = pd.concat(data_validation_sample_list, ignore_index=True)
    data_validation_sample = data_validation_sample.drop(columns=['present_in_allow_list'])

    now = datetime.now(UTC)
    logger.info("checkpoint_11: Starting validation sample export", extra={
        "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
    })
    last_checkpoint = now

    export_sample_to_bigquery(dataframe=data_validation_sample, sample_table_id=args.unsanitized_term_sample_destination, date=start_date)
    logger.info("Sanitation job complete!")

    now = datetime.now(UTC)
    logger.info("checkpoint_12: Job complete", extra={
        "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
    })

if __name__ == "__main__":
    run_sanitation(args=args)
