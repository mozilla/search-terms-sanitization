from datetime import datetime, timezone
import argparse
import logging
import tempfile
from contextlib import ExitStack

import pyarrow as pa
import pyarrow.parquet as pq

from query_sanitization import get_initial_term_stats, parse_run_date, stream_search_terms, detect_pii, export_search_queries_to_bigquery, export_sample_to_bigquery, record_job_metadata, load_nlp_model, resolve_nlp_n_process, filter_queries_for_sanitization, load_english_detection_model

import logging_config
import numpy
import pandas as pd
import spacy_fastlang

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
    logger.info("Starting sanitation job", extra={
        "start_date": start_date,
        "end_date": end_date,
    })
    logger.info("checkpoint_0: Job initialized", extra={
        "checkpoint_delta_seconds": 0,
    })

    data_validation_sample_list = []
    # use exit stack to avoid extra nesting from with blocks
    cleanup = ExitStack()

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

        unsanitized_search_term_stream = result_row_iter.to_dataframe_iterable()
        now = datetime.now(UTC)
        logger.info("checkpoint_3: Dataframe iterable created", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

        english_nlp = load_english_detection_model()

        nlp = load_nlp_model()
        nlp.add_pipe("language_detector")
        now = datetime.now(UTC)
        logger.info("checkpoint_3a: spaCy model loaded", extra={
            "checkpoint_delta_seconds": (now - last_checkpoint).total_seconds(),
        })
        last_checkpoint = now

        sanitized_terms_tmp = cleanup.enter_context(
            tempfile.NamedTemporaryFile(suffix=f"_sanitized_terms_{start_date}.parquet")
        )
        sanitized_terms_schema = pa.schema([
            ("timestamp", pa.timestamp("us", tz="UTC")),
            ("request_id", pa.string()),
            ("session_id", pa.string()),
            ("sequence_no", pa.string()),
            ("query", pa.string()),
            ("country", pa.string()),
            ("region", pa.string()),
            ("dma", pa.string()),
            ("form_factor", pa.string()),
            ("browser", pa.string()),
            ("os_family", pa.string()),
        ])
        parquet_writer = pq.ParquetWriter(sanitized_terms_tmp.name, sanitized_terms_schema)

        for idx, raw_page in enumerate(unsanitized_search_term_stream):
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

            pii_in_query_mask, run_data, language_data = detect_pii(terms_to_sanitize['query'], census_surnames, nlp, n_process=resolve_nlp_n_process(args.nlp_n_process))

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
                "request_id": "string", "session_id": "string", "sequence_no": "string",
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

run_sanitation(args=args)
