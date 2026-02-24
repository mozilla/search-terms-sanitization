from datetime import datetime, timezone
import argparse
import logging

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



class CheckPointer:
    """Class for helping us measure the time between parts of the job."""

    def __init__(self):
        self.last_checkpoint = datetime.now(UTC)

    def __call__(self, message: str):
        now = datetime.now(UTC)
        logger.info(message, extra = {"checkpoint_delta_seconds": (now - self.last_checkpoint).total_seconds()})
        self.last_checkpoint = now


def run_sanitation(args):
    check_point = CheckPointer()
    start_time = check_point.last_checkpoint

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
    check_point("checkpoint_0: Job initialized")

    data_validation_sample_list = []

    try:
        initial_stats = get_initial_term_stats(start_date=start_date, end_date=end_date)
        total_terms = initial_stats.loc[0].total_term_count
        total_blank = initial_stats.loc[0].total_blank_count
        check_point("checkpoint_1: Initial stats query completed")

        result_row_iter = stream_search_terms(start_date=start_date, end_date=end_date) # load unsanitized search terms
        logger.info("Fetched rows from bigquery", extra={
            "total_rows": result_row_iter.total_rows,
        })
        check_point("checkpoint_2: Stream search terms query completed")
        unsanitized_search_term_stream = result_row_iter.to_dataframe_iterable()

        check_point("checkpoint_3: Dataframe iterable created")

        english_nlp = load_english_detection_model()

        nlp = load_nlp_model()
        nlp.add_pipe("language_detector")
        check_point("checkpoint_3a: spaCy model loaded")

        for idx, raw_page in enumerate(unsanitized_search_term_stream):
            logger.info("Sanitizing dataframe of search terms", extra={
                "page_num": idx,
                "page_size": raw_page.shape[0],
            })
            check_point("checkpoint_4: Page received from iterator")

            total_run += raw_page.shape[0]

            one_percent_sample = raw_page.sample(frac = 0.01)
            data_validation_sample_list.append(one_percent_sample)

            allow_listed_terms_page = raw_page.loc[raw_page.present_in_allow_list]

            terms_to_sanitize = filter_queries_for_sanitization(english_nlp, raw_page.loc[~raw_page.present_in_allow_list])

            check_point("checkpoint_5: Dataframe filtering completed")

            pii_in_query_mask, run_data, language_data = detect_pii(terms_to_sanitize['query'], census_surnames, nlp, n_process=resolve_nlp_n_process(args.nlp_n_process))

            check_point("checkpoint_6: PII detection completed")

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

            delete_destination_partition = idx == 0

            check_point("checkpoint_7: Starting BigQuery export")

            export_search_queries_to_bigquery(
                dataframe=all_terms_to_keep,
                destination_table_id=args.sanitized_term_destination,
                date=start_date,
                delete_partition=delete_destination_partition
            )

            check_point("checkpoint_8: BigQuery export completed")


        check_point("checkpoint_9: All pages processed")

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

        check_point("checkpoint_10: Job metadata recorded")

    except Exception as e:
        record_job_metadata(
            status='FAILURE',
            started_at=start_time,
            ended_at=datetime.now(UTC),
            destination_table_id=args.job_reporting_destination,
            failure_reason=str(e)
        )
        raise e

    data_validation_sample = pd.concat(data_validation_sample_list, ignore_index=True)
    data_validation_sample = data_validation_sample.drop(columns=['present_in_allow_list'])

    check_point("checkpoint_11: Starting validation sample export")
    export_sample_to_bigquery(dataframe=data_validation_sample, sample_table_id=args.unsanitized_term_sample_destination, date=start_date)
    logger.info("Sanitation job complete!")

    check_point("checkpoint_12: Job complete")

run_sanitation(args=args)
