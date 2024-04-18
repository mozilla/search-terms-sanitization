from datetime import datetime, timezone
import argparse
import logging

from query_sanitization import get_initial_term_stats, parse_run_date, stream_search_terms, detect_pii, export_search_queries_to_bigquery, export_sample_to_bigquery, record_job_metadata
import logging_config
import numpy
import pandas as pd
import asyncio

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
args = parser.parse_args()

df = pd.read_csv('Names_2010Census.csv')
census_surnames = [str(name).lower() for name in df.name]

async def run_sanitation(args):
    start_time = datetime.now(UTC)
    
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
    
    data_validation_sample = pd.DataFrame()

    try:    
        initial_stats = get_initial_term_stats(start_date=start_date, end_date=end_date)
        total_terms = initial_stats.loc[0].total_term_count
        total_blank = initial_stats.loc[0].total_blank_count

        result_row_iter = stream_search_terms(start_date=start_date, end_date=end_date) # load unsanitized search terms
        logger.info("Fetched rows from bigquery", extra={
            "total_rows": result_row_iter.total_rows,
        })
        unsanitized_search_term_stream = result_row_iter.to_dataframe_iterable()
        for idx, raw_page in enumerate(unsanitized_search_term_stream):
            logger.info("Sanitizing dataframe of search terms", extra={
                "page_num": idx,
                "page_size": raw_page.shape[0],
            })
            total_run += raw_page.shape[0]
        
            one_percent_sample = raw_page.sample(frac = 0.01)
            data_validation_sample = pd.concat([data_validation_sample, one_percent_sample])
        
            allow_listed_terms_page = raw_page.loc[raw_page.present_in_allow_list]
            unsanitized_unallowlisted_terms = raw_page.loc[~raw_page.present_in_allow_list]

            pii_in_query_mask, run_data, language_data = await detect_pii(unsanitized_unallowlisted_terms['query'], census_surnames)
            # ~ reverses the mask so we get the queries WITHOUT PII in them
            sanitized_page = unsanitized_unallowlisted_terms.loc[~numpy.array(pii_in_query_mask)] 
            
            total_allow_listed += allow_listed_terms_page.shape[0]
            total_cleared_in_sanitation += sanitized_page.shape[0]
        
            summary_language_data = dict(functools.reduce(operator.add,
                            map(collections.Counter, [summary_language_data, language_data])))
            summary_run_data = dict(functools.reduce(operator.add,
                            map(collections.Counter, [summary_run_data, run_data])))
                
            all_terms_to_keep = pd.concat([allow_listed_terms_page, sanitized_page])
            all_terms_to_keep = all_terms_to_keep.drop(columns=['present_in_allow_list'])

            delete_destination_partition = idx == 0
        
            export_search_queries_to_bigquery(
                dataframe=all_terms_to_keep,
                destination_table_id=args.sanitized_term_destination,
                date=start_date,
                delete_partition=delete_destination_partition
            )
    
        end_time = datetime.now(UTC)
        
        implementation_notes = "Run with a page_size of UNLIMITED from script" 
        record_job_metadata(
            status='SUCCESS',
            started_at=start_time,
            ended_at=end_time,
            destination_table_id=args.job_reporting_destination,
            total_run=total_run,
            total_allow_listed=total_allow_listed,
            total_rejected=total_run - (total_allow_listed + total_cleared_in_sanitation),
            run_data=summary_run_data,
            language_data=summary_language_data,
            implementation_notes=implementation_notes,
            total_terms_inclusive=total_terms,
            total_blank=total_blank
        )

    except Exception as e:
        record_job_metadata(
            status='FAILURE', 
            started_at=start_time, 
            ended_at=datetime.now(UTC),
            destination_table_id=args.job_reporting_destination, 
            failure_reason=str(e)
        )
        raise e
    
    data_validation_sample = data_validation_sample.drop(columns=['present_in_allow_list'])
    export_sample_to_bigquery(dataframe=data_validation_sample, sample_table_id=args.unsanitized_term_sample_destination, date=start_date)
    logger.info("Sanitation job complete!")

asyncio.run(run_sanitation(args=args))
