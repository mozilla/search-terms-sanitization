from datetime import datetime, timedelta
import argparse

from query_sanitization import stream_search_terms, detect_pii, export_search_queries_to_bigquery, export_sample_to_bigquery, record_job_metadata
import numpy
import pandas as pd
import asyncio

import collections
import functools
import operator

parser = argparse.ArgumentParser(description="Sanitize Search Terms",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("--sanitized_term_destination", help="Destination table for sanitary search terms")
parser.add_argument("--job_reporting_destination", help="Destination table for sanitation job metadata")
parser.add_argument("--unsanitized_term_sample_destination", help="Destination table for a sample of unsanitized search terms")
args = parser.parse_args()

df = pd.read_csv('Names_2010Census.csv')
census_surnames = [str(name).lower() for name in df.name]

async def run_sanitation(args):
    start_time = datetime.utcnow()

    try:
        total_run = 0
        total_deemed_sanitary = 0
        summary_run_data = {}
        summary_language_data = {}
        yesterday = datetime.utcnow().date() - timedelta(days=1)
        
        data_validation_sample = pd.DataFrame()

        unsanitized_search_term_stream = stream_search_terms()  # load unsanitized search terms
        
        for raw_page in unsanitized_search_term_stream:
            total_run += raw_page.shape[0]
            
            one_percent_sample = raw_page.sample(frac = 0.01)
            data_validation_sample = data_validation_sample.append(one_percent_sample)
            
            pii_in_query_mask, run_data, language_data = await detect_pii(raw_page['query'], census_surnames)
            sanitized_page = raw_page.loc[
                ~numpy.array(pii_in_query_mask)]  # ~ reverses the mask so we get the queries WITHOUT PII in them
            total_deemed_sanitary += sanitized_page.shape[0]

            summary_language_data = dict(functools.reduce(operator.add,
                                                          map(collections.Counter,
                                                              [summary_language_data, language_data])))
            summary_run_data = dict(functools.reduce(operator.add,
                                                     map(collections.Counter, [summary_run_data, run_data])))

            yesterday = datetime.utcnow().date() - timedelta(days=1)
            export_search_queries_to_bigquery(dataframe=sanitized_page,
                                              destination_table_id=args.sanitized_term_destination, date=yesterday)
        end_time = datetime.utcnow()
        
        export_sample_to_bigquery(dataframe=data_validation_sample, sample_table_id=args.unsanitized_term_sample_destination, date=yesterday)

        implementation_notes = "Run with a page_size of UNLIMITED from script"
        record_job_metadata(status='SUCCESS', started_at=start_time, ended_at=end_time,
                            destination_table=args.job_reporting_destination, total_run=total_run,
                            total_rejected=total_run - total_deemed_sanitary, run_data=summary_run_data,
                            language_data=summary_language_data, implementation_notes=implementation_notes)
    except Exception as e:
        # TODO: Make this more robust in actual failure cases
        # Maybe include the reason? Or should the logs be elsewhere for that
        record_job_metadata(status='FAILURE', started_at=start_time, ended_at=datetime.utcnow(),
                            destination_table=args.job_reporting_destination, failure_reason=str(e))
        raise e

asyncio.run(run_sanitation(args=args))
