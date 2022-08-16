from google.cloud import bigquery
from datetime import date
import asyncio
import re
import json
import string


def calculate_data_validation_metrics(metadata_source, languages_source): 
    """
    Pull all the successful sanitization job runs from metadata stored on BigQuery.
    
    Arguments: 
    
    - metadata_source: a string. The name of the table containing the metadata to be fetched.
    
    Returns: A dataframe of the successful run data.
    """
    if re.fullmatch(r'[A-Za-z0-9\.\-\_]+', metadata_source):
        metadata_source_no_injection = metadata_source
    else:
        raise Exception("metadata_source in incorrect format. This should be a fully qualified table name like myproject.mydataset.my_table")
        
    if re.fullmatch(r'[A-Za-z0-9\.\-\_]+', languages_source):
        languages_source_no_injection = languages_source
    else:
        raise Exception("metadata_source in incorrect format. This should be a fully qualified table name like myproject.mydataset.my_table")

    # We are using f-strings here because BQ does not allow table names to be parametrized
    # and we need to be able to run the same script in the staging and prod db environments for reliable testing outcomes.
    SUCCESSFUL_SANITIZATION_JOB_RUN_METADATA = f"""
    SELECT
        finished_at,
        total_search_terms_removed_by_sanitization_job / total_search_terms_analyzed AS pct_sanitized_search_terms,
        contained_at / total_search_terms_analyzed AS pct_sanitized_contained_at,
        contained_numbers / total_search_terms_analyzed AS pct_sanitized_contained_numbers,
        contained_name / total_search_terms_analyzed AS pct_sanitized_contained_name,
        sum_terms_containing_us_census_surname / total_search_terms_analyzed AS pct_terms_containing_us_census_surname,
        sum_uppercase_chars_all_search_terms / sum_chars_all_search_terms AS pct_uppercase_chars_all_search_terms,
        sum_words_all_search_terms / total_search_terms_analyzed AS avg_words_all_search_terms,
        1 - languages.english_count / languages.all_languages_count AS pct_terms_non_english
        FROM `{metadata_source_no_injection}` AS metadata
    JOIN 
    (
        SELECT 
            job_start_time,
            max(case when language_code = 'en' then search_term_count end) english_count,
            sum(search_term_count) as all_languages_count,
        FROM `{languages_source_no_injection}` 
        GROUP BY job_start_time
    ) AS languages
    ON metadata.started_at = languages.job_start_time
    WHERE status = 'SUCCESS'
    ORDER BY finished_at DESC;
    """
    client = bigquery.Client()
    query_job = client.query(SUCCESSFUL_SANITIZATION_JOB_RUN_METADATA)
    df_generator = query_job.result().to_dataframe()

    return df_generator

def export_data_validation_metrics_to_bigquery(dataframe, destination_table_id):
    """
    Append data validation metrics to the BigQuery table tracking these metrics from job metadata.
    
    Arguments:
    - dataframe: A dataframe of validation metrics to be added. 
    - destination_table_id: the fully qualified name of the table for the data to be exported into.
    
    Returns: Nothing.
    It does print a result value as a cursory logging mechanism. That result object can be parsed and logged to wherever we like.
    """
    client = bigquery.Client()
    
    schema = [
         bigquery.SchemaField("finished_at", bigquery.enums.SqlTypeNames.STRING),
         bigquery.SchemaField("pct_sanitized_search_terms", bigquery.enums.SqlTypeNames.FLOAT64),
         bigquery.SchemaField("pct_sanitized_contained_at", bigquery.enums.SqlTypeNames.FLOAT64),
         bigquery.SchemaField("pct_sanitized_contained_numbers", bigquery.enums.SqlTypeNames.FLOAT64),
         bigquery.SchemaField("pct_sanitized_contained_name", bigquery.enums.SqlTypeNames.FLOAT64),
         bigquery.SchemaField("pct_terms_containing_us_census_surname", bigquery.enums.SqlTypeNames.FLOAT64),
         bigquery.SchemaField("pct_uppercase_chars_all_search_terms", bigquery.enums.SqlTypeNames.FLOAT64),
         bigquery.SchemaField("avg_words_all_search_terms", bigquery.enums.SqlTypeNames.FLOAT64),
         bigquery.SchemaField("pct_terms_non_english", bigquery.enums.SqlTypeNames.FLOAT64)
    ]

    destination_table = bigquery.Table(destination_table_id)
    job = client.insert_rows_from_dataframe(
        table=destination_table, dataframe=dataframe, selected_fields=schema
    )

    print(job)