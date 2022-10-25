from google.cloud import bigquery
from datetime import date
import spacy
import spacy_fastlang
import asyncio
import re
import json
import string


async def detect_pii(series, census_surnames):
    """
    Arguments: 
    - series: A dataframe series of search queries as strings
    - census_surnames: a list of names to check for in the queries: job metrics will indicate how many times a term included one of these names
    
    Returns: 
    A sequence of the same length as the series containing booleans representing whether each query should be removed from the dataset for sanitation. Can be used as a mask over the orgiginal series in the dataframe
    
    Why not use pandas `apply` and a function that takes in one query at a time? Because spaCy can do the nlp processing on a batch of queries much faster if passed all the queries at once, rather than individually.
    """
    pii_risk = [0] * len(series) # We prepopulate the sequence so we can mutate it at indices in separate tasks
    run_data = {
        'num_terms_containing_at': 0, 
        'num_terms_containing_numeral': 0, 
        'num_terms_name_detected': 0,
        'sum_chars_all_terms' : 0,
        'sum_uppercase_chars_all_terms' : 0,
        'sum_words_all_terms' : 0,
        'sum_terms_containing_us_census_surname' : 0
    }
    language_data = {}
    
    texts = list(series)
    tasks = []
    
    nlp = spacy.load("en_core_web_lg") 
    nlp.add_pipe("language_detector")
    docs = list(nlp.pipe(texts))
    
    query_data = list(zip(texts, docs))
                
    for idx, search_query in enumerate(query_data):
        task = asyncio.ensure_future(mutate_risk(pii_risk=pii_risk, run_data=run_data, language_data=language_data, idx=idx, query_info=search_query, census_surnames=census_surnames))
        tasks.append(task)
    await asyncio.gather(*tasks, return_exceptions=True)
    return pii_risk, run_data, language_data


async def mutate_risk(pii_risk, run_data, language_data, idx, query_info, census_surnames):
    """
    Sets mask value at index True if search query contains "@", a number, or 
    a name as determined by spaCy named entity recognition.
    Otherwise, sets mask value at index to False.
    
    Arguments:
    - pii_risk: a sequence of values representing all the queries
    - run_data: a Python dictionary with a variety of aggregate metrics in it about what was in the terms run
    - language_data: a Python dictionary counting the language categorizations for the terms run
    - idx: the index of the search query being sanitized
    - query_info: A list of tuples containing [0] the text and [1] spaCy NLP analysis of the query being analyzed
    - census_surnames: A preoppulated list of names to check for from the U.S. Census
    
    Returns: nothing
    
    MUTATES: 
    - The mask passed in as the first argument, at the index passed in the second argument, to the 
    analysis of whether the third argument contains pii (True or False).
    - A dictionary containing aggregate metrics for this entire sanitation job. We use these to 
    analyze changes in our constituents' search terms, which helps us monitor the effectiveness of our sanitation strategy.
    """
    query, doc = str(query_info[0]), query_info[1]
    
    # Sanitize Individual Queries
    if any(character in query for character in ["1", "2", "3", "4", "5", "6", "7", "8", "9", "0"]):
        pii_risk[idx] = True
        run_data['num_terms_containing_numeral'] +=1
        return
    if "@" in query:
        pii_risk[idx] = True
        run_data['num_terms_containing_at'] +=1
        return
    elif any([ent.text for ent in doc.ents if ent.label_ == 'PERSON']):
        pii_risk[idx] = True
        run_data['num_terms_name_detected'] += 1
        return
    else:
        pii_risk[idx] = False
        
    # Aggregate character, word, and uppercase metrics
    run_data['sum_chars_all_terms'] += len(query)    
    run_data['sum_words_all_terms'] += len(query.split())  
    run_data['sum_uppercase_chars_all_terms'] += len(re.findall(r'[A-Z]', query))
    
    # Language Detection
    # Chelsea Troy's visual analysis of 250 terms on May 31, 2022 determined that
    # 1. It takes about 6 characters for a human (well, for her at least) to be reasonably confident what language the term is in
    # 2. spaCy's model is usually getting the language right for terms of this length when the confidence score is > 0.2 (it is often confidently wrong about shorter terms)
    if len(query) > 5 and doc._.language_score > 0.2:
        if doc._.language in language_data:
            language_data[doc._.language] += 1
        else:
            language_data[doc._.language] = 1
    
    # Detect Surnames from the U.S. Census (2010) 
    query_words = [unprocessed_word.lower().strip().translate(str.maketrans('', '', string.punctuation)) for unprocessed_word in query.split()]

    for word in query_words:
        if word in census_surnames:
            run_data['sum_terms_containing_us_census_surname'] += 1
            return
        

UNSANITIZED_QUERIES_FOR_ANALYSIS_SQL = """
WITH approved_terms as (
    SELECT
    -- Each keyword should only appear once, but we add DISTINCT for protection
    -- in downstream joins in case the suggestions file has errors.
    DISTINCT query
    FROM
        `moz-fx-data-shared-prod.search_terms_derived.remotesettings_suggestions_v1`
    CROSS JOIN
    UNNEST(keywords) AS query
    )

SELECT
    search_logs.timestamp AS timestamp,
    search_logs.jsonPayload.fields.rid AS request_id,
    search_logs.jsonPayload.fields.session_id AS session_id,
    search_logs.jsonPayload.fields.sequence_no AS sequence_no,
    search_logs.jsonPayload.fields.query AS query,
    -- Merino currently injects 'none' for missing geo fields.
    NULLIF(search_logs.jsonPayload.fields.country, 'none') AS country,
    NULLIF(search_logs.jsonPayload.fields.region, 'none') AS region,
    NULLIF(search_logs.jsonPayload.fields.dma, 'none') AS dma,
    search_logs.jsonPayload.fields.form_factor AS form_factor,
    search_logs.jsonPayload.fields.browser AS browser,
    search_logs.jsonPayload.fields.os_family AS os_family,
    approved_terms.query IS NOT NULL AS present_in_allow_list
FROM `suggest-searches-prod-a30f.logs.stdout` AS search_logs
LEFT JOIN approved_terms on search_logs.jsonPayload.fields.query = approved_terms.query
WHERE 
    jsonPayload.type = "web.suggest.request"
    --Specifically get the previous day's data
    AND timestamp >= DATE_ADD(CURRENT_TIMESTAMP(), INTERVAL -1 DAY)
    AND DATE(timestamp) < CURRENT_DATE()
"""


def stream_search_terms(): 
    """
    Pull the full 2-day dataset of unsanitized search queries stored on BigQuery.
    
    Arguments: None
    
    Returns: A dataframe of the unsanitized search queries.
    """
    client = bigquery.Client()
    query_job = client.query(UNSANITIZED_QUERIES_FOR_ANALYSIS_SQL)
    df_generator = query_job.result().to_dataframe_iterable()
    # df_generator = query_job.result(page_size=75000).to_dataframe_iterable()
    return df_generator

def export_search_queries_to_bigquery(dataframe, destination_table_id, date):
    """
    Append more queries to the BigQuery table where we are keeping sanitized search queries.
    
    Arguments:
    - dataframe: A dataframe of queries to be added. Should include ONLY sanitary ones.
        Dataframe should include a timestamp field of the timestamp type, plus all fields listed in the schema variable in this function's implementation.
    - destination_table_id: the fully qualified name of the table for the data to be exported into.
    - date: The date for which these queries are being inserted. IMPORTANT: this function will overwrite EVERYTHING in the destination table at that date partition with the data in the dataframe passed in.
    
    Returns: Nothing.
    It does print a result value as a cursory logging mechanism. That result object can be parsed and logged to wherever we like.
    """
    client = bigquery.Client()
    
    partition = date.strftime("%Y%m%d")

    # For idempotency, we want to overwrite data on daily partitions
    # But the BQ role granted to the sanitizer service account does not include creating tables
    # Which WRITE_TRUNCATE to a partition requires, so the hack is to
    # Delete existing data from today before insertion of data from today.
    
    deletion_target = f'{destination_table_id}${partition}'
    client.delete_table(deletion_target, not_found_ok=True)
    
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected (particularly "object")
            bigquery.SchemaField("request_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("session_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("sequence_no", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("query", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("country", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("region", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("dma", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("form_factor", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("browser", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("os_family", bigquery.enums.SqlTypeNames.STRING)
        ]

    destination_table = bigquery.Table(destination_table_id, schema=schema)
    job = client.insert_rows_from_dataframe(
        table=destination_table, dataframe=dataframe
    )
    print(job)  # Wait for the job to complete.
    
def export_sample_to_bigquery(dataframe, sample_table_id, date):
    """
    Append unsanitized queries to the BigQuery table where we are keeping samples of one percent of each job's search volume for data validation purposes.
    
    Arguments:
    - dataframe: A dataframe of queries to be added. This is the 1% sample.
        Dataframe should include a timestamp field of the timestamp type, plus all fields listed in the schema variable in this function's implementation.
    - destination_table_id: the fully qualified name of the table for the data to be exported into.
    - date: The date for which these queries are being inserted. IMPORTANT: this function will overwrite EVERYTHING in the destination table at that date partition with the data in the dataframe passed in.
    
    Returns: Nothing.
    It does print a result value as a cursory logging mechanism. That result object can be parsed and logged to wherever we like.
    
    HUGE NOTE: You will notice that this function's implementation almost matches that of `export_search_queries_to_bigquery.` I deliberately did not extract out the common behavior. Why:
    - Two occurrences is not enough to convince me that two similar pieces of code in fact represent the same concept. I follow a rule of three.
    - Moreover they write to two different tables. So that decreases the likelihood that these two similar pieces of code represent the same concept anyway.
    - One of the functions (this one) deals with SENSITIVE UNSANITIZED search data, and the other one does not. What we DO NOT NEED is a situation where someone does not fully understand this and makes a change to a shared piece of code that exposes something that's a-ok for the sanitized data, but a problem for the unsanitized data.
    """
    client = bigquery.Client()
    
    partition = date.strftime("%Y%m%d")

    # For idempotency, we want to overwrite data on daily partitions
    # But the BQ role granted to the sanitizer service account does not include creating tables
    # Which WRITE_TRUNCATE to a partition requires, so the hack is to
    # Delete existing data from today before insertion of data from today.
    
    deletion_target = f'{sample_table_id}${partition}'
    client.delete_table(deletion_target, not_found_ok=True)
    
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        # Specify the type of columns whose type cannot be auto-detected (particularly "object")
            bigquery.SchemaField("request_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("session_id", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("sequence_no", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("query", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("country", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("region", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("dma", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("form_factor", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("browser", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("os_family", bigquery.enums.SqlTypeNames.STRING)
        ]

    destination_table = bigquery.Table(sample_table_id, schema=schema)
    job = client.insert_rows_from_dataframe(
        table=destination_table, dataframe=dataframe
    )
    print(job)  # Wait for the job to complete.


def record_job_metadata(status, started_at, ended_at, destination_table, total_run=0, total_rejected=0, run_data=None, language_data=None, failure_reason=None, implementation_notes=None):
    """
    Record metadata on a sanitation job run. There are two types of data:
    
    - Overall run health: Did it succeed? What was the failure message if it didn't? How long did it take?
    - Aggregate search term data: How many terms were there? How long were they? What was the distribution of capital letters? Etc.
    
    This second type of data allows us to continuously monitor for changes in the KIND of things people are searching for without storing the unsanitized terms THEMSELVES. We can use this to alert the team that there may be a reason to perform manual model evaluation on changing data.
    
    Arguments:
    - status: How the job finished
    - started_at: When the job began
    - ended_at: When the job ended
    - destination_table: where to log the job info
    - total_run: number of search terms evaluated for sanitation
    - total_rejected: number of search terms deemed at risk of containing personally identifiable information
    - run_data: a Python dictionary with a variety of aggregate metrics in it about what was in the terms run
    - language_data: a Python dictionary counting the language categorizations for the terms run
    - failure_reason: the exception message, if the run fails
    - implementation_notes: Any additional details we want to know about how the job was run that produced this metadata, i.e. for running experiments
    
    Returns: Nothing.
    It does print a result summary as a cursory logging mechanism for development.
    """
    if run_data is None:
        run_data = {}
    
    client = bigquery.Client()

    rows_to_insert = [
        {
         u"status": status, 
         u"total_search_terms_analyzed": total_run, 
         u"total_search_terms_removed_by_sanitization_job": total_rejected, 
         u"contained_numbers": run_data.get('num_terms_containing_numeral', 0),
         u"contained_at": run_data.get('num_terms_containing_at', 0),
         u"contained_name": run_data.get('num_terms_name_detected', 0),
         u"sum_chars_all_search_terms": run_data.get('sum_chars_all_terms', 0),
         u"sum_uppercase_chars_all_search_terms": run_data.get('sum_uppercase_chars_all_terms', 0),
         u"sum_words_all_search_terms": run_data.get('sum_words_all_terms', 0),
         u"sum_terms_containing_us_census_surname": run_data.get('sum_terms_containing_us_census_surname', 0),
         u"approximate_language_proportions_json": json.dumps(language_data),
         u"failure_reason": failure_reason,
         u"started_at": started_at.strftime("%Y-%m-%d %H:%M:%S"),
         u"finished_at": ended_at.strftime("%Y-%m-%d %H:%M:%S"),
         u"implementation_notes": implementation_notes
        },
    ]
    errors = client.insert_rows_json(destination_table, rows_to_insert)
    if errors == []:
        print("New row representing job run successfully added.")
    else:
        print("Encountered errors while inserting row: {}".format(errors))


