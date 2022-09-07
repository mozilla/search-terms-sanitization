from google.cloud import bigquery


UNSANITIZED_DATASET = "mozdata.search_terms_unsanitized_analysis"

bqclient = bigquery.Client()
        

def make_unsanitized_table_id(table_prefix, dfq):
    """Create a datestamped table ID within the appropriate GCP Dataset.
    
    table_prefix: an identifier for the table
    dfq: a DF containing the `merino_timestamp`s of the queries to be written.
    
    Returns a table ID in the `mozdata` unsanitized dataset datestamped
        by the earliest query date.
    """
    datestamp = str(dfq["merino_timestamp"].min())[:10]
    return f"{UNSANITIZED_DATASET}.{table_prefix}_{datestamp}"


def run_query(query_str):
    """Run a query against a BQ table and return the results as a DataFrame.
    
    Note that the table identifier needs to be surrounded by backticks.
    """
    return bqclient.query(query_str).to_dataframe()


class BQTable:
    def __init__(self, table_id):
        """Wrapper for working with BQ tables.
        
        This abstracts out the functionality we will use in the analysis.
        
        table_id: the fully-qualified table ID (<project>.<dataset>.<table>)
        """
        self.table_id = table_id
        self.table = None
    
    
    def create(self, schema_spec):
        """Create a new BQ table and retrieve the instance.
        
        schema_spec: a list of (<field name>, <BQ type>, <BQ mode>) defining the schema.
        """
        schema = [bigquery.SchemaField(name=s[0], field_type=s[1], mode=s[2]) for s in schema_spec]
        table = bigquery.Table(self.table_id, schema=schema)
        self.table = bqclient.create_table(table)


    def get(self):
        """Retrieve an existing Table instance."""
        self.table = bqclient.get_table(self.table_id)
    
    
    def insert(self, df):
        """Insert rows from a DataFrame into the table."""
        if not self.table:
            raise ValueError("Table must be created or loaded first")
        
        errors = bqclient.insert_rows_from_dataframe(self.table, df)
        
        # On success, returns a list containing empty lists.
        for e in errors:
            if e:
                return errors
        
    
    def overwrite(self, df):
        """Overwrite the contents of the table with the rows of a DataFrame."""
        if not self.table:
            raise ValueError("Table must be created or loaded first")
        
        job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
        overwrite_job = bqclient.load_table_from_dataframe(df, self.table, job_config=job_config)
        overwrite_job.result()
    
