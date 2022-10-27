FROM python:3.10

WORKDIR /usr/app/src

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY sanitation_job.py ./
COPY query_sanitization.py ./
COPY Names_2010Census.csv ./

CMD [ "python", "./sanitation_job.py", "--sanitized_term_destination", "mozdata.search_terms_unsanitized_analysis.prototype_sanitized_data", "--job_reporting_destination", "mozdata.search_terms_unsanitized_analysis.prototype_sanitation_job_metadata_v2", "--unsanitized_term_sample_destination", "mozdata.search_terms_unsanitized_analysis.prototype_unsanitized_sample_data"]
