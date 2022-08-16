# search-terms-sanitization
Code for evaluating and implementing search terms sanitization.

# Privacy & security warning
This repo may contain notebooks or outputs with aggregated search terms related data. 

# Working in this repo
## Making commits
Check with the previous author before over-writing previously committed Notebook outputs.

## Directory structure
This repo's directory structure is minimal for now. We'll add more structure as we go.

| | |
|---|---|
| assets | shareable data like US Census surnames |
| non_sensitive | analysis and related data that do not involve sensitive search data |
| notebooks | analysis involving sensitive search data. See [this example](https://github.com/MozillaDataScience/search-terms-sanitization/pull/4) for how to import code into notebooks |
| private_output | placeholder for writing non-shareable data. nothing in this folder is tracked by git.
| src | reusable python code |

## Modules

So far, we're just putting shared code and reusable functions in `src`. Here are some functions of note:
* The `str_to_words` function in `evaluate.py` should be used to standardize transforming an English string into words, unless you are using an NLP library like SpaCy that does its own text preprocessing.

# Set-up

1. Request access to the `search-terms-unsanitized@mozdata.iam.gserviceaccount.com` service account. [This documentation describes](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#notebooks-access-to-workgroup-confidential-datasets) how.
2. Create a GCP-hosted notebook environment and clone this repo into it. [This video tutorial demonstrates](https://drive.google.com/file/d/1WsDUmZSlRtE_tZ8siWZWXpxfhKII69SM/view?usp=sharing) how.
3. Install requirements:
```bash
pip install -r requirements.txt

# Presidio analyzer requires a spaCy language model.
# We should keep track of the language model version we're using. Can start with the latest.
python -m spacy download en_core_web_lg
```

# Outputs
The sanitization job writes data to
* sanitized search terms: `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3`
* the job metadata table: `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata`
* the job metadata languages table: `moz-fx-data-shared-prod.search_terms.sanitization_job_languages`

# Related artifacts
* [Search Terms Sanitization Proposal](https://docs.google.com/document/d/1juZjL1GusXNAFT3Zmzpi8zDuUH3kNEnjeQ1Hp-UQoSg/edit): lists goals, requirements, high-level sanitization strategies
* [Technical report](https://docs.google.com/document/d/1UbQpiWadpMCzSdis3y1Mk7yzpW74POx_71KQ8QzQFyQ/edit): Evaluation of PII removal strategies.
