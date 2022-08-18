# search-terms-sanitization
Code for evaluating and implementing search terms sanitization.

# Working in this repo
## Making commits
Open a PR and get one passing review before merging.

## Directory structure
This repo's directory structure is minimal for now. We'll add more structure as we go.

| | |
|---|---|
| .circleci | CircleCI |
| nightly-job | code for the sanitization job that runs nightly |
| assets | public data like US Census surnames |
| non_sensitive | analyses that do not involve sensitive search data |
| suggest_search_tools | reusable python code for the research team |

## Set-up
1. Request access to the `search-terms-unsanitized@mozdata.iam.gserviceaccount.com` service account. [This documentation describes](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#notebooks-access-to-workgroup-confidential-datasets) how.
2. Create a GCP-hosted notebook environment and clone this repo into it. [This video tutorial demonstrates](https://drive.google.com/file/d/1WsDUmZSlRtE_tZ8siWZWXpxfhKII69SM/view?usp=sharing) how.
3. Install requirements:
```bash
pip install -r requirements.txt

# We should keep track of the language model version we're using. Can start with the latest.
python -m spacy download en_core_web_lg
```
4. Optional: If you want to use the code in the `suggest_search_tools/` directory as a python library, you can pip install it:
```bash
cd suggest_search_tools/           # make sure you're in the suggest_search_tools/ directory
pip install -e .  # -e installs in editable (develop) mode
```

## Outputs
The nightly sanitization job writes data to
* sanitized search terms: `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3`
* the job metadata table: `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata`
* the job metadata languages table: `moz-fx-data-shared-prod.search_terms.sanitization_job_languages`

# Related artifacts
* [Search Terms Sanitization Proposal](https://docs.google.com/document/d/1juZjL1GusXNAFT3Zmzpi8zDuUH3kNEnjeQ1Hp-UQoSg/edit): lists goals, requirements, high-level sanitization strategies
* [Technical report](https://docs.google.com/document/d/1UbQpiWadpMCzSdis3y1Mk7yzpW74POx_71KQ8QzQFyQ/edit): Evaluation of PII removal strategies.
