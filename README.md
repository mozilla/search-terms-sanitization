# Search Terms Sanitization

This repository contains tools for filtering personally identifying information (PII) from search terms before sharing them with advertising partners.

## Repository Structure

```
search-terms-sanitization/
├── nightly-job/                # The production sanitization job
│   ├── sanitation_job.py       # Main entry point
│   ├── query_sanitization.py   # Core PII detection logic
│   ├── data_validation_job.py  # Monitors for pattern changes
│   ├── requirements.txt        # Python dependencies
│   ├── Dockerfile              # Container configuration
│   └── Names_2010Census.csv    # US Census surname data
├── suggest_search_tools/       # Reusable Python library for research
├── non_sensitive/              # Research notebooks using public data
├── assets/                     # Public data (census names, wordlists)
├── keyword_planning/           # Related research work
├── .circleci/                  # CI/CD configuration
└── ANNOTATION.md               # PII annotation guidelines
```

## Deployment Location

Because Airflow requires looser data viewing permissions than _unsanitized search queries_ allow, we run this job outside of Airflow.

Here's the config:
https://github.com/mozilla-services/cloudops-infra/blob/master/projects/merino/tf/prod/project/k8s/search-terms-sanitization.prod.yaml#L33 
https://github.com/mozilla-services/cloudops-infra/blob/master/projects/merino/tf/prod/project/k8s/search-terms-sanitization.prod.yaml#L44 

## How to Run

### Prerequisites

1. **GCP Access**: Request access to `search-terms-unsanitized@mozdata.iam.gserviceaccount.com`. [This documentation describes how](https://docs.telemetry.mozilla.org/cookbooks/bigquery/access.html#notebooks-access-to-workgroup-confidential-datasets).
2. Create a GCP-hosted notebook environment and clone this repo into it. [This video tutorial demonstrates how](https://drive.google.com/file/d/1WsDUmZSlRtE_tZ8siWZWXpxfhKII69SM/view?usp=sharing).
3. **Python 3.10**

### Installation

```bash
cd nightly-job/
pip install -r requirements.txt
```

Optional: If you want to use the code in `suggest_search_tools/` as a Python library (needed to run notebooks in `non_sensitive/`):

```bash
cd search-terms-sanitization/
pip install -e .
```

### Running the Job

```bash
# Run for yesterday's data (default)
python sanitation_job.py \
  --sanitized_term_destination "project.dataset.sanitized_table" \
  --job_reporting_destination "project.dataset.metadata_table" \
  --unsanitized_term_sample_destination "project.dataset.sample_table"

# Run for a specific date
python sanitation_job.py \
  --run_date "2024-01-23" \
  --sanitized_term_destination "project.dataset.sanitized_table" \
  --job_reporting_destination "project.dataset.metadata_table" \
  --unsanitized_term_sample_destination "project.dataset.sample_table"
```

### Running Tests

```bash
cd nightly-job/
pytest test_query_sanitization.py -v
```

### Docker

```bash
cd nightly-job/
docker build -t search-terms-sanitization .
docker run search-terms-sanitization
```

## PII Filtering Strategy

The job uses a multi-layered approach to identify and remove search terms that might contain PII:

### Layer 1: Allow List

Terms matching an approved allow list (from `remotesettings_suggestions_v1`) bypass all checks. These are pre-vetted, known-safe search terms provided to us by an ad partner.

### Layer 2: Rule-Based Detection

A search term is flagged and **removed** if it contains any of:

| Pattern | Rationale |
|---------|-----------|
| Any numeral (0-9) | Could be phone numbers, addresses, zip codes, IDs, SSNs |
| @ symbol | Could be email addresses or social media handles |
| Person name (via NER) | Detects names using named entity recognition |

### Additional Monitoring

The job also checks for US Census surnames but only for monitoring purposes (not removal). This helps track patterns without over-filtering common words that happen to be surnames (like "Black" or "White").

### Design Philosophy

The approach is intentionally **conservative**: when in doubt, remove it. This means some false positives (legitimate queries incorrectly flagged as PII), but prioritizes user privacy over data completeness.

## About the Named Entity Recognition

The "machine learning part" is not custom ML work. The job uses **spaCy's `en_core_web_lg` model**, an off-the-shelf English language model downloaded directly from spaCy's releases. We do not train, fine-tune, or modify this model in any way.

Here's the entirety of how we use it:

```python
nlp = spacy.load("en_core_web_lg")
# ...
doc = nlp(search_term)
has_person_name = any(ent.label_ == 'PERSON' for ent in doc.ents)
```

That's it. The model identifies named entities, and we check if any are labeled as `PERSON`. The interesting decisions in this project are about which rules to apply and how conservative to be—not about the NER model itself.

## Outputs

The nightly sanitization job writes to:

* **Sanitized search terms**: `moz-fx-data-shared-prod.search_terms_derived.merino_log_sanitized_v3`: this is where the non-PII search terms go
* **Job metadata**: `moz-fx-data-shared-prod.search_terms_derived.sanitization_job_metadata_v2`: this is some aggregate information about the search terms for the day, which we use to validate that the way we assume people use search isn't changing.
* **Language distribution**: `moz-fx-data-shared-prod.search_terms.sanitization_job_languages`: this tracks language distribution of search terms. The NLP model is trained for use on the English language, so we need to verify that this sanitation script is not being expected to correctly sanitize searches in other languages in large volumes.

A 1% sample of unsanitized queries is also exported, so that if the data validation alarms go off, data scientists can use the sample to understand the problem.

## Working in This Repo

### Making Commits

Open a PR and get one passing review before merging.

## Related Artifacts

* [Search Terms Sanitization Proposal](https://docs.google.com/document/d/1juZjL1GusXNAFT3Zmzpi8zDuUH3kNEnjeQ1Hp-UQoSg/edit): lists goals, requirements, high-level sanitization strategies
* [Technical report](https://docs.google.com/document/d/1UbQpiWadpMCzSdis3y1Mk7yzpW74POx_71KQ8QzQFyQ/edit): Evaluation of PII removal strategies
