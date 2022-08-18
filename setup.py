from setuptools import setup

setup(
    name="suggest_search_tools",
    version="0.1.0",
    packages=["suggest_search_tools"],
    python_requires=">=3.7",
    install_requires=[
        "spacy",
        # We should keep track of the language model version we're using. Can start with the latest.
        "en_core_web_lg @ https://github.com/explosion/spacy-models/releases/download/en_core_web_lg-3.3.0/en_core_web_lg-3.3.0.tar.gz",
        "nltk",
        "presidio-analyzer",
        "pandas",
        "numpy",
        "numba",
        "google-cloud-bigquery",
        "google-cloud-dlp",
    ]
)
