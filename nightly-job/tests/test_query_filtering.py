"""Test that we correctly skip over short queries and queries that we are not confident are english."""

from collections import defaultdict
from types import SimpleNamespace
from query_sanitization import filter_queries_for_sanitization
import pandas as pd


class MockLanguageDetector:
    """A mock language detection model that returns languages based on a lookup dict.

    Mimics the interface of a spaCy model with the language_detector pipe,
    so we can test filtering logic without depending on model accuracy.
    """

    def __init__(self, language_map):
        """language_map: dict mapping query string -> language code (e.g. 'en', 'fr')"""
        self.language_map = language_map

    def pipe(self, queries):
        for query in queries:
            lang = self.language_map[query]
            score = 1.0 if lang != "unknown" else 0.0
            extensions = SimpleNamespace(language=lang, language_score=score)
            yield SimpleNamespace(_=extensions)


def test_filter_non_english_queries():
    mixed_language_queries = {
        "query": [
            "John Smith dentist near me",
            "Sebastian Vettel Formel eins",
            "blau himmel foto",
            "Juan Garcia dentista cerca",
            "Marie Curie recherche"
        ],
        "language":["en", "de", "de", "es", "fr"],
    }
    data = pd.DataFrame.from_dict(mixed_language_queries)
    language_map = dict(zip(data["query"], data["language"]))
    mock_language_detector = MockLanguageDetector(language_map)
    filtered_queries = filter_queries_for_sanitization(mock_language_detector, data)
    assert len(filtered_queries) == sum(data.language == "en")


def test_filter_short_queries():
    """test that we correctly filter out rows with queries under `MINIMUM_TERM_LENGTH`"""
    short_and_long_queries = {
        "query": ["hi", "hey", "ab", "longer query", "another long one", "search term"],
    }
    data = pd.DataFrame.from_dict(short_and_long_queries)
    always_english_detector = MockLanguageDetector(defaultdict(lambda: "en"))
    filtered = filter_queries_for_sanitization(always_english_detector, data)
    assert len(filtered) == 3
    assert list(filtered["query"]) == ["longer query", "another long one", "search term"]
