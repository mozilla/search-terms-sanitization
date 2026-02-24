"""Test that we correctly skip over short queries and queries that we are not confident are english."""

import pytest
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


@pytest.fixture
def mock_language_detector(test_data):
    """A mock language detector that agrees with the test data's language column."""
    language_map = dict(zip(test_data["query"], test_data["language"]))
    return MockLanguageDetector(language_map)


def test_filter_non_english_queries(test_data, mock_language_detector):
    test_data["present_in_allow_list"] = False
    filtered_queries = filter_queries_for_sanitization(mock_language_detector, test_data)
    assert len(filtered_queries) == sum(test_data.language == "en")


@pytest.fixture
def always_english_detector():
    """A mock language detector that treats every query as English with score 1.0."""
    return MockLanguageDetector(defaultdict(lambda: "en"))


def test_filter_short_queries(always_english_detector):
    """test that we correctly filter out rows with queries under `MINIMUM_TERM_LENGTH`"""
    data = pd.DataFrame({
        "query": ["hi", "hey", "ab", "longer query", "another long one", "search term"],
        "present_in_allow_list": [False] * 6,
    })
    filtered = filter_queries_for_sanitization(always_english_detector, data)
    assert len(filtered) == 3
    assert list(filtered["query"]) == ["longer query", "another long one", "search term"]


def test_filter_present_in_allow_list(always_english_detector):
    """test that we correctly filter out rows that show up in the allow list"""
    mixed_allow_list_data = {
        "query": ["allowed query", "another allowed", "not allowed query", "also not allowed"],
        "present_in_allow_list": [True, True, False, False],
    }
    data = pd.DataFrame.from_dict(mixed_allow_list_data)
    filtered = filter_queries_for_sanitization(always_english_detector, data)
    assert len(filtered) == 2
    assert list(filtered["query"]) == ["not allowed query", "also not allowed"]