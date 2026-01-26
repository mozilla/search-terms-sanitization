"""
Integration tests for spaCy NER name detection in the search terms sanitization job.

These tests verify that the spaCy NER component:
1. Correctly identifies and flags queries containing person names
2. Does NOT flag queries containing common words that happen to also be surnames
3. Does NOT flag queries that contain no names at all

Test data distribution (approximately 500 rows):
- English: 400 rows (133 should_remove, 133 common_word, 134 no_name)
- German: 80 rows (26 should_remove, 27 common_word, 27 no_name)
- Spanish: 10 rows (3 should_remove, 3 common_word, 4 no_name)
- French: 10 rows (3 should_remove, 3 common_word, 4 no_name)

Note: The job uses spaCy's en_core_web_lg (English) model for NER on all languages.
This is intentional - we're testing actual system behavior with multilingual input.
"""

import pytest
import pandas as pd
import spacy_fastlang
import os
from pathlib import Path
from query_sanitization import detect_pii, load_nlp_model

# Use a minimal set of census surnames for testing
# We're not testing census surname detection here, just NER
TEST_CENSUS_SURNAMES = {"smith", "johnson", "williams", "brown", "jones", "garcia", "miller", "davis"}

TEST_DATA_PATH = Path(__file__).parent / "test_data" / "ner_integration_test_data.csv"


@pytest.fixture(scope="module")
def nlp_model():
    """Load the spaCy model once for all tests in this module."""
    nlp = load_nlp_model()
    nlp.add_pipe("language_detector")
    return nlp


@pytest.fixture(scope="module")
def test_data():
    """Load the test data CSV."""
    return pd.read_csv(TEST_DATA_PATH)


class TestNERIntegrationEnglish:
    """Integration tests for English language queries."""

    @pytest.mark.asyncio
    async def test_english_names_should_be_removed(self, nlp_model, test_data):
        """
        Queries containing clear person names (like 'John Smith', 'Barack Obama', etc.)
        should be flagged for removal by spaCy NER.
        """
        english_should_remove = test_data[
            (test_data['language'] == 'en') & (test_data['category'] == 'should_remove')
        ]

        queries = pd.Series(english_should_remove['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_flagged = sum(pii_risk)
        total = len(pii_risk)
        accuracy = correctly_flagged / total if total > 0 else 0

        # We expect high accuracy (>= 80%) for clear English names
        assert accuracy >= 0.80, (
            f"Expected at least 80% of English name queries to be flagged, "
            f"but only {accuracy*100:.1f}% ({correctly_flagged}/{total}) were flagged. "
            f"run_data: {run_data}"
        )

        # Verify that names were detected (not just numerals or @)
        assert run_data['num_terms_name_detected'] > 0, (
            "Expected some names to be detected via NER, but num_terms_name_detected was 0"
        )

    @pytest.mark.asyncio
    async def test_english_common_words_should_not_be_removed(self, nlp_model, test_data):
        """
        Queries containing common words that happen to also be surnames
        (like 'black shoes', 'white wedding dress', 'rose gold ring')
        should NOT be flagged for removal.
        """
        english_common_word = test_data[
            (test_data['language'] == 'en') & (test_data['category'] == 'common_word')
        ]

        queries = pd.Series(english_common_word['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        # Count how many were correctly NOT flagged
        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)
        accuracy = correctly_not_flagged / total if total > 0 else 0

        # We expect high accuracy (>= 90%) for common word queries
        # They should not be flagged as containing names
        assert accuracy >= 0.90, (
            f"Expected at least 90% of common word queries to NOT be flagged, "
            f"but only {accuracy*100:.1f}% ({correctly_not_flagged}/{total}) were correctly kept. "
            f"run_data: {run_data}"
        )

    @pytest.mark.asyncio
    async def test_english_no_names_should_not_be_removed(self, nlp_model, test_data):
        """
        Queries containing no names at all (like 'best pizza recipes', 'weather forecast')
        should NOT be flagged for removal.
        """
        english_no_name = test_data[
            (test_data['language'] == 'en') & (test_data['category'] == 'no_name')
        ]

        queries = pd.Series(english_no_name['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        # Count how many were correctly NOT flagged
        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)
        accuracy = correctly_not_flagged / total if total > 0 else 0

        # We expect very high accuracy (>= 95%) for queries with no names
        assert accuracy >= 0.95, (
            f"Expected at least 95% of no-name queries to NOT be flagged, "
            f"but only {accuracy*100:.1f}% ({correctly_not_flagged}/{total}) were correctly kept. "
            f"run_data: {run_data}"
        )


class TestNERIntegrationGerman:
    """
    Integration tests for German language queries.

    KNOWN LIMITATION: The job uses spaCy's en_core_web_lg (English) model for NER.
    This model has significant false positive issues with German text because:
    1. German capitalizes all nouns, which the English model interprets as proper nouns
    2. German words can look like surnames to the English model (e.g., "Mueller", "Fischer")
    3. The model doesn't understand German word patterns

    These tests document the actual behavior as regression tests.
    If German NER accuracy is important, consider adding de_core_news_lg model.
    """

    @pytest.mark.asyncio
    async def test_german_names_removal_rate(self, nlp_model, test_data):
        """
        Test NER detection of German names.

        Note: We use the English spaCy model, so detection of German names
        may be less accurate than English. We track the detection rate
        to understand multilingual performance.
        """
        german_should_remove = test_data[
            (test_data['language'] == 'de') & (test_data['category'] == 'should_remove')
        ]

        queries = pd.Series(german_should_remove['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_flagged = sum(pii_risk)
        total = len(pii_risk)
        accuracy = correctly_flagged / total if total > 0 else 0

        # German names may be less reliably detected with English model
        # We set a lower threshold but still expect reasonable detection
        assert accuracy >= 0.50, (
            f"Expected at least 50% of German name queries to be flagged, "
            f"but only {accuracy*100:.1f}% ({correctly_flagged}/{total}) were flagged. "
            f"run_data: {run_data}"
        )

    @pytest.mark.asyncio
    async def test_german_common_words_false_positive_rate(self, nlp_model, test_data):
        """
        Track false positive rate on German common word queries.
        """
        german_common_word = test_data[
            (test_data['language'] == 'de') & (test_data['category'] == 'common_word')
        ]

        queries = pd.Series(german_common_word['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)
        accuracy = correctly_not_flagged / total if total > 0 else 0

        assert accuracy >= 0.35, (
            f"Expected at least 35% of German common word queries to NOT be flagged "
            f"(known limitation of English NER on German text), "
            f"but only {accuracy*100:.1f}% ({correctly_not_flagged}/{total}) were correctly kept. "
            f"If this drops significantly, investigate model changes. "
            f"run_data: {run_data}"
        )

    @pytest.mark.asyncio
    async def test_german_no_names_false_positive_rate(self, nlp_model, test_data):
        """
        Track false positive rate on German no-name queries.

        KNOWN ISSUE: Similar to common words, German queries without names
        are often incorrectly flagged due to the English model's limitations.
        """
        german_no_name = test_data[
            (test_data['language'] == 'de') & (test_data['category'] == 'no_name')
        ]

        queries = pd.Series(german_no_name['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)
        accuracy = correctly_not_flagged / total if total > 0 else 0

        # Known limitation: English model has ~45-50% accuracy on German text
        assert accuracy >= 0.40, (
            f"Expected at least 40% of German no-name queries to NOT be flagged "
            f"(known limitation of English NER on German text), "
            f"but only {accuracy*100:.1f}% ({correctly_not_flagged}/{total}) were correctly kept. "
            f"run_data: {run_data}"
        )


class TestNERIntegrationSpanish:
    """
    Integration tests for Spanish language queries.

    KNOWN LIMITATION: The English spaCy model also has false positive issues
    with Spanish text, though less severe than German. Spanish words like
    "verde", "mejor", "vuelos" may be incorrectly flagged as PERSON entities.
    """

    @pytest.mark.asyncio
    async def test_spanish_names_removal_rate(self, nlp_model, test_data):
        """
        Test NER detection of Spanish names.
        Small sample size (3 queries) so we just check detection occurs.
        """
        spanish_should_remove = test_data[
            (test_data['language'] == 'es') & (test_data['category'] == 'should_remove')
        ]

        queries = pd.Series(spanish_should_remove['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_flagged = sum(pii_risk)
        total = len(pii_risk)

        # With only 3 samples, we just verify at least one is detected
        assert correctly_flagged >= 1, (
            f"Expected at least 1 Spanish name query to be flagged, "
            f"but {correctly_flagged}/{total} were flagged. "
            f"Queries: {queries.tolist()}"
        )

    @pytest.mark.asyncio
    async def test_spanish_common_words_false_positive_rate(self, nlp_model, test_data):
        """
        Track false positive rate on Spanish common word queries.

        KNOWN ISSUE: Some Spanish words trigger false positives in the English model.
        This is a regression test - with 3 samples, we expect at least 1 to pass.
        """
        spanish_common_word = test_data[
            (test_data['language'] == 'es') & (test_data['category'] == 'common_word')
        ]

        queries = pd.Series(spanish_common_word['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)

        # With small sample size, allow for some false positives
        assert correctly_not_flagged >= 1, (
            f"Expected at least 1 of {total} Spanish common word queries to NOT be flagged, "
            f"but all were incorrectly flagged as containing names."
        )

    @pytest.mark.asyncio
    async def test_spanish_no_names_false_positive_rate(self, nlp_model, test_data):
        """
        Track false positive rate on Spanish no-name queries.
        """
        spanish_no_name = test_data[
            (test_data['language'] == 'es') & (test_data['category'] == 'no_name')
        ]

        queries = pd.Series(spanish_no_name['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)

        # With small sample size (4), expect at least 1 to pass
        assert correctly_not_flagged >= 1, (
            f"Expected at least 1 of {total} Spanish no-name queries to NOT be flagged, "
            f"but all were incorrectly flagged."
        )


class TestNERIntegrationFrench:
    """Integration tests for French language queries."""

    @pytest.mark.asyncio
    async def test_french_names_removal_rate(self, nlp_model, test_data):
        """
        Test NER detection of French names.
        Small sample size (3 queries) so we just check detection occurs.
        """
        french_should_remove = test_data[
            (test_data['language'] == 'fr') & (test_data['category'] == 'should_remove')
        ]

        queries = pd.Series(french_should_remove['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_flagged = sum(pii_risk)
        total = len(pii_risk)

        # With only 3 samples, we verify at least one is detected
        assert correctly_flagged >= 1, (
            f"Expected at least 1 French name query to be flagged, "
            f"but {correctly_flagged}/{total} were flagged. "
            f"Queries: {queries.tolist()}"
        )

    @pytest.mark.asyncio
    async def test_french_common_words_should_not_be_removed(self, nlp_model, test_data):
        """French queries with common words (colors) should not be flagged."""
        french_common_word = test_data[
            (test_data['language'] == 'fr') & (test_data['category'] == 'common_word')
        ]

        queries = pd.Series(french_common_word['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)

        assert correctly_not_flagged == total, (
            f"Expected all {total} French common word queries to NOT be flagged, "
            f"but only {correctly_not_flagged} were correctly kept."
        )

    @pytest.mark.asyncio
    async def test_french_no_names_should_not_be_removed(self, nlp_model, test_data):
        """French queries with no names should not be flagged."""
        french_no_name = test_data[
            (test_data['language'] == 'fr') & (test_data['category'] == 'no_name')
        ]

        queries = pd.Series(french_no_name['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)

        assert correctly_not_flagged == total, (
            f"Expected all {total} French no-name queries to NOT be flagged, "
            f"but only {correctly_not_flagged} were correctly kept."
        )


class TestNERIntegrationSummary:
    """Summary statistics across all languages."""

    @pytest.mark.asyncio
    async def test_overall_should_remove_detection(self, nlp_model, test_data):
        """
        Aggregate test: Verify overall name detection rate across all languages.
        """
        should_remove = test_data[test_data['category'] == 'should_remove']

        queries = pd.Series(should_remove['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_flagged = sum(pii_risk)
        total = len(pii_risk)
        accuracy = correctly_flagged / total if total > 0 else 0

        # Overall, we expect at least 70% detection rate
        # Lower than English-only due to multilingual queries
        assert accuracy >= 0.70, (
            f"Expected at least 70% overall name detection rate, "
            f"but only {accuracy*100:.1f}% ({correctly_flagged}/{total}) were flagged. "
            f"num_terms_name_detected: {run_data['num_terms_name_detected']}"
        )

    @pytest.mark.asyncio
    async def test_overall_should_keep_accuracy(self, nlp_model, test_data):
        """
        Aggregate test: Verify overall false positive rate (queries wrongly flagged).

        Note: Due to the English model's limitations on German/Spanish text,
        the overall false positive rate is high-ish.
        We are fine with this because we have deliberately taken a conservative
        approach to sanitization anyway; it's false negatives we're most keen to
        avoid.
        This test documents the current behavior as a regression baseline.
        """
        should_keep = test_data[test_data['category'].isin(['common_word', 'no_name'])]

        queries = pd.Series(should_keep['query'].tolist())
        pii_risk, run_data, _ = await detect_pii(queries, TEST_CENSUS_SURNAMES, nlp_model)

        correctly_not_flagged = sum(1 for risk in pii_risk if not risk)
        total = len(pii_risk)
        accuracy = correctly_not_flagged / total if total > 0 else 0

        # Overall accuracy is ~85% due to German/Spanish false positives
        # English-only accuracy is ~95%+, but multilingual drags down the average
        assert accuracy >= 0.80, (
            f"Expected at least 80% of safe queries to NOT be flagged, "
            f"but only {accuracy*100:.1f}% ({correctly_not_flagged}/{total}) were correctly kept. "
            f"Note: Lower accuracy is expected due to English model on German/Spanish text."
        )

    @pytest.mark.asyncio
    async def test_data_distribution_sanity_check(self, test_data):
        """
        Verify the test data has the expected distribution.
        """
        # Check total row count is approximately 500
        total_rows = len(test_data)
        assert 490 <= total_rows <= 510, (
            f"Expected approximately 500 rows, but got {total_rows}"
        )

        # Check language distribution
        en_count = len(test_data[test_data['language'] == 'en'])
        de_count = len(test_data[test_data['language'] == 'de'])
        es_count = len(test_data[test_data['language'] == 'es'])
        fr_count = len(test_data[test_data['language'] == 'fr'])

        assert 390 <= en_count <= 410, f"Expected ~400 English rows, got {en_count}"
        assert 75 <= de_count <= 85, f"Expected ~80 German rows, got {de_count}"
        assert 8 <= es_count <= 12, f"Expected ~10 Spanish rows, got {es_count}"
        assert 8 <= fr_count <= 12, f"Expected ~10 French rows, got {fr_count}"

        # Check category distribution for English
        en_data = test_data[test_data['language'] == 'en']
        en_should_remove = len(en_data[en_data['category'] == 'should_remove'])
        en_common_word = len(en_data[en_data['category'] == 'common_word'])
        en_no_name = len(en_data[en_data['category'] == 'no_name'])

        # Each category should be roughly 1/3 of English data
        assert en_should_remove >= 100, f"Expected ~133 English should_remove, got {en_should_remove}"
        assert en_common_word >= 100, f"Expected ~133 English common_word, got {en_common_word}"
        assert en_no_name >= 100, f"Expected ~134 English no_name, got {en_no_name}"
