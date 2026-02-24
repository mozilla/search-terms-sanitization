import pytest
import pandas as pd
from pathlib import Path

TEST_DATA_PATH = Path(__file__).parent / "test_data" / "ner_integration_test_data.csv"

@pytest.fixture(scope="module")
def test_data():
    """Load the test data CSV."""
    return pd.read_csv(TEST_DATA_PATH)