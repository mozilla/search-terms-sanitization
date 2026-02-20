import pytest
from query_sanitization import detect_pii, load_nlp_model
import pandas as pd
import spacy_fastlang

FAKE_CENSUS_SURNAMES = {"troy", "stuckey", "klukas", "burwei", "zeber", "reid", "dawson", "bozo"}

@pytest.fixture(scope="module")
def nlp_model():
    nlp = load_nlp_model()
    nlp.add_pipe("language_detector")
    return nlp

def test_detect_pii_replaces_none(nlp_model):
    """
    spaCy hates it when we pass `None` instead of a string for analysis, apparently.
    This test ensures that our function doesn't error out on that edge case.
    """    
    pii_risk, _, _ = detect_pii(pd.Series([None]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [False] 

def test_detect_pii_removes_numerals(nlp_model):
    """
    Currently, we use rules to determine which search terms 
    might contain personally identifying information (PII).
    
    Numerals are required to search for phone numbers or addresses, so
    we mark any search that contains them as a PII risk.
    """    
    pii_risk, _, _ = detect_pii(pd.Series(["2 cups of sugar"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [True]
    
    pii_risk, _, _ = detect_pii(pd.Series(["two cups of sugar"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [False]
    
    pii_risk, _, _ = detect_pii(pd.Series(["912 Riverview Drive"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [True]
    
    pii_risk, _, _ = detect_pii(pd.Series(["Riverview Drive"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [False]
    
def test_detect_pii_removes_at_symbol(nlp_model):
    """
    Currently, we use rules to determine which search terms 
    might contain personally identifying information (PII).
    
    The @ symbol appears in searches for email addresses or handles, so
    we mark any search that contains them as a PII risk.
    """    
    pii_risk, _, _ = detect_pii(pd.Series(["hi@hello.com"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [True]
    
    pii_risk, _, _ = detect_pii(pd.Series(["hi at hello dot com"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [False]
    
    pii_risk, _, _ = detect_pii(pd.Series(["@mozilla on Twitter"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [True]
    
    pii_risk, _, _ = detect_pii(pd.Series(["mozilla on Twitter"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert pii_risk == [False]
    
def test_detect_pii_marks_common_surnames(nlp_model):
    """
    Currently, we use rules to determine which search terms 
    might contain personally identifying information (PII).
    
    It's an extremely sensitive and NOT very specific.
    
    We have a list of common names from the 2010 census, and
    we identify any queries contain them for model monitoring.
    
    For now we do not remove them, because they contain a lot of
    words that are USUALLY not used as names, like 'black' or 'brown' or 'white'
    """    
    _, run_data, _ = detect_pii(pd.Series(["Will bozo ever stop being a clown"]), FAKE_CENSUS_SURNAMES, nlp_model)
    print(run_data)
    assert run_data['sum_terms_containing_us_census_surname'] == 1
    
    _, run_data, _ = detect_pii(pd.Series(["The future of clowns"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert run_data['sum_terms_containing_us_census_surname'] == 0
    
    # Deliberately skips common surnames inside another word
    _, run_data, _ = detect_pii(pd.Series(["summer reiding program"]), FAKE_CENSUS_SURNAMES, nlp_model)
    assert run_data['sum_terms_containing_us_census_surname'] == 0    
