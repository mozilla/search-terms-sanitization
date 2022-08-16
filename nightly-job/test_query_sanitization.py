import pytest
from query_sanitization import detect_pii

FAKE_CENSUS_SURNAMES = ["troy", "stuckey", "klukas", "burwei", "zeber", "reid", "dawson"] 

@pytest.mark.asyncio
async def test_detect_pii_removes_numerals():
    """
    Currently, we use rules to determine which search terms 
    might contain personally identifying information (PII).
    
    Numerals are required to search for phone numbers or addresses, so
    we mark any search that contains them as a PII risk.
    """    
    pii_risk, _, _ = await detect_pii(["2 cups of sugar"], FAKE_CENSUS_SURNAMES)
    assert pii_risk == [True]
    
    pii_risk, _, _ = await detect_pii(["two cups of sugar"], FAKE_CENSUS_SURNAMES)
    assert pii_risk == [False]
    
    pii_risk, _, _ = await detect_pii(["912 Riverview Drive"], FAKE_CENSUS_SURNAMES)
    assert pii_risk == [True]
    
    pii_risk, _, _ = await detect_pii(["Riverview Drive"], FAKE_CENSUS_SURNAMES)
    assert pii_risk == [False]
    
@pytest.mark.asyncio
async def test_detect_pii_removes_at_symbol():
    """
    Currently, we use rules to determine which search terms 
    might contain personally identifying information (PII).
    
    The @ symbol appears in searches for email addresses or handles, so
    we mark any search that contains them as a PII risk.
    """    
    pii_risk, _, _ = await detect_pii(["hi@hello.com"], FAKE_CENSUS_SURNAMES)
    assert pii_risk == [True]
    
    pii_risk, _, _ = await detect_pii(["hi at hello dot com"], FAKE_CENSUS_SURNAMES)
    assert pii_risk == [False]
    
    pii_risk, _, _ = await detect_pii(["@mozilla on Twitter"], FAKE_CENSUS_SURNAMES)
    assert pii_risk == [True]
    
    pii_risk, _, _ = await detect_pii(["mozilla on Twitter"], FAKE_CENSUS_SURNAMES)
    assert pii_risk == [False]
    
@pytest.mark.asyncio
async def test_detect_pii_marks_common_surnames():
    """
    Currently, we use rules to determine which search terms 
    might contain personally identifying information (PII).
    
    It's an extremely sensitive and NOT very specific.
    
    We have a list of common names from the 2010 census, and
    we identify any queries contain them for model monitoring.
    
    For now we do not remove them, because they contain a lot of
    words that are USUALLY not used as names, like 'black' or 'brown' or 'white'
    """    
    _, run_data, _ = await detect_pii(["Will chelsea troy ever stop being a clown"], FAKE_CENSUS_SURNAMES)
    assert run_data['sum_terms_containing_us_census_surname'] == 1
    
    _, run_data, _ = await detect_pii(["The future of clowns"], FAKE_CENSUS_SURNAMES)
    assert run_data['sum_terms_containing_us_census_surname'] == 0
    
    # Deliberately skips common surnames inside another word
    _, run_data, _ = await detect_pii(["summer reiding program"], FAKE_CENSUS_SURNAMES)
    assert run_data['sum_terms_containing_us_census_surname'] == 0    
