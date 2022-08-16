# Code for internal sanitization.
# The code below checks one string at a time. 
# If batch processing is preferred, we can use spaCy's `pipe`: https://spacy.io/usage/processing-pipelines#processing

from presidio_analyzer import AnalyzerEngine

analyzer = AnalyzerEngine()

def contains_pii(x):
    """
    Returns True if `x` contains "@", a number, or a name as
    determined by Presidio.
    """
    x = str(x)
    for char in x:
        if char in ["@", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"]:
            return True
    results = analyzer.analyze(text=x, entities=["PERSON"], language='en')
    if len(results) > 0:
        return True
    return False


if __name__ == "__main__":
    assert contains_pii("username@domain.com") == True
    assert contains_pii("abc-123-XYZ") == True
    assert contains_pii("hillary clinton") == True
    assert contains_pii("latest iphone") == False
