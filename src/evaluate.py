"""
Apply PII detection approaches.
"""

import string
from presidio_analyzer import AnalyzerEngine
import spacy
from helpers import *

surnames = get_surnames()
scrabble = get_scrabble()


def str_series_to_words_expl(x):
    """Text preprocessing goes here. We want to be consistent.
    Split query strings into words on whitespace. ignore capitalization. remove punctuation.

    x: series of strings

    Returns a series of exploded words, one entry per word, indexed as x.
    """
    words = x.str.split().explode()
    return words.str.lower().str.strip().str.translate(str.maketrans('', '', string.punctuation))


def word_list_prefix_counts(data_strings, word_list, return_matched=False):
    """Check an array of strings for membership in a word list (dictionary).

    Membership can mean the word list contains the string exactly, or has
    words which start with the string.

    data_strings: array of strings to test for membership
    word_list: sorted list of dictionary words
    return_matched: should the matched dictionary words also be returned?

    Returns a dict with entries:
    - `data_strings`: the sorted list of data strings
    - `num_superstrings`: the number of dictionary words starting with each data string
    - `list_contains_word`: does the dictionary contain the data string exactly?
    - `superstrings`: the list of matched dictionary words (if `return_matched` is `True`)
    """
    data_strings = sorted(data_strings)

    list_curr_idx = 0
    num_superstr = [0 for _ in range(len(data_strings))]
    contained = [False for _ in range(len(data_strings))]
    results = {
        "data_string": data_strings,
        "num_superstrings": num_superstr,
        "list_contains_word": contained,
    }
    if return_matched:
        superstr_w = [[] for _ in range(len(data_strings))]
        results["superstrings"] = superstr_w

    # Suppose a given data string is inserted into the sorted dictionary list.
    # If it is a prefix for any words in the dictionary, those superstrings will appear
    # immediately after in sorted order.
    for i in range(len(data_strings)):
        curr_data_str = data_strings[i]
        # Start checking membership at the dictionary word which appears next
        # after the current data string in sorted order.
        while curr_data_str > word_list[list_curr_idx]:
            list_curr_idx += 1
            if list_curr_idx >= len(word_list):
                # Any remaining data strings appear after the last dictionary word
                return results

        if curr_data_str == word_list[list_curr_idx]:
            contained[i] = True
        j = list_curr_idx
        # Superstrings, if any, will appear in sequence right after the current word.
        # As soon as we hit a dictionary word which is not a superstring, we are done checking.
        while j < len(word_list) and word_list[j].startswith(curr_data_str):
            num_superstr[i] += 1
            if return_matched:
                superstr_w[i].append(word_list[j])
            j += 1

    return results


def str_to_words(x):
    """Text preprocessing goes here. We want to be consistent.
    Split query strings into words on whitespace. ignore capitalization. remove punctuation."""
    return [w.lower().strip().translate(str.maketrans('', '', string.punctuation)) for w in x.split()]

def contains_surname(words):
    if any([x in surnames for x in words]):
        return True
    return False

def contains_surname_prefix(words):
    """Returns True if any word (1) occurs in `surnames` and (2) occurs as
    the prefix of <5 words in `scrabble`."""
    for word in words:
        if word in surnames:
            counter = 0
            for s in scrabble:
                if s.startswith(word):
                    counter +=1
                if counter >= 5:
                    break
            if counter < 5:
                return True
    return False

def contains_nondict(words):
    if any([(x not in scrabble) for x in words]):
        return True
    return False

def contains_nondict_prefix(words):
    """Returns True if any word does not occur as the prefix of any word 
    in `scrabble`."""
    for word in words:
        flag = "not ok"
        for s in scrabble:
            if s.startswith(word):
                flag = "ok"
                break
        if flag == "not ok":
            return True
    return False

analyzer = AnalyzerEngine()

def contains_presidio_name(x):
    results = analyzer.analyze(text=x, entities=["PERSON"], language='en')
    if len(results) > 0:
        return True
    return False

nlp = spacy.load("en_core_web_lg") 
def contains_noun(text):
    doc = nlp(text)
    return any([x.pos_ == "NOUN" for x in doc])
