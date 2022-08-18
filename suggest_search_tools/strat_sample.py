"""
Utilities to take a stratified sample with strata defined by SpaCy's NER confidence scores.
"""

import pandas as pd
import spacy
from copy import deepcopy

nlp = spacy.load("en_core_web_lg")

def get_ner_confidence(text):
    """Computes NER confidences over beam search. Explanation: https://towardsdatascience.com/foundations-of-nlp-explained-visually-beam-search-how-it-works-1586b9849a24"""
    docs = [nlp(text)]
    
    # Number of alternate analyses to consider. More is slower, and not necessarily better -- you need to experiment on your problem.
    beam_width = 16
    # This clips solutions at each step. We multiply the score of the top-ranked action by this value, and use the result as a threshold. This prevents the parser from exploring options that look very unlikely, saving a bit of efficiency. Accuracy may also improve, because we've trained on greedy objective.
    beam_density = 0.0001 
    beams = nlp.get_pipe("ner").beam_parse(docs, beam_width=beam_width, beam_density=beam_density)

    for doc, beam in zip(docs, beams):
        entity_scores = {}
        for score, ents in nlp.get_pipe("ner").moves.get_beam_parses(beam):
            for start, end, label in ents:
                if label == "PERSON":  # ignore non-person entities
                    # sum up the total confidence for each entity found in `text`
                    entity_scores[(start, end, label)] = entity_scores.get((start, end, label), 0) + score
    if len(entity_scores.values()) == 0:
        return 0
    return max(entity_scores.values())  # return max confidence over all entities found in `text`

def get_stratified_sample(df, stratum_colname, strata, n_per_stratum):
    """
    Return a stratified sample of `df` with `n_per_stratum` rows per stratum.
    The sample is stratified with respect to `stratum_colname` and the strata
    are defined by `strata`.
    
    Args:
    * strata - list of dicts. each dict should have keys for "start" and "end"
      defining the cutoffs for that stratum. can optionally include keys for 
      "left_operator" and "right_operator" indicating if stratum are closed or
      open at endpoints; defaults to open.
    """
    operator_lookup = {
        "left":
            {"closed": "ge", "open": "gt"},
        "right":
            {"closed": "le", "open": "lt"}
    }
    sample = []
    for s in strata:
        _str = s.get("left_operator", "open")
        left_operator = operator_lookup["left"][_str]
        _str = s.get("right_operator", "open")
        right_operator = operator_lookup["right"][_str]
        s_df = df[getattr(df[stratum_colname], left_operator)(s["start"]) & getattr(df[stratum_colname],right_operator)(s["end"])]
        n = min(len(s_df), n_per_stratum)  # if there aren't enough rows to choose from, choose all of them.
        print(f"Sampling from stratum {s}: {n} out of {len(s_df)} rows.")
        sample.append(s_df.sample(random_state=1, n=n))
    return pd.concat(sample)

def get_stratified_sample_fast(df, strata, n_per_stratum, random_state=1):
    """
    Return a stratified sample of `df` with `n_per_stratum` rows per stratum.
    The sample is stratified with respect to Presidio's NER confidence scores 
    and the strata are defined by `strata`.
    
    NER confidence scores take a long time to compute. This function runs
    faster by only computing NER confidences that are needed to take the 
    stratified sample.
    
    Args:
    * strata - list of dicts. each dict should have keys for "start" and "end"
      defining the cutoffs for that stratum. can optionally include keys for 
      "left_operator" and "right_operator" indicating if stratum are closed or
      open at endpoints; defaults to open.
    """
    operator_lookup = {
        "left":
            {"closed": "__ge__", "open": "__gt__"},
        "right":
            {"closed": "__le__", "open": "__lt__"}
    }
    sample = deepcopy(strata)
    for s in sample:
        s["queries"] = []
        _str = s.get("left_operator", "open")
        s["left_operator"] = operator_lookup["left"][_str]
        _str = s.get("right_operator", "open")
        s["right_operator"] = operator_lookup["right"][_str]
    
    # Iterate over the de-duplicated shuffled data. 
    # Add queries to each stratum until you've sampled the required number.
    df = df.sample(frac=1, random_state=random_state).reset_index(drop=True)
    idx = 0
    while any([len(x["queries"]) < n_per_stratum for x in sample]) and (idx < len(df)):
        query = df["query"][idx]
        confidence = float(get_ner_confidence(query))
        for s in sample:
            if (getattr(confidence, s["left_operator"])(s["start"])) & (getattr(confidence, s["right_operator"])(s["end"])):
                if len(s["queries"]) < n_per_stratum:
                    s["queries"].append(query)
                break
        idx += 1
    return pd.concat([pd.DataFrame({"query": s["queries"]}) for s in sample])
