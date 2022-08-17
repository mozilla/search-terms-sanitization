from nltk import edit_distance, ngrams
from random import sample as random_sample
from statistics import mean
import spacy
import pandas as pd
import os

from .evaluate import str_to_words
from .helpers import get_queries, get_scrabble

class SearchTermsCounter:
    def __init__(self, dataset, terminal_only=True):
        self.dataset = dataset.lower()  # 'sanitized' or 'unsanitized'
        self.terminal_only = terminal_only
        self.phrase_extraction = "ngrams"
        self.similarity = "spacy"
        self.nlp = spacy.load("en_core_web_lg")
        self._set_stopwords()
        self._set_spellchecker()
    
    def load_texts(self):
        self.texts = getattr(self, f"_load_texts_from_{self.dataset}")()
        self.phrases = getattr(self, f"_extract_phrases_via_{self.phrase_extraction}")()
    
    def _load_texts_from_unsanitized(self):
        df = get_queries(terminal_only=self.terminal_only)
        return df["query"].apply(lambda x: " ".join(str_to_words(x))).tolist()
    
    def _load_texts_from_sanitized(self):
        pass

    def _extract_phrases_via_ngrams(self):
        """
        Returns all 1, 2, and 3-grams as phrases.
        """
        return [[" ".join(y) for x in range(1,4) for y in ngrams(text.split(), x)] for text in self.texts]
    
    def _set_stopwords(self):
        sw = self.nlp.Defaults.stop_words
        nouns = []
        for word in sw:
            doc = self.nlp(word)
            if doc[0].pos_ in ["NOUN", "PROPN"]:
                nouns.append(word)
        self.stopwords = [x for x in sw if x not in nouns]
    
    def _set_spellchecker(self):
        _dir = os.path.dirname(os.path.realpath(__file__))
        with open(f"{_dir}/../assets/american.txt") as f:
            self.dictionary = [x.strip() for x in f.readlines()]
    
    def set_keyphrases(self, keyphrases):
        self.keyphrases = [" ".join(str_to_words(x)) for x in keyphrases]
    
    def count(self):
        """
        Returns the number of self.texts containing any phrase in self.keyphrases.
        """
        return len([x for x in self.texts if any([
            (k == x) or x.startswith(f"{k} ") or (f" {k} " in x) or x.endswith(f" {k}")
            for k in self.keyphrases])])
    
    def validate(self, target_phrases, return_n):
        """
        Returns a random sample of `return_n` search terms containing at least
        one of the target_phrase's. 
        
        Non-deterministic!
        
        target_phrases (list of strings): List of phrases to search for. 
        return_n (int): Number of search terms to return.
        """
        target_phrases = [" ".join(str_to_words(x)) for x in target_phrases]
        shuffled = random_sample(self.texts, len(self.texts))
        validation = []
        while (len(validation) < return_n) and (len(shuffled) > 0):
            text = shuffled.pop(0)
            for target in target_phrases:
                if (target == text) or text.startswith(f"{target} ") or (f" {target} " in text) or text.endswith(f" {target}"):
                    validation.append(text)
                    break
        return sorted(validation)
    
    def show_common_misspellings(self, return_n, max_distance=2):
        results = self.find_common_misspellings(return_n, max_distance)
        for phrase in results:
            words = phrase.split()
            if any([w not in self.dictionary for w in words]):
                print(phrase)
                print(self.validate([phrase], 10))
                print()

    def find_common_misspellings(self, return_n, max_distance=2):
        """
        Returns a list of the top `return_n` misspellings of self.keyphrases
        that appear in self.texts. Misspellings are defined as 
        edit distance < `max_distance`.

        return_n (int): Number of misspellings to return
        max_distance (int): Maximum edit distance used to define a misspelling
        """
        mis_spell = {}
        for phrases in self.phrases:
            for phrase in phrases:
                words = phrase.split()
                if any([edit_distance(k, phrase) <= max_distance for k in self.keyphrases]):
                    mis_spell[phrase] = mis_spell.get(phrase, 0) + 1
        return [x[0] for x in sorted(mis_spell.items(), key=lambda x:x[1], reverse=True)[:return_n]]
    
    def show_similar_phrases(self, df, group_similarity=0.6, phrase_similarity=0.25):
        grouped = df.sort_values(["group_similarity", "phrase_similarity"], ascending=False) \
            [(df.group_similarity > group_similarity) | (df.phrase_similarity > phrase_similarity)] \
            .groupby("group")
        for group, df in grouped:
            print(group)
            print(f"Examples: {self.validate([group], 10)}")
            print(f"More specific keyphrases: {[x for x in df.phrase.tolist() if x != group]}")
            print()
            
    def find_similar_phrases(self, return_n):
        return getattr(self, f"_find_similar_phrases_{self.similarity}")(return_n)
    
    def _find_similar_phrases_spacy(self, return_n):
        """
        Uses spacy similarity scores to return similar phrases.
        """
        docs = [self.nlp(x) for x in self.keyphrases]
        similar_phrases = {}
        for idx, phrases in enumerate(self.phrases):
            context_similarity = self._get_similarity(self.texts[idx], docs)
            for phrase in phrases:
                if not any([k in phrase for k in self.keyphrases]) and not all([x in self.stopwords for x in phrase.split()]):
                    if phrase not in similar_phrases.keys():
                        phrase_similarity_weight = self._get_similarity(phrase, docs)
                        word_similarities = {word: self._get_similarity(word, docs) for word in phrase.split()}
                        similar_phrases[phrase] = {
                            "group": max(word_similarities, key=word_similarities.get), 
                            "group_similarity": max(word_similarities.values()),
                            "phrase_similarity_weight": phrase_similarity_weight,
                            "context_similarities": []
                        }                         
                    similar_phrases[phrase]["context_similarities"].append(context_similarity)
        similar_phrases = pd.DataFrame([{
            "group": _dict["group"],
            "group_similarity": _dict["group_similarity"],
            "phrase": phrase, 
            "phrase_similarity": _dict["phrase_similarity_weight"]*mean(_dict["context_similarities"])  # or divided by ceil(n)
        } for phrase, _dict in similar_phrases.items()])
        similar_phrases = similar_phrases.sort_values("phrase_similarity", ascending=False).reset_index(drop=True)[:return_n]
        
        # remove phrases with nondictionary words
        drop_indices = []
        for idx, row in similar_phrases.iterrows():
            words = row["phrase"].split()
            if any([w not in self.dictionary for w in words]):
                drop_indices.append(idx)
        similar_phrases = similar_phrases.drop(drop_indices, axis=0)
    
        # drop phrases that are less similar than their group word.
        # drop phrases that are substrings or superstrings of more similar phrases in their group.
        keep = []
        for group, df in similar_phrases.groupby("group"):
            tmp = df.sort_values("phrase_similarity", ascending=False).reset_index(drop=True)
            tmp2 = tmp[group == tmp["phrase"]]
            if len(tmp2) == 0:
                idx = len(tmp) - 1
            else:
                idx = tmp2.index[0]
            relevant = tmp.iloc[:idx+1]  # drop phrases less similar than their group word
            nonintersecting = []
            for _, row in relevant.iterrows():
                if not any([(row["phrase"] in x) or (x in row["phrase"]) for x in nonintersecting]):
                    nonintersecting.append(row["phrase"])  # drop phrases that are substrings or superstrings of more similar phrases in their group.
            nonintersecting.append(group)
            keep.append(relevant[relevant.phrase.isin(nonintersecting)])
        return pd.concat(keep).sort_values(["group_similarity", "phrase_similarity"], ascending=False).reset_index(drop=True)
    
    def _get_similarity(self, _string, docs):
        return max([x.similarity(self.nlp(_string)) for x in docs])
                         
