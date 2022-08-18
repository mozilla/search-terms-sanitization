"""
Wrappers for interacting with external sanitization libraries.
"""

from collections import namedtuple, defaultdict

import pandas as pd
from presidio_analyzer import AnalyzerEngine
from google.cloud import dlp_v2


EntityResult = namedtuple("EntityResult", ["type", "text", "score"])


class PresidioScanner:
    def  __init__(self, entities=None, exclude_entities=None):
        """Set up a scanner for Presidio.

        entities: list of Presidio entity ID strings to check for
        exclude_entities: list of Presidio entities to exclude.

        If only `exclude_entities` is specified, use all PII types
        except for the listed ones. Otherwise, use the types given in `entities`.
        """
        self.scanner = AnalyzerEngine()

        if not entities and exclude_entities:
            all_entities = self.scanner.get_supported_entities()
            entities = [e for e in all_entities if e not in exclude_entities]

        self.entities = entities
    
    
    def _format_result(self, scan_result, original_text):
        """Convert a Presidio `RecognizerResult` into our result format."""
        return EntityResult(
            scan_result.entity_type,
            original_text[scan_result.start:scan_result.end],
            scan_result.score
        )

    def scan_single_string(self, text):
        """Scan a single string for PII using Presidio.
        
        Returns a list of `EntityResult`s, or `None` if no results were found.
        """
        results = self.scanner.analyze(text, language="en", entities=self.entities)
        if not results:
            return None

        return [self._format_result(r, text) for r in results]
    
    def scan_strings(self, text_series):
        """Scan a series of strings for PII using Presidio.
        
        Currently, Presidio doesn't operate in batch mode over a list of strings,
        so this just runs the scanner on each string separately.
        
        Returns a like-indexed series with corresponding results. Each entry contains
            a list of `EntityResult`s, or `None` if no results were found.
        """
        return text_series.map(self.scan_single_string)


class DLPScanner:
    DLP_PARENT_PROJECT = "projects/search-sanitization-dev"

    def  __init__(self, entities=None):
        """Set up a scanner for DLP.
        
        entities: list of DLP InfoType ID strings to check for
        """
        self.client = dlp_v2.DlpServiceClient()
        self.entities = entities


    def _issue_dlp_request(self, text_list):
        """Issue an API request for inspection results.
        
        text_list: list-like of strings to scan. Length and data size of this list
            must be limited for the request to be successful.
            
        Returns an iterable of DLP `Finding`s.
        """
        request_flds = {
            "parent": self.DLP_PARENT_PROJECT,
            "inspect_config": {
                "info_types": [{"name": x} for x in self.entities],
                # Return results of any likelihood for testing purposes.
                "min_likelihood": "VERY_UNLIKELY",
                # Return the substring detected as PII in each case
                "include_quote": True,
            },
            "item": {
                "table": {
                    "headers": [{"name": "query"}],
                    "rows": [{"values": [{"string_value": x}]} for x in text_list],
                }
            }
        }
        response = self.client.inspect_content(request=request_flds)
        if response.result.findings_truncated:
            # Shouldn't happen in testing. This case is not explicitly handled.
            print("findings truncated")

        return response.result.findings


    def _format_result(self, finding):
        """Convert a DLP `Finding` into our result format.
        
        Returns (positional index (int), EntityResult).
        """
        # Results are a nested dictionaries containing result details.
        # Location of detected PII substring is included in a list.
        # List length always seems to be 1
        assert len(finding.location.content_locations) == 1

        # Results are matched to the original text by positional index
        # and do not necessarily appear in order.
        # Each original string may have multiple associated results.
        i = finding.location.content_locations[0].record_location.table_location.row_index
        r = EntityResult(
            type=finding.info_type.name, text=finding.quote, score=finding.likelihood.name
        )
        
        return i, r
        
    def scan_strings(self, text_series):
        """Scan a series of strings for PII using GCP DLP.
        
        text_series: a pandas Series of strings
        
        Returns a like-indexed series with corresponding results. Each entry contains
            a list of `EntityResult`s, or `None` if no results were found.
        """
        all_results = []
        
        # API has a per-request cap both in terms of number of records and total data size.
        # To avoid hitting this, split data into 10K chunks and submit separate requests for each.
        bounds = list(range(0, len(text_series), 10000))
        for i in range(len(bounds)):
            start = bounds[i]
            end = bounds[i+1] if i+1 < len(bounds) else None
            chunk = text_series.iloc[start:end]
            
            findings = self._issue_dlp_request(chunk)
            result_dict = defaultdict(list)
            for x in findings:
                # Index i refers to the position in the chunk list.
                i, r = self._format_result(x)
                result_dict[i].append(r)
            result_ser = pd.Series(result_dict, index=pd.RangeIndex(len(chunk)))
            all_results.append(result_ser)

        # Make the result Series index line up with the input
        result_combined = pd.concat(all_results).set_axis(text_series.index)

        return result_combined
