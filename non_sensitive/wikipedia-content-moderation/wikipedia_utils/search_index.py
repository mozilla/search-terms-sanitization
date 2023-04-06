"""Tools for processing & exploring Wikipedia CirrusSearch index dump."""

from pathlib import Path
import gzip

# from io import StringIO
# import json
import re
from tqdm import tqdm

# import warnings

# import pandas as pd
import requests


FULL_SEARCH_INDEX_FILE = "cirrussearch-content-full.json.gz"

LATEST_DUMP_INFO_URL = "https://dumps.wikimedia.org/other/cirrussearch/current/"
LATEST_DUMP_REGEX = '"(enwiki-(\d{8})-cirrussearch-content.json.gz)"'
FULL_SEARCH_INDEX_URL = "https://dumps.wikimedia.org/other/cirrussearch/{date}/enwiki-{date}-cirrussearch-content.json.gz"


def get_latest_search_dump_date():
    """Pull info for the latest CirrusSearch dump from Wikimedia dumps.

    Returns the date in yyyymmdd format together with the file size.
    """
    r = requests.get(LATEST_DUMP_INFO_URL)
    r.raise_for_status()
    # This gives HTML listing links to dump files on separate lines.
    dump_match = re.search(LATEST_DUMP_REGEX, r.text)
    assert dump_match

    dump_file = dump_match.group(1)
    dump_date = dump_match.group(2)

    listing_line = [x for x in r.text.splitlines() if dump_file in x][0]
    dump_size = int(listing_line.split()[-1])
    return dump_date, dump_size


def fetch_search_index(date, data_dir):
    """Download the CirrusSearch index file from Wikimedia dumps.

    The file is huge (~35 GB) and will take a long time to download. Make sure enough
    free disk space is available before attempting to download.

    date: the dump date in yyyymmdd format
    data_dir: an existing dir where the search index data files will be written
    """
    url = FULL_SEARCH_INDEX_URL.format(date=date)
    dest_file = Path(data_dir) / FULL_SEARCH_INDEX_FILE
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


class IndexStream:
    """Stream the full ElasticSearch index and process each record.

    data_dir: an existing dir where the search index data files will be written
    """

    def __init__(self, data_dir):
        self.index_file = Path(data_dir) / FULL_SEARCH_INDEX_FILE
        self.fw = None
        self.n_kept = 0

    def _apply_full_index(self, max_records=None):
        """Read through the full index line by line and apply processing to each article listing.

        Index lines are skipped. Optionally stop after `max_records` article records.
        """
        # If processing the whole file, use a ballpark for total lines
        # so as to get reasonable progress monitoring.
        progress_total = 2 * max_records if max_records else 13_250_000
        with gzip.open(self.index_file, "rt") as fr:
            for i, line in tqdm(enumerate(fr), total=progress_total):
                if max_records and i >= max_records * 2:
                    break
                # Skip index lines
                if i % 2 == 0:
                    continue
                self._process_record(line, i)

    def _process_record(self, line, i):
        # What to do for each line (JSON record)
        # Call self._write_to_output(line) here if using output file
        raise NotImplementedError

    def _write_to_output(self, line):
        self.n_kept += 1
        self.fw.write(line)

    def run(self, output_file=None, max_records=None):
        """Start processing the index file.

        output_file: a gzipped file to write output to.
        max_records: if supplied, stop after this number of records
        """
        self.n_kept = 0
        if output_file:
            self.fw = gzip.open(output_file, "wt")
        try:
            self._apply_full_index(max_records=max_records)
        finally:
            if self.fw:
                self.fw.close()
