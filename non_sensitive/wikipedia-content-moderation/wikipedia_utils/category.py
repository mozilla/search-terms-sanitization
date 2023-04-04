"""Tools for processing & exploring Wikipedia category data."""

from pathlib import Path
import gzip
from io import StringIO
import json
import re
import warnings

import pandas as pd
import requests


# Data files for Wikipedia categories are stored in ./category_data
DATA_DIR = Path("category_data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

CATEGORY_RDF_FILE = "category_index.ttl.gz"
CATEGORY_INFO_JSON = "category_info.json.gz"
CATEGORY_LINKAGE_JSON = "category_linkage.json.gz"
CATEGORY_DF_PKL = "category_df.pkl"

LATEST_DUMP_DATE_URL = (
    "https://dumps.wikimedia.org/other/categoriesrdf/lastdump/enwiki-categories.last"
)
CATEGORY_RDF_URL_TEMPLATE = "https://dumps.wikimedia.org/other/categoriesrdf/{date}/enwiki-{date}-categories.ttl.gz"


def get_latest_category_dump_date():
    """Pull the date of the latest dump from Wikimedia dumps.

    Returns a string in yyyymmdd format.
    """
    r = requests.get(LATEST_DUMP_DATE_URL)
    r.raise_for_status()
    return r.text[:8]


def fetch_category_index(date, data_dir):
    """Download the category index file from Wikimedia dumps.

    The file is usually 80-85 MB and so may take a while to download.

    date: the dump date in yyyymmdd format
    data_dir: an existing dir where the category data files will be written
    """
    url = CATEGORY_RDF_URL_TEMPLATE.format(date=date)
    dest_file = Path(data_dir) / CATEGORY_RDF_FILE
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)


def parse_category_index(data_dir):
    """Parse the RDF (TTL) index file according to its ontology.

    There are two types of records: category information and category linkage.
    Records of each type are converted to JSON and written to separate files.
    Fields are defined according the ontology at https://www.mediawiki.org/ontology/ontology.owl.

    data_dir: an existing dir where the category data files will be written
    """

    def parse_key(k):
        # Pull out canonical page URIs from full URLs
        # eg. '<https://en.wikipedia.org/wiki/Category:Songs>' -> 'Songs'
        return k.strip("<>").split("/Category:")[-1]

    def handle_entry(x):
        row = {}
        x = x.rstrip(" .")
        key, info = x.split(maxsplit=1)
        row["key"] = parse_key(key)
        if info.startswith("a mediawiki:Category"):
            # category property listing
            row["hidden"] = "HiddenCategory" in info
            props = info.split(" ;")
            for p in props:
                if p.startswith("rdfs:label"):
                    row["name"] = p[12:-1]
                elif p.startswith("mediawiki:pages"):
                    row["num_pages"] = int(p[17:-14])
                elif p.startswith("mediawiki:subcategories"):
                    row["num_subcats"] = int(p[25:-14])
        elif info.startswith("mediawiki:isInCategory"):
            # category membership listing
            info = info.split(maxsplit=1)[-1]
            links = info.split(">,<")
            row["memberof"] = [parse_key(x) for x in links]
        else:
            row = None

        return row

    data_dir = Path(data_dir)
    with (
        gzip.open(data_dir / CATEGORY_INFO_JSON, "wt") as fwc,
        gzip.open(data_dir / CATEGORY_LINKAGE_JSON, "wt") as fwcl,
        gzip.open(data_dir / CATEGORY_RDF_FILE, "rt") as fr,
    ):
        entry = StringIO()
        for line in fr:
            entry.write(line.strip())
            # RDF entries span multiple lines and terminate with '.'
            if entry.getvalue().endswith("."):
                row = handle_entry(entry.getvalue())
                if row:
                    rowstr = json.dumps(row) + "\n"
                    if "memberof" in row:
                        fwcl.write(rowstr)
                    else:
                        fwc.write(rowstr)
                entry.close()
                entry = StringIO()
        entry.close()


def create_combined_df(data_dir):
    """Read the JSON index files and join into a single DataFrame.

    The DF is written to a pickle file, which will probably by 300-400 MB.

    data_dir: an existing dir where the category data files will be written
    """
    data_dir = Path(data_dir)
    df_info = pd.read_json(data_dir / CATEGORY_INFO_JSON, lines=True).set_index("key")
    df_link = pd.read_json(data_dir / CATEGORY_LINKAGE_JSON, lines=True).set_index(
        "key"
    )

    # Listed categories should be unique
    assert df_info.index.value_counts().unique() == 1
    assert df_info["name"].value_counts().unique() == 1

    df_combined = pd.merge(
        df_info, df_link, left_index=True, right_index=True, how="outer"
    )
    df_combined = df_combined.rename(columns={"memberof": "parents"})

    def _fill_missing_list(x):
        if not isinstance(x, list):
            return []
        return x

    # `parents` column should contain lists. Fill missing values.
    df_combined["parents"] = df_combined["parents"].map(_fill_missing_list)

    # For each category, find the subsets of parents and children which are visible.
    # These are appended to df_combined.
    parents_flat = pd.merge(
        df_combined[["name", "hidden"]],
        df_combined["parents"].explode(),
        left_index=True,
        right_index=True,
    )
    # Ignore categories with no parents
    parents_flat = parents_flat.query("parents.notna()")
    # Look up display name & hidden indicator for parents
    n_parents_before = len(parents_flat)
    parents_flat = pd.merge(
        parents_flat,
        df_combined[["hidden", "name"]],
        left_on="parents",
        right_index=True,
        suffixes=("_cat", "_parent"),
        how="inner",
    )
    n_parents_after = len(parents_flat)
    # There are a few parents not found in the main category listing.
    # These are dropped silently by the use of the inner join.
    print(f"Unknown parents: {n_parents_before - n_parents_after:,}")

    # Pull out lists of visible parents.
    # These lists contain display names, not keys.
    visible_parents = (
        parents_flat.query("~hidden_parent")
        .groupby("key")
        .agg({"name_parent": lambda s: s.to_list()})
    )
    # Pull out lists of visible subcategories.
    # These lists contain display names, not keys.
    visible_subcats = (
        parents_flat.query("~hidden_cat")
        # `parents` contains keys
        .groupby("parents").agg({"name_cat": lambda s: s.to_list()})
    )

    df_combined["parents_visible"] = visible_parents["name_parent"]
    df_combined["subcats_visible"] = visible_subcats["name_cat"]
    # After left-joining in these columns, there may be some missing values introduced.
    # Replace with empty lists.
    df_combined["parents_visible"] = df_combined["parents_visible"].map(
        _fill_missing_list
    )
    df_combined["subcats_visible"] = df_combined["subcats_visible"].map(
        _fill_missing_list
    )

    # Combined dataset should contain exactly the categories in the flat listing
    assert len(df_combined) == len(df_info)
    assert df_combined.isna().sum().sum() == 0

    df_combined.to_pickle(data_dir / CATEGORY_DF_PKL)


class CategoryIndex:
    """Wrapper for the prepared category index providing exploration functionality.

    This assumes that the category index DF has already been built and the `CATEGORY_DF_PKL`
    file has been written. The full DF is loaded on initialization and can be accessed
    using the `cat_df` property.

    data_dir: an existing dir where category data files are written
    """

    def __init__(self, data_dir):
        self.data_dir = Path(data_dir)
        self.cat_df = pd.read_pickle(self.data_dir / CATEGORY_DF_PKL)

    def find_matching_categories(self, catlist=None, regex=None, nonempty=True):
        """Find a list of visible categories matching a list or regex.

        This can be used to build a list of categories from seeds. Seeds can be
        specified as a list of exact category names, a regex to match category
        names, or both.

        catlist: a list of exact category names
        regex: a regex to match category names
        nonempty: should empty categories (no pages or subcategories) be removed?

        Returns a subset of the category index DF corresponding to the search terms
        with additional columns:
        - `seed` gives the portion of the category name that was matched
        - `has_parent_in_list` indicates whether the category has a parent that is also in the result set
        """
        # First find the matching rows in the category info table
        cat_df_visible = self.cat_df.query("~hidden")
        search_results = []
        if catlist is not None:
            search_results.append(
                cat_df_visible[cat_df_visible["name"].isin(catlist)].assign(
                    seed=lambda d: d["name"]
                )
            )
        if regex is not None:
            re_matches = cat_df_visible["name"].str.extract(
                f"({regex})", flags=re.IGNORECASE
            )[0]
            search_results.append(
                cat_df_visible[re_matches.notna()].assign(seed=re_matches)
            )
        matching_cats = pd.concat(search_results, ignore_index=False)
        matching_cats = matching_cats[~matching_cats.index.duplicated()]

        # Some categories have no member pages or subcategories. Prune if requested
        if nonempty:
            matching_cats = matching_cats.query("num_pages + num_subcats > 0")

        # Remove unnecessary columns, as we are only working with visible categories
        matching_cats = matching_cats.drop(columns=["hidden", "parents"])

        # For each category in the list, check whether one of its parents is also in the list
        matching_cats_parents = self.find_parents_listed(matching_cats)
        matching_cats["has_parent_in_list"] = matching_cats_parents.groupby("key").agg(
            {"parent_in_list": "any"}
        )

        return matching_cats

    def find_parents_listed(self, cats):
        """Expand parents for a list of categories and flag parents which are also in the list.

        This is useful when using `find_matching_categories` to search for categories
        on a topic. A regex may match both a parent category and its subcategory, in
        which case only the parent may be needed.

        cats: a subset of the category index with columns `name`, `parents_visible`.

        Returns a DF exploding the parents for each category name in `cats`, with
        column `parent_in_list` indicating whether each parent was also listed in
        `cats`.
        """
        return (
            pd.merge(
                cats["name"],
                cats["parents_visible"].explode().rename("parent"),
                on="key",
            )
            .reset_index()
            .merge(
                cats["name"],
                left_on="parent",
                right_on="name",
                how="left",
                suffixes=("", "_in_list"),
            )
            .assign(parent_in_list=lambda d: d["name_in_list"].notna())
            .drop(columns="name_in_list")
            .set_index("key")
        )

    def category_bfs(
        self, cat_subset, ignore_cats=None, ignore_re=None, max_level=None
    ):
        """Given a set of categories, walk the graph of category->subcategory links for each one.

        This will discover all subcategories belonging to a set of categories using BFS.
        Certain subcategories can be ignored, and the depth of exploration can be limited.

        cat_subset: a subset of the category index with columns `name`, `subcats_visible`, `seed`
            as returned by `find_matching_categories()`.
        ignore_cats: a list of exact category names to exclude
        ignore_re: a regex to exclude
        max_level: if supplied, stop after this level

        Returns a DF with columns:
        - name: the category name discovered
        - seed: the original seed which generated the top-level category containing this one
        - parent: the immediate parent that led to this category
        - level: the number of steps from the seed category to this one
        """
        curr_cat_links = cat_subset
        # Apply exclusions
        if ignore_cats:
            curr_cat_links = curr_cat_links[~curr_cat_links["name"].isin(ignore_cats)]
        if ignore_re:
            with warnings.catch_warnings():
                # Ignore warning about matching groups.
                warnings.simplefilter("ignore", UserWarning)
                curr_cat_links = curr_cat_links[
                    ~curr_cat_links["name"].str.contains(ignore_re, case=False)
                ]
        bl_rows = (
            curr_cat_links[["name", "seed"]]
            .assign(parent=None, level=0)
            .reset_index(drop=True)
        )
        i = 0

        while len(curr_cat_links) > 0:
            i += 1
            if max_level and i > max_level:
                break
            print(f"Level: {i}", end="\r")
            # Next level of categories are visible subcategories of current list
            new_cats = (
                curr_cat_links["subcats_visible"]
                .explode()
                .to_frame()
                .join(curr_cat_links[["name", "seed"]])
                .reset_index(drop=True)
                .query("subcats_visible.notna()")
            )
            # Apply exclusions
            if ignore_cats:
                new_cats = new_cats[~new_cats["subcats_visible"].isin(ignore_cats)]
            if ignore_re:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", UserWarning)
                    new_cats = new_cats[
                        ~new_cats["subcats_visible"].str.contains(ignore_re, case=False)
                    ]

            # Drop previously seen categories.
            bl_new = new_cats.rename(
                columns={"subcats_visible": "name", "name": "parent"}
            ).assign(level=i)
            bl_rows = pd.concat([bl_rows, bl_new], ignore_index=True).drop_duplicates(
                subset="name"
            )
            curr_cat_list = bl_rows.query(f"level == {i}")[["name", "seed"]]
            curr_cat_links = curr_cat_list.merge(self.cat_df, on="name", how="left")

        return bl_rows
