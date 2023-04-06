"""Tools for processing & exploring Wikipedia category data."""

from pathlib import Path
import gzip
from io import StringIO
import json
import re
import warnings

import pandas as pd
import requests
import yaml


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


def load_blocklist_seeds(seed_file):
    """Load a YAML file listing category seeds for various topics.

    seed_file: path to the file

    Expects a file with the format
    ```
    blocklist:
      - topic: <topic_key>
        seed_categories:
          - <exact category name>
        seed_re:
          - <regex to match category names>
        ignore_categories:
          - <exact category name>
        ignore_re:
          - <regex to match category names>
        max_level: <max depth to search category digraph>
    ```

    Returns a dict with the format
    ```
    {
      <topic>: {
        "topic": <topic>,
        "seed_categories": [...],
        "seed_re": [...],
        "ignore_categories": [...],
        "ignore_re": [...],
        "max_level": n,
      },
      ...
    }
    ```
    for the blocklist topics, where all dict entries aside from `topic` are optional.
    """
    with open(seed_file) as f:
        seed_list = yaml.safe_load(f)

    # Curently only working with blocklist
    seed_list = seed_list["blocklist"]

    return {s["topic"]: s for s in seed_list}


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

    def build_category_list_for_topic(self, topic_seeds, top_level_seeds=True):
        """Build category list for a topic from the given seeds.

        This is done by identifying all Wikipedia categories matched by the seeds
        and searching through all of their subcategories.

        topic_seeds: a topic dict as returned by `load_blocklist_seeds()`
        top_level_seeds: seeds could match both a subcategory and its parent. If `True`,
            only keep the highest-level categories (with no matched parent) when identifying
            categories to search from.

        Returns a DF listing all categories generated from the seeds with columns:
        - name: category name
        - seed: the individual seed entry which generated the top-level category containing this one
        - parent: the immediate parent that led to this category
        - level: the number of steps from the seed category to this one
        - topic: the topic identifier provided in the input
        """
        seed_cats = topic_seeds.get("seed_categories")
        seed_re = topic_seeds.get("seed_re")
        if seed_re:
            # Combine to a single regex
            seed_re = "|".join(seed_re)
        ignore_cats = topic_seeds.get("ignore_categories")
        ignore_re = topic_seeds.get("ignore_re")
        if ignore_re:
            # Combine to a single regex
            ignore_re = "|".join(ignore_re)
        max_level = topic_seeds.get("max_level")

        include_cats = self.find_matching_categories(catlist=seed_cats, regex=seed_re)
        if top_level_seeds:
            include_cats = include_cats.query("~has_parent_in_list")
        all_cats = self.category_bfs(
            include_cats,
            ignore_cats=ignore_cats,
            ignore_re=ignore_re,
            max_level=max_level,
        )
        all_cats["topic"] = topic_seeds["topic"]

        return all_cats

    def build_full_category_list(self, seed_dict, output_csv):
        """Build a full category list from seeds across multiple topics.

        This is done by building a category list for each topic and combining.
        Duplicated categories, which may have arisen from multiple topics, are removed.
        The final table is written to file in CSV format.

        seed_dict: a seed dict as returned by `load_blocklist_seeds()`
        output_csv: path to the CSV file to write
        """
        # Exclusions should apply globally.
        all_ignore_cats = []
        all_ignore_re = []
        for d in seed_dict.values():
            all_ignore_cats.extend(d.get("ignore_categories", []))
            all_ignore_re.extend(d.get("ignore_re", []))

        all_ignore_cats = list(set(all_ignore_cats))
        all_ignore_re = list(set(all_ignore_re))

        cat_list_results = []
        for d in seed_dict.values():
            topic_seeds = dict(d)
            topic_seeds["ignore_categories"] = all_ignore_cats
            topic_seeds["ignore_re"] = all_ignore_re
            cat_list_results.append(self.build_category_list_for_topic(topic_seeds))

        full_list = pd.concat(cat_list_results, ignore_index=True)
        # Deduplicate, in case the same categories arose for multiple topics
        full_list = full_list.drop_duplicates(subset="name", ignore_index=True)

        full_list.to_csv(output_csv, index=False)


def compare_categories_lists(
    prev_cat_list, new_cat_list, differing_only=True, topic=None
):
    """Present two category lists in a convenient format for exploring differences.

    prev_cat_list: the previous category list, as returned by `build_full_category_list()`
    new_cat_list: the new category list, as returned by `build_full_category_list()`
    differing_only: should the output show only categories that differ (`True`) or all categories (`False`)
    topic: a single topic to optionally restrict comparison to

    Returns a DF with 1 row per (topic, category name) showing info from previous and new lists
        side by side.
    """

    def clear_row(r):
        # When doing outer join, fill in entries that are missing from one side with placeholder
        if r["type_previous"] != "previous":
            for x in r.index:
                if x.endswith("previous"):
                    r[x] = "--"
        if r["type_new"] != "new":
            for x in r.index:
                if x.endswith("_new"):
                    r[x] = "--"
        return r

    def diff_type(r):
        # Indicator of the difference type (add/remove/change)
        if r["seed_previous"] == "--":
            return "added"
        if r["seed_new"] == "--":
            return "removed"
        for c in ["seed", "parent", "level"]:
            if r[c + "_previous"] != r[c + "_new"]:
                return "changed"
        return ""

    combined_cat_list = (
        pd.concat(
            [prev_cat_list.assign(type="previous"), new_cat_list.assign(type="new")],
            ignore_index=True,
        )
        # Replace NaNs with "" for clearer display
        .assign(parent=lambda d: d["parent"].fillna(""))
    )
    comp_df = (
        # Create side-by-side view of previous and new info for each category
        pd.merge(
            combined_cat_list.query("type == 'previous'"),
            combined_cat_list.query("type == 'new'"),
            on=["name", "topic"],
            how="outer",
            suffixes=["_previous", "_new"],
        )
        # Level was converted to float because of NaNs - convert back to int
        .assign(
            level_previous=lambda d: d["level_previous"].fillna(-1).astype(int),
            level_new=lambda d: d["level_new"].fillna(-1).astype(int),
        )
        # Fill in entries missing from one side with placeholder
        .apply(clear_row, axis="columns")
        .drop(columns=["type_previous", "type_new"])
        # Add indicator of difference type
        .assign(diff=lambda d: d.apply(diff_type, axis="columns"))
        .sort_values(["topic", "level_previous", "name"])
    )
    # Sometimes a category appears under a different topic in the new list.
    # The outer join will produce duplicated rows in this case.
    comp_df.loc[comp_df.duplicated(subset="name", keep=False), "diff"] = "moved"

    if topic:
        comp_df = comp_df[comp_df["topic"] == topic].drop(columns="topic")
    if differing_only:
        comp_df = comp_df[
            (comp_df["seed_previous"].astype(str) != comp_df["seed_new"].astype(str))
            | (
                comp_df["parent_previous"].astype(str)
                != comp_df["parent_new"].astype(str)
            )
            | (
                comp_df["level_previous"].astype(str)
                != comp_df["level_new"].astype(str)
            )
        ]

    return comp_df


def style_diff(diff_df):
    """Apply styling to highlight differences in the list comparison.

    diff_df: a DF as returned by `compare_categories_lists()`

    Displays a DF in the notebook with colouring applied.
    """

    def diff_style(r):
        styles = pd.Series(None, index=r.index)
        if r["seed_previous"] == "--":
            for x in ["name", "seed_new", "parent_new", "level_new"]:
                styles[x] = "background-color: lightgreen"
        elif r["seed_new"] == "--":
            for x in ["name", "seed_previous", "parent_previous", "level_previous"]:
                styles[x] = "background-color: lightsalmon"
        else:
            for c in ["seed", "parent", "level"]:
                cc = c + "_previous"
                cn = c + "_new"
                if r[cc] != r[cn]:
                    styles[cc] = "background-color: gold"
                    styles[cn] = "background-color: gold"
        if r["diff"] == "moved":
            styles["name"] = "background-color: cyan"
        return styles

    return diff_df.style.apply(diff_style, axis="columns")
