"""
Implementation of heuristic to identify intermediate search queries.
"""


import pandas as pd
import numpy as np
import numba as nb


def label_intermediate_queries(query_df, window_interval, min_length_diff, max_length_diff):
    """Identify intermediate queries and group them by their associated full query.

    Intermediate queries are prefixes of another query which occured soon after in the logs
    and is not too much longer than its prefix.

    query_df: DF with columns `query` and `merino_timestamp`
    window_interval: pandas time offset string representing the maximum time lag between
        an prefix (intermediate query) and its superstring.
    min_length_diff: min difference in length between the prefix and superstring
    max_length_diff: max difference in length between the prefix and superstring

    Returns a DF sorted by increasing timestamp with columns `merino_timestamp`, `query`, and:
    - `is_prefix`: is this query the prefix of another query according to the criteria above
    - `full_query_idx`: row index of the corresponding full query (can be used to group)

    For example, for `query_df`:
    `
                merino_timestamp      query
    0 2022-03-02 22:51:12.817297        ama
    1 2022-03-02 22:51:13.225711       amaz
    2 2022-03-02 22:51:13.494124      amazo
    3 2022-03-02 22:51:13.619602  wikipedia
    4 2022-03-02 22:51:13.726663     amazon
    5 2022-03-02 22:51:14.863453   amazon p
    `
    running `label_intermediate_queries(query_df, window_interval="1s", min_length_diff=1, max_length_diff=3)`
    will return
    `
                merino_timestamp      query  is_prefix  full_query_idx
    0 2022-03-02 22:51:12.817297        ama       True               4
    1 2022-03-02 22:51:13.225711       amaz       True               4
    2 2022-03-02 22:51:13.494124      amazo       True               4
    3 2022-03-02 22:51:13.619602  wikipedia      False               3
    4 2022-03-02 22:51:13.726663     amazon      False               4
    5 2022-03-02 22:51:14.863453   amazon p      False               5
    `
    """
    annotated = _annotate_prefixes(query_df, window_interval, min_length_diff, max_length_diff)
    with_full = _annotate_full_queries(annotated)
    return with_full[["merino_timestamp", "query", "is_prefix", "full_query_idx"]]


def _annotate_prefixes(query_df, window_interval, min_length_diff, max_length_diff):
    """Identify intermediate queries.

    Flags queries which are prefixes of another query which occured soon after in the logs
    and is not too much longer than its prefix.

    query_df: DF with columns `query` and `merino_timestamp`
    window_interval: pandas time offset string representing the maximum time lag between
        an prefix (intermediate query) and its superstring.
    min_length_diff: min difference in length between the prefix and superstring
    max_length_diff: max difference in length between the prefix and superstring

    Returns a DF sorted by increasing timestamp with columns `merino_timestamp`, `query` as well as:
    - `window_max_idx`: the index of the last row in the window.
        Ie, the lookahead window for row i is from i+1 to window_max_idx
    - `is_prefix`: is this query the prefix of another query according to the criteria above
    - `prefixes`: list of query rows that are prefixes of this query
    - `prefix_of`: query row that this query is a prefix of (or -1 if not a prefix)

    For example, for `query_df`:
    `
                merino_timestamp      query
    0 2022-03-02 22:51:12.817297        ama
    1 2022-03-02 22:51:13.225711       amaz
    2 2022-03-02 22:51:13.494124      amazo
    3 2022-03-02 22:51:13.619602  wikipedia
    4 2022-03-02 22:51:13.726663     amazon
    5 2022-03-02 22:51:14.863453   amazon p
    `
    running `_annotate_prefixes(query_df, window_interval="1s", min_length_diff=1, max_length_diff=3)`
    will return
    `
                merino_timestamp      query  window_max_idx  is_prefix prefixes prefix_of
    0 2022-03-02 22:51:12.817297        ama               4       True       []         1
    1 2022-03-02 22:51:13.225711       amaz               4       True      [0]         2
    2 2022-03-02 22:51:13.494124      amazo               4       True      [1]         4
    3 2022-03-02 22:51:13.619602  wikipedia               4      False       []        -1
    4 2022-03-02 22:51:13.726663     amazon               4      False      [2]        -1
    5 2022-03-02 22:51:14.863453   amazon p               5      False       []        -1
    `
    """
    # For each query, we will check if it is the prefix of another query
    # in a forward-looking time-interval window.
    #
    # Since pandas window functions don't support aggregating on string columns,
    # implement window computation directly.
    #
    # First, ensure the DF is sorted.
    query_df = query_df.sort_values("merino_timestamp", ignore_index=True)
    # Record the index delimiting the right edge of the window.
    # Need to run in reverse for the window to be forward-looking,
    # as window agg result gets associated with the right endpoint of the window.
    idx = pd.Series(query_df.index, index=query_df["merino_timestamp"])
    window_max_idx = idx[::-1].rolling(window_interval).agg("max").astype(int)
    window_max_idx = window_max_idx[::-1].values
    # We will use numba to compile the main computation.
    # List of strings passed to numba must be explicitly typed.
    nb_queries = nb.typed.List(query_df["query"])

    @nb.njit
    def find_prefixes(queries, window_max):
        is_prefix = [False for _ in range(len(queries))]
        prefix_of = [-1 for _ in range(len(queries))]
        # To use a list of lists in numba, must be instantiated explicitly.
        prefixes = nb.typed.List()
        for _ in range(len(queries)):
            x = nb.typed.List()
            # Dummy value to set type
            x.append(-1)
            prefixes.append(x)

        # For query i, check for valid superstrings in the window
        # from i+1 to window_max[i]
        for i in range(len(queries)):
            current_query = queries[i]
            for j in range(i+1, window_max[i]+1):
                test_super_query = queries[j]
                len_diff = len(test_super_query) - len(current_query)
                # Take advantage of short-circuiting for efficiency
                if (len_diff >= min_length_diff
                        and len_diff <= max_length_diff
                        and test_super_query.startswith(current_query)):
                    is_prefix[i] = True
                    prefixes[j].append(i)
                    prefix_of[i] = j
                    break
        return is_prefix, prefix_of, prefixes

    is_pref, pref_of, prefs = find_prefixes(nb_queries, window_max_idx)

    # Return the sorted DF with indexing rows appended
    query_df["window_max_idx"] = window_max_idx
    query_df["is_prefix"] = is_pref
    # Remove initial dummy value
    query_df["prefixes"] = [x[1:] for x in prefs]
    query_df["prefix_of"] = pref_of

    return query_df


def _annotate_full_queries(annotated_query_df):
    """Backtrace prefixes to identify full queries associated with each prefix.

    annotated_query_df: query DF annotated by `_annotate_prefixes()`

    Returns a copy the `annotated_query_df` with column `full_query_idx` appended
    listing the row index of the corresponding full query.
    """
    @nb.njit
    def label_queries(is_prefix, prefixes, prefix_of):
        # Each chain of prefixes is represented as a tree with the full query as its root.
        # Walk each tree and label all queries in the tree with the index of the full query.
        full_idx = [i for i in range(len(is_prefix))]

        for i in range(len(is_prefix)):
            if not is_prefix[i] and prefixes[i]:
                to_visit = []
                to_visit.extend(prefixes[i])
                while to_visit:
                    j = to_visit.pop(0)
                    full_idx[j] = i
                    to_visit.extend(prefixes[j])

        return full_idx

    is_pref = annotated_query_df["is_prefix"].values
    pref_of = annotated_query_df["prefix_of"].values
    prefs = nb.typed.List()
    for x in annotated_query_df["prefixes"].values:
        prefs.append(x)

    full_query_idx = label_queries(is_pref, prefs, pref_of)

    return annotated_query_df.assign(full_query_idx=full_query_idx)
