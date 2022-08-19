"""
Tools for exploring search data.
"""

from IPython.display import display, IFrame
import numpy as np
import pandas as pd
from collections import namedtuple


def google_iframe(qstr):
    """Open a Google search result in an iframe in a notebook."""
    display(
        IFrame(f"https://www.google.com/search?igu=1&q={qstr}", width=1000, height=400)
    )


class SearchSessionSampler:
    """Show a random search session.

    df_search: a DF of the format returned by `get_queries` with columns
        `query`, `session_id`, `sequence_no`.
    random_seed: random seed passed to numpy.
    """

    def __init__(self, df_search, random_seed=543):
        np.random.seed(random_seed)
        self.session_ids = df_search["session_id"].unique()
        self.df = df_search.set_index("session_id")

    def sample(self):
        samp = self.df.loc[[np.random.choice(self.session_ids)]]
        display(samp.sort_values("sequence_no").reset_index(drop=True))


class SearchAnnotator:
    """Tool to annotate unsanitized search queries for validation.

    Runs through a set of search queries, one at a time. For each query, a tag
    can be assigned through a text-based UI.

    When a run is exited, progress is saved in the instance. Use `get_tags()`
    to get the annotation tags assigned so far.

    Eg.
    ```
    sa = SearchAnnotator(df)
    sa.run()
    # Run through queries and exit
    sa.get_tags()
    ```

    df_search: a DF of the format returned by `get_queries`
    """

    Option = namedtuple("Option", ["key", "display_name", "tag"])
    # Options for the UI
    tag_options = [
        Option("n", "PII - nonfamous name", "pii_name"),
        Option("f", "famous name", "famous_name"),
        Option("p", "PII - numeric/at", "pii_numat"),
        Option("r", "mark for review", "_review"),
    ]
    control_options = {
        "back": Option("b", "back", None),
        "google": Option("g", "search in Google", None),
        "exit": Option("x", "exit", None),
    }

    def __init__(self, df_search):
        self.df = df_search
        self.tags = pd.Series(None, index=self.df.index)

        self.current_i = 0
        self.should_continue = True

        prompt_opts = "\n".join(
            [
                f"{o.key}: {o.display_name}"
                for o in self.tag_options + list(self.control_options.values())
            ]
        )
        self.prompt = f"\nPII?\n{prompt_opts}\n\n"
        self.google = False

    def request_input(self):
        current = self.df.iloc[[self.current_i]]
        display(current, clear=True)
        print(f"Progress: {self.current_i + 1} of {len(self.df)}")
        if self.google:
            google_iframe(current.iloc[-1]["query"])
            self.google = False

        typed = input(self.prompt)

        for o in self.tag_options:
            if typed == o.key:
                self.tags.iloc[self.current_i] = o.tag
                self.current_i += 1
                return

        if typed == self.control_options["google"].key:
            self.google = True
        elif typed == self.control_options["back"].key and self.current_i > 0:
            self.current_i -= 1
        elif typed == self.control_options["exit"].key:
            self.should_continue = False
        else:
            # Default is to not mark the session
            self.current_i += 1

    def run(self):
        """Start running the tool."""
        self.should_continue = True
        while self.current_i < len(self.df) and self.should_continue:
            self.request_input()

    def get_tags(self):
        """Return the annotation results.

        This is a Series indexed like the input DF containing tag strings or None.
        """
        return self.tags


class BatchedSearchAnnotator(SearchAnnotator):
    """Tool to annotate unsanitized search queries for validation using batching.

    For efficiency, a batch of search queries is shown. This can be helpful
    if most queries don't need to be annotated.

    A single entry in a batch can be annotated using its row number, eg. enter
    `n 15` to tag row 15 as containing a name.

    Enter `e` to run through the current batch one query at a time.

    df_search: a DF of the format returned by `get_queries`
    batch_size: number of queries to show each time
    """

    Option = namedtuple("Option", ["key", "display_name", "tag"])
    # Options for the UI
    control_options = {
        "batch": Option("e", "enter batch", None),
        "back": Option("b", "back", None),
        "exit": Option("x", "exit", None),
        "continue": Option("<other>", "continue", None),
    }

    def __init__(self, df_search, batch_size=20):
        super().__init__(df_search)

        self.batch_size = batch_size

    def request_input(self):
        start_i = self.current_i
        end_i = self.current_i + self.batch_size
        current = self.df.iloc[start_i:end_i]
        display(current, clear=True)
        print(f"Progress: {start_i + 1} of {len(self.df)}")
        if self.google:
            google_iframe(current.iloc[-1]["query"])
            self.google = False

        typed = input(self.prompt)

        for o in self.tag_options:
            if typed.startswith(o.key):
                _, tag_i = typed.split(" ", maxsplit=1)
                self.tags.loc[int(tag_i)] = o.tag
                return

        if typed == self.control_options["back"].key and self.current_i > 0:
            self.current_i = self.current_i - self.batch_size
        elif typed == self.control_options["exit"].key:
            self.should_continue = False
        elif typed == self.control_options["batch"].key:
            subanot = SearchAnnotator(current)
            subanot.run()
            self.tags.iloc[start_i:end_i] = subanot.get_tags().values
        else:
            # Default is to not mark the session
            self.current_i = end_i
