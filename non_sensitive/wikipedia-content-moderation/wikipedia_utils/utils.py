import pandas as pd
from IPython.display import display


def display_pd(x, rows=None, cols=None, colwidth=None, seqitems=None, full=True):
    """Display pandas object with different display settings."""
    opts = []
    if full:
        rows = rows or -1
        cols = cols or -1
        colwidth = colwidth or -1
        seqitems = seqitems or -1
    if rows:
        rows = None if rows < 0 else rows
        opts.append("display.max_rows")
        opts.append(rows)
    if cols:
        cols = None if cols < 0 else cols
        opts.append("display.max_columns")
        opts.append(cols)
    if colwidth:
        colwidth = None if colwidth < 0 else colwidth
        if colwidth is None:
            # Adjust max seq items as well to make display unlimited
            seqitems = -1
        opts.append("display.max_colwidth")
        opts.append(colwidth)
    if seqitems:
        seqitems = None if seqitems < 0 else seqitems
        opts.append("display.max_seq_items")
        opts.append(seqitems)
    if opts:
        with pd.option_context(*opts):
            display(x)
    else:
        display(x)
