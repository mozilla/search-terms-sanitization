Content moderation for Dynamic Wikipedia
========================================

This dir contains code and assets used for building a list of Wikipedia categories to exclude from Dynamic Wikipedia results.

More details on the approach can be found in the
[Proposal](https://docs.google.com/document/d/1O906d8t6CPEd9Tys_vhXPjll8FIM2ltLumwulB3dHsw/)
and [Working doc](https://docs.google.com/document/d/1FR3_xxuR-1yQGt5uA1fLboPBZtyxO3_AQizJHLNmhdI/).


The list is built from a manually curated set of __seeds__, consisting of exact category names, as well as regexes to match category names, which are relevant to the topics we wish to block.
The full list of categories is built by finding all categories matching the seeds as well as all of their subcategories.

- The current list of seeds is maintained in [moderation_category_seeds.yml](./moderation_category_seeds.yml).
- The current list of blocked categories is available in [blocklist_cats.csv](./blocklist_cats.csv).


Using the tools
---------------

1. First, install dependencies:
```bash
pip install -r requirements.txt
```
2. Run the notebook `build_category_index.ipynb` to download and process the latest Wikipedia category data dump into a more convenient format.

### To rebuild the blocklist from the current seed list:

- Run the notebook `build_blocklist.ipynb`. This writes a new blocklist CSV to `category_data/blocklist_cats.csv`. This may differ from the current blocklist if the Wikipedia category graph has changed. The notebook also presents tools for comparing the new blocklist table to the current one. When updating the current blocklist to the new one, overwrite the main CSV file:
```bash
cp category_data/blocklist_cats.csv ./blocklist_cats.csv
```
This serves as a reference for this repo but doesn't affect any production systems.

### To update the seed list:

- Edit the YAML file [moderation_category_seeds.yml](./moderation_category_seeds.yml). Choosing what seeds to use requires manual exploration. Tools for this are presented in `explore_categories.ipynb` and `build_blocklist.ipynb`.

### To pull the list of pages that would be blocked:

- Use the notebook `search_index.ipynb`. This will download the full CirrusSearch index dump and extract the subset of records matching the current blocklist. This can be used for validation purposes. __Warning:__  the full search index is 35 GB (!)

### Other exploration:

- Further exploration of available Wikipedia data is detailed in the notebook `wikipedia-exploration.ipynb`. This code is still experimental and not required for building the production blocklist.
