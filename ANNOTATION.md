# PII annotation

As a part of the work to develop PII sanitization capabilities, we create annotated datasets that can be used for evaluation.
These are typically small subsets of the sensitive search data for which we have manually labeled individual search queries according to their content type,
as well as whether or not they should be considered PII.

The content type categorizations cover both PII and non-PII, and are useful for understanding what proportions of PII types we are seeing,
as well as what other types of queries may be caught by sanitization approaches.

This page describes some guidelines we use for determining content type and PII status.

In our preliminary analysis, we have split the search data into the following 3 distinct groups, which we have considered separately:
- queries containing `@`
- queries containing a numeral (and no `@`)
- remaining queries

Different types of content and PII are considered for each group.
In the future, we will likely want to combine these into a single categorization covering all queries.

## Queries containing numerals

These queries contain at least one digit character, ie. `[0-9]`, and account for 8-9% of searches.
The majority of PII types will fall into this category, as they involve some form of number.

Queries are assigned one or more `type` categorizations describing the __purpose of the numerals__ in the query,
as well as a boolean `is_pii` indicating whether the query contains PII.
A query can have multiple type labels if it uses numerals in more than one way.

We currently use the following type categories and associated PII determination:

- `address`: street addresses or portions of one, eg. a zip or postal code.
	* a street name and number, or a complete address, is labeled _PII_
	* a zip code alone is _not PII_
	* a city/state name or address portion that doesn't include a number is _not_ labeled `address`
- `phonenumber`: phone numbers
	* all are labeled _PII_
	* includes phone numbers in standard formats for US or other countries
	* includes 1-800 type numbers
	* 10-digit numbers with no grouping or `-` characters were included if it could be determined from context that these are phone numbers
- `idnumber`: strings that look like an ID number
	* all are labeled _PII_
	* this category is not easy to infer without context in a lot of cases, and can be reconsidered in the future.
	  It may include things that are not actually PII.
	* queries are assigned this category if they include something that "looks like" an ID number.
	  For example, a longer string (>5 characters) that includes numbers, letters and/or separator characters like `-`
- `IPaddress`: IPv4 or IPv6 addresses
	* all are labeled _PII_
	* includes private IP address, eg. 192.168.x.x, as well as localhost (127.0.0.1).
	  These may not actually be PII, as they are not meaningful outside their network, but may be sensitive nonetheless.
- `financial`: financial-related numbers, eg. bank account numbers.
	* all are labeled _PII_
	* this category is difficult to infer without context.
- `date`: dates or portions of one, eg. year, month & date, times
	* a full date that can be inferred from context to be a birthdate is labeled _PII_
	* other instances are labeled _not PII_
- `measurement`: numbers used to represent amounts, measurements, ages
	* eg. "in 3 years", "2 cups of sugar", "5 inches", "10 USD in EUR"
	* all are labeled _not PII_
	* there are some queries using the search engine as a calculator, eg. "(2 + 3) * 4".
	  These were also labeled `measurement`
- `entityname`: numbers used as a part of an entity name
	* eg "python 3", "covid-19", "die hard 2", "3rd generation airpods"
	* all are labeled _not PII_
	* includes product identifiers such as model numbers
	* includes titles of movies/songs/books that have numbers
	* instances of this category are usually inferred from context.
	  For example, "1-inch screw" is labeled `entityname` and not `measurement`
	* years/dates that form part of an entity name are labeled `entityname` and not `date`, eg. "windows server 2003".
	  In particular, the model year of a car is labeled `entityname`, eg. "2022 honda civic"
- `other`: uses of numbers that don't fit into the above categories
	* these are labeled _not PII_ unless there is evidence to the contrary
	* often these are numbers without clear context

An annotated dataset containing 2000 queries is stored in BigQuery as `mozdata.search_terms_unsanitized_analysis.dzeber_labeled_queries_num_2022-03-08`.

## Queries containing `@`

These queries contain a `@` character. These only account for 0.1% of searches.
The main types of PII that these will include are email addresses and usernames.
Although this is a very small group, we apply a categorization in order to better understand the proportions of different types we are seeing.

Queries are assigned one or more `type` categorizations describing the __purpose of the `@` sign__ in the query,
as well as a boolean `is_pii` indicating whether the query contains PII.
A query can have multiple type labels if it uses numerals in more than one way.

- `email`: email addresses
	* all are labeled _PII_
- `username`: strings prefixed by `@` that look like usernames or handles, eg. `@user123`
	* all are labeled _PII_
	* note that usernames without the `@`, which may appear here or in other query groups, will generally not be flagged
- `code`: coding-related usage, eg. code snippets or package names
	* all are labeled _not PII_
- `at`: uses of `@` to mean "at", eg "meet @ 2 pm"
	* all are labeled _not PII_
- `other`: uses of `@` that don't fit into the above categories
	* these are labeled _not PII_ unless there is evidence to the contrary

An annotated dataset containing 500 queries is stored in BigQuery as `mozdata.search_terms_unsanitized_analysis.dzeber_labeled_queries_at_2022-03-08`.

## Name detection

Currently we do not apply a content type categorization to the remaining queries, which contain neither `@` nor any numerals.
However, these are labeled according to whether or not they contain a person's name, and whether these instances should be considered PII.

- `famous_name`: contains the first and last name of a famous person
- `nonfamous_name`: contains the first and last name of a person who is not famous

To determine whether or not a name is famous, put the name into Google search. If the top 3 non-advertisement Google search results do not indicate this person is famous, and there is no Google Knowledge Panel about the person, then the person is not famous. Examples of non-famous names may include: names with no Wikipedia article, names of small business owners, names of medical professionals. If a Google Knowledge Panel only appears for a business, but not the small business owner (the person themself), then the name is considered non-famous.

## Annotation procedure

Annotation for `@` and numeral groups is carried out in the Jupyter notebook environment using the following procedure:

1. Take a sample of the required size from the full set of search queries as a DataFrame
2. Add empty columns `type` and `is_pii`
3. Write locally to a CSV file
4. Open the CSV file using the JupyterLab [Spreadsheet Editor](https://github.com/jupyterlab-contrib/jupyterlab-spreadsheet-editor)
5. For each query, type the first letter of the appropriate categories in the `type` column
6. Enter T/F in the `is_pii` column only if necessary to override the default determinations listed above
7. Save the changes in the spreadsheet editor
8. In the notebook, load the modified CSV file to a DataFrame
9. Replace the category codes with full names, and fill in missing `is_pii` values using default determinations
10. Write the resulting DataFrame to a BigQuery table, named using the pattern
	`mozdata.search_terms_unsanitized_analysis.<username>_<table_name>_<yyyy-mm-dd>`,
	where the date is taken from the earliest timestamp in the search dataset.

An example of this procedure is shown in [this notebook](https://github.com/MozillaDataScience/search-terms-sanitization/blob/main/notebooks/unsanitized-exploration-dzeber.ipynb).
