# TemporalReconstruction_SE
A few scripts to get vote history from publicly available SE data (Internet archive)

Download any site's data from https://archive.org/details/stackexchange
(recommended 7z format and extract)

Dependencies: pandas (uses version 0.17.1, sqlite3 (python interface), numpy and python 2.7+)
On the folder wher site's data was extracted:
- python makedb.py
- python create_tables.py
- python create_votes.py
- python clean_format_votes.py

result should be in 'VotesRaw.csv'

We also include two notebooks : Extract_votes.ipynb and Clean_format_votes.ipynb
if interested in modifying the code it is advised to use those notebooks to print
iteratively the tables during debugging. (Scripts contain the same code but are not
meant to be readable, notebooks are a much better development environment for pandas).

- For efficiency:
run the queries in create_tables.py in command line mode. Python sqlite interface
makes it much slower. (e.g. .mode csv .output 'filename' run query)
- these scripts have been tested for various site sizes. It should also work for
Stack Overflow. *These are quite expensive transformations*.
However, it is advised to use a machine with more than 16 GB RAM.
If you have a server with 1TB or similar there should be no problem.
