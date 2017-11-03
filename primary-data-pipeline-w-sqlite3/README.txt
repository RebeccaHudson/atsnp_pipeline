This directory contains the old pipeline code that does not rely on the 
Rpy2 Python module. It instead opens each RData file in an R script, 
re-saves the contents of the file to sqlite3 format, then opens the same
file agian in Python for processing and indexing to Elasticsearch.

I'm keeping this around for now in case Rpy2 becomes unavailable for some reason.
