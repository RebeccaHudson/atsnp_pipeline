import sqlite3
import json
import sys
import os
import shared_pipe

"""
A script determine which rows from an sqlite3 file containing atsnp data 
match the specified cutoffs.
Nothing is added to or removed from Elasticsearch.
"""
#name_of_db should be a valid path to a sqlite3 database
def connect_to_sqlite_db(name_of_db):
    connection = sqlite3.connect(name_of_db)
    return connection.cursor()

#All zeros initally.
def setup_initial_counts():
    shared_pipe.init()
    empty_dict = dict.fromkeys(shared_pipe.NAMED_CUTOFFS.keys(), 0)
    empty_dict.update({'counted': 0}) 
    return empty_dict

def setup_count_query(cutoff_value):
    pvalues = ['pval_rank', ' OR pval_ref', ' OR pval_snp' ]
    query = 'SELECT COUNT(*) FROM scores_data WHERE '
    for pvalue in pvalues:
        query += ' '.join([pvalue,'<=',str(cutoff_value)])
    query +=';' 
    print "query : " + query
    return query

#All of the data minification has been removed. We're just counting.
#Wait, I don't need to open the file and iterate it, I can simply do a query.
def count_one_file_of_input_data(sqlite_file, countLog):
    sqlite_cursor = connect_to_sqlite_db(sqlite_file)
    summary = setup_initial_counts() 

    for one_cutoff in shared_pipe.NAMED_CUTOFFS.keys():  
        cutoff_value = shared_pipe.NAMED_CUTOFFS[one_cutoff]
        query = setup_count_query(cutoff_value)    
        sqlite_cursor.execute(query) 
        result = sqlite_cursor.fetchone()
        summary[one_cutoff] = result[0]

    sqlite_cursor.execute('SELECT count(*) from scores_data;') 
    summary['counted'] = sqlite_cursor.fetchone()[0]
    return summary 

def run_single_file(filepath):
    countlog = open('countlog.txt', 'ar+')
    summary = count_one_file_of_input_data(filepath, countlog)
    #This is where all of the counts are to be funneled back to.
    rowcount = summary['counted'] 
    countlog.write( "completed file: " + filepath + " with N= " + \
                      str(rowcount) + " rows of data.\n")
    print "summary " + repr(summary)
    countlog.close()
    return summary 
