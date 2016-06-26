import sqlite3
from elasticsearch import Elasticsearch, helpers
import json
import sys
import requests
import os
#TODO: parametrize the number of rows to do... this is currently hardcoded.

"""
A script to import sqlite3 tables into Elasticsearch
To use: give it one argument: the name of the directory to find the input files
in with no trailling space.
 then change DRY_RUN to False
"""
DRY_RUN = False 

#name_of_db should be a valid path to a sqlite3 database
def connect_to_sqlite_db(name_of_db):
  connection = sqlite3.connect(name_of_db)
  return connection.cursor()

def get_colnames_for_sqlite_table(sqlite_cursor):
  cursor_desc = sqlite_cursor.description
  headers = [ one_list[0] for one_list in cursor_desc ] 
  return headers

#takes a tuple that was returned from 'fetchone' 
def parse_one_sqilte_row_into_json_data(colnames, fetched_record):
    happy = zip(colnames, fetched_record)
    happy = dict(happy)
    happy = json.dumps(happy)
    return happy


#helpers.bulk handles chunking too, but I'm a control freak.
def put_bulk_json_into_elasticsearch(es, action_list):
    son = json.dumps(action_list)
    result = None 
    if DRY_RUN is False:
        result = helpers.bulk(es, 
                              action_list, 
                              index="atsnp_data", 
                              doc_type="atsnp_output")
    return result



def process_one_file_of_input_data(path_to_file, es):
    sqlite_file  = path_to_file 
    sqlite_table_name = 'scores_data'
    #sqlite_table_name = '[db_1_1.RData]'

    sqlite_cursor = connect_to_sqlite_db(sqlite_file)
    sqlite_cursor.execute("SELECT * FROM " + sqlite_table_name )
    colnames = get_colnames_for_sqlite_table(sqlite_cursor)
    i = 0
    bulk_loading_chunk_size = 10000 
    one_sqlite_record = sqlite_cursor.fetchone() 
    actions = []
    while one_sqlite_record is not None:
        i += 1

        json_data = parse_one_sqilte_row_into_json_data(colnames, one_sqlite_record)
        actions.append(json_data)
        if i % bulk_loading_chunk_size == 0:
            if i % (bulk_loading_chunk_size * 10):
                print("reached "+ str(i) + ' rows')
            put_bulk_json_into_elasticsearch(es, actions)
            actions = []

        one_sqlite_record = sqlite_cursor.fetchone()

    print "placing the last " + str(len(actions)) + " rows from file into ES."
    put_bulk_json_into_elasticsearch(es, actions)

#parametrize input to work with a whole directory of files. 
#make a DRY RUN mode to test all logic first.

#rsync the files from the JASPAR pipeline into the source_dir (first argument?)



#Point this script at one of the sync folders here, that's 
#where it will look for input.
#/z/Comp/kelesgroup/rhudson/jaspar_sqlite/sync1

input_path = None 

print(str(sys.argv))

if len(sys.argv) == 2:
  input_path = sys.argv[1]
else:
  exit(1)

es = Elasticsearch(['atsnp-db1','atsnp-db2','atsnp-db3'],sniff_on_start=True, sniff_on_connection_fail=True, sniffer_timeout=60)

for one_file in os.listdir(input_path):
    fpath = input_path + "/" + one_file
    print "processing file : "  + fpath
    process_one_file_of_input_data(fpath, es)



