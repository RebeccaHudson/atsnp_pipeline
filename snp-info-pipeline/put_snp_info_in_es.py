import sqlite3
from elasticsearch import Elasticsearch, helpers, ConnectionError
import elasticsearch
import time
import json
import sys
import requests
import os
import shared_pipe
"""
A script to import sqlite3 tables into Elasticsearch
then change DRY_RUN to False
"""
#DRY_RUN = True 
shared_pipe.init()
DRY_RUN = shared_pipe.DRY_RUN

#name_of_db should be a valid path to a sqlite3 database
def connect_to_sqlite_db(name_of_db):
  connection = sqlite3.connect(name_of_db)
  return connection.cursor()

def get_colnames_for_sqlite_table(sqlite_cursor):
  cursor_desc = sqlite_cursor.description
  headers = [ one_list[0] for one_list in cursor_desc ] 
  return headers

def build_unique_id_for_es_document(record_dict):
    print "record_dict " + repr(record_dict)
    return record_dict['snpids']

def update_summary_counts(summary_chunk, total_summary):
    total_summary['added'] += summary_chunk['added']
    total_summary['rejected'] += summary_chunk['rejected']
    total_summary['other'] += summary_chunk['other']
    return total_summary

#This method took hours to write, could not make the ES bulk helper 
#accept specific IDs on documents. Ended up using the 5/5/16 answer from:
#http://stackoverflow.com/questions/26159949/elasticsearch-python-bulk-api-elasticsearch-py
def parse_one_sqilte_row_into_json_data(colnames, fetched_record):
    happy = zip(colnames, fetched_record)
    happy = dict(happy)
    doc = []
    meta = {}
    meta['create'] = {}
    meta['create'] ['_id'] = build_unique_id_for_es_document(happy)
    del happy['snpids'] #This is included as the ID.

    meta['create']['_type'] = 'sequence'
    meta['create']['_index'] = shared_pipe.SETTINGS['index_name'] # 'snp_info'
    doc = [ meta, happy ]   
    docs_as_string = json.dumps(doc[0]) + '\n' + json.dumps(doc[1]) + '\n'
    return docs_as_string 

#not  using helpers.bulk; segmentation is now manual
def put_bulk_json_into_elasticsearch(es, action_list, elasticLog):
    result = None 
    summary = { 'added' : 0, 'rejected': 0, 'other': 0 }
    #duplicate records will be rejected
    if DRY_RUN is False:
        try:
            result = es.bulk(body=action_list)
        except elasticsearch.exceptions.ConnectionError:
            elasticLog.write("an ES connection error!" \
                             "Waiting 100 seconds to retry.\n")
            return put_bulk_json_into_elasticsearch(es, action_list, elasticLog)
        for item in result['items']:
            action_status = item['create']['status']
            if action_status == 409: 
                summary['rejected'] += 1
            elif action_status == 201:
                summary['added'] += 1
            elif action_status == 429:
                elasticLog.write('got a 429 status (bulk queue full) '\
                                 ' waiting 100 to retry\n')
                time.sleep(100)    
                elasticLog.write('retrying same call...\n')       
                return put_bulk_json_into_elasticsearch(es, action_list, elasticLog)
                #recursion actually appropriate?  All previous counts are rejected.
            else: 
                summary['other'] += 1
                elasticLog.write('status code other than 201, 409, or 429:' + \
                                 str(action_status)+"\n")
                elasticLog.write('contents of unknown thing: ' +\
                                  str(item)+"\n" )
    return summary 

def process_one_file_of_input_data(path_to_file, es, elasticLog):
    sqlite_file  = path_to_file 
    sqlite_table_name = 'snpinfo_hg38'

    sqlite_cursor = connect_to_sqlite_db(sqlite_file)
    sqlite_cursor.execute("SELECT * FROM " + sqlite_table_name )
    colnames = get_colnames_for_sqlite_table(sqlite_cursor)

    summary = { 'added' : 0, 'rejected': 0, 'other': 0 }
    i = 0
    bulk_loading_chunk_size = 5000  #TODO: crank up this chunk size
    one_sqlite_record = sqlite_cursor.fetchone() 
    #maybe worth using cursor.fetchmany(size)?
    actions = []
    while one_sqlite_record is not None:
        i += 1
        json_data =   \
           parse_one_sqilte_row_into_json_data(colnames, one_sqlite_record)
        actions.append(json_data)  
        if i % bulk_loading_chunk_size == 0:
            elasticLog.write( "loading batch into ES : (0th record from "\
                              + " current bulk batch):" \
                              + str(actions[0]) + "\n")
            summary_chunk = put_bulk_json_into_elasticsearch(es, actions, elasticLog)
            summary = update_summary_counts(summary_chunk, summary)
            actions = []
        one_sqlite_record = sqlite_cursor.fetchone()

    elasticLog.write("placing the last " + str(len(actions)) + \
                     " rows from file into ES.\n")
    summary_chunk = put_bulk_json_into_elasticsearch(es, actions, elasticLog)
    summary = update_summary_counts(summary_chunk, summary)
    return summary 

def run_single_file(filepath):
    elasticLog = open('elasticlog.txt', 'ar+')
    es = Elasticsearch(['atsnp-db1','atsnp-db2','atsnp-db3'], 
                        timeout=200, 
                        dead_timeout=100)
    summary = process_one_file_of_input_data(filepath, es, elasticLog)
    rowcount = summary['added'] + summary['rejected'] + summary['other']
    elasticLog.write( "completed file: " + filepath + " with N= " + \
                      str(rowcount) + " rows of data.\n")
    elasticLog.close()
    return summary 
