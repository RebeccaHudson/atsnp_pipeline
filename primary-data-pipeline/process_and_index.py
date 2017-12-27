import sqlite3
from elasticsearch import Elasticsearch, helpers, ConnectionError
import elasticsearch
import time
import json
import sys
import requests
import os
import zlib
import math
import pickle

import shared_pipe

shared_pipe.init()
pStates = shared_pipe.PROGRESS_STATES
cutoffs = shared_pipe.PVALUE_CUTOFFS
use_cutoffs = shared_pipe.RESTRICT_BY_PVALUE

"""
A script that processes a subset of an R DataFrame contained in one
of the atSNP output RData files, obtained using the rpy2 module.
The processed data is fed into an Elasticsearch cluster.

To use: give it one argument: the name of the directory to find the input files
in with no trailling space.
 then (in the shared_pipe settings file) change DRY_RUN to False.
"""
DRY_RUN = shared_pipe.DRY_RUN #False 

#name_of_db should be a valid path to a sqlite3 database
def connect_to_sqlite_db(name_of_db):
    connection = sqlite3.connect(name_of_db)
    return connection.cursor()

def get_colnames_for_sqlite_table(sqlite_cursor):
    cursor_desc = sqlite_cursor.description
    headers = [ one_list[0] for one_list in cursor_desc ] 
    return headers

def build_unique_id_for_es_document(record_dict):
    field_names = ['motif', 'snpid', 'snpAllele']
    record_id = "_".join(   \
                    [record_dict[field_name] for field_name in field_names])
    record_id = record_id.replace(".", "_")
    return record_id 

def update_summary_counts(summary_chunk, total_summary):
    total_summary['added'] += summary_chunk['added']
    total_summary['skipped'] += summary_chunk['skipped']
    total_summary['other'] += summary_chunk['other']
    return total_summary




#This method took hours to write, could not make the ES bulk helper 
#accept specific IDs on documents. Ended up using the 5/5/16 answer from:
#http://stackoverflow.com/questions/26159949/elasticsearch-python-bulk-api-elasticsearch-py
def parse_into_json(fetched_record, motif_ic, elasticlog):
    #fetched_record should be the dict that used to get build right here.
    #happy = zip(colnames, fetched_record)
    #happy = dict(happy)
    #Can I just pass a dict into here?
    happy = fetched_record
    doc = []
    meta = {}
    meta['create'] = {}
    meta['create'] ['_id'] = build_unique_id_for_es_document(happy)
    meta['create'] ['_type'] = 'atsnp_output'
    meta['create']['_index'] = shared_pipe.SETTINGS['index_name'] #'atsnp_reduced_test'
    happy = add_log_lik_rank(happy, elasticlog )
    happy = collapse_strand_info(happy)
    happy = translate_alleles(happy)  #can probably be ultimately omitted.
    happy = change_chromosome_to_byte(happy)
    happy = pick_seq_start_and_end(happy)
    happy = convert_snpid_to_numeric(happy)
    happy = remove_fields(happy)
    happy = reduce_fields(happy)
    happy = force_offsets_to_ints(happy)
    happy = append_motif_ic_to_record(happy, motif_ic)
  
    doc = [ meta, happy ]   
    docs_as_string = json.dumps(doc[0]) + '\n' + json.dumps(doc[1]) + '\n'
    return docs_as_string 

def append_motif_ic_to_record(dict_data, motif_ic_pickle):
    dict_data['motif_ic'] = motif_ic_pickle[dict_data['motif']]
    return dict_data

#There is a chromosome called 'MT'; should this be its own thing?
def change_chromosome_to_byte(dict_data):
    cipher = { 'X': 23, 'Y':24, 'M':25, 'MT':25 }
    #TODO: determine if 'MT' is a chromosome unto itself, or, if for our purposes, it's part of 'M'
    t = dict_data['chr'].replace('ch', '')
    if not t.isdigit():
        t = cipher[t]  
    dict_data['chr'] = int(t)  #was a string, now a byte.
    return dict_data

def force_offsets_to_ints(dict_data):
    dict_data['snp_extra_pwm_off'] = int(dict_data['snp_extra_pwm_off']) 
    dict_data['ref_extra_pwm_off'] = int(dict_data['ref_extra_pwm_off'])
    return dict_data

def add_log_lik_rank(dict_data, elasticlog):
    valU = None
    try: 
        #Missed the natural logarithm before; this is the corrected version.       
        #(NOTE: math.log with no 2nd argument is w/ base 'e' (the natural log).)
        #ln(pval_ref + 1e-10) - ln(pval_snp + 1e-10)
        valU = math.log(float(dict_data['pval_ref']) + 1e-10) - \
               math.log(float(dict_data['pval_snp']) + 1e-10  )
        # INCORRECT FORMULA: 
        # valU = math.log(float(dict_data['pval_ref'])/float(dict_data['pval_snp']))
    except ZeroDivisionError:
        elasticlog.write( "caught ZeroDivisionError pval_ref: "+ \
              dict_data['pval_ref'] + " pval snp: " + dict_data['pval_snp'])
        #Fail here. Should NOT happen!
        exit(1)
    else:
        dict_data['log_lik_rank'] = valU
    return dict_data

def cutdown(fields_to_cut, decimal_count, data_dict):
    for x in fields_to_cut:
         data_dict[x] = round(data_dict[x], decimal_count)
    return data_dict

def convert_snpid_to_numeric(dict_data):
    snpid = dict_data['snpid'].replace('rs', '')
    dict_data['snpid'] = long(snpid)
    return dict_data

def translate_alleles(dict_data):
    cipher = { 'A': 1, 'C': 2, 'G': 3, 'T': 4}
    dict_data['snpAllele'] = cipher[dict_data['snpAllele']]
    dict_data['refAllele'] = cipher[dict_data['refAllele']]
    return dict_data
 
def reduce_fields(dict_data):
    searched_fields = ['pval_rank', 'pval_ref', 'pval_snp']
    dict_data = cutdown(searched_fields, 8, dict_data)
    
    other_pvalues = ['pval_cond_snp', 'pval_diff', 'pval_cond_ref']
    dict_data = cutdown(other_pvalues, 5, dict_data)
  
    log_likelihoods = ['log_lik_rank', 'log_lik_snp', 'log_enhance_odds',
                       'log_lik_ref', 'log_lik_ratio', 'log_reduce_odds']
    dict_data = cutdown(log_likelihoods, 5, dict_data)
    return dict_data

def collapse_strand_info(dict_data):
    cipher = { '++': 1, '+-': 2, '-+': 3, '--': 4 }
    combo = ''.join([dict_data['ref_strand'], dict_data['snp_strand']])
    dict_data['ref_and_snp_strand'] = cipher[combo]
    del dict_data['ref_strand']
    del dict_data['snp_strand']
    return dict_data

def remove_fields(dict_data):
    for x in ['snp_ref_length', 'snp_location', 'ref_aug_match_seq',
              'snp_aug_match_seq', 'motif_len', 'snpAllele', 'refAllele']:
        del dict_data[x]
    return dict_data

def pick_seq_start_and_end(dict_data):
    dict_data['seq_start'] = min(\
                             int(dict_data['snp_start']),
                             int(dict_data['ref_start']))
    dict_data['seq_end'] = max(\
                           int(dict_data['snp_end']),
                           int(dict_data['ref_end']))
    for x in ['snp_start','snp_end', 'ref_start', 'ref_end']:
        del dict_data[x] 
    return dict_data

#not  using helpers.bulk; segmentation is now manual
def put_bulk_json_into_elasticsearch(es, action_list, elasticLog):
    result = None 
    summary = { 'added' : 0, 'skipped': 0, 'other': 0 }
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
                summary['skipped'] += 1
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


#Used for obtaining counts
#Needs to be verified.
#def setup_cutoffs_clause(name_of_data_frame):
#    
#    clause_parts = []
#    for pvalue in cutoffs.keys():
#        clause_part = ''.join(['(', name_of_data_frame, '$', pvalue, \
#                         '<=', str(cutoffs[pvalue]), ')' ])
#        clause_parts.append(clause_part)
#      
#    clause = ''.join(['[', '|'.join(clause_parts), ',]'])
#    print "does this make sense rt now? " + clause
#    assert False
#    return clause 
#    #original version using sqlite3:
#    #for pvalue in cutoffs.keys():
#    #    clause_parts.append(' '.join([pvalue, '<=', str(cutoffs[pvalue])]))
#    #clause += ' OR '.join(clause_parts)
#    #return clause 
#
##use cutoffs will matter for this.
#def setup_query_to_pull_records(name_of_data_frame):
#    #query = "SELECT * FROM " + sqlite_table_name 
#    query_base = name_of_data_frame 
#    if use_cutoffs:  #setup at the top of the file.
#        query += setup_cutoffs_clause(name_of_data_frame)
#        #append the where clause.
#    #query += ";"
#    print "pulling records w: " + query
#    return query 

#Used for obtaining verification counts of data that matches our cutoffs
#prior to any loading into Elasticsearch.
#def setup_count_query():
#    query = ' '.join(["SELECT COUNT(*) FROM",sqlite_table_name,\
#                     setup_cutoffs_clause(), ';'])
#    #print "counting with the following query: " + query
#    return query
     

#Maybe do some kind of in-R query here for counting? 
#Run a query against the thing?
#def get_count_matching_cutoffs(sqlite_cursor):
#    query = setup_count_query()
#    sqlite_cursor.execute(query)
#    result = sqlite_cursor.fetchone()[0]
#    #print "this many results within the cutoff: " + str(result)
#    return result


#Re-written for processing the data right out of the R file. 
def process_one_file_of_input_data(all_rows, headers, es, elasticLog, motif_ic):
    #Ensure the data looks like we expect it to.
    assert headers[13] == 'pval_rank'
    assert headers[21] == 'pval_snp' 
    assert headers[20] == 'pval_ref'
    summary = { 'added' : 0, 'skipped': 0, 'other': 0 }
    i = 0
    bulk_loading_chunk_size = 20000 
    actions = []
    for i in range(len(all_rows[1])):
        one_row = {}
        for j in range(len(all_rows)):
           key_for_data = headers[j]
           one_row[key_for_data] = all_rows[j][i]
        json_data = parse_into_json(one_row, motif_ic, elasticLog)
        actions.append(json_data)  
        if i % bulk_loading_chunk_size == 0:
            elasticLog.write( "loading batch into ES : (0th record from "\
                              + " current bulk batch):" \
                              + str(actions[0]) + "\n")
            summary_chunk = put_bulk_json_into_elasticsearch(es, actions, elasticLog)
            summary = update_summary_counts(summary_chunk, summary)
            actions = []
    #Catch the case when a file contains N % chunkSize == 0 records.
    if len(actions) > 0:
        elasticLog.write("placing the last " + str(len(actions)) + \
                         " rows from file into ES.\n")
        summary_chunk = put_bulk_json_into_elasticsearch(es, actions, elasticLog)
        summary = update_summary_counts(summary_chunk, summary)
    return summary 
    #The original logic down there:
    #while one_sqlite_record is not None:
    #    i += 1
    #    json_data =   \
    #       parse_one_sqilte_row_into_json_data(headers, one_sqlite_record, 
    #                                           motif_ic, elasticLog)
    #    # huge amount of data printed w/ the following
    #    # print repr(json_data)
    #    #print "check that this record makes sense"
    #    #Now, check that the decompress record method works.
    #    actions.append(json_data)  
    #    if i % bulk_loading_chunk_size == 0:
    #        elasticLog.write( "loading batch into ES : (0th record from "\
    #                          + " current bulk batch):" \
    #                          + str(actions[0]) + "\n")
    #        summary_chunk = put_bulk_json_into_elasticsearch(es, actions, elasticLog)
    #        summary = update_summary_counts(summary_chunk, summary)
    #        actions = []
    #    one_sqlite_record = sqlite_cursor.fetchone()

    #elasticLog.write("placing the last " + str(len(actions)) + \
    #                 " rows from file into ES.\n")
    #summary_chunk = put_bulk_json_into_elasticsearch(es, actions, elasticLog)
    #summary = update_summary_counts(summary_chunk, summary)
    #return summary 

def load_motif_ic_pkl(fname):
   with open(fname + '.pkl', 'rb') as f:
       return pickle.load(f) 


#Don't need to pass all_rows around. 
def process_data_from_one_file(all_rows, headers):
    elasticLog = open('elasticlog.txt', 'ar+')
    es = Elasticsearch(shared_pipe.CLUSTER_URLS,
                       timeout=200, 
                       dead_timeout=100)
    #Motif information content to include with each datum.
    motif_ic_pickle = load_motif_ic_pkl('ic_stats') 
    summary = \
           process_one_file_of_input_data(all_rows, headers, es, 
                                          elasticLog, motif_ic_pickle)
    rowcount = summary['added'] + summary['skipped'] + summary['other']
    elasticLog.write( "completed file:  with N= " + \
                      str(rowcount) + " rows of data.\n")
    elasticLog.close()
    return summary 
