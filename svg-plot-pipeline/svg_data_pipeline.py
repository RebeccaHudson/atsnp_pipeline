#from elasticsearch import Elasticsearch, helpers
import json
import sys
import requests
import os
from os.path import basename
#TODO: parametrize the number of rows to do... this is currently hardcoded.

"""
A script to import sqlite3 tables into Elasticsearch
To use: give it one argument: the name of the directory to find the input files
in with no trailling space.
 then change DRY_RUN to False
"""
DRY_RUN = False 

def set_has_plot_flag_on_atsnp_output_doc(plot_info):
    atsnp_data_search_url = 'http://atsnp-db3.biostat.wisc.edu:9200/atsnp_data/atsnp_output/_search'
    q = { "query" : {
        "bool" : {
          "must" : [
                   { "match" : { "snpid"     : repr(plot_info['snpid'])     } },
                   { "match" : { "motif"     : repr(plot_info['motif'])     } },
                   { "match" : { "snpAllele" : repr(plot_info['snpAllele']) } },
                 ] 
          } } }
    results = requests.post(atsnp_data_search_url, data=json.dumps(q))
    if len(results.json()['hits']['hits']) > 0:
        id_of_match = results.json()['hits']['hits'][0]['_id'] 
        update_url = 'http://atsnp-db3.biostat.wisc.edu:9200/atsnp_data/atsnp_output/' + str(id_of_match) + '/_update'
        data_to_add = { "doc" : { "plot_available" : True } }

        if DRY_RUN is False:
            update_result = requests.post(update_url, data=json.dumps(data_to_add))
        return True
    else:
        print "did not find a matching record to update for " + str(q)
        return False

def get_plot_info_from_filename(fpath):
    nm = os.path.basename(fpath)
    no_ext = os.path.splitext(nm)[0]
    splitup = no_ext.split("_")
    return { 'motif'    : splitup[0] ,
             'snpid'    : splitup[1] ,
             'snpAllele': splitup[2]}


def get_es_url_for_plots():
    es_url = 'http://atsnp-db2.biostat.wisc.edu:9200'
    es_index = '/atsnp_data'
    es_type = '/svg_plots'
    update_endpoint = "/_update" 
    return es_url + es_index + es_type
    

def process_one_file_of_input_data(path_to_file):
    #file should be an svg.
    plot_info = get_plot_info_from_filename(path_to_file)
    
    if set_has_plot_flag_on_atsnp_output_doc(plot_info):
        print "found a match for " + str(plot_info)
        f = open(path_to_file, 'r')
        svg_data = f.read()
        #use the file name to grab enough data to add to the plotting data for querying.
        plotting_data = { 'snpid'      : plot_info['snpid'], 
                          'motif'      : plot_info['motif'], 
                          'snp_allele' : plot_info['snpAllele'],
                          'svg_plot'   : svg_data }
        plotting_json = json.dumps(plotting_data)
        #print "plotting json to put in " + plotting_json
        url_for_plot = get_es_url_for_plots()
        if DRY_RUN is False:
            requests.post(url_for_plot, data=plotting_json)
    else:
        print "did not find a match, skipping... "

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
i = 0
for one_file in os.listdir(input_path):
    fpath = input_path + "/" + one_file
    print "processing file : "  + fpath
    process_one_file_of_input_data(fpath)
    i += 1

