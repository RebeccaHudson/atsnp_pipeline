import re
import pickle
import json
from elasticsearch import Elasticsearch, helpers

# This works in place in a very short time.
# Don't hestate to just delete the whole index and rebuild.

DRY_RUN = False  
#Enables a run without indexing into Elasticsearch, in case something looks fishy.
CLUSTER_URLS = ['db05']
#old cluster: CLUSTER_URLS = ['atsnp-db1','atsnp-db2','atsnp-db3']

#INDEX_NAME = 'motif_plotting_data_test_1'
INDEX_NAME = 'motif_plotting_data'
DOC_TYPE = 'motif_bits'

motif_data_files = ['JASPARmotifs.json', 'ENCODEmotifs.json']


def setup_motif_meta(motif_name):
    meta = {}
    meta['create'] = {}
    meta['create']['_id'] = motif_name.replace('.', '_')
    meta['create']['_type'] = DOC_TYPE
    meta['create']['_index'] = INDEX_NAME
    return meta

def apply_es_bulk_format(doc):
    as_str = [ json.dumps(x) for x in doc ] 
    as_str.append('') #should end w/ a newline.
    for_bulk = '\n'.join(as_str)
    return for_bulk

def parse_one_motif(one_motif):
    forward = one_motif['forward']
    reverse = one_motif['reverse']
    motif_nm = one_motif['motif'].replace('.', '_')
    motif_data = {'plotting_bits': 
                 { 'forward': forward , 
                   'reverse': reverse  }
                 }
    return {'motif_data' : motif_data,  
            'id': motif_nm   }

motif_data = None

def load_motifs_from_one_file(motif_data):
   motifs = motif_data['list']
   
   for one_motif in motifs:
       j_dict = parse_one_motif(one_motif) 
       print "placing motif " + j_dict['id'] + " into Elasticsearch."
       es.index(index=INDEX_NAME, doc_type=DOC_TYPE, 
                id=j_dict['id'], body=j_dict['motif_data'])
   
es = Elasticsearch(CLUSTER_URLS)

for file_name in motif_data_files:
    with open(file_name) as f:
        motif_data = json.load(f)
        load_motifs_from_one_file(motif_data)

