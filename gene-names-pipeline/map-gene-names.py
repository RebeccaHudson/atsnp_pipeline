import re
import pickle
import json
from elasticsearch import Elasticsearch, helpers

# This works in place in very short time.
# Don't hestate to just delete the whole index and rebuild.
def get_one_bulk_action_json(json_record):
    bulkj = {
    '_index': 'atsnp_data',
    '_type' : 'gene_names',
    '_source':  json_record 
    }
    return bulkj

def put_bulk_json_into_elasticsearch(es, action):
    print "length of action : " + str(len(action))
    son = json.dumps(action)
    result = helpers.bulk(es, action, index="atsnp_data", doc_type="gene_names")
    return result

gene_map_file = 'geneTrack.refSeq.complete' 
with open(gene_map_file) as f:
    lines = f.readlines()

es = Elasticsearch('atsnp-db2')

action = []
es_chunk_size = 150 
i = 0
for line in lines:
    split_line = line.split()
    chromosome = split_line[0]
    start_pos = split_line[1]
    end_pos = split_line[2]
    gene_name = split_line[3]
      
    j_dict = { "chr" : chromosome, 
               "start_pos" : start_pos, 
               "end_pos"   : end_pos,
               "gene_name" : gene_name
             }
    action.append(j_dict)
    i = i + 1
    if i % es_chunk_size  == 0:
        print "reached  " + str(i) + " rows." 
        result = put_bulk_json_into_elasticsearch(es, action)
        action = []

print "placing the last " + str(len(action)) + " gene names into the database."
put_bulk_json_into_elasticsearch(es, action)

