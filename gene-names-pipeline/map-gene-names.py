import re
import pickle
import json
from elasticsearch import Elasticsearch, helpers

DRY_RUN = False  
#Run without indexing into Elasticsearch, in case something looks fishy.
INDEX_NAME = 'gencode_genes_test_1'
ELASTIC_URLS = [ 'atsnp-db1', 'atsnp-db2', 'atsnp-db3']

# This works in place in very short time.
# Don't hestate to just delete the whole index and rebuild.
def get_one_bulk_action_json(json_record):
    #index was atsnp_data
    bulkj = {
    '_index': INDEX_NAME, #'gencode_genes', #'atsnp_data',
    '_type' : 'gencode_gene_symbols',
    '_source':  json_record 
    }
    return bulkj

def put_bulk_json_into_elasticsearch(es, action):
    #print "length of action : " + str(len(action))
    son = json.dumps(action)
    #index="gencode_genes",
    result = \
       helpers.bulk(es, action, index=INDEX_NAME,
                    doc_type="gencode_gene_symbols")
    return result

gene_map_file = 'correct-gencode-genes' 
with open(gene_map_file) as f:
    lines = f.readlines()

es = Elasticsearch(ELASTIC_URLS)

action = []
es_chunk_size = 150 
i = 0
#skip the header line.
for line in lines[1:]:
    split_line = line.split()
    chromosome = split_line[2]
    start_pos = split_line[4]
    end_pos = split_line[5]
    gene_symbol = split_line[12]
    #print "line ; "   debug purposes only:
    #for oneitem, k in enumerate(split_line): 
    #    print str(k), oneitem 
    j_dict = { "chr" : chromosome, 
               "start_pos" : start_pos, 
               "end_pos"   : end_pos,
               "gene_symbol" : gene_symbol
             }
    #print "jdict : " + repr(j_dict)
    assert(start_pos.isdigit())
    assert(end_pos.isdigit())
    #assert(chromosome.replace('chr', '').isdigit()) 
    action.append(j_dict)
    i = i + 1
    if i % es_chunk_size  == 0:
        print "reached  " + str(i) + " rows." 
        if not DRY_RUN:
          result = put_bulk_json_into_elasticsearch(es, action)
        action = []

print "placing the last " + str(len(action)) + " gene names into the database."
put_bulk_json_into_elasticsearch(es, action)

