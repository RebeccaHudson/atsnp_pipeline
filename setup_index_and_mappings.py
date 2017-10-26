import requests
import json
#url for old cluster: URL_BASE = "http://atsnp-db2:9200"

#url for new cluster.
URL_BASE = "http://db05:9200"



TEST_MODE = False 
#False removes '_test_1' from the names of created indices.

TEST_NUMBER = 1

INDEX_NAMES = { 'GENE_NAMES' : 'gencode_genes',
                'ATSNP_DATA' : 'atsnp_data',
                'SNP_INFO'   : 'snp_info', 
                'MOTIF_DATA' : 'motif_plotting_data'
              }

def setup_index_name(which_data):
    nm = INDEX_NAMES[which_data]  
    if TEST_MODE: 
        nm = '_'.join([nm, 'test', str(TEST_NUMBER)])
    #print "using index name: " + nm
    return nm

def setup_an_index(index_name, mapping_data, data_type, settings=None): 
    url = '/'.join([URL_BASE, index_name])
    r = None
    if settings is None:
        r = requests.put(url)
    else:
        r = requests.put(url, data = json.dumps(settings))
    #print "r.status " +  str(r.status_code )
    if r.status_code != 200:
        print "Failed to create index. " + index_name + \
              "\nBe sure the index does not already exist."    
        print "Error: " + r.text
        return           
        
    url = '/'.join([URL_BASE, index_name,"_mapping", data_type])
    r = requests.put(url, data = json.dumps(mapping_data))
    #print "r.text " + r.text
    print " ".join(["setup index", index_name, "data type:", data_type])

def setup_gene_names_index():
    index_name = setup_index_name('GENE_NAMES')
    mapping_data = {"properties":
                         {"chr"        :{"type":"text"},
                          "end_pos"    :{"type":"integer"},
                          "gene_symbol":{"type":"keyword"},
                          "start_pos"  :{"type":"integer"}
                         }
                   }
    setup_an_index(index_name, mapping_data, 'gencode_gene_symbols')
    
def setup_snp_info_index():
    index_name = setup_index_name('SNP_INFO')
    mapping_data = {"properties":
      {"create":
       {"properties":
        {"_id"            :
            {"type":"text",
             "fields":{"keyword":{"type":"keyword"}}
            }
        }
       },
     "ref_base"       :{"type":"keyword","index":False,"doc_values":False},
     "sequence_matrix":{"type":"keyword","index":False,"doc_values":False},
     "snp_base"       :{"type":"keyword","index":False,"doc_values":False}
    }
    }
    setup_an_index(index_name, mapping_data, 'sequence')

def setup_atsnp_data_index():
    index_name = setup_index_name('ATSNP_DATA')
    mapping_data =\
      {"properties":
        {
        "chr"   : 
          {"type":"byte"},
        "create": 
           {"properties":
             {"_id": 
                {"type":"text","fields":
                  { "text": {"type":"text"} }
                }
             }
           },
        "log_enhance_odds":
          {"type"          : "scaled_float",
           "index"         : False,
           "doc_values"    : False,
           "scaling_factor": 1000.0},
        "log_lik_rank":
          {"type"          : "scaled_float",
           "index"         : False,
           "doc_values"    : False,
           "scaling_factor": 1000.0},
        "log_lik_ratio":
          {"type"          : "scaled_float",
           "index"         : False, 
           "doc_values"    : False, 
           "scaling_factor": 1000.0},
        "log_lik_ref":
          {"type"          : "scaled_float",
           "index"         : False,
           "doc_values"    : False,
           "scaling_factor": 1000.0},
        "log_lik_snp":
          {"type"          : "scaled_float",
           "index"         : False,
           "doc_values"    : False,
           "scaling_factor": 1000.0},
        "log_reduce_odds":
          {"type"          : "scaled_float",
           "index"         : False,
           "doc_values"    : False,
           "scaling_factor": 1000.0},
        "motif": 
          {"type":"keyword"},
        "motif_ic":
          {"type":"byte"},
        "pos":
          {"type":"integer"},
        "pval_cond_ref":
          {"type"          : "scaled_float",
           "index"         : False,
           "doc_values"    : False, 
           "scaling_factor": 100000.0},
        "pval_cond_snp":
          {"type"          : "scaled_float",
           "index"         : False, 
           "doc_values"    : False, 
           "scaling_factor": 100000.0},
        "pval_diff":
          {"type"          : "scaled_float",
           "index"         : False,
           "doc_values"    : False, 
           "scaling_factor": 100000.0},
        "pval_rank":
          {"type"           : "scaled_float",
            "scaling_factor": 1.0E8},
        "pval_ref":
          {"type"           : "scaled_float",
           "scaling_factor" : 1.0E8},
        "pval_snp":
          {"type"           : "scaled_float",
           "scaling_factor" : 1.0E8},
        "ref_and_snp_strand":
          {"type"       : "byte",
           "index"      : False,
           "doc_values" : False},
        "ref_extra_pwm_off":
          {"type"       : "byte", 
           "index"      : False,
           "doc_values" : False},
        "seq_end":
          {"type"       : "byte",
           "index"      : False,
           "doc_values" : False},
        "seq_start":
          {"type"       : "byte",
           "index"      : False,
           "doc_values" : False},
        "snp_extra_pwm_off":
          {"type"       : "byte",
           "index"      : False,
           "doc_values" : False},
        "snpid":
          {"type":"long"}
       }}

    #The default shard and replica count is OK for all others.
    settings =  \
       {"settings" : {
           "number_of_shards" : 50,
           "number_of_replicas" : 2
          } 
       }
    setup_an_index(index_name, mapping_data, 'atsnp_output', settings)



def setup_motif_data_index():
    index_name = setup_index_name('MOTIF_DATA')
    mapping_data =\
      {"properties":
        {
        "plotting_bits"   : #can be unjson'd for 'forward' and 'reverse'
          {"type":"object",
           "enabled" : False,
          },
        "create": 
           {"properties":
             {"_id": 
                {"type":"text","fields":
                  { "text": {"type":"text"} }
                }
             }
           },
       }}
    setup_an_index(index_name, mapping_data, 'motif_bits')

setup_gene_names_index()
setup_snp_info_index()
setup_atsnp_data_index()
setup_motif_data_index()

print "Done setting up test indices"

# What to do to get rid of one of the indexes created here?
# curl -XDELETE atsnp-db2:9200/<index name>  
#example :
#  curl -XDELETE atsnp-db2:9200/gencode_genes_test_1
