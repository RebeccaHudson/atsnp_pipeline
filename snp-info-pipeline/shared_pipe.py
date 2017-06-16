#shared_pipe.py
#stuff shared by all the pipeline scripts.
def init():
    global SETTINGS, PROGRESS_STATES, SNP_INFO_PARENT_DIR, DRY_RUN

    DRY_RUN = False    #If trye, don't put data into Elasticsearch.

    #These settings can be left alone. Tuned pretty well for SNP-INFO.
    SETTINGS = {
         'chunk_count': 10,
         'n_submit_files' : 2, #number of separate condor submit files.
         'index_name' : 'snp_info_test_1'
    }  

    #Should match the definition in the per-job scripts;
    #they could eventually import these settings to DRY up the code.
    PROGRESS_STATES = { 'NOT_STARTED': 0, 
                        'IN_PROGRESS': 1,
                        'COMPLETE'   : 2  
                       } 

    SNP_INFO_PARENT_DIR = '/z/Comp/kelesgroup/atsnp/SNP_INFO'
