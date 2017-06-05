#shared_pipe.py
#stuff shared by all counting pipeline scripts.
#(it's the 'counting pipeline' because we don't do anything but count)

def init():
    global SETTINGS, PROGRESS_STATES, NAMED_CUTOFFS
    SETTINGS = {
         'chunk_count': 100,
         'n_submit_files' : 4 #number of separate condor submit files.
    }  

    #Should match the definition in the per-job scripts;
    #they could eventually import these settings to DRY up the code.
    PROGRESS_STATES = { 'NOT_STARTED': 0, 
                        'IN_PROGRESS': 1,
                        'COMPLETE'   : 2  
                       } 
   
    #If the value is None; it will not be taken into account
    #pvalue cutoffs are ALWAYS less than, (at least here)
    #They appear in the following order: pvalue_rank, pvalue_ref, pvalue_snp    
    #If any specified thresholds are met, then the whole record 
    #will be added to that named count

    #use our default cutoff; take them all into account
    #NAMED_CUTOFFS = { 'WREN'       : [ 0.05,  0.05, 0.05 ],      
    #                  'FINCH'      : [ 0.1 ,  0.1,  0.1  ], 
    #                  'ROBIN'      : [ 0.2 ,  0.2,  0.2  ], 
    #                  'WOODPECKER' : [ None,  0.1,  0.1  ],
    #                  'CROW'       : [ 0.1 ,  None, None ],
    #                  'FALCON'     : [ 0.2 ,  0.2,  0.2  ],
    #                  'HAWK'       : [ 0.3 ,  0.3,  0.3  ], 
    #                  'HERON'      : [ 0.5 ,  0.5,  0.5  ], 
    #                  'CRANE'      : [ 0.5 ,  None, None ] 
    #                }
    NAMED_CUTOFFS = { 'SWALLOW'  : 0.03, 
                      'RAVEN'    : 0.05 }
