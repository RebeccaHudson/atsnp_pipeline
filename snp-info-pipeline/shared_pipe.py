#shared_pipe.py
#stuff shared by all the pipeline scripts.
def init():
    global SETTINGS, PROGRESS_STATES
    SETTINGS = {
         'chunk_count': 10,
         'n_submit_files' : 2 #number of separate condor submit files.
    }  

    #Should match the definition in the per-job scripts;
    #they could eventually import these settings to DRY up the code.
    PROGRESS_STATES = { 'NOT_STARTED': 0, 
                        'IN_PROGRESS': 1,
                        'COMPLETE'   : 2  
                       } 
