#shared_pipe.py
#stuff shared by all the pipeline scripts.
def init():
   #If more globals are added, they must appear in this list or they 
   #will not be accessible outside of this file.
   global SETTINGS, PROGRESS_STATES, PARENT_DIR, PARENT_DIRS

   #More realistic for our datasets.
   #SETTINGS = {
   #     'chunk_count': 100,
   #     'n_submit_files' : 5 #number of separate condor submit files.
   #}  


   #Numeric settings tailored for test data sets.
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
 
   #PARENT_DIR = '/z/Comp/kelesgroup/atsnp/JASPAR/BIGTABLES'
   PARENT_DIR = '/z/Comp/kelesgroup/atsnp/JASPAR/BIGTABLES/204' #testing set.


   #What we ultimately want.
   #PARENT_DIRS = { 'ENCODE' : '/z/Comp/kelesgroup/atsnp/ENCODE/TABLES',
   #                'JASPAR' : '/z/Comp/kelesgroup/atsnp/JASPAR/TABLES'
   #              }


   #TABLES OR BIGTABLES
   set_of_data = 'BIGTABLES'
   data_root = '/z/Comp/kelesgroup/atsnp'
   PARENT_DIRS = {'encode':'/'.join([data_root,'ENCODE',set_of_data,'593']),
                  'jaspar':'/'.join([data_root,'JASPAR',set_of_data,'151'])
                 }

   #Vision for this: 
   #setup_jobs_and_dirs encode
     #move everything needed into the 'encode' directory.
   #setup the jobs and directories to run in there. 
   #condor sumibt files get put in primary_pipeline/encode
   #

   
#Add another transcription factor library down here too. 
class WhichTFLibs:
    #If msg_for_no_args is None or left off, then there is 
    #no error message displayed and it is assumed both data sets
    #are being queried.
    valid_args = ['encode', 'jaspar']

    def __init__(self, cmd_args, msg_for_no_args=None):
        self.run_these = self.which_datasets(cmd_args, msg_for_no_args) 

    def which_datasets(self, cmd_args, msg_for_no_args):
        run_these = []
        #print "sys.argv  " + repr(cmd_args)
        #print "len(sys.argv)" +  str(len(cmd_args))
        if len(cmd_args) >= 2 and len(cmd_args) <= 3:
           sets_to_run = cmd_args[1:]
           while len(sets_to_run) > 0:
               g = sets_to_run.pop()
               if str.lower(g) not in self.valid_args: 
                   msg = "The only valid arguments are 'encode' and/or 'jaspar'\
                       (case insensitive). \n Enter 'jaspar' or 'encode' or both."
                   print msg
                   exit(1)
               else:
                   run_these.append(str.lower(g))
        if len(run_these) == 0:
            if msg_for_no_args is not None:
                print msg_for_no_args
                exit(1)
            run_these = self.valid_args 
        return run_these




