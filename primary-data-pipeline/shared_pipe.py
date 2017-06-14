#shared_pipe.py
#stuff shared by all the pipeline scripts.
def init():
   #If more globals are added, they must appear in this list or they 
   #will not be accessible outside of this file.
   global SETTINGS, PROGRESS_STATES, PARENT_DIR, PARENT_DIRS, \
          RESTRICT_BY_PVALUE,  PVALUE_CUTOFFS 

   #More realistic for our datasets.
   #SETTINGS = {
   #     'chunk_count': 100,
   #     'n_submit_files' : 5 #number of separate condor submit files.
   #}  


   #Numeric settings tailored for test data sets.
   SETTINGS = {
        'chunk_count': 2, #10, 
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


   #TABLES OR BIGTABLES
   set_of_data = 'BIGTABLES' #BIGTABLES is needed (vs. TABLES).
   data_root = '/z/Comp/kelesgroup/atsnp'
   #data_root = '/z/Comp/kelesgroup/rhudson/tiny-test'  #MEGA test mode.
   PARENT_DIRS = {'encode':'/'.join([data_root,'ENCODE',set_of_data,'593']),
                  'jaspar':'/'.join([data_root,'JASPAR',set_of_data,'151'])
                 }

   #to get out of SUPER test mode, remove this line
   PARENT_DIRS['encode'] = '/z/Comp/kelesgroup/rhudson/tiny-test/JASPAR'
 

   #If this is true; only records with pvalues <= to all of the  
   #cutoffs below will loaded.
   RESTRICT_BY_PVALUE = True
   #Used to restrict which records are loaded.
   PVALUE_CUTOFFS = { 'pval_rank': 0.3, #0.05,
                      'pval_snp' : 0.3, #0.05,
                      'pval_ref' : 0.3  #0.05
                     }
  
    



#assumes error (red)
def print_with_color(msg, why='error'):
    end_color = "\033[1;m"
    start_color = "\033[1;33m" #default to yellow if reason is unspecified.
    if why == 'error':    
        print "STOP"
        #print "\033[1;41mSTOP" + end_color #\033[1;m"
        start_color = "\033[1;31m"
    elif why == 'success': 
        start_color = "\033[1;32m" #green    
    elif why == 'warn':
        #print "\033[1;43mWARN" + end_color
        print "WARN" 
        pass #go with the yellow. 
    print ''.join([start_color, msg, end_color])
    
   
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
        if len(cmd_args) >= 2 and len(cmd_args) <= 3:
           sets_to_run = cmd_args[1:]
           while len(sets_to_run) > 0:
               g = sets_to_run.pop()
               if str.lower(g) not in self.valid_args: 
                   msg = "The only valid arguments are 'encode' and/or "+\
                         "'jaspar' (case insensitive). \n Enter 'jaspar' or"+\
                         " 'encode' or both."
                   #print msg
                   print_with_color(msg)
                   exit(1)
               else:
                   run_these.append(str.lower(g))
        if len(run_these) == 0:
            if msg_for_no_args is not None:
                print_with_color(msg_for_no_args)
                exit(1)
            run_these = self.valid_args 
        return run_these


