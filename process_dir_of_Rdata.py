import os
import sys

base_script_cmd = 'Rscript process-Rdata-dir.R'
source_dir_parent =  '/z/Comp/kelesgroup/rhudson/R-pipeline/condor_pipe_data_from_MERGE_SUBSET'
number_dir_to_process = int(sys.argv[1])
dest_dir = '/z/Comp/kelesgroup/rhudson/condor_jaspar_sqlite/sync' + str(number_dir_to_process)

if not os.path.exists(dest_dir):
    os.makedirs(dest_dir)

i = number_dir_to_process
source_for_run = source_dir_parent + "/" + str(i) + "/" 
cmd = " ".join([base_script_cmd, source_for_run, dest_dir])
print "running " +  cmd
os.system(cmd)
print "completed processing R data for " + str(i)
