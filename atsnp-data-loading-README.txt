Very Basic Instructions for (Re)Loading the atSNP Output Data into atSNP DB.


0.  Access a machine on the biostat network (*.biostat.wisc.edu) which: 
    can cd to the /z/Comp directory, and
    has condor version 8.6 installed.

    To determine the installed condor version on a machine, run the following: 

       condor_version

       $CondorVersion: 8.6.3 May 08 2017 BuildID: 404928 $
       $CondorPlatform: x86_64_RedHat7 $
       (This version requirement may turn out to be less specific, I have seen
        version 8.1 not work properly for this task) 


1. Navigate to the pipeline directory:

    cd /z/Comp/kelesgroup/atsnp/atsnp_pipeline/primary_data_pipeline



2.  Read and understand the configuration file values. Adjust as needed, 
    save any changes.
   
    less shared_pipe.py  
    vim shared_pipe.py       



3.  Run the setup script with arguments indicating which data sets (identified 
    by their associated transcription factor libraries) to prepare a run for.
 
    python setup_jobs_and_dirs.py jaspar
   
     *If the directory 'encode' or 'jaspar', is already setup, you will have to
      delete it for this script to setup the files agian. Be careful not to 
      remove a run that is supposed to continue. 



4.  Navigate to the 'encode' or 'jaspar' directory

    cd jaspar  



5.  Select one or more submit files, review them, and pass them to
    condor_submit. (I reccommend submitting them in the numbered order.)
     
    cat condor_submit01.sub condor_submit02.sub 
    condor_submit condor_submit01.sub condor_submit02.sub 



6. Monitor the progress of the data loading. 
  
      Run the collect_progress_info.py script in the primary pipeline directory.
      Specify which data set to check the status of ('encode' or 'jaspar'),
      If no data set is specified, this script will check both data sets. 
 
        python collect_progress_info.py
      OR 
        python collect_progress_info.py jaspar

      This script will indicate when processing is complete.
      If one chunk seems to be hanging behind or stuck; drop that condor job,
      (or the whole cluster) and re-launch it with condor_submit. 
       All in-progress jobs will be resumed on the same file they left off on,
       as indicated in the file chunk<N>/progress.txt.













      

   






