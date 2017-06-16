This README is for loading the atSNP output data. 
If you have not reviewed the file data-loading-README.txt in the 
parent directory (atsnp_pipeline), you should do that now.

This is a large dataset (~20tB); expect it to take time to load.


Primary @SNP data:
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

1. Navigate to the primary data pipeline directory:
    cd /z/Comp/kelesgroup/atsnp/atsnp_pipeline/primary-data-pipeline

2. Setup your terminal so the correct user group and permissions are active.
   ./setup_shell.sh

3.  Read and understand the configuration file values. 
    Adjust as needed, save any changes:
   
    less shared_pipe.py 
    look for 
          DRY_RUN :  if True, no data will be loaded. 
          SETTINGS['index_name'] : atsnp_data 
                                   (or atsnp_data_test_1, for testing)

4.  Ensure that the target index exists:
    (replace <index_name> with the name of the index from step 3.  
      curl -XGET http://atsnp-db2:9200/_cat/indices?v | grep <index_name>

    !! ***If the previous command displayed no output, you must setup 
    the indices and mappings, as described in:
            atsnp_pipeline/data-loading-README.txt
 
    Ensure that mappings are setup:
      curl -XGET http://atsnp-db2:9200/<index_name>/_mappings 


5.  Run the setup script with arguments indicating which data sets (identified 
    by their associated transcription factor libraries) to prepare a run for.
 
    python setup_jobs_and_dirs.py jaspar
   
     *If the directory 'encode' or 'jaspar', is already setup, you will have to
      delete it for this script to setup the files agian. Be careful not to 
      remove a run that is supposed to continue. 


6.  Navigate to the 'encode' or 'jaspar' directory

    cd jaspar  

7.  Select one or more submit files, review them, and pass them to
    condor_submit. (I reccommend submitting them in the numbered order.)
     
    cat condor_submit01.sub condor_submit02.sub 
    condor_submit condor_submit01.sub
    

7. Monitor the progress of the data loading. 

  A.  
     Run the collect_progress_info.py script in the primary pipeline directory.
     Specify which data set to check the status of ('encode' or 'jaspar'),
     If no data set is specified, this script will check both data sets. 

       python collect_progress_info.py
     OR 
       python collect_progress_info.py jaspar

     This script will indicate when processing is complete.
     Check output from this script in primary-data-pipeline/progress.txt
     If one chunk seems to be hanging behind or stuck; drop that condor job,
     (or the whole cluster) and re-launch it with condor_submit. 
      All in-progress jobs will be resumed on the same file they left off on,
      as indicated in the file chunk<N>/progress.txt.


  B. 
     Get a count of the number of Elasticsearch 'documents' 
     (individual data rows) have been added to the index.

     curl -XGET http://atsnp-db2:9200/<index_name>/_count?pretty

     
       







