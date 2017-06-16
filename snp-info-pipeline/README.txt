 How to run the SNP info pipeline:

 0.  Access a machine on the biostat network (*.biostat.wisc.edu) which:
     can cd to the /z/Comp directory, and
     has condor version 8.6 installed.
 
     To determine the installed condor version on a machine, run the following:
 
        condor_version
 
        $CondorVersion: 8.6.3 May 08 2017 BuildID: 404928 $
        $CondorPlatform: x86_64_RedHat7 $
        (This version requirement may turn out to be less specific, I have seen
         version 8.1 not work properly for this task)
 
1. You should be in the directory, snp_info_pipeline.
   cd /z/Comp/kelesgroup/atsnp/atsnp_pipeline/snp-info-pipeline 
   pwd    #print your working directory
    
 
2. Open and inspect shared_pipe.py
        DRY_RUN : 
          if True, no data will be loaded into Elasticsearch.

        SETTINGS['index_name'] : 'snp_info' or 'snp_info_test_1'
            Should end in _test_1 if you are testing.

     (For this data set, I believe that 'chunk_size' and 'n_submit_files'
      are already at good settings).

3. Ensure that the target index exists:
  (replace <index_name> with the name of the index from step 3.
     curl -XGET http://atsnp-db2:9200/_cat/indices?v | grep <index_name>

    !! ***If the previous command displayed no output, you must setup 
    the indices and mappings, as described in:
       atsnp_pipeline/data-loading-README.txt

   Ensure that mappings are setup:
     curl -XGET http://atsnp-db2:9200/<index_name>/_mappings 
 
4. Run the setup_shell script to set correct perimssions 
   ./setup_shell.sh
 
5. Run the setup script to setup the data for loading. 
   python setup_load_for_snp_info.py

6. Launch one of the condor jobs.
   condor_submit condor-submit00.sub
 
7. Check progress with 'condor_q' and/or the provided python script:
   python collect_progress_info.py


