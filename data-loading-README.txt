Data Loading README:

This README covers the overall process for loading all needed data into the 
Elasticsearch cluser for atSNP DB.

Setting up Indices and Mappings:

0. Before loading data, ensure that the Elasticsearch indices and mappings 
   are in place.
   Ensure you are on a machine where you can reach the Elasticsearch cluster:
     curl -XGET atsnp-db1:9200/_cluster/health

    If this gives you a response, continue. 
     If not, you must get onto a machine in the biostat network.
 
1. Navigate to the pipeline directory:
     cd /z/Comp/kelesgroup/atsnp/atsnp_pipeline/

2. Check the top of the script, 'setup_index_and_mappings.py' . 
   If TEST_MODE is true, all of the indices
   created will have '_test_1' added to the end of their names.

3. Check to see if the indices are already in place:
    curl -XGET http://atsnp-db2:9200/_cat/indices?v

4. Run the script: 
   /s/bin/python setup_index_and_mappings.py
  
   If "index_already_exists" exceptions are reported, no action will
   be taken, as the indices this error occurs for already exist.
   Delete the indices if nessecary and try agian. 

   
To Load data into the Elasticsearch Cluster:

   0. Ensure that:
         a. You are on a machine that can communicate with the   
            Elasticsearch cluster (step 0 above)      

         b. That the required Elasticsearch Indices Mappings are ready,
            as described above.


   1. Navigate to the atsnp_pipeline directory.
         cd /z/Comp/kelesgroup/atsnp/atsnp_pipeline/


   2. Select the set of data to load, navigate to that directory.
      Proceed from the instructions in the README.txt file in each directory.
     If no data is loaded yet, I reccomend loading in the following order:
         (this order goes from smallest data set to largest)
         
         a. Load gencode gene names: 
               cd gene-names-pipeline
               refer to README.txt in this directory for instructions.

         b. Load the motif plotting information.
               cd motif-plotting-pipeline
               refer to README.txt in this directory for instructions.
 
         b. Load the SNP info (sequence data):
               (this may take up to a few days)
               cd snp-info-pipeline
               refer to README.txt in this directory for instructions.
 
         c. Load the (primary) atSNP data:
               (this data set will take a relatively long time)
               cd primary-data-pipeline
               refer to README.txt in this directory for instructions.
     

*********  NOTE *********
   See the file 'monitoring-progress.txt' in the directory atsnp_docs/data-pipeline
   for information about how to monitor progress of (b) and (c). 



