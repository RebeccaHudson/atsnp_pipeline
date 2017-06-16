How to run the gene_names pipeline:
 
    1. check the top of the file 'map-gene-names.py'
       check DRY_RUN (should be False)
       check INDEX_NAME,
         (If this is for real, make sure it's not _test_1)
    
    2. Ensure that the target index exists:
       (replace <index_name> with the name of the index from step 1.
         curl -XGET http://atsnp-db2:9200/_cat/indices?v | grep <index_name>

        !! ***If the previous command displayed no output, you must setup
           the indices and mappings, as described in:
              atsnp_pipeline/data-loading-README.txt
 
    3. Check if the pipeline has already run/what the counts are.
       curl -XGET http://atsnp-db2:9200/gencode_genes_test_1/_count?pretty
 
    4. Run the pipeline (not a condor job, runs directly in the terminal)
       /s/bin/python map_gene_names.py
       Watch the output go by (should complete in just a few minutes)
 
    5. Check the count of the records, in another terminal, and when done.
       curl -XGET http://atsnp-db2:9200/gencode_genes_test_1/_count?pretty
     (to view one gene:  
     curl -XGET http://atsnp-db2:9200/gencode_genes_test_1/_search?size=1)

