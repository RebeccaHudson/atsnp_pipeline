

What needs to happen to every numbered directory in JASPAR.
    /afs/cs.wisc.edu/p/keles/DBSNP/JASPAR2/MERGE_SUBSET/

Example of this working properly.

1. Grabbing a directory:
   rsync -rvPh rhudson@ramiz01.stat.wisc.edu:/afs/cs.wisc.edu/p/keles/DBSNP/JASPAR2/MERGE_SUBSET/23
     

2. Processing a directory into sqlite data:
   python process_dir_of_Rdata.py 23

      The script looks for data here: /z/Comp/kelesgroup/rhudson/R-pipeline/condor_pipe_data_from_MERGE_SUBSET
      (was formerly:  /z/Comp/kelesgroup/rhudson/R-pipeline/data_from_MERGE_SUBSET/,
       if you want to skip rsyncing each of the .Rdata files for JASPAR that are already copied, 
         you can point the script at : /z/Comp/kelesgroup/rhudson/R-pipeline/data_from_MERGE_SUBSET )
      
      and now it takes 1 argument, the number of the directory to process files from (there will be 30 files per directory).
      a destination directory is created for the files loaded.
      The destination directory for those 30 files will be: /z/Comp/kelesgroup/rhudson/condor_jaspar_sqlite/sync23
   
            

 
3. Processing a direcotory of sqlite data into elasticsearch: 
   python elasticsearch_data_pipeline.py /z/Comp/kelesgroup/rhudson/condor_jaspar_sqlite/sync23 
      
      This actually ingests data into elasticsearch.
      IMORTANT NOTE: The elasticsearch ingestion script requires the elasticsearch 
                     python clinet, I've been including it out of a virtualenv in 
                     my home directory, I'm not sure where you would install it for condor.

      


