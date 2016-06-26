README for ATSNP Search Data Pipeline Scripts

Introduction: 
    Congratulations on opening the README! 
    This document contains details about how to put data into the ATSNP.biostat.wisc.edu 
    application.  Feel free to append notes to this document.


Table of Contents:
    Part 1: (Re)Building/expanding the ElasticSearch indices:
        A. Primary pipeline from R data to elasticsearch.
        B. Pipeline for gene names.
        C. Pipeline for plots.

    Part 2: External data that does NOT involve the ElasticSearch index
            (Unlikely you'll have to use this, see D. for more information.)
        D. Motif/TF lookup tables  (probably you don't need this) 




Part 1: 

    A. PRIMARY pipeline from R data files to elasticsearch:

        What needs to happen to every numbered directory in JASPAR.
            /afs/cs.wisc.edu/p/keles/DBSNP/JASPAR2/MERGE_SUBSET/

        Example of this working properly.

        1. Grabbing a directory:
          rsync -rvPh 
             rhudson@ramiz01.stat.wisc.edu:/afs/cs.wisc.edu/p/keles/DBSNP/JASPAR2/MERGE_SUBSET/23
             

        2. Processing a directory into sqlite data:
           python process_dir_of_Rdata.py 23

             The script looks for data here: 
              /z/Comp/kelesgroup/rhudson/R-pipeline/condor_pipe_data_from_MERGE_SUBSET
              (was formerly:  /z/Comp/kelesgroup/rhudson/R-pipeline/data_from_MERGE_SUBSET/,
               if you want to skip rsyncing each of the .Rdata files for JASPAR that are 
               already copied, you can point the script at : 
               /z/Comp/kelesgroup/rhudson/R-pipeline/data_from_MERGE_SUBSET )

              The script now takes 1 argument, the number of the directory to process files from.
              There will be 30 files per directory; 
              a destination directory is created for the files loaded.
              The destination directory for those 30 files will be: 
                  /z/Comp/kelesgroup/rhudson/condor_jaspar_sqlite/sync23

         
        3. Processing a direcotory of sqlite data into elasticsearch: 
           python elasticsearch_data_pipeline.py 
                        /z/Comp/kelesgroup/rhudson/condor_jaspar_sqlite/sync23 
              
              This actually ingests data into elasticsearch.
              IMORTANT NOTE: The elasticsearch ingestion script requires the elasticsearch 
                          python clinet, I've been including it out of a virtualenv in 
                          my home directory, I'm not sure where you would install it for condor.



    B.   Pipeline for gene names.
 
         This is only needed one time if/whenever the whole elasticsearch index is rebuilt.
         Find the needed script in the directory: gene-names-pipeline 
         As long as the data file 'geneTrack.refSeq.complete' is in the same directory you
         run the script out of (it's also in gene-names-pipeline).

         using it: python map-gene-names.py




    C.   Pipeline for plots:

         There are only a few (~1000) plots so far, this script does not use the elasticsearch
         bulk helpers because the SVG plots are pretty large, actually. 
         Expect this script will run especially slowly 
       
         Find the script in svg-plot-pipeline, it takes 1 argument; the path to the 
         directory full of plots. (Example directory shown.)

         Run it like this: 
             python svg_data_pipeline.py /z/Comp/kelesgroup/rhudson/figures/MA0002.2_1 

              NOTE: The naming convention for plots is <MOTIF_SNPID_REFALLELE>.
              The svg_data_pipeline script requires this convention be followed in order
              to index the plot data correctly.
  




Part 2: External data that does NOT involve the ElasticSearch index

    D.  Motif - Transcription factor lookup tables. 
        (Does not interact with Elasticsearch!, no need to run this if rebuilding the ES index) 
       
        Motif lookup tables; this should not be needed at any time unless new motifs need to 
        be added/ motif/transcription factor arguments should be updated.
        The directory motif-lookups conatins the input data: pfm_vertebrates_motif-lookups.txt 
        The script takes no arguments; the input file is hardcoded into the script.
        
           run it like this: python motif-lut.py
         
        It produces Python 'pickle' files for looking up motifs by transcption factor and   
        for looking up which transcription factor a motif value is associated with. 
        These files belong in ss_search_viewer/ss_viewer/lookup-tables

     

