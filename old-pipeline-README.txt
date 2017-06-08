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

    A. PRIMARY Data Pipeline from R data files to Elasticsearch:

        Each numbered directory for JASPAR and ENCODE data must be processed. 
        These directories are located at the following locations:
          /z/Comp/kelesgroup/atsnp/ENCODE/TABLES  and 
          /z/Comp/kelesgroup/atsnp/JASPAR/TABLES 

        To use the pipeline scripts to ingest one of these; 
           go into primary-data-pipeline/
           
          1.   Edit the path at the top of the file 'setup_jobs_and_dirs.py'
               to be either the path for ENCODE or the path for JASPAR data.
        
          2.   Edit the settings in shared_pipe.py to adjust 
                 * The number of 'chunks' to divide
                   The work into (each 'chunk' will be worked on by one condor node);
                 * The number of condor_submit files to ultimately submit.
                   (chunks/number of submit files = 
                    number of jobs running per condor cluster)

          2.   Make sure the working directory is clear of any chunk<N> directories or 
               already-existing condor_submit files.
        
          3.   Run the script to setup_jobs_and_dirs: /s/bin/python setup_jobs_and_dirs.py
 
          4.   If all of this has worked correctly, you should be able to submit 
               the jobs to condor.     
               For each condor submit file (condor-submit-N.sub) submit it to condor: 
                 condor_submit condor-submit-N.sub   
                             
          4.  (CONDOR NOTE!  ):
               (What about the condor version problems?)
               Before sumbitting; check the condor version on the machine you are using.
               use the program 'condor_version'
               Look for a condor_version of 8.6 or greater to process successfully.                 
               
 

    B.   Pipeline for gene names.
 
         This is only needed one time if/whenever the whole elasticsearch index is rebuilt.
         Find the needed script in the directory: gene-names-pipeline 
         As long as the data file 'correct-gencode-genes' is in the same directory you
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

     

