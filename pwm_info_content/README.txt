How to setup new motif information content:

0. Evaluate if you can do it at this time.
   In order to use our current system to put new information content values
   onto motifs, you will have to re-create the atsnp_data (or atsnp_data_test)
   Elasticsearch index.

   If you are not rebuilding the index, you will not be able to add the new
   information content until you do.
   
   If you are about to load data into the atsnp_data index from a fresh,
   empty index, then you can proceed.


1. Replace/update the text file 'ic_stat.txt'. 
   This file must be formatted such that the first value on each line 
   is the motif, and the last value on each line is the desired information
   content label.
   This file is the only input for processing information content.


2. Run the python script to produce the pickle with the information content:
   python handle-pwm-ic.py


3. Copy the resulting pickle file, ic_stat.txt into the 
   primary-data-pipeline directory.
   cp ic_stat.txt ../primary-data-pipeline

4. Run the primary data pipeline according to the instructions 
   in that directory.


5. The new information content will be added alongside the atSNP data in
   Elasticsearch.
