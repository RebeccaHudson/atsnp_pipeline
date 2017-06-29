How to Load Motif Plotting Data into Elasticsearch:

1. Ensure you are on a machine that can talk to Elasticsearch.
   curl -XGET atsnp-db1:9200/_cluster/health


2. Open the script setup-motif-data.py and ensure that DRY_RUN (near the top) is False.
   (Unless you want to test the script without putting any data into Elasticsearch.)
 

3. Run the script /s/bin/python setup-motif-data.py
   This will produce some simple output.
   It should not take a very long time.


4. Check that the plotting data is now in place.
   Count should show 2270 motifs.

   curl -XGET http://atsnp-db2:9200/motif_plotting_data/_count?pretty



