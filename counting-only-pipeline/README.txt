README:

This is not part of the primary pipeline; it does not produce ANY data
that goes into the atsnp Elasticsearch index or any other part of the 
atsnp project.

  It produces a data product for data footprint estimation/tuning.


This pipeline is specifically for obtaining counts from the dataset.
Not targeted at determinnig the whole data count as much as targeted
at determining how many records fall within certian conditions.

Currently this is configured to count how many records meet a 
specified p-value threshold. 


Could be modified and used to obtain counts for any other condition.
