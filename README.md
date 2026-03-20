The PySpark code contains 
1. Implementation of SCD2 for Customer Dimension. 
Implemented the SCD logic using : 
   1. PySpark (Functions/API's)
   2. PySpark (SQL syntax)

The data is stored data/snapshots directory.
Load jan24 as initial load and then load feb24 & mar24 snapshots to achive historical record storage / SCD2.
This is initial version, plan to make more changes in future.

2. Implementation of Star Schema. 
A sample implementation for fact/dimension model where order facts is joined with customer,
product and date dimensions. Refer /data/sample_dat/*dim.csv for sample dimension data. 

3. Basic DQ Framework. Sample DQ rules and Data file in sample_data folder. 

4. Full Fledged GCP-Bigquery archival solution using Python + SQL. This can be used in production with small tweaks.

