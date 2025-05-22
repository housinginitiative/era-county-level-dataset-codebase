# Data cleaning and aggregation

This directory contains R scripts which together form a pipeline from data ingestion to aggregation.

## Index

- `0_grantee_metadata`: Prepare grantee information such as IDs and geographies. *Runtime: ~1 min 28 sec*
- `1_geocode`: Check that HUD-supplied geocodes join cleanly to payment files, and generate geocodes for any records without successful HUD geocodes. *Runtime: ~10 min 59 sec*
- `2_initial_validation`: Perform basic data cleaning and type normalization. *Runtime: ~10 min 19 sec*
- `3_deduplication`: Deduplicate duplicate records. *Runtime: ~85 min 37 sec*
- `4_county_imputation`: Impute county location for records, to supplement geocoded county *Runtime: ~6 min 10 sec*
- `5_variable_checks:` Generate variable goodness metrics and create summaries of metrics by grantee. *Runtime: ~12 min 21 sec*
- `6_thresholding`: Determine and apply thresholds for keeping or dropping grantee records based on goodness metrics generated above. *Runtime: ~43 sec*
- `7_pre_aggregation`: Perform last data processing steps before aggregation. *Runtime: ~54 min 7 sec*
- `8_aggregation`: Perform aggregation for dollar amounts and unique addresses served. *Runtime: ~17 sec*

More detailed summaries of each step are found in README files hosted in the respective subdirectories.

## Environment setup

The location of the directory containing the data directory should be stored as an REnvironment variable as `LOCAL_PATH`. The data directory itself should be named `era-county-level-dataset-data`, and the subdirectory structure should follow the paths specified in the scripts.

Intermediate data files are written out with the date of file creation suffixed to the filename for version control purposes. Users should replace dates in filenames being read into scripts accordingly.

Note: Large outputs will be written as Parquet files, which will be generated throughout the pipeline. To improve runtime efficiency and file storage, large raw data has been converted to Parquet format. For consistency, users are recommended to do the following before running the pipeline for the first time: 

1. Read in raw files using the following code (adjusting the read function based on input type)
   `raw_data <- read_csv(str_c(data_path,
                 "file_name.csv",
                 sep = "/"),
           col_types = list(.default = col_character())) %>%
  type_convert()`
2. Write those objects to a parquet with the same name (but different file extension)
    `arrow::write_parquet(raw_data, str_c(data_path, file_name.parquet, sep = "/")`

This will only need to be done once if writing to parquet.



