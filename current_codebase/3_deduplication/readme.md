# Step 3: Deduplication

In this step, we deduplicated each PHPDF independently.

In the data, three patterns of duplication were discernible, so we deduplicated in three stages.

### Across-grantee

Identical payments could be reported by more than one grantee. For the purposes of this stage, 'identical' means having the same values across:

-   address_line_1
-   address_line_2
-   address_line_3
-   city_name
-   state_code
-   zip5
-   zip4
-   payee_type
-   type_of_assistance
-   amount_of_payment
-   date_of_payment
-   start_date
-   end_date
-   program

How such payments were deduplicated depended on the cause of the duplication.

If the duplication was due to grantees having overlapping geographies, all records made by the smallest jurisdiction were kept and others dropped (e.g., keep records from the City of Pittsburgh grantee, drop records duplicated in the Allegheny County grantee).

If the duplication was due to misattribution of records to grantees with similar names, we dropped the records attributed to the wrong grantee, by inspecting the location of the payments (e.g., records attributed to Cleveland County, OK but were actually made by Cleveland, OH).

### Across-file

In a given PHPDF, a grantee could submit data to Treasury in multiple files. In some cases (very commonly in the ERA1 PHPDF), multiple files with near-identical contents were included from the same grantee. If identical records (using the same definition as above) were included in multiple files from the same grantee, we kept all records from the file with the largest number of records, and dropped duplicated records from all other files of the same grantee.

### Within-file

Records could also be duplicated within a given file from a given grantee. Here, we define identical records more conservatively, since missing data across the identifying columns mean that multiple distinct payments could look the same if they are all missing critical elements like street address. Therefore, if any of the following variables were NA, we gave each NA value a temporary unique value to avoid using these missing values in identifying duplication.

-   address_line_1
-   payee_type
-   type_of_assistance
-   amount_of_payment
-   date_of_payment
-   start_date

Within a given file from a given grantee, we keep the duplicate record with the lowest row number and drop all others.

### Extent of duplication

We report the following figures to illustrate the extent of de/duplication in each PHPDF:

- ERA1 closeout: 5,621,334 rows dropped (42%)
    - The large percentage here is due to duplicate PHPDFs with different names stored in Treasury’s reporting system
- ERA2 Q4: 54,293 rows dropped (0.9%)
- ERA2 Q2: 12,261 rows dropped (0.3%)
- ERA2 Q1: 98,317 rows dropped (2%)
- ERA2 reachback: 74 rows dropped (\~0%)
