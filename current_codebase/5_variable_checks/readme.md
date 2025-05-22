# Step 5: Variable checks

In this step, we generated data validation metrics for each PHPDF.

We first generated a series of variable-specific data quality checks, testing each row for:

-   Whether the record was within the geographic jurisdiction of the grantee
-   Whether the record was locatable to a specific county
-   Whether the payment amount was recorded, and if so, whether it was negative, zero, or anomalously large
    -   We defined 'anomalously large' as an amount exceeding the 99.9th percentile value of all records in ERA1 closeout and ERA2 Q4 (which was $73,541).
-   Whether the date of payment was recorded, and if so, whether it was impossibly early (before January, 1, 2021 for ERA1, or before March 1, 2021 for ERA2) or late (after December 31, 2022 for ERA1, or after the end of the reporting quarter for ERA2)
-   Whether the payee type (landlord, utility, tenant) was recorded
-   Whether the assistance type (rent, utilities, other) was recorded
-   Whether the record included a valid address
-   The geocoding quality of the record, as given by HUD's geocoding process

For each aggregation type, we selected a subset of these variable quality tests to calculate grantee-level variable quality.

- For the county-month dataset, we employed the first 4 tests
- For the county-total dataset we imposed the first 3 tests

For each grantee, we calculated the percentage of its records that met all applicable variable quality tests for the relevant aggregation scenario.

For each grantee, we also calculated the percentage of its aggregate spending in the PHPDF compared to:

- Its total allocation for the applicable program (ERA1 or 2), inclusive of any reallocations 
- The amount reported in Treasury's publicly released aggregate summary reporting, which was itself compiled from aggregate reporting submitted by grantees to Treasury
- The amounts in the above bullet, but calculated at the state level (i.e., PHPDF spending added together for all grantees in a state, divided by aggregate reporting together for all grantees in a state)
