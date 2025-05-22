# Validation metrics data dictionaries

## Variable checks summary

This file provides a summary of variable-level (i.e., by-column) validation checks for each grantee. Each PHPDF has its own output. The following is a description of each column in the CSV file.

- `program`: Identifies ERA1 or ERA2
- `grantee_id_combined`: Unique ID of the grantee across both ERA1 and ERA2
- `grantee_name`: Name of the grantee (note that names can vary between different datasets; to join by grantee, use `grantee_id_combined`)
- `grantee_state`: State of the grantee's geographical jurisdiction
- `grantee_type`: State/DC, Local Government (i.e., county or city), Territorial Government
- `total_n`: Number of records (rows) from the grantee in the PHPDF
- `variable`: The name of the validation metric
- `status`: Diagnostic categories for the validation metric
- `n`: Number of rows (records) falling into the `variable`:`status` category
- `percent`: `n` divided by `total_n`

### Validation metrics descriptions

**check_address_within_jurisdiction**  
Whether the record is within the geographic jurisdiction of the grantee

* Nominal
  * For state-level grantees: the geocoded state or the state of the imputed county is in the same state
  * For county-level grantees: the geocoded county or the imputed county is in the same county
  * For city-level grantees: the geocoded Place, Minor Civil Division, or Tract (taking the first non-empty value in that order) is in the Place or MCD corresponding to the city, or a Tract which at least partially overlaps the city. For records with imputed county, the county is any among the counties which at least partially overlap the city.
* Address outside grantee jurisdiction: Neither `Nominal` nor `Data not adequate to check`
* Data not adequate to check: No geospatial information exists to check

**check_amount_of_payment**  
Whether the payment amount was recorded, and if so, whether it was negative, zero, or anomalously large

* Nominal: Falls into none of the below categories (Note: none of the records in the PHPDFs had missing payment amount)
* Negative amount: Dollar amount below zero
* Zero amount: Dollar amount equal to zero
* Amount above 99.9th percentile: Dollar amount exceeding the 99.9th percentile value of all records in ERA1 closeout and ERA2 Q4 ($73,541)

**check_county_assignment**  
Whether the record is locatable to a specific county

* Nominal: Records assigned to geographic county (whether geocoded or imputed)
* No county assignment: Records not assigned to geographic county

**check_date_of_payment**  
Whether the date of payment was recorded, and if so, whether it was impossibly early or late. Not used for county total aggregation.

* Nominal: Falls into none of the below categories
* No value: Date missing
* Incorrectly late: Date after December 31, 2022 for ERA1, or after the end of the reporting quarter for ERA2
* Incorrectly early: Date before January, 1, 2021 for ERA1, or before March 1, 2021 for ERA2

**check_geocode_address_accuracy**  
HUD's geocoding accuracy measure. Not used, for reference only.

* Nominal: Geocoding accuracy 80% or above
* Not geocoded or geocoding metrics unavailable
* Geocoded with less than 80% accuracy

**check_geocode_to_rooftop**  
HUD's rooftop geocoding measure. Not used, for reference only.

* Nominal: Geocode labeled as "Street-Level Rooftop"
* Geocoded not to rooftop
* Not geocoded or geocoding metrics unavailable

**check_geocode_zip_match**  
Whether the geocoded ZIP code matches the street address ZIP code. Not used, for reference only.

* Nominal: ZIP5 matches between street address and geocode
* Data not adequate to check: Street address ZIP code and/or geocoded ZIP code missing
* ZIP mismatch: ZIP5 does not match between street address and geocode

**check_payee_type**  
Whether the payee type (landlord, utility, tenant) was recorded. Not used, for reference only.

* Nominal: Has a value
* No value: Missing value

**check_raw_address**  
Whether the record includes a valid address. Not used, for reference only.

* Nominal: Address is recorded, and meets all following criteria:
  * Not a PO Box
  * Starts with a numeric character
  * Includes at least one alphabet character
* ZIP + city only: Address is not nominal, but record has city name and ZIP code recorded
* No usable address: All other cases

**check_type_of_assistance**  
Whether the assistance type (rent, utilities, other) was recorded. Not used, for reference only.

* Nominal: Has a value
* No value: Missing value

## Grantee goodness checks summary

This file provides a summary of grantee-level validation checks. Each PHPDF has its own output. The following is a description of each column in the CSV file.

- `grantee_id_combined`: Unique ID of the grantee across both ERA1 and ERA2
- `program`: Identifies ERA1 or ERA2
- `grantee_name`: Name of the grantee (note that names can vary between different datasets; to join by grantee, use `grantee_id_combined`)
- `grantee_state`: State of the grantee's geographical jurisdiction
- `grantee_type`: State/DC, Local Government (i.e., county or city), Territorial Government
- `scenario`: The aggregation scenario
- `ok_n`: The number of records (rows) which meet all tests specified in the aggregation scenario
- `ok_percent`: `ok_n` divided by `total_n`
- `total_n`: Number of records (rows) from the grantee in the PHPDF
- `percent_rent_records`: Percentage of rows recorded as for assistance type of rent or rental arrears (denominator includes NAs)
- `sum_spent`: The sum of non-negative payments made by the grantee in the PHPDF
- `percent_of_allocation_spent`: `sum_spent` divided by `total_updated_allocation`
- `total_updated_allocation`: The grantee's allocation, including reallocations as of June 2024, for the program
- `pct_of_spending_in_php_file_quarterly`: `sum_spent` divided by the sum of columns B to I ('Expenditure on Assistance to Households' by quarter) in Treasury's aggregate reporting spreadsheet
- `pct_of_spending_in_php_file_summation`: `sum_spent` divided by column Z ('Cumulative Expenditures Q1 2021 - Q4 2022') in Treasury's aggregate reporting spreadsheet
- `state_pct_of_spending_in_php_file_quarterly`: Same as `pct_of_spending_in_php_file_quarterly`, except both numerator and denominator are amounts summed over all grantees in grantee's state
- `state_pct_of_spending_in_php_file_summation`: Same as `pct_of_spending_in_php_file_summation`, except both numerator and denominator are amounts summed over all grantees in grantee's state

### Aggregation scenarios

**County total only**
- `check_county_assignment` is "Nominal"
- `nominal_jurisdiction` is "Nominal"
- `check_amount_of_payment` is "Nominal"

**County + month**
- All tests for "County total"
- Plus `check_date_of_payment` is "Nominal"

**County + month + assistance type**
- All tests for "County + month"
- Plus `check_type_of_assistance` is "Nominal"

**County + month + payee type**
- All tests for "County + month"
- Plus `check_payee_type` is "Nominal"

**County + month + payee type + assistance type**
- All tests for "County + month"
- Plus `check_payee_type` is "Nominal"
- Plus `check_type_of_assistance` is "Nominal"