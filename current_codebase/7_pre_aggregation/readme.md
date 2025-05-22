# Step 7: Pre-aggregation

In this step, we prepared the final data to be aggregated.

First, we bound the ERA1 data together with the ERA2 data. We selected the vintage of the ERA2 data by each grantee, as specified in Step 6. 

Second, we identified geographic counties with coverage issues due to incomplete grantee coverage. For example, if the Cook County, IL, grantee failed, any payments that the State of Illinois grantee made in Cook County should also drop. Note that this screening of counties at the *geographic* level was in addition to the screening of counties/cities at the *grantee* level in Step 6.

Third, we filtered the joined data to only include the records to be included in the final aggregation. Namely, we only kept records where:

- The grantee passed all Step 6 thresholds
- The record was assigned to a county
- The county did not fall out due to the geographic screening described above
- The record passed all variable-level checks necessary for the applicable aggregation type

Fourth, we constructed unique address IDs. To do this, we first extracted unit number information from the following fields, in order of availability:

- Geocoded address unit number
- Address line 2/3
- Address line 1

We then assigned a unique ID to each unique concatenated value of:

- Geocoded address 
	- If geocoded address was missing, we used the original address if available
	- If there was no address at all, we used the record's row number
- Unit number
- Geocoded ZIP code
- Geocoded state