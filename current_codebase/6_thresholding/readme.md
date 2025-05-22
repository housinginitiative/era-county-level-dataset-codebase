# Step 6: Thresholding


In this step, we specified acceptable data quality thresholds for all grantees.

First, for each PHPDF, we calculated whether each grantee met the following thresholds:

- Variable quality: At least 79% of records had acceptable data across all variables needed for the applicable aggregation type (see Step 5 above for details on the tests)
- Spending completeness: The aggregate sum of the grantee's payments (excluding negative payments) were:
	- Between 80% and 110% of its allocation (ERA1) or 50% and 110% of its allocation (ERA2); or
	- If between 50% and 80% of its allocation (ERA1) or 25% and 50% of its allocation (ERA2), the reported spending was within 20% of the aggregate spending as reported to Treasury, either individually or for all grantees in the state together; or
	- Confirmed by Treasury to be an accurate reflection of low spending by the grantee
	
Second, we picked an ERA2 PHPDF source for each grantee, taking the most recent PHPDF for which a grantee passed (if any quarters passed) or the most recent PHPDF we had data for the grantee (if all quarters failed).

Third, for any grantees which participated in either program (n = 405), we joined the diagnostic data for ERA1 and ERA2 to derive overall threshold checks. A grantee passed if it:

- Submitted data for all programs it participated in 
- Passed the variable quality threshold for all programs it participated in 
- Passed the spending completeness threshold for all programs it participated in 

Fourth, we applied a geographic threshold: if an otherwise passing grantee significantly overlapped in the area of its jurisdiction with a failing grantee, then it failed this threshold. This was done because, in geographic areas served by multiple grantees, missing or bad-quality data from one grantee may have impacted a significant percentage of ERA activity in that geographic area overall.

We defined 'significantly overlap' as: more than 20% of the population of the geographic area served by the passing grantee being located in the overlap(s) with the geographic area(s) served by non-passing grantee(s). This was always taken to be the case for county and city grantees vis-à-vis their state grantees.

Flowcharts illustrating these thresholding steps and the number of grantees dropped at each juncture are available under `outputs/thresholding_descriptives`.

