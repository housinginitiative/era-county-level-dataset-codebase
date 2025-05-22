# Step 8: Aggregation

In this step, we performed the final aggregation.

Taking the data output from the previous step, we grouped the data by variables identifying each cell in the final output (e.g., county GEOID and payment month for the county + month aggregation), then calculated two quantities for each cell:

-   The sum of payment amounts
-   The number of unique assisted addresses

We then suppressed cell values where the number of unique assisted addresses is less than 11, and the corresponding payment amount sum. The suppressed values are encoded with the value `-99999`.