# Aggregation outputs data dictionary

The columns for the aggregation outputs are described below.

For more information on which grantees and records are included in the aggregation, see the documentation on the data processing pipeline, or the [Methods page](https://housinginitiative.github.io/era-county-level-dataset-public/methods.html) on the project website.

For more information on data coverage for each aggregation output, see the [Data Coverage page](https://housinginitiative.github.io/era-county-level-dataset-public/data_coverage_descriptives.html) on the project website.

## Columns

- `county_geoid_coalesced`: The Census GEOID (i.e., FIPS code) for the geographic county or county-equivalent. Note: the county geographies are vintage 2000; in Connecticut, these refer to the pre-2022 county-equivalents.
- `month_of_payment`: *For county + month aggregation only*. The calendar month of the payment, as recorded by grantees. Format is YYYY-MM-DD (DD being `01` in all cases).
- `sum_assistance_amount`: The sum of non-negative payments in the cell, for any type of assistance to households. Values are nominal US dollars. Suppressed with value `-99999` if value of `unique_assisted_addresses` was less than 11.
- `unique_assisted_addresses`: The count of unique addresses (taking into account unit numbers) assisted in the cell. Suppressed with value `-99999` if value was less than 11.

## Citation

The aggregate data produced by this project may be cited as:

Kim, Chi-Hyun, Grace Hartley, Jacob Haas, Tim Thomas, Rebecca Yae, and Peter Hepburn. County Emergency Rental Assistance Spending. 2025. Accessed via https://housinginitiative.github.io/era-county-level-dataset-public/.
