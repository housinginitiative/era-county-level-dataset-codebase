# Step 4: County imputation

In this step, we imputed county locations for payments that were missing a geocoded county.

Before imputation, 54,831 rows in the ERA1 PHPDF and 265,703 rows in the ERA2 (2023 Q4) PHPDF were missing county location.

Our imputation process assigned county locations to 29,445 rows previously missing county locations in ERA1 (54% of missing-county records) and to 123,788 rows for ERA2 (47%).

For each PHPDF, we used two methods:

### Use grantee geography for counties/single-county cities

For county-level grantees and city-level grantees whose jurisdictions were included in only one geographic county, we imputed as the county of payment the geographic county of the grantee's jurisdiction.

### Use City + ZIP: county crosswalk for states

For state programs, this was a bit more complicated. We utilized a [ZIP code-county crosswalk](https://www.huduser.gov/portal/datasets/usps_crosswalk.html) from HUD.

First, we determined which zip codes in the crosswalk fell into just one county. Similarly to above, we then joined the single-county zips to the payment files, but this time, by zip code and state. We found that joining by city was too limiting, as the cities were described differently in each file (for example, the same address could be described as being in Las Vegas or North Las Vegas).

The next, slightly more complicated step, was to join zip codes that fell within multiple counties. To be able to join one-to-one, we filtered the HUD county-zip crosswalk file to include counties where 95% of a zip code was within the county. After that, we could simply join by zip code and state.

### Coalesce `county_geoid`

We then coalesced from `geocode_county_geoid` and `imputed_county_geoid`. If a payment already had a value for `geocode_county_geoid`, then we kept that value. If not, it took on the value of `imputed_county_geoid`. We used this coalesced county assignment variable in all subsequent data processing steps.
