# Step 2: Initial validation

In this step, we performed preliminary data-cleaning steps to normalize the formatting of the data. Each PHPDF (one for ERA1 and four for ERA2) was processed independently.

-   Standardizing variable names
    -   Ensuring compatibility between PHPDF files
-   Standardizing variable types
    -   Treating GEOIDs as appropriately left-padded strings
    -   Converting dates to ISO 8601 format
-   Standardizing NA strings
    -   Turning variously-encoded missing values into proper NAs
-   Dealing with garbled character encodings in source data
-   Standardizing grantee identification
    -   Correcting misspellings and errors
    -   Validating grantee IDs
-   Removing sentinel values (e.g., totals rows)
-   Correcting shifted columns (some grantees submitted data with columns in a different order than required)
-   Joining relevant geocode data from geocode file