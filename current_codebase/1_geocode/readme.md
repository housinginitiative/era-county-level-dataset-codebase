# Step 1: Geocode

The ERA1 closeout PHPDHF as well as the ERA2 Q4 and reachback PHPDFs had geocoding outputs in a separate file. In this step, we compared the payment files and their corresponding geocodes, and verified that all rows join.

The following number of rows were present in the payments file but not in the geocoded file: we geocoded these ourselves, using the Census geocoder.

- ERA1 closeout: 77 rows (87.03% successfully geocoded)
- ERA2 Q4: 24,394 rows (99.95% successfully geocoded)
- ERA2 reachback: 0 rows (all rows already geocoded by HUD)

The other ERA2 PHPDFs already had geocodes appended to the payment data in a single file. 
