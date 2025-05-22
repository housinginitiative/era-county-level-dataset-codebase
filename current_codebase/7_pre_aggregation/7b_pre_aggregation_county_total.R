# Purpose -----------------------------------------------------------------
# Final data processing before aggregation

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(tictoc)
library(arrow)
library(campfin)

tic()

# Locally specific part of data folder path already stored in Renviron file
data_path <- str_c(Sys.getenv("LOCAL_PATH"), "era-county-level-dataset-data", sep = "/")

# Read data -----------------------------------------------------------------------------------

# Payments data -------------------------------------------------------------------------------

# ERA1 data
era1 <-
  read_parquet(
    str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
      "era1_with_variable_checks_2025-03-21.parquet",
      sep = "/"
    )
  )

# ERA2 data
era2_q4 <-
  read_parquet(
    str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
      "era2_q4_with_variable_checks_2025-03-21.parquet",
      sep = "/"
    )
  )

era2_q2 <-
  read_parquet(
    str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
      "era2_q2_with_variable_checks_2025-03-21.parquet",
      sep = "/"
    )
  )

era2_q1 <-
  read_parquet(
    str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
      "era2_q1_with_variable_checks_2025-03-21.parquet",
      sep = "/"
    )
  )

era2_reachback <-
  read_parquet(
    str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
      "era2_reachback_with_variable_checks_2025-03-21.parquet",
      sep = "/"
    )
  )

# Grantee thresholds --------------------------------------------------------------------------

# Grantees with threshold metrics
grantees_with_thresholds <-
  read_csv(
    str_c(data_path, "1_intermediates", "phpdfs", "6_thresholding",
      "county_total_selected_grantees_2025-04-14.csv",
      sep = "/"
    )
  )

# Threshold for anomalously large payments
payment_quantile_999 <-
  readRDS(
    str_c(data_path,
      "1_intermediates",
      "phpdfs",
      "5_variable_checks",
      "amount_of_payment_quantile_999_2025-03-21.rds",
      sep = "/"
    )
  )

# Grantee geographies -------------------------------------------------------------------------

# County grantee geographies
grantees_geographies_counties <-
  readRDS(str_c(
    data_path,
    "1_intermediates",
    "grantees",
    "grantees_geographies_counties_2025-03-20.RDS",
    sep = "/"
  ))

# City to geographic county xwalk
grantees_city_geographic_county_crosswalk <-
  readRDS(str_c(
    data_path,
    "1_intermediates",
    "grantees",
    "grantees_city_geographic_county_crosswalk_2025-03-20.RDS",
    sep = "/"
  ))

# Pre-aggregation preparation -----------------------------------------------------------------

# Geographic filter targets -------------------------------------------------------------------

# Grantees passing all thresholds
grantees_passing <- grantees_with_thresholds %>%
  filter(threshold_passing_with_complete_geography)

# Get county grantees that do not pass
failing_county_grantees <- grantees_with_thresholds %>%
  filter(
    geographic_level == "County",
    threshold_passing_with_complete_geography == FALSE
  )

# Get GEOIDs for geographic counties affected by failing county-level grantees
geographic_counties_to_remove_county_grantees <- failing_county_grantees %>%
  left_join(
    grantees_geographies_counties,
    by = join_by("grantee_id_combined" == "grantee_id_combined")
  ) %>%
  select(grantee_id_combined, geoid_jurisdiction_county)

# Get city grantees that do not pass
failing_city_grantees <- grantees_with_thresholds %>%
  filter(
    geographic_level == "City",
    threshold_passing_with_complete_geography == FALSE
  )

# Get GEOIDs for geographic counties affected by failing city-level grantees
geographic_counties_to_remove_city_grantees <- failing_city_grantees %>%
  left_join(
    grantees_city_geographic_county_crosswalk,
    by = join_by("grantee_id_combined" == "grantee_id_combined")
  ) %>%
  # For each county with at least some overlap with failing city grantee(s), only select
  # counties with more than 20% of county population affected by such city grantee(s)
  group_by(county_geoid) %>%
  mutate(county_pop_affected = sum(county_to_city_overlap_percentage)) %>%
  ungroup() %>%
  filter(county_pop_affected > 0.2)

# Vector of all geographic county GEOIDs which are affected by bad local grantees
geographic_counties_to_remove_all <-
  c(
    geographic_counties_to_remove_county_grantees$geoid_jurisdiction_county,
    geographic_counties_to_remove_city_grantees$county_geoid
  ) %>%
  unique() %>%
  sort()

# Combine data --------------------------------------------------------------------------------

# For each ERA2 quarter, filter for grantees using that quarter's data

era2_q4_grantees <- grantees_with_thresholds %>%
  filter(data_source_era2 == "2023 Q4") %>%
  pull(grantee_id_combined)
#filter: removed 89 rows (22%), 316 rows remaining

era2_q2_grantees <- grantees_with_thresholds %>%
  filter(data_source_era2 == "2023 Q2") %>%
  pull(grantee_id_combined)
# filter: removed 374 rows (92%), 31 rows remaining

era2_q1_grantees <- grantees_with_thresholds %>%
  filter(data_source_era2 == "2023 Q1") %>%
  pull(grantee_id_combined)
# filter: removed 398 rows (98%), 7 rows remaining

era2_reachback_grantees <- grantees_with_thresholds %>%
  filter(data_source_era2 == "2023 reachback") %>%
  pull(grantee_id_combined)
# filter: removed 399 rows (99%), 6 rows remaining

era2_q4_relevant <- era2_q4 %>%
  filter(grantee_id_combined %in% era2_q4_grantees) %>%
  mutate(source = "ERA2 2023 Q4")
# filter: removed 200,426 rows (3%), 5,700,182 rows remaining

era2_q2_relevant <- era2_q2 %>%
  filter(grantee_id_combined %in% era2_q2_grantees) %>%
  mutate(source = "ERA2 2023 Q2")
# filter: removed 4,462,017 rows (91%), 423,898 rows remaining

era2_q1_relevant <- era2_q1 %>%
  filter(grantee_id_combined %in% era2_q1_grantees) %>%
  mutate(source = "ERA2 2023 Q1")
# filter: removed 4,006,267 rows (99%), 30,731 rows remaining

era2_reachback_relevant <- era2_reachback %>%
  filter(grantee_id_combined %in% era2_reachback_grantees) %>%
  mutate(source = "ERA2 2023 reachback")
# filter: removed 6,243,115 rows (>99%), 15,813 rows remaining

# remove obsolete objects from envrionment to prevent crash
rm(era2_q4)
rm(era2_q2)
rm(era2_q1)
rm(era2_reachback)
gc()

# Combine ERA1 with ERA2 data, selecting ERA2 data from available quarters by grantee
era_combined_all <- era1 %>%
  mutate(source = "ERA1 closeout") %>%
  bind_rows(era2_q4_relevant) %>%
  bind_rows(era2_q2_relevant %>% mutate(geocode_std_zip5 = as.character(geocode_std_zip5))) %>%
  bind_rows(era2_q1_relevant %>% mutate(geocode_std_zip5 = as.character(geocode_std_zip5))) %>%
  bind_rows(era2_reachback_relevant %>% mutate(geocode_std_zip5 = as.character(geocode_std_zip5)))

# remove obsolete objects from envrionment to prevent crash
rm(era2_q4_relevant)
rm(era2_q2_relevant)
rm(era2_q1_relevant)
rm(era2_reachback_relevant)
rm(era1)
gc()

# Filter data ---------------------------------------------------------------------------------

# Filter out rows which will not be included in the aggregation
era_combined_filtered <- era_combined_all %>%
  # Grantees:
  # Exclude grantees not passing threshold checks
  filter(grantee_id_combined %in% grantees_passing$grantee_id_combined) %>%
  # filter: removed 3,620,879 rows (26%), 10,324,517 rows remaining
  # Geography:
  # Drop records without county assignment
  filter(!is.na(county_geoid_coalesced)) %>%
  # filter: removed 11,784 rows (<1%), 10,312,733 rows remaining
  # Drop payments in counties significantly overlapping with non-passing grantees
  filter(!county_geoid_coalesced %in% geographic_counties_to_remove_all) %>%
  # filter:removed 1,099,613 rows (11%), 9,213,120 rows remaining
  # Drop records not in geographic jurisdiction of grantee
  filter(check_address_within_jurisdiction != "Address outside grantee jurisdiction") %>%
  # filter:removed 9,881 rows (<1%), 9,203,239 rows remaining
  # Payment amount:
  # Drop records missing amount of payment
  filter(!is.na(amount_of_payment)) %>%
  # filter: no rows removed
  # Drop 0-value payments
  filter(amount_of_payment != 0) %>%
  # filter: removed 8,799 rows (<1%), 9,194,440 rows remaining
  # Drop negative payments
  filter(amount_of_payment > 0) %>%
  #filter: removed 72,794 rows (1%), 9,121,646 rows remaining
  # Drop large payments
  filter(amount_of_payment < payment_quantile_999) %>%
  # filter: removed 317 rows (<1%), 9,121,329 rows remaining
  # Payment date: exclude payments known to be made after earliest used ERA2 source quarter
  filter(date_of_payment <= "2023-03-31" |
           is.na(date_of_payment)) %>% 
  # filter:removed 257,066 rows (3%), 8,864,263 rows remaining
  # Light normalization of address fields in preparation for future steps
  mutate(across(c(address_line_1, address_line_2, address_line_3, geocode_std_addr, geocode_apt_no), 
                ~ str_to_upper(.))) %>%
  mutate(across(c(address_line_1, address_line_2, address_line_3, geocode_std_addr, geocode_apt_no), 
                ~ str_replace_all(., "\\.|,|-", " "))) %>%
  mutate(across(c(address_line_1, address_line_2, address_line_3, geocode_std_addr, geocode_apt_no), 
                ~ str_squish(.))) %>% 
  mutate(across(c(address_line_1, address_line_2, address_line_3, geocode_std_addr, geocode_apt_no), 
                ~ na_if(., "")))

# remove obsolete objects from envrionment to prevent crash
rm(era_combined_all)
gc() 

# Extract unit numbers ----------------------------------------------------

# Prepare address-related data for extracting cleaner unit numbers
units_initial <- era_combined_filtered %>%
  select(
    source, file_specific_row_id, grantee_name, contains("address_line_"),
    geocode_std_addr, geocode_apt_no, geocode_std_zip5, geocode_std_st
  )

# # Checking common types
#
# check_raw_address_2 <- units_initial %>%
#   tabyl(address_line_2) %>%
#   arrange(desc(n))
#
# check_raw_address_3 <- units_initial %>%
#   tabyl(address_line_3) %>%
#   arrange(desc(n))
#
# check_raw_geocode_apt_no <- units_initial %>%
#   tabyl(geocode_apt_no) %>%
#   arrange(desc(n))

street_abbs <- campfin::usps_street %>%
  # Remove entries which may be confused with parts of unit numbers
  # filter: removed 4 rows (1%), 364 rows remaining
  filter(str_length(abb) > 1) %>%
  # Additional misspellings
  add_row(full = "STREER", abb = "ST") %>%
  add_row(full = "STREEET", abb = "ST") %>%
  add_row(full = "STEET", abb = "ST") %>%
  add_row(full = "STERET", abb = "ST") %>%
  add_row(full = "TERR", abb = "TER") %>%
  add_row(full = "PKY", abb = "PKWY") %>%
  add_row(full = "ROW", abb = "ROW") %>%
  add_row(full = "BOULEVRD", abb = "BLVD") %>%
  add_row(full = "VOULEBARD", abb = "BLVD") %>%
  add_row(full = "BL", abb = "BLVD") %>%
  add_row(full = "BLV", abb = "BLVD") %>%
  add_row(full = "BLD", abb = "BLVD") %>%
  add_row(full = "BOUELVARD", abb = "BLVD") %>%
  add_row(full = "BOULEVAR", abb = "BLVD")

units_extracted <- units_initial %>%
  # If geocoded unit number is unavailable, turn to NA
  mutate(geocode_apt_no = na_if(geocode_apt_no, "1SUD")) %>%
  # Convert all potential street types in address_1 into USPS-standard abbreviations
  mutate(
    address_1_unit =
      campfin::normal_address(address_line_1,
        abbs = street_abbs,
        abb_end = FALSE
      )
  ) %>%
  # Retain only the part of address_1 which follows the last occurrence of any
  # USPS street type abbreviation in the address. This will sometimes contain information
  # other than a unit number, but should not disturb uniqueness of addresses when used along
  # with the other address fields.
  mutate(
    address_1_unit =
      str_remove(
        address_1_unit,
        str_c(
          ".*\\b(",
          str_flatten(unique(street_abbs$abb),
            collapse = "|"
          ),
          ")\\b"
        )
      )
  )

# Consolidate unit information into one field
units_selected <- units_extracted %>%
  mutate(address_1_unit = str_squish(address_1_unit)) %>%
  mutate(address_1_unit = na_if(address_1_unit, "")) %>%
  # Combine address_2 and address_3 information
  mutate(
    address_2_3_unit = str_c(
      replace_na(address_line_2, ""),
      replace_na(address_line_3, "")
    ),
    .before = address_1_unit
  ) %>%
  mutate(address_2_3_unit = na_if(address_2_3_unit, "")) %>%
  # For the extracted unit number field:
  # If there is a value for geocoded unit, take that value;
  # otherwise, take the address_2/address_3 value;
  # otherwise, take potential unit information from address_1
  mutate(
    unit_selected =
      coalesce(
        geocode_apt_no,
        address_2_3_unit,
        address_1_unit
      )
  ) %>%
  # Standardize extracted unit numbers further
  mutate(
    unit_selected =
      str_remove_all(
        unit_selected,
        "\\.|,|#|-|APT|APARTMENT|UNIT|AP|APTO|APPT|APARTAMENTO|SUIT|SUITE|STE"
      )
  ) %>%
  mutate(unit_selected = str_squish(unit_selected)) %>%
  mutate(unit_selected = na_if(unit_selected, ""))

# # Check output
# check <- units_selected %>%
#   filter(!is.na(unit_selected)) %>%
#   arrange(geocode_std_addr, unit_selected)

# Final pre-aggregation output ----------------------------------------------------------------

final_pre_aggregation <- era_combined_filtered %>%
  # Join cleaned unit number
  left_join(
    units_selected %>%
      select(source, file_specific_row_id, unit_selected),
    by = c("source", "file_specific_row_id")
  ) %>%
  # Create unique address id: if geocoded address is missing, 
  # use address_line_1, or else the row number
  mutate(street_address = 
           coalesce(geocode_std_addr, 
                    address_line_1, 
                    as.character(row_number()))) %>%
  group_by(street_address, unit_selected, geocode_std_zip5, geocode_std_st) %>%
  mutate(unique_address_id = cur_group_id()) %>%
  ungroup() %>%
  # Selecting only needed columns
  select(
    program,
    source,
    file_specific_row_id,
    grantee_id_combined,
    grantee_name,
    grantee_state,
    grantee_geographic_level,
    street_address,
    unit_selected,
    geocode_std_zip5,
    geocode_std_st,
    unique_address_id,
    county_geoid_coalesced,
    date_of_payment,
    amount_of_payment
  )

# Missing address summary -------------------------------------------------

# Generate by-county summary % of records affected by missing data issue

missing_address_summary <- final_pre_aggregation %>% 
  mutate(missing_address = street_address == as.character(row_number())) %>%
  group_by(county_geoid_coalesced, missing_address) %>% 
  summarize(n = n()) %>% 
  mutate(percent = n / sum(n)) %>% 
  filter(missing_address == TRUE) %>% 
  arrange(desc(percent))

# Write out data ----------------------------------------------------------

write_parquet(
  final_pre_aggregation,
  str_c(data_path, "1_intermediates", "phpdfs", "7_pre_aggregation",
    str_c("county_total_data_ready_for_aggregation_", Sys.Date(), ".parquet"),
    sep = "/"
  )
)

write_csv(
  missing_address_summary,
  str_c(data_path, "1_intermediates", "phpdfs", "7_pre_aggregation",
        str_c("county_total_missing_address_summary_", Sys.Date(), ".csv"),
        sep = "/"
  )
)

toc()
# 1213.173 sec elapsed
