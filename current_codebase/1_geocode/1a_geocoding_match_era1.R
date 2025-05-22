# Purpose -----------------------------------------------------------------
# Validate the join between payments and geocodes, and geocode any records
# missing in the geocodes file.

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(janitor)
library(tidygeocoder)
library(tictoc)
library(arrow)

tic()

# Locally specific part of data folder path already stored in Renviron file
data_path <- str_c(Sys.getenv("LOCAL_PATH"),
  "era-county-level-dataset-data",
  sep = "/"
)

# Load data ---------------------------------------------------------------
# Geocodes
geocodes_raw <-
  read_parquet(
    str_c(
      data_path,
      "0_raw_data",
      "phpdf_ERA1_Final_Report_GSC_Results.parquet",
      sep = "/"
    )
  )

geocodes_ready <- geocodes_raw %>%
  clean_names() %>%
  select(-oid) %>%
  # Removing 7 empty columns of 78 columns total
  remove_empty("cols", quiet = FALSE) %>%
  rename(development_code = userkeys)

# Payments
payments_raw <-
  read_parquet(
    str_c(
      data_path,
      "0_raw_data",
      "phpdf_ERA1_final_report_Data_Relate_table.parquet",
      sep = "/"
    )
  )

payments_ready <- payments_raw %>%
  # Standardize variable names
  clean_names() %>%
  rename(
    grantee_name = recipient_name,
    grantee_type = recipient_type,
    grantee_state = recipient_state
  ) %>%
  select(-unnamed_0, -era_application_number) %>%
  # Row/file ID variables
  mutate(file_specific_row_id = row_number(), .before = everything()) %>%
  mutate(original_filename = "phpdf_ERA1_final_report_Data_Relate_table.csv", .before = everything())

# Geocodes matching ------------------------------------------------------


geocodes_test_phpdf_nogeocode <- payments_ready %>%
  anti_join(geocodes_ready, by = "development_code")

# anti_join: added no columns
# > rows only in x           77
# > rows only in y  (         2)
# > matched rows    (13,396,033)
# >                 ============
#   > rows total               77

geocodes_test_geocode_nophpdf <- geocodes_ready %>%
  anti_join(payments_ready, by = "development_code")

# anti_join: added no columns
# > rows only in x            2
# > rows only in y  (        77)
# > matched rows    (13,396,033)
# >                 ============
#   > rows total                2

# Geocoding missing records -----------------------------------------------

geocodes_missing <- payments_ready %>%
  filter(development_code %in% geocodes_test_phpdf_nogeocode$development_code) %>%
  select(
    development_code,
    address_line_1,
    address_line_2,
    address_line_3,
    city_name,
    state_code,
    zip5,
    zip4
  )

# # only 16 addresses, each of which has some type of escape character or double quote corrupting the string
# geocodes_missing %>% select(-development_code) %>% distinct()

# Clean addresses to geocode
geocodes_missing_to_geocode <- geocodes_missing %>%
  distinct() %>%
  mutate(zip5 = as.character(zip5), zip4 = as.character(zip4)) %>%
  mutate(
    zip5 = str_pad(zip5, 5, "left", "0"),
    zip4 = str_pad(zip4, 4, "left", "0")
  ) %>%
  select(address_line_1:zip4, development_code) %>%
  # No rows removed
  distinct() %>%
  # 0 rows affected by this step
  mutate(across(where(is.character), ~ str_remove_all(.x, "Data Not Collected"))) %>%
  mutate(across(where(is.character), ~ replace_na(.x, " "))) %>%
  mutate(geo_input_address = str_squish(
    paste(
      address_line_1,
      address_line_2,
      address_line_3,
      city_name,
      state_code,
      zip5
    )
  )) %>%
  # No rows removed
  distinct(geo_input_address, development_code)

geocodes_missing_geocoded <- geocodes_missing_to_geocode %>%
  geocode(
    address = geo_input_address,
    method = "census",
    full_results = TRUE,
    api_options = list(census_return_type = "geographies"),
    verbose = TRUE,
    progress_bar = TRUE
  ) %>%
  select(
    development_code,
    matched_address,
    lat,
    lon = long,
    state = state_fips,
    curcnty = county_fips,
    tract = census_tract
  ) %>%
  separate_wider_delim(
    cols = "matched_address",
    delim = ", ",
    names = c("std_addr", "std_city", "std_st", "std_zip5"),
    too_few = "align_end",
    cols_remove = TRUE
  ) %>%
  left_join(
    tigris::fips_codes %>%
      select(
        state = state_code,
        curcnty = county_code,
        curcnty_nm = county
      )
  ) %>%
  mutate(curcnty_nm = str_remove_all(curcnty_nm, " County$"))

message("pct successful geocode: ", ((nrow(geocodes_missing) - nrow(geocodes_missing_geocoded %>% filter(is.na(std_addr))))/
          nrow(geocodes_missing))*100, "%")

geocodes_fixed_all <- geocodes_ready %>%
  bind_rows(geocodes_missing_geocoded) %>%
  arrange(development_code)

# # Confirm row congruence
# payments_ready %>%
#   anti_join(geocodes_fixed_all, by = "development_code")
#
# geocodes_fixed_all %>%
#   anti_join(payments_ready, by = "development_code")
#
# # Confirm column congruence
# setdiff(names(geocodes_fixed_all), names(geocodes_ready))
# setdiff(names(geocodes_ready), names(geocodes_fixed_all))

# Write out ---------------------------------------------------------------
#
# write_parquet(geocodes_fixed_all,
#               str_c(data_path,
#                     "1_intermediates",
#                     "phpdfs",
#                     "1_geocode",
#                     str_c("era1_geocodes_all_", Sys.Date(),".parquet"),
#                     sep = "/"))

toc()
#237.865 sec elapsed
