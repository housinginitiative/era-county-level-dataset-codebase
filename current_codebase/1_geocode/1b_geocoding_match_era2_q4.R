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
      "phpdf_ERA2_Q4_23_Geocoding_Results.parquet",
      sep = "/"
    )
  )

geocodes_ready <- geocodes_raw %>%
  clean_names() %>%
  select(-oid) %>%
  # Removing 8 empty columns of 75 columns total
  remove_empty("cols", quiet = FALSE) %>%
  # Renaming join keys to be correct, since the current ones misidentify the source program
  mutate(development_code = str_replace(development_code, "ERA1_", "ERA2Q42023_")) %>%
  # Standardized some other join keys
  mutate(development_code = str_remove(development_code, "VERA1_"))


# Payments
payments_raw <-
  read_parquet(
    str_c(
      data_path,
      "0_raw_data",
      "phpdf_ERA2_Q4_23_valid_Data_Relate_table.parquet",
      sep = "/"
    )
  )

payments_ready <- payments_raw %>%
  # Standardize variable names
  clean_names() %>%
  rename(
    grantee_name = recipient_name,
    grantee_id_era2 = slt_application_number,
    grantee_type = recipient_type,
    grantee_state = recipient_state
  ) %>%
  select(-unnamed_0, -era_application_number) %>%
  # Row/file ID variables
  mutate(file_specific_row_id = row_number(), .before = everything()) %>%
  mutate(original_filename = "phpdf_ERA2_Q4_23_valid_Data_Relate_table.csv", .before = everything())

# Geocodes matching ------------------------------------------------------

# Many of these are inappropriately missing because of technical errors from
# HUD's process. Any non-territorial address here will need separate geocoding.
geocodes_test_phpdf_nogeocode <- payments_ready %>%
  anti_join(geocodes_ready, by = "development_code")

# anti_join: added no columns
# > rows only in x      24,394
# > rows only in y  (        0)
# > matched rows    (5,930,510)
# >                 ===========
#   > rows total          24,394

geocodes_test_geocode_nophpdf <- geocodes_ready %>%
  anti_join(payments_ready, by = "development_code")

# anti_join: added no columns
# > rows only in x           0
# > rows only in y  (   24,394)
# > matched rows    (5,930,510)
# >                 ===========
#   > rows total               0

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

# # ~12k distinct addresses
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

# Batch function for passing 10k records into Census geocode at once
census_geo <- function(df, batch_num) {
  message(paste("Processing batch", batch_num, "of", ceiling(nrow(df) / 10000)))

  d <- df %>%
    geocode(
      address = geo_input_address,
      method = "census",
      full_results = TRUE,
      api_options = list(census_return_type = "geographies"),
      verbose = TRUE,
      progress_bar = TRUE
    )

  return(d)
}

# ==========================================================================
# Geocode
# ==========================================================================

batch_size <- 10000

n_batches <- ceiling(seq_len(nrow(geocodes_missing_to_geocode) / batch_size))

df_batches <- split(geocodes_missing_to_geocode, n_batches)

geocodes_missing_ready <- df_batches %>%
  map2(n_batches, census_geo) %>%
  list_rbind() %>%
  select(
    development_code,
    lat,
    lon = long,
    matched_address,
    state2ky = state_fips,
    curcnty = county_fips,
    tract2ky = census_tract
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
        state2ky = state_code,
        curcnty = county_code,
        curcnty_nm = county
      )
  ) %>%
  mutate(curcnty_nm = str_remove_all(curcnty_nm, " County$"))

geocodes_fixed_all <- geocodes_ready %>%
  bind_rows(geocodes_missing_ready) %>%
  arrange(development_code)

message("pct successful geocode: ", ((nrow(geocodes_missing) - nrow(geocodes_missing_geocoded %>% filter(is.na(std_addr))))/
          nrow(geocodes_missing))*100, "%")

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

# write_parquet(geocodes_fixed_all,
#           str_c(data_path,
#                 "1_intermediates",
#                 "phpdfs",
#                 "1_geocode",
#                 str_c("era2_q4_geocodes_all_", Sys.Date(),".parquet"),
#                 sep = "/"))

toc()
#290.344 sec elapsed
