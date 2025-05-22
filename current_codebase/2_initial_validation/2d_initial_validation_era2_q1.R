# Purpose -----------------------------------------------------------------
# Validation concerning: rows and columns, general data type standardization,
# and grantee identification

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(janitor)
library(tictoc)
library(arrow)

tic()
# Locally specific part of data folder path already stored in Renviron file
data_path <- str_c(Sys.getenv("LOCAL_PATH"),
  "era-county-level-dataset-data",
  sep = "/"
)

# Load data ---------------------------------------------------------------

# Grantee information.
# RDS generated from NLIHC's grantee crosswalk file, under:
# code_phpdf/grantee_metadata/1.grantee_identities.R

grantees <-
  read_rds(
    str_c(
      data_path,
      "1_intermediates",
      "grantees",
      "grantees_combined_2025-03-06.RDS",
      sep = "/"
    )
  )

# Payments + geocodes
step_0 <-
  read_parquet(
    str_c(
      data_path,
      "0_raw_data",
      "phpdf_ERA2_Q1_2023__geo.parquet",
      sep = "/"
    )
  )

# Initial cleaning --------------------------------------------------------

# Standardize variable names
step_1 <- step_0 %>% 
  clean_names() %>% 
  select(address_line_1, address_line_2, address_line_3,
         city_name, state_code, zip5, zip4,
         payee_type, amount_of_payment, date_of_payment, type_of_assistance,
         start_date, end_date,
         file,
         program,
         grantee_name = recipient_name, 
         grantee_id_era2 = slt_application_number, 
         grantee_state = recipient_state,
         grantee_type = recipient_type, 
         quarter, year,
         geocode_std_addr = std_addr, 
         geocode_apt_no = apt_no, 
         geocode_std_zip5 = std_zip5,
         geocode_state2ky = state2ky, geocode_std_st = std_st,
         geocode_curcnty = curcnty, geocode_curcnty_nm = curcnty_nm,
         geocode_curcosub = curcosub, geocode_curcosub_nm = curcosub_nm,
         geocode_place2ky = place2ky, geocode_place_nm2ky = place_nm2ky,
         geocode_tract2ky = tract2ky,
         geocode_lat = lat, geocode_lon = lon,
         geocode_msg2ky = msg2ky, geocode_msgusps = msgusps) %>% 
  # Row/file ID variables
  mutate(file_specific_row_id = row_number(), .before = everything()) %>%
  mutate(original_filename = "phpdf_ERA2_Q1_2023__geo.csv", 
         .before = everything()) 

# Fix wrong variable type in valid data
step_2 <- step_1 %>%
  # handle numeric -> character conversion for spatial fields
  mutate(zip5 = str_pad(zip5, 5, "left", "0")) %>%
  mutate(zip4 = str_pad(zip4, 4, "left", "0")) %>% 
  mutate(geocode_state2ky = str_pad(geocode_state2ky, 2, "left", "0")) %>%
  mutate(geocode_curcnty = str_pad(geocode_curcnty, 3, "left", "0")) %>%
  mutate(geocode_curcosub = str_pad(geocode_curcosub, 5, "left", "0")) %>%
  mutate(geocode_place2ky = str_pad(geocode_place2ky, 5, "left", "0")) %>%
  mutate(geocode_tract2ky = str_pad(geocode_tract2ky, 6, "left", "0")) %>%
  # Clean up datetimes
  mutate(across(contains("date"), ~ date(mdy_hms(.))))

# Preserve as placeholder
step_3 <- step_2

# Basic standardization ---------------------------------------------------

# # See how redacted values are exactly marked
# redacted_test <- step_3 %>%
#   filter(if_any(where(is.character), ~str_detect(str_to_upper(.), "REDACT")))
# confidential_test <- step_3 %>%
#   filter(if_any(where(is.character), ~str_detect(str_to_upper(.), "CONFIDENTIAL")))

na_strings <-
  str_c("^(", str_flatten(
    c(
      "NA",
      "N/A",
      "N\\\\A",
      "NULL",
      "-",
      "--",
      "---",
      "X",
      "XX",
      "XXX",
      "Redacted for PII",
      "REDACTED",
      "Redacted - PII",
      "Data Not Collected",
      "CONFIDENTIAL"
    ),
    "|"
  ), ")$")

step_4 <- step_3 %>%
  mutate(across(where(is.character), ~ if_else(str_detect(
    ., regex(na_strings, ignore_case = TRUE)
  ), NA, .))) %>%
  mutate(across(where(is.character), ~ na_if(., "")))

# Non-ASCII characters ----------------------------------------------------

# Some records use non-ASCII characters. Because most of this is due to misencoding,
# and because most are records from the Puerto Rico grantees, we simply remove
# such characters so that they do not interfere with future procedures which use
# the address fields.

# See where the problems are
encoding_nonascii <- step_4 %>%
  filter(!state_code %in% "PR") %>%
  select(
    original_filename,
    file_specific_row_id,
    address_line_1,
    address_line_2,
    address_line_3,
    city_name
  ) %>%
  filter(if_any(where(is.character), ~ str_detect(., "[^\\x00-\\x7F]+"))) %>%
  mutate(
    address_line_1_nonascii =
      str_extract_all(address_line_1, "[^\\x00-\\x7F]+")
  ) %>%
  mutate(
    address_line_2_nonascii =
      str_extract_all(address_line_2, "[^\\x00-\\x7F]+")
  ) %>%
  mutate(
    address_line_3_nonascii =
      str_extract_all(address_line_3, "[^\\x00-\\x7F]+")
  ) %>%
  mutate(
    city_name_nonascii =
      str_extract_all(city_name, "[^\\x00-\\x7F]+")
  )

# Unique values of nonascii characters
encoding_nonascii_values_address1 <-
  encoding_nonascii$address_line_1_nonascii %>%
  unlist() %>%
  tabyl() %>%
  arrange(desc(n))

encoding_nonascii_values_address2 <-
  encoding_nonascii$address_line_2_nonascii %>%
  unlist() %>%
  tabyl() %>%
  arrange(desc(n))

encoding_nonascii_values_city_name <-
  encoding_nonascii$city_name_nonascii %>%
  unlist() %>%
  tabyl() %>%
  arrange(desc(n))

# Fix encoding for all non-PR grantees
encoding_fix <- step_4 %>%
  # Minimize size of dataset to be worked on
  filter(!state_code %in% "PR") %>%
  select(
    original_filename,
    file_specific_row_id,
    address_line_1,
    address_line_2,
    address_line_3,
    city_name
  ) %>%
  filter(if_any(where(is.character), ~ str_detect(., "[^\\x00-\\x7F]+"))) %>%
  # The character ½ is likely intended to represent a fractional address
  mutate(across(where(is.character), ~ str_replace_all(., "½", " 1/2 "))) %>%
  # Delete all remaining non-ASCII characters
  mutate(across(
    where(is.character),
    ~ str_remove_all(., "[^\\x00-\\x7F]+"),
    .names = "{.col}_fixed"
  )) %>%
  mutate(across(contains("fixed"), ~ str_remove_all(., "\\?"))) %>%
  mutate(across(contains("fixed"), ~ str_squish(.)))

# Import fixes to main file
step_5 <- step_4 %>%
  left_join(
    encoding_fix %>%
      select(original_filename, file_specific_row_id, contains("fixed")),
    by = c("original_filename", "file_specific_row_id")
  ) %>%
  mutate(address_line_1 = coalesce(address_line_1_fixed, address_line_1)) %>%
  mutate(address_line_2 = coalesce(address_line_2_fixed, address_line_2)) %>%
  mutate(address_line_3 = coalesce(address_line_3_fixed, address_line_3)) %>%
  mutate(city_name = coalesce(city_name_fixed, city_name)) %>%
  select(-contains("fixed")) %>%
  mutate(across(contains("address") |
    contains("city"), ~ na_if(., "")))

# Grantee information -----------------------------------------------------

# Get distinct combinations of grantee information in the dataset
grantee_info_original <- step_5 %>%
  distinct(
    grantee_id_era2,
    grantee_name,
    grantee_state,
    grantee_type,
    file
  )

# Check that grantee ID is not duplicated
grantee_id_dupes_original <- grantee_info_original %>%
  select(-file) %>% 
  distinct() %>% 
  get_dupes(grantee_id_era2)

# Fix records with no grantee ID
step_6 <- step_5 %>% 
  # No issues identified above
  # Join in ERA1 grantee IDs
  left_join(
    grantees %>% 
      select(
        grantee_id_era1, 
        grantee_id_era2, 
        grantee_id_combined,
        grantee_geographic_level = geographic_level
      ),
    by = "grantee_id_era2"
  ) %>% 
  relocate(grantee_id_era1, grantee_id_combined, .after = grantee_id_era2) %>%
  relocate(grantee_geographic_level, .after = grantee_type)

# Fixed distinct combinations of grantee information in the dataset
grantee_info_new <- step_6 %>%
  distinct(grantee_id_era2, grantee_name, grantee_state, grantee_type)

# Check that grantee ID is not duplicated
# Nominal
grantee_id_dupes_new <- grantee_info_new %>%
  get_dupes(grantee_id_era2)

# Compare grantee info with known gold standard
grantee_info_validation <- grantee_info_new %>%
  rename_with(~ str_c(., "_phpdf")) %>%
  left_join(
    grantees %>%
      rename_with(~ str_c(., "_nlihc")),
    by = c(grantee_id_era2_phpdf = "grantee_id_era2_nlihc")
  )

# Nominal
grantee_check_state <- grantee_info_validation %>%
  filter(grantee_state_phpdf != grantee_state_nlihc)

# Nominal
grantee_check_type <- grantee_info_validation %>%
  filter(grantee_type_phpdf != grantee_type_nlihc)

# Nominal
grantee_check_name <- grantee_info_validation %>%
  filter(str_to_upper(grantee_name_phpdf) != grantee_name_nlihc)

# Examine filenames
# Some grantees have multiple files
grantee_files <- step_6 %>%
  distinct(
    original_filename,
    grantee_id_era2,
    grantee_name,
    grantee_state,
    grantee_type,
    file
  ) %>%
  group_by(grantee_id_era2) %>%
  mutate(file_number = row_number()) %>%
  ungroup() %>%
  pivot_wider(
    names_prefix = "file_number_",
    names_from = file_number,
    values_from = file
  ) %>%
  arrange(grantee_state, desc(grantee_type), grantee_name)

# Write out
write_csv(grantee_files,
           str_c(data_path, "1_intermediates", "grantees",
                 str_c("era2_q1_grantee_files_", Sys.Date(),".csv"),
                 sep = "/"))

# Get grantee matches
grantee_matches <- grantees %>%
  filter(!is.na(grantee_id_era2)) %>%
  left_join(
    grantee_files %>%
      select(grantee_id_era2, original_filename),
    by = "grantee_id_era2"
  )

# Write out
write_csv(grantee_matches,
          str_c(data_path, "1_intermediates", "grantees",
                str_c("era2_q1_grantees_", Sys.Date(),".csv"),
                sep = "/"))

# Sentinel values ---------------------------------------------------------

# Grantees may have had summary rows which are not actual data

sentinel_test0 <- step_6 %>%
  # Create field for number of NAs in a row
  mutate(na_count = rowSums(is.na(.)), .before = everything())

sentinel_test1 <- sentinel_test0 %>%
  tabyl(na_count)

sentinel_test2 <- sentinel_test0 %>%
  filter(na_count > 10) %>%
  filter(is.na(payee_type) | is.na(type_of_assistance)) %>%
  filter(grantee_name != "Calcasieu Parish, Louisiana") %>%
  arrange(desc(na_count))

# Filtering out rows detected as problematic above
step_7 <- step_6 %>%
  # filter: removed 2 rows (<1%), 4,135,315 rows remaining
  filter(!address_line_1 %in% c("Total", "Powered by: Middlelayers.org"))

# Shifted columns testing -------------------------------------------------

# A series of tests to determine if any grantees shifted or mislabeled columns

# Testing if amount_of_payment could be an Excel-coded date
# Somerset County (NJ) is shifted
shift_test_amount_of_payment <- step_7 %>%
  filter(amount_of_payment > 44000 & amount_of_payment < 46000) %>%
  tabyl(grantee_name) %>%
  arrange(desc(n))

somerset <- step_7 %>% 
  filter(grantee_name == "Somerset County, New Jersey")

# Testing if city_name is actually a state
# Nominal
shift_test_city_name <- step_7 %>%
  filter(city_name %in% state.abb | city_name %in% state.name) %>%
  filter(city_name != state_code | is.na(state_code)) %>%
  tabyl(grantee_name) %>%
  arrange(desc(n))

# Testing if payee_type has nonnominal values
# Nominal
tabyl(step_7$payee_type)

shift_test_payee_type <- step_7 %>%
  filter(
    !payee_type %in% c(
      "Landlord or Owner",
      "Other Housing Services and Eligible Expenses Provider",
      "Tenant",
      "Utility / Home Energy Service Provider",
      NA
    )
  )

# Testing if date_of_payment was a non-date value coerced to Excel format
# Nominal
shift_test_date_of_payment <- step_7 %>%
  filter(date_of_payment < "2021-01-01" |
    date_of_payment > "2023-12-31")

# Testing if type_of_assistance has nonnominal values
# Nominal
tabyl(step_7$type_of_assistance)

shift_test_type_of_assistance <- step_7 %>%
  filter(!str_detect(type_of_assistance, "Financial Assistance"))

# Shifted columns fixing --------------------------------------------------

# Take bad grantees out
step_8a <- step_7 %>% 
  # filter: removed 56 rows (<1%), 4,135,259 rows remaining
  filter(grantee_name != "Somerset County, New Jersey")

# Fix Somerset County
somerset_fixed <- somerset %>% 
  select(-date_of_payment) %>% 
  rename(date_of_payment = amount_of_payment,
         amount_of_payment = payee_type,
         payee_type = zip4,
         zip4 = zip5,
         zip5 = state_code,
         state_code = city_name,
         city_name = address_line_3,
         address_line_3 = address_line_2,
         address_line_2 = address_line_1) %>% 
  mutate(date_of_payment = excel_numeric_to_date(date_of_payment)) %>% 
  mutate(amount_of_payment = as.numeric(amount_of_payment))

# skim(somerset_fixed)

# Rejoin fixed grantees
step_8b <- step_8a %>% 
  bind_rows(somerset_fixed)

# Preparing geocoded variables ----------------------------------------------

step_9 <- step_8b %>%
  mutate(
    geocode_county_geoid = str_c(geocode_state2ky, geocode_curcnty),
    .before = geocode_curcnty
  ) %>%
  mutate(
    geocode_county_subdivision_geoid =
      str_c(geocode_county_geoid, geocode_curcosub),
    .before = geocode_curcosub
  ) %>%
  mutate(
    geocode_place_geoid = str_c(geocode_state2ky, geocode_place2ky),
    .before = geocode_place2ky
  ) %>%
  mutate(
    geocode_tract_geoid =
      str_c(geocode_county_geoid, geocode_tract2ky),
    .before = geocode_tract2ky
  )

# Export ------------------------------------------------------------------

# Data ready for next script
write_parquet(step_9,
              str_c(data_path, "1_intermediates", "phpdfs", "2_initial_validation",
                    str_c("era2_q1_initial_validation_", Sys.Date(),".parquet"),
                    sep = "/"))

toc()

#143.325 sec elapsed