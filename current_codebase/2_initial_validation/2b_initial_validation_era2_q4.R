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

# Payments
step_0 <-
  read_parquet(
    str_c(
      data_path,
      "0_raw_data",
      "phpdf_ERA2_Q4_23_valid_Data_Relate_table.parquet",
      sep = "/"
    )
  )

# Geocodes
geocodes <-
  read_parquet(
    str_c(
      data_path,
      "1_intermediates",
      "phpdfs",
      "1_geocode",
      "era2_q4_geocodes_all_2025-03-06.parquet",
      sep = "/"
    )
  )



# Initial cleaning --------------------------------------------------------

# Standardize variable names
step_1 <- step_0 %>%
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

# Fix wrong variable type in valid data
step_2 <- step_1 %>%
  # handle numeric -> character conversion for zip fields
  mutate(zip5 = str_pad(zip5, 5, "left", "0")) %>%
  mutate(zip4 = str_pad(zip4, 4, "left", "0"))

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
# There is an issue with City of Milwaukee and St. Louis County
# where one file appears to be from the City and another from the County
grantee_id_dupes_original <- grantee_info_original %>%
  get_dupes(grantee_id_era2)

# Fix records with no grantee ID
step_6 <- step_5 %>%
  # Remove test row from Treasury
  # filter: removed one row (<1%), 5,954,903 rows remaining
  filter(grantee_name != "treasury.gov") %>%
  # Fixing the issues identified above
  mutate(
    grantee_name =
      case_when(
        (grantee_name == "DoÃ±a Ana County") |
          (grantee_name == "DOÑA ANA COUNTY") ~ "Doña Ana County",
        grantee_name == "CITY OF ST LOUIS" ~ "City of St Louis",
        file == "Milwaukee County, Wisconsin - ERA2_COMBINED_Participant_Household_Payment_Data_File_Q4_2023.xlsx" ~ "Milwaukee County, Wisconsin",
        file == "St Louis ERA2 - Q4 2023 REISSUED 2.92024-240410.csv" ~ "City of St Louis",
        .default = grantee_name
      )
  ) %>%
  mutate(
    grantee_id_era2 =
      case_when(
        grantee_name == "Doña Ana County" ~ "SLT-0138",
        grantee_name == "Hialeah city, Florida" ~ "SLT-11854",
        file == "Milwaukee County, Wisconsin - ERA2_COMBINED_Participant_Household_Payment_Data_File_Q4_2023.xlsx" ~ "SLT-0242",
        file == "St Louis ERA2 - Q4 2023 REISSUED 2.92024-240410.csv" ~ "SLT-0267",
        .default = grantee_id_era2
      )
  ) %>%
  mutate(
    grantee_state =
      case_when(
        grantee_name == "Doña Ana County" ~ "New Mexico",
        grantee_name == "Hialeah city, Florida" ~ "Florida",
        .default = grantee_state
      )
  ) %>%
  mutate(
    grantee_type =
      case_when(
        grantee_name == "Doña Ana County" ~ "Local Government",
        grantee_name == "Hialeah city, Florida" ~ "Local Government",
        .default = grantee_type
      )
  ) %>%
  # Join in ERA1 grantee IDs and geographic level
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

# # Write out
 # write_csv(grantee_files,
 #            str_c(data_path, "1_intermediates", "grantees",
 #                  str_c("era2_q4_grantee_files_", Sys.Date(),".csv"),
 #                  sep = "/"))

# Get grantee matches
grantee_matches <- grantees %>%
  filter(!is.na(grantee_id_era2)) %>%
  left_join(
    grantee_files %>%
      select(grantee_id_era2, original_filename),
    by = "grantee_id_era2"
  )

# # Write out
 # write_csv(grantee_matches,
 #           str_c(data_path, "1_intermediates", "grantees",
 #                 str_c("era2_q4_grantees_", Sys.Date(),".csv"),
 #                 sep = "/"))

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
  # filter: removed 2 rows (<1%), 5,954,901 rows remaining
  filter(!address_line_1 %in% c("Total", "Powered by: Middlelayers.org"))

# Shifted columns testing -------------------------------------------------

# A series of tests to determine if any grantees shifted or mislabeled columns

# Testing if amount_of_payment could be an Excel-coded date
# Calcasieu Parish and Jefferson County (CO) are shifted
shift_test_amount_of_payment <- step_7 %>%
  filter(amount_of_payment > 44000 & amount_of_payment < 46000) %>%
  tabyl(grantee_name) %>%
  arrange(desc(n))

calcasieu <- step_7 %>%
  filter(grantee_name == "Calcasieu Parish, Louisiana")

jefferson <- step_7 %>%
  filter(grantee_name == "Jefferson County Colorado")

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
  filter(!grantee_name %in% c("Calcasieu Parish, Louisiana", "Jefferson County Colorado"))

# Fix Calcasieu Parish
calcasieu_fixed <- calcasieu %>%
  select(-date_of_payment) %>%
  rename(
    date_of_payment = amount_of_payment,
    amount_of_payment = payee_type,
    payee_type = zip4,
    zip4 = zip5,
    zip5 = state_code,
    state_code = city_name,
    city_name = address_line_3,
    address_line_3 = address_line_2,
    address_line_2 = address_line_1
  ) %>%
  mutate(date_of_payment = excel_numeric_to_date(date_of_payment)) %>%
  mutate(amount_of_payment = as.numeric(amount_of_payment))

# Fix Jefferson County
jefferson_fixed <- jefferson %>%
  select(-date_of_payment, -state_code) %>%
  rename(
    date_of_payment = amount_of_payment,
    amount_of_payment = payee_type,
    payee_type = zip4,
    state_code = city_name,
    city_name = address_line_3
  ) %>%
  mutate(date_of_payment = excel_numeric_to_date(date_of_payment)) %>%
  mutate(amount_of_payment = as.numeric(amount_of_payment))

# Rejoin bad grantees
step_8b <- step_8a %>%
  bind_rows(calcasieu_fixed) %>%
  bind_rows(jefferson_fixed)

# Geocodes validation -----------------------------------------------------

geocodes_joined <- step_8b %>%
  left_join(
    geocodes %>%
      select(
        development_code,
        std_addr,
        apt_no,
        std_zip5,
        state2ky,
        std_st,
        curcnty,
        curcnty_nm,
        curcosub,
        curcosub_nm,
        place2ky,
        place_nm2ky,
        tract2ky,
        lat,
        lon,
        msg2ky,
        msgusps
      ),
    by = "development_code"
  )

# # Initial comparison of geocode goodness
# states <- tigris::states() %>% sf::st_drop_geometry()
#
# geocodes_validation <- geocodes_joined %>%
#   left_join(states %>% select(GEOID, geocoded_state = STUSPS),
#             by = c(state2ky = "GEOID")) %>%
#   mutate(state_match = state_code == geocoded_state) %>%
#   mutate(zip_match = zip5 == std_zip5)
#
# tabyl(geocodes_validation$state_match)
#
# # geocodes_validation$state_match       n     percent valid_percent
# # FALSE   13055 0.002192312   0.002252874
# # TRUE 5781765 0.970925461   0.997747126
# # NA  160081 0.026882227            NA
#
# tabyl(geocodes_validation$zip_match)
#
# # geocodes_validation$zip_match       n    percent valid_percent
# # FALSE  161337 0.02709315    0.02835948
# # TRUE 5527661 0.92825405    0.97164052
# # NA  265903 0.04465280            NA

# Preparing geocoded variables ----------------------------------------------

step_9 <- geocodes_joined %>%
  rename_with(~ case_when(. %in% names(step_8b) ~ ., .default = str_c("geocode_", .))) %>%
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

# # Data ready for next script
# write_parquet(step_9,
#           str_c(data_path, "1_intermediates", "phpdfs", "2_initial_validation",
#                 str_c("era2_q4_initial_validation_", Sys.Date(),".parquet"),
#                 sep = "/"))

toc()
# 145.646 sec elapsed
