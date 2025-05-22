# Purpose -----------------------------------------------------------------
# Validation concerning: rows and columns, general data type standardization,
# and grantee identification.

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(janitor)
library(arrow)
library(tictoc)
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
      "phpdf_ERA1_final_report_Data_Relate_table.parquet",
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
      "era1_geocodes_all_2025-03-06.parquet",
      sep = "/"
    )
  )
# Initial cleaning --------------------------------------------------------

# Standardize variable names
step_1 <- step_0 %>%
  clean_names() %>%
  rename(
    grantee_name = recipient_name,
    grantee_id_era1 = era_application_number,
    grantee_type = recipient_type,
    grantee_state = recipient_state
  ) %>%
  # grantee_id_era1 is an empty variable
  select(-unnamed_0, -id, -grantee_id_era1) %>%
  # Row/file ID variables
  mutate(file_specific_row_id = row_number(), .before = everything()) %>%
  mutate(original_filename = "phpdf_ERA1_final_report_Data_Relate_table.csv", .before = everything())

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

# # Get distinct combinations of grantee information in the dataset
# grantee_info_original <- step_5 %>%
#   distinct(grantee_name, grantee_state, grantee_type, file)
#
# # Check that grantee ID is not duplicated
# grantee_id_dupes_original <- grantee_info_original %>%
#   get_dupes(grantee_name, grantee_state)

# Fix records with bad grantee data (first round)
step_6a <- step_5 %>%
  mutate(
    grantee_name =
      case_when(grantee_name == "DoÃÂ±a Ana County" ~ "Doña Ana County", .default = grantee_name)
  ) %>%
  mutate(
    grantee_state =
      case_when(
        grantee_name == "Doña Ana County" ~ "NM",
        grantee_name == "Washington County, Oregon" ~ "OR",
        .default = grantee_state
      )
  ) %>%
  mutate(
    grantee_type =
      case_when(
        grantee_name == "Doña Ana County" ~ "Local Government",
        grantee_name == "Washington County, Oregon" ~ "Local Government",
        .default = grantee_type
      )
  )

# Because the data don't include grantee ID, need to match from existing crosswalk

grantee_names_phpdfs <- step_6a %>%
  distinct(grantee_name, grantee_state) %>%
  mutate(
    grantee_name_original_phpdf = grantee_name,
    .before = everything()
  ) %>%
  mutate(grantee_name = str_to_upper(grantee_name)) %>%
  mutate(grantee_name = str_remove(grantee_name, ",.+")) %>%
  mutate(grantee_name = str_remove(grantee_name, "GOVERNMENT")) %>%
  mutate(
    grantee_name =
      if_else(
        str_detect(grantee_name, "^COUNTY OF "),
        str_c(grantee_name, " COUNTY"),
        grantee_name
      )
  ) %>%
  mutate(grantee_name = str_remove(grantee_name, "COUNTY OF")) %>%
  mutate(grantee_name = str_remove(grantee_name, "(?<=COUNTY).+")) %>%
  mutate(
    grantee_name =
      if_else(
        str_detect(grantee_name, "^CITY OF "),
        str_c(grantee_name, " CITY"),
        grantee_name
      )
  ) %>%
  mutate(grantee_name = str_remove(grantee_name, "CITY OF")) %>%
  mutate(grantee_name = str_remove(grantee_name, "(?<=CITY).+")) %>%
  mutate(grantee_name = str_remove(grantee_name, "(DEPARTMENT|DEPT).+")) %>%
  mutate(grantee_name = str_squish(grantee_name)) %>%
  mutate(
    grantee_name =
      case_when(
        grantee_name_original_phpdf == "DEPARTMENT OF ADMINISTRATION, GOVERNMENT OF GUAM" ~ "OF GUAM",
        grantee_name_original_phpdf == "American Samoa Government" ~ "OF AMERICAN SAMOA",
        grantee_name_original_phpdf == "Arizona Department of Economic Security" ~ "STATE OF ARIZONA",
        grantee_name_original_phpdf == "Augusta" ~ "AUGUSTA CITY",
        grantee_name_original_phpdf == "CA Department of Housing and Community Development" ~ "STATE OF CALIFORNIA",
        grantee_name_original_phpdf == "City of Jacksonville Duval County portion" ~ "JACKSONVILLE CITY",
        grantee_name_original_phpdf == "City of Port St. Lucie, Florida" ~ "PORT ST LUCIE CITY",
        grantee_name_original_phpdf == "City of Portland Oregon" ~ "PORTLAND CITY",
        grantee_name_original_phpdf == "City of St. Louis Missouri" ~ "ST LOUIS CITY",
        grantee_name_original_phpdf == "Commonwealth of Massachusetts" ~ "STATE OF MASSACHUSETTS",
        grantee_name_original_phpdf == "Commonwealth of Puerto Rico" ~ "OF PUERTO RICO",
        grantee_name_original_phpdf == "Connecticut Office of Policy and Management" ~ "STATE OF CONNECTICUT",
        grantee_name_original_phpdf == "County of Lancaster Pennsylvania" ~ "LANCASTER COUNTY",
        grantee_name_original_phpdf == "Delaware State Housing Authority" ~ "STATE OF DELAWARE",
        grantee_name_original_phpdf == "Denver City, Colorado" ~ "CITY",
        grantee_name_original_phpdf == "DuPage, County Of" ~ "DUPAGE COUNTY",
        grantee_name_original_phpdf == "etropolitan Government of Nashville and Davidson County" ~ "NASHVILLE AND DAVIDSON COUNTY",
        grantee_name_original_phpdf == "Government of the United States Virgin Islands" ~ "OF THE VIRGIN ISLANDS",
        grantee_name_original_phpdf == "Governor David Y. Ige and the State of Hawaii" ~ "STATE OF HAWAII",
        grantee_name_original_phpdf == "IL Dept of Commerce & Economic Opportunity" ~ "STATE OF ILLINOIS",
        grantee_name_original_phpdf == "Iowa Finance Authority" ~ "STATE OF IOWA",
        grantee_name_original_phpdf == "KS Housing Resources Corp for the State of Kansas" ~ "STATE OF KANSAS",
        grantee_name_original_phpdf == "Lexington" ~ "LEXINGTON-FAYETTE URBAN COUNTY",
        grantee_name_original_phpdf == "Louisville" ~ "LOUISVILLE/JEFFERSON COUNTY",
        grantee_name_original_phpdf == "Miami" ~ "MIAMI-DADE COUNTY",
        grantee_name_original_phpdf == "Middlesex, County of (INC)" ~ "MIDDLESEX COUNTY",
        grantee_name_original_phpdf == "Minnesota" ~ "STATE OF MINNESOTA",
        grantee_name_original_phpdf == "Modesto, City of" ~ "MODESTO CITY",
        grantee_name_original_phpdf == "Monmouth, County of" ~ "MONMOUTH COUNTY",
        grantee_name_original_phpdf == "New York State" ~ "STATE OF NEW YORK",
        grantee_name_original_phpdf == "North Carolina" ~ "STATE OF NORTH CAROLINA",
        grantee_name_original_phpdf == "North Dakota Department of Human Services" ~ "STATE OF NORTH DAKOTA",
        grantee_name_original_phpdf == "Prince George's County, Maryland" ~ "PRINCE GEORGES COUNTY",
        grantee_name_original_phpdf == "Rutherford, County of" ~ "RUTHERFORD COUNTY",
        grantee_name_original_phpdf == "South Dakota Housing Development Authority" ~ "STATE OF SOUTH DAKOTA",
        grantee_name_original_phpdf == "St. Joseph County" ~ "ST JOSEPH COUNTY",
        grantee_name_original_phpdf == "St. Louis County Missouri" ~ "ST LOUIS COUNTY",
        grantee_name_original_phpdf == "St. Lucie County, Florida" ~ "ST LUCIE COUNTY",
        grantee_name_original_phpdf == "St. Petersburg city, Florida" ~ "ST PETERSBURG CITY",
        grantee_name_original_phpdf == "St. Tammany Parish, Louisiana" ~ "ST TAMMANY PARISH",
        grantee_name_original_phpdf == "State of New Hampshire Treasury" ~ "STATE OF NEW HAMPSHIRE",
        grantee_name_original_phpdf == "Texas Department of Housing and Community Affairs" ~ "STATE OF TEXAS",
        grantee_name_original_phpdf == "The Board of Mahoning County Commissioners" ~ "MAHONING COUNTY",
        grantee_name_original_phpdf == "The County of Volusia, Florida" ~ "VOLUSIA COUNTY",
        grantee_name_original_phpdf == "Town of Islip CDA" ~ "TOWN OF ISLIP",
        grantee_name_original_phpdf == "VA Department of Housing & Community Development" ~ "STATE OF VIRGINIA",
        grantee_name_original_phpdf == "Washington State Governor's Office" ~ "STATE OF WASHINGTON",
        grantee_name_original_phpdf == "Winston" ~ "WINSTON-SALEM CITY",
        grantee_name_original_phpdf == "Wisconsin, State of" ~ "STATE OF WISCONSIN",
        grantee_name_original_phpdf == "Yuma, County of" ~ "YUMA COUNTY",
        .default = grantee_name
      )
  ) %>%
  rename(grantee_state_abb = grantee_state)

grantee_names_nlihc <- grantees %>%
  filter(!is.na(grantee_id_era1)) %>%
  mutate(
    grantee_name_original_nlihc = grantee_name,
    .before = everything()
  ) %>%
  rename(grantee_state_name = grantee_state) %>%
  left_join(
    tibble(state_abb = state.abb, state_name = state.name) %>%
      add_row(state_abb = "AS", state_name = "American Samoa") %>%
      add_row(state_abb = "DC", state_name = "District of Columbia") %>%
      add_row(state_abb = "GU", state_name = "Guam") %>%
      add_row(state_abb = "MP", state_name = "Northern Mariana Islands") %>%
      add_row(state_abb = "PR", state_name = "Puerto Rico") %>%
      add_row(state_abb = "VI", state_name = "U.S. Virgin Islands"),
    by = c(grantee_state_name = "state_name")
  ) %>%
  rename(grantee_state_abb = state_abb) %>%
  select(
    grantee_name_original_nlihc,
    grantee_name,
    grantee_state_name,
    grantee_state_abb,
    grantee_id_era1
  ) %>%
  mutate(grantee_name = str_remove(grantee_name, ",.+")) %>%
  mutate(grantee_name = str_remove(grantee_name, "GOVERNMENT")) %>%
  mutate(
    grantee_name =
      if_else(
        str_detect(grantee_name, "^COUNTY OF "),
        str_c(grantee_name, " COUNTY"),
        grantee_name
      )
  ) %>%
  mutate(grantee_name = str_remove(grantee_name, "COUNTY OF")) %>%
  mutate(grantee_name = str_remove(grantee_name, "(?<=COUNTY).+")) %>%
  mutate(
    grantee_name =
      if_else(
        str_detect(grantee_name, "^CITY OF "),
        str_c(grantee_name, " CITY"),
        grantee_name
      )
  ) %>%
  mutate(grantee_name = str_remove(grantee_name, "CITY OF")) %>%
  mutate(grantee_name = str_remove(grantee_name, "(?<=CITY).+")) %>%
  mutate(grantee_name = str_remove(grantee_name, "(DEPARTMENT|DEPT).+")) %>%
  mutate(grantee_name = str_squish(grantee_name))

grantee_ids <- grantee_names_phpdfs %>%
  left_join(grantee_names_nlihc,
    by = c("grantee_name", "grantee_state_abb")
  )

# Add in grantee IDs
step_6b <- step_6a %>%
  left_join(
    grantee_ids %>%
      select(
        grantee_name = grantee_name_original_phpdf,
        grantee_state_name,
        grantee_state_abb,
        grantee_id_era1
      ),
    by = c("grantee_name", grantee_state = "grantee_state_abb")
  ) %>%
  # Join in ERA2 grantee IDs and geographic level
  left_join(
    grantees %>%
      select(
        grantee_id_era1,
        grantee_id_era2,
        grantee_id_combined,
        grantee_geographic_level = geographic_level
      ),
    by = "grantee_id_era1"
  ) %>%
  relocate(grantee_id_era1, .after = grantee_name) %>%
  relocate(grantee_id_era2, grantee_id_combined, .after = grantee_id_era1) %>%
  relocate(grantee_geographic_level, .after = grantee_type) %>%
  # Replace grantee_state from abbreviations to written-out names
  relocate(grantee_state_name, .after = grantee_state) %>%
  select(-grantee_state) %>%
  rename(grantee_state = grantee_state_name)

# Fixed distinct combinations of grantee information in the dataset
grantee_info_new <- step_6b %>%
  distinct(grantee_id_era1, grantee_name, grantee_state, grantee_type)

# Check that grantee ID is not duplicated
# No duplicate combinations found of: grantee_id_era1
grantee_id_dupes_new <- grantee_info_new %>%
  get_dupes(grantee_id_era1)

# Compare grantee info with known gold standard
grantee_info_validation <- grantee_info_new %>%
  rename_with(~ str_c(., "_phpdf")) %>%
  left_join(
    grantees %>%
      rename_with(~ str_c(., "_nlihc")),
    by = c(grantee_id_era1_phpdf = "grantee_id_era1_nlihc")
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
# Most grantees have multiple files
grantee_files <- step_6b %>%
  distinct(
    original_filename,
    grantee_id_era1,
    grantee_name,
    grantee_state,
    grantee_type,
    file
  ) %>%
  group_by(grantee_id_era1) %>%
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
 #                  str_c("era1_grantee_files_", Sys.Date(),".csv"),
 #                  sep = "/"))

# Get grantee matches
grantee_matches <- grantees %>%
  filter(!is.na(grantee_id_era1)) %>%
  left_join(
    grantee_files %>%
      select(grantee_id_era1, original_filename),
    by = "grantee_id_era1"
  )

# # Write out
 # write_csv(grantee_matches,
 #           str_c(data_path, "1_intermediates", "grantees",
 #                 str_c("era1_grantees_", Sys.Date(),".csv"),
 #                 sep = "/"))

# Sentinel values ---------------------------------------------------------

# Grantees may have had summary rows which are not actual data

sentinel_test0 <- step_6b %>%
  # Create field for number of NAs in a row
  mutate(na_count = rowSums(is.na(.)), .before = everything())

sentinel_test1 <- sentinel_test0 %>%
  tabyl(na_count)

sentinel_test2 <- sentinel_test0 %>%
  filter(na_count > 10) %>%
  filter(is.na(payee_type) | is.na(type_of_assistance)) %>%
  arrange(desc(na_count))

# Filtering out rows detected as problematic above
step_7 <- step_6b %>%
  # filter: removed 4 rows (<1%), 13,396,106 rows remaining
  # Butler County, OH
  filter(!address_line_1 %in% c("Total", "Powered by: Middlelayers.org"))

# Shifted columns ---------------------------------------------------------

# A series of tests to determine if any grantees shifted or mislabeled columns

# Testing if amount_of_payment could be an Excel-coded date
# Nominal, except MN is bad for other reasons
shift_test_amount_of_payment <- step_7 %>%
  filter(amount_of_payment > 44000 & amount_of_payment < 46000) %>%
  tabyl(grantee_name) %>%
  arrange(desc(n))

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
    date_of_payment > "2022-12-31")

# Testing if type_of_assistance has nonnominal values
# Nominal
tabyl(step_7$type_of_assistance)

shift_test_type_of_assistance <- step_7 %>%
  filter(!str_detect(type_of_assistance, "Financial Assistance"))

# Geocodes validation -----------------------------------------------------

geocodes_ready <- geocodes %>%
  select(
    development_code,
    std_addr,
    apt_no,
    std_zip5,
    state2ky = state,
    std_st,
    curcnty,
    curcnty_nm,
    curcosub,
    curcosub_nm,
    place2ky = place,
    place_nm2ky = place_nm,
    tract2ky = tract,
    lat,
    lon,
    msg2ky = msg,
    msgusps
  )

geocodes_joined <- step_7 %>%
  left_join(geocodes_ready, by = "development_code")

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
# # geocodes_validation$state_match        n      percent valid_percent
# # FALSE     9210 0.0006875132  0.0006942002
# # TRUE 13257856 0.9896798368  0.9993057998
# # NA   129040 0.0096326500            NA
#
# tabyl(geocodes_validation$zip_match)
#
# # geocodes_validation$zip_match        n    percent valid_percent
# # FALSE   301135 0.02247929    0.02271392
# # TRUE 12956594 0.96719106    0.97728608
# # NA   138377 0.01032964            NA

# Preparing geocoded variables ----------------------------------------------

step_8 <- geocodes_joined %>%
  rename_with(~ case_when(. %in% names(step_7) ~ ., .default = str_c("geocode_", .))) %>%
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
# write_parquet(step_8,
#            str_c(data_path,
#                  "1_intermediates",
#                  "phpdfs",
#                  "2_initial_validation",
#                  str_c("era1_initial_validation_", Sys.Date(),".parquet"),
#                  sep = "/"))

toc()
# 323.371 sec elapsed
