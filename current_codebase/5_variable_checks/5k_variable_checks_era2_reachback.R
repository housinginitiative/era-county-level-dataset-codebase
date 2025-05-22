# Purpose -----------------------------------------------------------------
# Data quality validation for each variable, plus summaries thereof.

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(janitor)
library(readxl)
library(tictoc)
library(arrow)

tic()

# Locally specific part of data folder path already stored in Renviron file
data_path = str_c(Sys.getenv("LOCAL_PATH"), "era-county-level-dataset-data", sep = "/")

# Load PHPDF data ---------------------------------------------------------

# Deduplicated ERA2_reachback
era2_reachback_initial <- read_parquet(str_c(data_path, "1_intermediates", 
                                   "phpdfs",
                                   "4_county_imputation",
                                   "era2_reachback_imputed_counties_2025-03-21.parquet",
                                   sep = "/"))
  
  # Load grantee geographies ------------------------------------------------

grantees_states <- 
  read_rds(str_c(data_path, "1_intermediates", "grantees",
                 str_c("grantees_geographies_states", "_", "2025-03-20", ".RDS"),
                 sep = "/")) %>% 
  mutate(in_jurisdiction_state = TRUE)

grantees_counties <- 
  read_rds(str_c(data_path, "1_intermediates", "grantees",
                 str_c("grantees_geographies_counties", "_", "2025-03-20", ".RDS"),
                 sep = "/")) %>% 
  mutate(in_jurisdiction_county = TRUE)

grantees_cities_places <- 
  read_rds(str_c(data_path, "1_intermediates", "grantees",
                 str_c("grantees_geographies_cities_places", "_", "2025-03-20", ".RDS"),
                 sep = "/")) %>% 
  mutate(in_jurisdiction_city_place = TRUE)

grantees_cities_mcds <- 
  read_rds(str_c(data_path, "1_intermediates", "grantees",
                 str_c("grantees_geographies_cities_mcds", "_", "2025-03-20", ".RDS"),
                 sep = "/")) %>% 
  mutate(in_jurisdiction_city_mcd = TRUE)

grantees_cities_tracts <- 
  read_rds(str_c(data_path, "1_intermediates", "grantees",
                 str_c("grantees_geographies_cities_tracts", "_", "2025-03-20", ".RDS"),
                 sep = "/")) %>% 
  mutate(in_jurisdiction_city_tract = TRUE)

city_county_crosswalk <- 
  read_rds(str_c(data_path, "1_intermediates", "grantees",
                 str_c("grantees_city_geographic_county_crosswalk", "_", "2025-03-20", ".RDS"),
                 sep = "/")) %>% 
  mutate(in_jurisdiction_city_imputed_county = TRUE)

grantees_combined <- read_rds(str_c(data_path, "1_intermediates", "grantees",
                                    str_c("grantees_combined", "_", "2025-03-06", ".RDS"),
                                    sep = "/"))

# Load allocation data ----------------------------------------------------

allocations <- 
  read_excel(str_c(data_path, 
                   "0_raw_data", 
                   "public_data",
                   "treasury_data", 
                   "ERA1 & ERA2 Allocations as of 06.21.2024.xlsx", 
                   sep = "/"),
             sheet = "ERA2") %>% 
  clean_names()

# Load treasury aggregates analysis -----------------------------------------------------------

# Analysis of % of reported aggregate spending spent
treasury_aggregates <- read_rds(str_c(data_path, 
                                      "1_intermediates", 
                                      "phpdfs", 
                                      "5_variable_checks",
                                      "phpdf_by_program_era2_reachback_2025-03-21.rds",
                                      sep = "/"))
  
  # Extreme values ----------------------------------------------------------

# 99.9th percentile value for both ERA1 and ERA2 (Q4) calculated above in step 5f
# $73,541
large_dollar_threshold <- 
  read_rds(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                 "amount_of_payment_quantile_999_2025-03-21.rds",
                 sep = "/"))

# Row-level checks (address) ----------------------------------------------

# In-jurisdiction check
checks_jurisdiction_era2_reachback <- era2_reachback_initial %>% 
  # The following joins will compare the relevant GEOIDs of the grantee jurisdictions to the geocodes:
  # state GEOID if the grantee is a state entity, county GEOID for county-level grantees,
  # and Place/MCD/tract GEOIDs for city grantees.
  left_join(grantees_states %>% 
              filter(!is.na(grantee_id_era2)) %>% 
              select(grantee_id_era2, geoid_jurisdiction_state, in_jurisdiction_state),
            by = c("grantee_id_era2", state_geoid_coalesced = "geoid_jurisdiction_state")) %>% 
  left_join(grantees_counties %>% 
              filter(!is.na(grantee_id_era2)) %>% 
              select(grantee_id_era2, geoid_jurisdiction_county, in_jurisdiction_county),
            by = c("grantee_id_era2", county_geoid_coalesced = "geoid_jurisdiction_county")) %>% 
  left_join(grantees_cities_places %>%
              filter(!is.na(grantee_id_era2)) %>%
              select(grantee_id_era2, geoid_jurisdiction_place, in_jurisdiction_city_place),
            by = c("grantee_id_era2", geocode_place_geoid = "geoid_jurisdiction_place")) %>%
  left_join(grantees_cities_mcds %>%
              filter(!is.na(grantee_id_era2)) %>%
              select(grantee_id_era2, geoid_jurisdiction_mcd, in_jurisdiction_city_mcd),
            by = c("grantee_id_era2", geocode_county_subdivision_geoid = "geoid_jurisdiction_mcd")) %>%
  left_join(grantees_cities_tracts %>% 
              filter(!is.na(grantee_id_era2)) %>% 
              select(grantee_id_era2, geoid_jurisdiction_tract, in_jurisdiction_city_tract),
            by = c("grantee_id_era2", geocode_tract_geoid = "geoid_jurisdiction_tract")) %>% 
  left_join(city_county_crosswalk %>%
              filter(!is.na(grantee_id_era2)) %>%
              select(grantee_id_era2, county_geoid, in_jurisdiction_city_imputed_county),
            by = c("grantee_id_era2", imputed_county_geoid = "county_geoid")) %>%
  # Coalesce to one variable; for city grantees, privileging the most boundary-specific geographies
  mutate(in_jurisdiction =
           coalesce(in_jurisdiction_state, 
                    in_jurisdiction_county, 
                    in_jurisdiction_city_place, 
                    in_jurisdiction_city_mcd,
                    in_jurisdiction_city_tract,
                    in_jurisdiction_city_imputed_county)) %>% 
  # If there is no join, NAs are actually FALSE, unless there was no geocode to begin with
  mutate(in_jurisdiction = replace_na(in_jurisdiction, FALSE)) %>% 
  mutate(in_jurisdiction =
           case_when(grantee_geographic_level == "State" & 
                       is.na(state_geoid_coalesced) ~
                       NA,
                     grantee_geographic_level == "County" & 
                       is.na(county_geoid_coalesced) ~
                       NA,
                     grantee_geographic_level == "City" &
                       (is.na(geocode_place_geoid) & 
                          is.na(geocode_county_subdivision_geoid) & 
                          is.na(geocode_tract_geoid)& is.na(imputed_county_geoid)) ~
                       NA,
                     .default = in_jurisdiction)) %>% 
  # Factorized version of variable
  mutate(check_address_within_jurisdiction =
           case_when(in_jurisdiction == TRUE ~ "Nominal",
                     in_jurisdiction == FALSE ~ "Address outside grantee jurisdiction",
                     is.na(in_jurisdiction) ~ "Data not adequate to check"),
         .before = original_filename) %>% 
  mutate(check_address_within_jurisdiction = as_factor(check_address_within_jurisdiction))

# Other address checking
checks_address_era2_reachback <- era2_reachback_initial %>% 
  # Raw addresses
  mutate(po_box_status = str_detect(str_to_upper(str_remove_all(address_line_1, "\\.|,|-| ")),
                                    "POBOX"),
         .before = original_filename) %>% 
  mutate(invalid_address = 
           case_when(is.na(address_line_1) ~ TRUE,
                     po_box_status == TRUE ~ TRUE,
                     !str_detect(address_line_1, "^[0-9]") ~ TRUE,
                     !str_detect(address_line_1, "[a-z]|[A-Z]") ~ TRUE,
                     !str_detect(address_line_1, "[0-9]") ~ TRUE,
                     .default = FALSE),
         .before = original_filename) %>%
  mutate(check_raw_address = 
           case_when(invalid_address == FALSE ~ "Nominal",
                     invalid_address & !is.na(city_name) & !is.na(zip5) ~ "ZIP + city only",
                     .default = "No usable address"),
         .before = original_filename) %>% 
  mutate(check_raw_address = as_factor(check_raw_address)) %>% 
  mutate(check_county_assignment = 
           case_when(!is.na(county_geoid_coalesced) ~ "Nominal",
                     .default = "No county assignment"),
         .before = original_filename) %>% 
  mutate(check_county_assignment = as_factor(check_county_assignment)) %>% 
  # Geocoding goodness
  mutate(check_geocode_to_rooftop = 
           case_when(geocode_msg2ky == "** Street-Level Rooftop **" ~ 
                       "Nominal",
                     is.na(geocode_msg2ky) ~ 
                       "Not geocoded or geocoding metrics unavailable",
                     .default = "Geocoded not to rooftop"),
         .before = original_filename) %>% 
  mutate(check_geocode_to_rooftop = as_factor(check_geocode_to_rooftop)) %>% 
  mutate(check_geocode_address_accuracy = 
           case_when(str_detect(geocode_msgusps, "(8|9|10)0%") ~ 
                       "Nominal",
                     is.na(geocode_msgusps) ~ 
                       "Not geocoded or geocoding metrics unavailable",
                     .default = "Geocoded with less than 80% accuracy"),
         .before = original_filename) %>% 
  mutate(check_geocode_address_accuracy = as_factor(check_geocode_address_accuracy)) %>% 
  mutate(check_geocode_zip_match =
           case_when(is.na(zip5) | is.na(geocode_std_zip5) ~ "Data not adequate to check",
                     zip5 == geocode_std_zip5 ~ "Nominal",
                     zip5 != geocode_std_zip5 ~ "ZIP mismatch",
                     .default = NA),
         .before = original_filename) %>% 
  mutate(check_geocode_zip_match = as_factor(check_geocode_zip_match))

# Row-level checks (non-address) ------------------------------------------

checks_nonaddress_era2_reachback <- era2_reachback_initial %>% 
  # Payee type
  mutate(check_payee_type =
           case_when(is.na(payee_type) ~ "No value",
                     .default = "Nominal"),
         .before = original_filename) %>% 
  mutate(check_payee_type = as_factor(check_payee_type)) %>% 
  # Assistance type
  mutate(check_type_of_assistance =
           case_when(is.na(type_of_assistance) ~ "No value",
                     .default = "Nominal"),
         .before = original_filename) %>% 
  mutate(check_type_of_assistance = as_factor(check_type_of_assistance)) %>% 
  # Amount of payment
  mutate(check_amount_of_payment =
           case_when(is.na(amount_of_payment) ~ "No value",
                     amount_of_payment < 0 ~ "Negative amount",
                     amount_of_payment == 0 ~ "Zero amount",
                     amount_of_payment > large_dollar_threshold ~ "Amount above 99.9th percentile",
                     .default = "Nominal"),
         .before = original_filename) %>% 
  mutate(check_amount_of_payment = as_factor(check_amount_of_payment)) %>% 
  # Date of payment
  mutate(check_date_of_payment =
           case_when(is.na(date_of_payment) ~ "No value",
                     date_of_payment < "2021-03-01" ~ "Incorrectly early",
                     date_of_payment > "2023-12-31" ~ "Incorrectly late",
                     .default = "Nominal"),
         .before = original_filename) %>% 
  mutate(check_date_of_payment = as_factor(check_date_of_payment))

# Row-level checks (collate) ----------------------------------------------

checks_all_era2_reachback <- era2_reachback_initial %>% 
  left_join(checks_jurisdiction_era2_reachback %>% 
              select(file_specific_row_id, contains("check_")),
            by = "file_specific_row_id") %>% 
  left_join(checks_address_era2_reachback %>% 
              select(file_specific_row_id, contains("check_")),
            by = "file_specific_row_id") %>% 
  left_join(checks_nonaddress_era2_reachback %>% 
              select(file_specific_row_id, contains("check_")),
            by = "file_specific_row_id")

# Row-level goodness labels ------------------------------------------------------------

checks_labeled_era2_reachback <- checks_all_era2_reachback %>% 
  mutate(nominal_county = 
           check_county_assignment == "Nominal") %>% 
  mutate(nominal_jurisdiction = 
           check_address_within_jurisdiction == "Nominal") %>% 
  mutate(nominal_payment_amount = 
           check_amount_of_payment == "Nominal") %>% 
  mutate(nominal_payment_date = 
           check_date_of_payment == "Nominal") %>% 
  mutate(nominal_payee_type = 
           check_payee_type == "Nominal") %>% 
  mutate(nominal_assistance_type = 
           check_type_of_assistance == "Nominal") %>% 
  mutate(ok_county_total =
           if_else(nominal_county & nominal_jurisdiction & nominal_payment_amount, 
                   TRUE, FALSE)) %>% 
  mutate(ok_county_month =
           if_else(nominal_county & nominal_jurisdiction & nominal_payment_amount &
                     nominal_payment_date,
                   TRUE, FALSE)) %>% 
  mutate(ok_county_month_payee_type =
           if_else(nominal_county & nominal_jurisdiction & nominal_payment_amount &
                     nominal_payment_date & nominal_payee_type,
                   TRUE, FALSE)) %>% 
  mutate(ok_county_month_assistance_type =
           if_else(nominal_county & nominal_jurisdiction & nominal_payment_amount &
                     nominal_payment_date & nominal_assistance_type,
                   TRUE, FALSE)) %>% 
  mutate(ok_county_month_payee_type_assistance_type =
           if_else(nominal_county & nominal_jurisdiction & nominal_payment_amount &
                     nominal_payment_date & nominal_payee_type & nominal_assistance_type,
                   TRUE, FALSE)) 

# Summary checks functions --------------------------------------------------------------------

# Summary by single check variable + grantee
summarize_by_column_single <- function(data, variable) {
  
  output <- data %>% 
    count(grantee_id_combined, program, grantee_name, grantee_state, grantee_type, .data[[variable]],
          .drop = FALSE) %>% 
    group_by(grantee_id_combined, program, grantee_name, grantee_state, grantee_type) %>% 
    mutate(percent = n / sum(n)) %>% 
    ungroup() %>% 
    mutate(variable = variable, .after = grantee_type) %>% 
    rename(status = .data[[variable]]) %>% 
    complete(variable, status, fill = list(n = 0))
  
}

# Summary by all check variables + grantee
summarize_by_column_all <- function(data) {
  
  size <- data %>% 
    summarize(.by = c(grantee_id_combined, program, grantee_name, grantee_state, grantee_type),
              total_n = n())
  
  status_variables <- names(data)[str_detect(names(data), "check_")]
  
  output <- status_variables %>% 
    map_dfr(~ summarize_by_column_single(data, .)) %>% 
    left_join(size %>% 
                select(grantee_id_combined, total_n),
              by = "grantee_id_combined") %>% 
    relocate(program, grantee_id_combined, grantee_name, grantee_state, grantee_type,
             total_n, variable, status, n, percent) %>% 
    arrange(grantee_state, desc(grantee_type), grantee_name, variable, status)
  
}

# Summarize by row + grantee
summarize_by_row_grantees <- function(data_labeled) {
  
  size <- data_labeled %>% 
    summarize(.by = c(grantee_id_combined, program, grantee_name, grantee_state, grantee_type),
              total_n = n())
  
  county_total <- data_labeled %>% 
    count(grantee_id_combined, program, grantee_name, grantee_state, grantee_type, 
          ok_county_total,
          .drop = FALSE) %>% 
    group_by(grantee_id_combined, program, grantee_name, grantee_state, grantee_type) %>% 
    mutate(ok_percent = n / sum(n)) %>% 
    ungroup() %>% 
    filter(ok_county_total == "TRUE") %>% 
    select(-ok_county_total) %>% 
    mutate(scenario = "County total only", .before = everything())
  
  county_month <- data_labeled %>% 
    count(grantee_id_combined, program, grantee_name, grantee_state, grantee_type, 
          ok_county_month,
          .drop = FALSE) %>% 
    group_by(grantee_id_combined, program, grantee_name, grantee_state, grantee_type) %>% 
    mutate(ok_percent = n / sum(n)) %>% 
    ungroup() %>% 
    filter(ok_county_month == "TRUE") %>% 
    select(-ok_county_month) %>% 
    mutate(scenario = "County + month", .before = everything())
  
  county_payee_type <- data_labeled %>% 
    count(grantee_id_combined, program, grantee_name, grantee_state, grantee_type, 
          ok_county_month_payee_type,
          .drop = FALSE) %>% 
    group_by(grantee_id_combined, program, grantee_name, grantee_state, grantee_type) %>% 
    mutate(ok_percent = n / sum(n)) %>% 
    ungroup() %>% 
    filter(ok_county_month_payee_type == "TRUE") %>% 
    select(-ok_county_month_payee_type) %>% 
    mutate(scenario = "County + month + payee type", .before = everything())
  
  county_assistance_type <- data_labeled %>% 
    count(grantee_id_combined, program, grantee_name, grantee_state, grantee_type, 
          ok_county_month_assistance_type,
          .drop = FALSE) %>% 
    group_by(grantee_id_combined, program, grantee_name, grantee_state, grantee_type) %>% 
    mutate(ok_percent = n / sum(n)) %>% 
    ungroup() %>% 
    filter(ok_county_month_assistance_type == "TRUE") %>% 
    select(-ok_county_month_assistance_type) %>% 
    mutate(scenario = "County + month + assistance type", .before = everything())
  
  county_payee_type_assistance_type <- data_labeled %>% 
    count(grantee_id_combined, program, grantee_name, grantee_state, grantee_type, 
          ok_county_month_payee_type_assistance_type,
          .drop = FALSE) %>% 
    group_by(grantee_id_combined, program, grantee_name, grantee_state, grantee_type) %>% 
    mutate(ok_percent = n / sum(n)) %>% 
    ungroup() %>% 
    filter(ok_county_month_payee_type_assistance_type == "TRUE") %>% 
    select(-ok_county_month_payee_type_assistance_type) %>% 
    mutate(scenario = "County + month + payee type + assistance type", .before = everything())
  
  bound_county <- county_total %>% 
    bind_rows(county_month) %>% 
    bind_rows(county_payee_type) %>% 
    bind_rows(county_assistance_type) %>% 
    bind_rows(county_payee_type_assistance_type) %>% 
    rename(ok_n = n) %>% 
    complete(nesting(grantee_id_combined, program, grantee_name, grantee_state, grantee_type),
             scenario,
             fill = list(ok_n = 0, ok_percent = 0)) %>% 
    left_join(size %>%  
                select(grantee_id_combined, total_n),
              by = "grantee_id_combined")
  
  # Insert grantees which had 0 records ok for county_total
  empty_grantees <- size %>% 
    filter(!grantee_id_combined %in% bound_county$grantee_id_combined) %>% 
    cross_join(tibble(scenario = unique(bound_county$scenario))) %>%
    mutate(ok_n = 0,
           ok_percent = 0)
  
  out <- bound_county %>% 
    bind_rows(empty_grantees) %>% 
    arrange(grantee_state, desc(grantee_type), grantee_name, scenario)
  
}

# Produce summary outputs ---------------------------------------------------------------------

# Summary by column
summary_variable_era2_reachback <- summarize_by_column_all(checks_all_era2_reachback)

# Summary by scenarios
summary_scenarios_era2_reachback <- summarize_by_row_grantees(checks_labeled_era2_reachback)

# Percentage payments for rental assistance
summary_rent_percent <- era2_reachback_initial %>% 
  mutate(rent = str_detect(type_of_assistance, "Rent")) %>% 
  mutate(rent = as_factor(rent)) %>% 
  count(grantee_id_combined, rent, .drop = FALSE) %>% 
  group_by(grantee_id_combined) %>% 
  mutate(percent_rent_records = n / sum(n)) %>% 
  ungroup() %>% 
  filter(rent == TRUE) 

# Percentage spent of allocation
summary_allocation_percent <- era2_reachback_initial %>% 
  # filter: 
  filter(amount_of_payment > 0) %>% 
  summarize(.by = c(grantee_id_era2, grantee_id_combined, grantee_name),
            sum_spent = sum(amount_of_payment)) %>% 
  left_join(allocations %>% 
              select(grantee_id_era2 = slt_application_number,
                     total_updated_allocation),
            by = "grantee_id_era2") %>% 
  mutate(percent_of_allocation_spent = sum_spent / total_updated_allocation) %>% 
  arrange(percent_of_allocation_spent)

# Joined
summary_by_grantee_era2_reachback <- summary_scenarios_era2_reachback %>% 
  left_join(summary_rent_percent %>% 
              select(grantee_id_combined, percent_rent_records),
            by = "grantee_id_combined") %>% 
  left_join(summary_allocation_percent %>% 
              select(grantee_id_combined, sum_spent, percent_of_allocation_spent),
            by = "grantee_id_combined") %>% 
  mutate(across(c(sum_spent, percent_of_allocation_spent), ~replace_na(., 0))) %>% 
  left_join(grantees_combined %>% select(grantee_id_era2, grantee_id_combined), by = "grantee_id_combined") %>%
  left_join(allocations %>% 
              select(grantee_id_era2 = slt_application_number,
                     total_updated_allocation),
            by = "grantee_id_era2") %>% 
  left_join(treasury_aggregates %>% 
              distinct() %>% 
              select(grantee_id_combined, pct_of_spending_in_php_file_quarterly, 
                     pct_of_spending_in_php_file_summation, 
                     state_pct_of_spending_in_php_file_quarterly, 
                     state_pct_of_spending_in_php_file_summation), 
            by = "grantee_id_combined")

# Write out data ----------------------------------------------------------

# Full data including check variables
write_parquet(checks_labeled_era2_reachback,
              str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                    str_c("era2_reachback_with_variable_checks_", Sys.Date(),".parquet"),
                    sep = "/"))

# Variable-level variable checks summary
write_csv(summary_variable_era2_reachback,
          str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                str_c("era2_reachback_variable_checks_summary_", Sys.Date(),".csv"),
                sep = "/"))

# Row-level variable checks summary
write_csv(summary_by_grantee_era2_reachback,
          str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                str_c("era2_reachback_grantee_goodness_checks_summary_", Sys.Date(),".csv"),
                sep = "/"))

toc()
#128.62 sec elapsed
