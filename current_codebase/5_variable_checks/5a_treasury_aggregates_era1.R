#### DESCRIPTION ####
# This script filters the ERA data to only include good-quality data.
# Filter to state/local programs where we have >X% good geocodes/dates/etc.
# Filter to programs where the agg amounts are valid compared to Dept of Treasury data

# Input: ERA data merged with geocodes, de-duped; treasury agg data files
# Output: filtered cleaned ERA data

# Jacob H. (Last Updated Feb. 2025)

#### SETUP ####

library(tidyverse)
library(readxl)
library(tigris)
library(sf)
library(tidylog)
library(tictoc)
library(arrow)

tic()
options(scipen = 999)

# Locally specific part of data folder path already stored in Renviron file
data_path <- str_c(Sys.getenv("LOCAL_PATH"), 
                   "era-county-level-dataset-data", 
                   sep = "/")

options(tigris_use_cache = TRUE)

#### USER-INPUT VARIABLES ####

# what bounds do you want to set to validate against treasury-reported aggregates?
# e.g., 50 = 50% of treasury aggs
treasury_lower_bound <- .5

treasury_upper_bound <- 1.5

#### LOAD DATA ####
phpdf_ERA1_cleaned <- read_parquet(
  str_c(
    data_path,
    "1_intermediates", 
    "phpdfs", 
    "4_county_imputation",
    "era1_imputed_counties_2025-03-21.parquet",
    sep = "/")
)

era1_expenditures_by_program <- read_excel(
  str_c(
    data_path,
    "0_raw_data", 
    "public_data",
    "treasury_data", 
    "Q1-2021-Q4-2022-ERA-Demographic-Data.xlsx",
    sep = "/"
  ),
  sheet = "ERA1 State & Local Expenditure"
)

ERA_grantees_combined <- read_rds(
  str_c(data_path,
        "1_intermediates",
        "grantees",
        "grantees_combined_2025-03-06.RDS", 
        sep ="/"
  )
)

#get xwalk of state names to abbreviations
territory_info <- usa::territory %>%
  select(name, abb) %>%
  rename(
    State = name,
    State_Abb = abb
  ) 

state_info <- tibble(
  State = state.name,
  State_Abb = state.abb
) %>%
  bind_rows(territory_info)

state_fips_codes <- tigris::fips_codes %>%
  select(state, state_code) %>%
  distinct() %>%
  rename(state_abb = state)

#### CLEAN TREASURY PROGRAM FILES ####

era1_expenditures_by_program_cleaned <- era1_expenditures_by_program %>%
  mutate(across(2:9, ~ as.numeric(.x))) %>%
  mutate(
    total_assistance_spending = rowSums(.[2:9], na.rm = TRUE),
    total_spending = as.numeric(.$...26), 
    total_award = as.numeric(.$...28),
  ) %>% 
  select(
    1,
    total_assistance_spending,
    total_spending, 
    total_award
  ) %>%
  rename(program = 1) %>%
  filter(!str_detect(program, "^[0-9]")) %>% 
  mutate(state = if_else(program %in% state_info$State,
                         program,
                         NA_character_
  )) %>%
  fill(state,
       .direction = "down"
  ) %>%
  mutate(
    state = if_else(
      state == "Wyoming" & program != "State of Wyoming",
      str_extract(program, "(?<=, ).*"),
      state
    ),
    state = if_else(
      program == "Commonwealth Of The Northern Mariana Islands",
      "Northern Mariana Islands",
      state
    ),
    state = if_else(program == "United States Virgin Islands",
                    "U.S. Virgin Islands",
                    state
    ),
    state = if_else(state == "MICHIGAN",
                    "Michigan",
                    state
    )
  ) %>%
  mutate(grantee_type = if_else(program %in% c("State/DC", 
                                               "Local Government", 
                                               "Territorial Government"), 
                                program, 
                                NA_character_)) %>% 
  fill(grantee_type,
       .direction = "down"
  ) %>%
  filter(
    !is.na(program),
    !is.na(total_assistance_spending) |
      !is.na(total_spending) |
      !is.na(total_award),
    !program %in% c(
      "State/DC",
      "Local Government",
      "Total Reported as of End of Q1 2024",
      "Territorial Government",
      "Grand Total",
      "Government of the United States Virgin Islands", 
      "Department of Hawaiian Homelands"
    ),
    !str_detect(program, "Non-Submitters"),
    !(program %in% state_info$State 
      & !program %in% c("District of Columbia", "Northern Mariana Islands")), 
    #get rid of duplicate DC row
    !(program == "District of Columbia" & grantee_type == "Territorial Government"), 
    !(program == "Northern Mariana Islands" & grantee_type == "State/DC")
  ) %>%
  left_join(state_info, 
            by = c("state" = "State")) %>% 
  mutate(across(c(total_assistance_spending, 
                  total_spending,
                  total_award),
                ~ replace_na(.x, 0))) %>% 
  mutate(grantee_name = str_replace(program, "\\bSt\\.", "St"), 
         grantee_name = str_remove(grantee_name, "'"),
         grantee_name = case_when(grantee_name == "City of Salt Lake City" ~ "Salt Lake City", 
                                  grantee_name == "United States Virgin Islands" ~ "GOVERNMENT OF THE VIRGIN ISLANDS",
                                  TRUE ~ grantee_name),
         grantee_name = str_to_upper(grantee_name))

# Create ERA1_grantees_w_expenditures
ERA1_grantees_w_expenditures <- ERA_grantees_combined %>%
  left_join(era1_expenditures_by_program_cleaned,
            by = c("grantee_name",
                   "grantee_type",
                   "grantee_state" = "state"
            )
  ) %>% 
  select(
    grantee_name, 
    grantee_type, 
    grantee_state,
    grantee_id_era1, 
    grantee_id_combined,
    program_id_era1,
    total_spending,
    total_assistance_spending, 
    total_award
  )


#### GET COMPARISON TO TREASURY AGGS BY PROGRAM ####

phpdf_by_program <- phpdf_ERA1_cleaned %>%
  group_by(program,
           grantee_id_combined,
           grantee_name,
           grantee_type,
           grantee_state) %>%
  summarize(total_amount = sum(amount_of_payment, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(ERA1_grantees_w_expenditures, by =  "grantee_id_combined") %>%
  # three different 'total' columns in the treasury file to compare to
  mutate(
    pct_of_spending_in_php_file_quarterly = total_amount / total_assistance_spending,
    pct_of_spending_in_php_file_summation = total_amount / total_spending,
    pct_of_allocation = total_amount / total_award,
    across(
      c(
        pct_of_spending_in_php_file_quarterly,
        pct_of_spending_in_php_file_summation,
        pct_of_allocation
      ),
      ~ if_else(.x == Inf, NA_real_, .x)
    )
  ) %>%
  group_by(grantee_state.x) %>% 
  mutate(
    state_total_amount = sum(total_amount, na.rm = TRUE),
    state_pct_of_spending_in_php_file_quarterly = state_total_amount / sum(total_assistance_spending, na.rm = TRUE),
    state_pct_of_spending_in_php_file_summation = state_total_amount / sum(total_spending, na.rm = TRUE),
    state_pct_of_allocation = state_total_amount / sum(total_award, na.rm = TRUE),
    across(
      c(state_pct_of_spending_in_php_file_quarterly,
        state_pct_of_spending_in_php_file_summation,
        state_pct_of_allocation),
      ~ if_else(.x == Inf, NA_real_, .x)
    )
  ) %>%
  ungroup() %>% 
  select(-grantee_name.y, 
         -grantee_type.y, 
         -grantee_state.y) %>%
  rename(grantee_name = grantee_name.x,
         grantee_type = grantee_type.x,
         grantee_state = grantee_state.x)


# we only want programs if they fall within a certain range of treasury-reported aggregates
#  here, set to between 50 and 150% of agg data
#  only want programs where we have good data based on previous criteria for >85% of program
good_programs <- phpdf_by_program %>%
  select(program, 
         grantee_id_combined, 
         grantee_name, 
         grantee_type, 
         grantee_state, 
         total_amount,
         state_total_amount,
         pct_of_spending_in_php_file_quarterly, 
         pct_of_spending_in_php_file_summation, 
         pct_of_allocation) %>% 
  filter((pct_of_spending_in_php_file_quarterly <= treasury_upper_bound &
            pct_of_spending_in_php_file_quarterly >= treasury_lower_bound) |
           (pct_of_spending_in_php_file_summation <= treasury_upper_bound &
              pct_of_spending_in_php_file_summation >= treasury_lower_bound) |
           (pct_of_allocation <= treasury_upper_bound &
              pct_of_allocation >= treasury_lower_bound))

# Look when filtering by state
good_programs_state <- phpdf_by_program %>%
  select(program, 
         grantee_id_combined, 
         grantee_name, 
         grantee_type, 
         grantee_state, 
         total_amount,
         state_total_amount,
         state_pct_of_spending_in_php_file_quarterly, 
         state_pct_of_spending_in_php_file_summation, 
         state_pct_of_allocation) %>% 
  filter((state_pct_of_spending_in_php_file_quarterly <= treasury_upper_bound &
            state_pct_of_spending_in_php_file_quarterly >= treasury_lower_bound) |
           (state_pct_of_spending_in_php_file_summation <= treasury_upper_bound &
              state_pct_of_spending_in_php_file_summation >= treasury_lower_bound) |
           (state_pct_of_allocation <= treasury_upper_bound &
              state_pct_of_allocation >= treasury_lower_bound))


bad_programs <- phpdf_by_program %>%
  filter(!grantee_id_combined %in%
           good_programs$grantee_id_combined)


# #### SANITY-CHECK PROGRAM FILTERS ####

#X% of programs
nrow(good_programs) / nrow(phpdf_by_program)

#X% of programs
nrow(good_programs_state) / nrow(phpdf_by_program)

# X% of total spending included
sum(good_programs$total_amount, na.rm = TRUE) /
  sum(phpdf_by_program$total_amount, na.rm = TRUE)


# X% of total spending included (State)
sum(good_programs_state$total_amount, na.rm = TRUE) /
  sum(phpdf_by_program$total_amount, na.rm = TRUE)


# save files
write_rds(
  phpdf_by_program,
  str_c(data_path,
        "1_intermediates",
        "phpdfs",
        "5_variable_checks",
        str_c("phpdf_by_program_era1_", Sys.Date(), ".rds"),
        sep = "/"
  ))

toc()
#7.613 sec elapsed
