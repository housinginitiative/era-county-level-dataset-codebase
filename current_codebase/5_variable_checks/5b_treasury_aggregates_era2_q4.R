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
phpdf_ERA2_Q4_cleaned <- read_parquet(
  str_c(
    data_path,
    "1_intermediates", 
    "phpdfs", 
    "4_county_imputation",
    "era2_q4_imputed_counties_2025-03-21.parquet",
    sep = "/")
)

era2_q4_expenditures_by_program <- read_excel(
  str_c(
    data_path,
    "0_raw_data", 
    "public_data",
    "treasury_data", 
    "ERA2-Cumulative-Program-Data-Q2-2021-Q3-2024.xlsx",
    sep = "/"
  ),
  sheet = "ERA2 Obligations & Expenditures"
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

era2_q4_expenditures_by_program_cleaned <-  era2_q4_expenditures_by_program %>% 
  rename(program = 1, 
         total_assistance_spending = 2,  
         total_spending = 12,
         total_award = 14) %>% 
  select(program, 
         total_assistance_spending, 
         total_spending,
         total_award) %>% 
  mutate(across(c(total_assistance_spending, 
                  total_spending, 
                  total_award), ~ as.numeric(.x))) %>%
  mutate(program = str_remove(program, ", .*$"), 
         program = str_remove(program, ",.*$"),
         state = if_else(program %in% state_info$State, 
                         program, 
                         NA_character_)) %>% 
  fill(state, 
       .direction = "down") %>% 
  mutate(state = case_when(program == "Government Of Guam" ~ "Guam", 
                           program == "City of Newark" & state == "Wyoming" ~ "New Jersey", 
                           program == "Durham City" & state == "Wyoming" ~ "North Carolina", 
                           program == "Lake County"& state == "Wyoming" ~ "Ohio", 
                           program == "City Of Detroit" & state == "Wyoming" ~ "Michigan",
                           program == "Gaston County" & state == "Wyoming" ~ "North Carolina", 
                           program == "Hawaii County" & state == "Wyoming" ~ "Hawaii", 
                           program == "Sacramento County" & state == "Wyoming" ~ "California", 
                           program == "Santa Ana City" & state == "Wyoming" ~ "California",
                           .default = state)) %>% 
  #TO-DO: should replace with case_when
  mutate(state = if_else(state == "Wyoming" & program != "State of Wyoming", 
                         str_extract(program, "(?<=, ).*"),
                         state), 
         state = if_else(program == "Commonwealth Of The Northern Mariana Islands", 
                         "Northern Mariana Islands", 
                         state),
         state = if_else(program == "Government of the United States Virgin Islands", 
                         "U.S. Virgin Islands", 
                         state), 
         state = if_else(state == "MICHIGAN", 
                         "Michigan", 
                         state)) %>% 
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
      "Virgin Islands", 
      "Department of Hawaiian Homelands"
    ),
    !str_detect(program, "Non-Submitters"),
    !(program %in% state_info$State 
      & !program %in% c("District of Columbia", "Northern Mariana Islands", "Minnesota")), 
    #get rid of duplicate DC row
    !(program == "District of Columbia" & grantee_type == "Territorial Government"), 
    !(program == "Northern Mariana Islands" & grantee_type == "State/DC"),
    !(program == "Minnesota" & grantee_type == "Local Government")
  ) %>%
  left_join(state_info, 
            by = c("state" = "State"))%>% 
  mutate(across(c(total_assistance_spending, 
                  total_spending,
                  total_award),
                ~ replace_na(.x, 0))) %>% 
  mutate(grantee_name = str_replace(program, "\\bSt\\.", "St"), 
         grantee_name = str_remove(grantee_name, "'"),
         grantee_name = str_remove(grantee_name, "\\bFl\\b"), 
         grantee_name = case_when(grantee_name == "City of Salt Lake City" ~ "Salt Lake City", 
                                  grantee_name == "United States Virgin Islands" ~ "GOVERNMENT OF THE VIRGIN ISLANDS",
                                  grantee_name == "Yuma" ~ "YUMA COUNTY",
                                  grantee_name == "Winnebago County, Illinois" ~ "WINNEBAGO COUNTY",
                                  grantee_name == "CA Department of Housing and Community Development" ~ "State of California",
                                  grantee_name == "City Of Jacksonville Duval County Portion" ~ "City of Jacksonville", 
                                  grantee_name == "Augusta-Richmond" ~ "City of Augusta",
                                  grantee_name == "City Of Indianapolis and Marion County" ~ "City of Indianapolis",
                                  grantee_name == "City Of St Paul" ~ "CITY OF SAINT PAUL",
                                  grantee_name == "Baton Rouge City/Parish" ~ "Baton Rouge City",
                                  grantee_name == "Dupage" ~ "Dupage County",
                                  grantee_name == "Louisville-Jefferson County Metro Government" ~ "Louisville/Jefferson County",
                                  grantee_name == "Richland County Government" ~ "Richland County",
                                  grantee_name == "Rutherford" ~ "Rutherford County",
                                  grantee_name == "Metropolitan Government Of Nashville And Davidson County" ~ "Nashville and Davidson County",
                                  grantee_name == "VA Department of Housing & Community Development" ~ "State of Virginia",
                                  grantee_name == "Commonwealth Of Puerto Rico" ~ "Government of Puerto Rico",
                                  grantee_name == "Oregon Housing and Community Services" ~ "State of Oregon",
                                  grantee_name == "Human Services" ~ "Commonwealth of Pennsylvania",
                                  grantee_name == "Government of the United States Virgin Islands" ~ "Government of the Virgin Islands", 
                                  TRUE ~ grantee_name),
         grantee_name_v2 = str_replace(grantee_name, "([A-Za-z ]+) City", "City of \\1"), 
         grantee_name_v2 = str_replace(grantee_name_v2, "(County (Of|of)) ([A-Za-z ]+)", "\\3 County"),
         grantee_name_v2 = str_replace(grantee_name_v2, "(COUNTY OF) ([A-Za-z ]+)", "\\2 County"),
         grantee_name_v2 = str_remove(grantee_name_v2, "^The "),
         grantee_name_v2 = str_replace(grantee_name_v2, "City of Jersey", "City of Jersey City"),
         grantee_name = str_trim(str_squish(str_to_upper(grantee_name))), 
         grantee_name_v2 = str_trim(str_squish(str_to_upper(grantee_name_v2)))
  ) %>% 
  mutate(grantee_type = if_else(grantee_name == "GOVERNMENT OF THE VIRGIN ISLANDS", 
                                "Territorial Government", 
                                grantee_type)) 


# Create era2_q4_grantees_w_expenditures
ERA2_Q4_grantees_w_expenditures <- ERA_grantees_combined %>%
  mutate(grantee_type = if_else(grantee_name == "GOVERNMENT OF THE VIRGIN ISLANDS", 
                                "Territorial Government", 
                                grantee_type)) %>% 
  left_join(era2_q4_expenditures_by_program_cleaned,
            by = c("grantee_name",
                   "grantee_type",
                   "grantee_state" = "state"
            )
  ) %>% 
  left_join(era2_q4_expenditures_by_program_cleaned, 
            by = c("grantee_name_alt" = "grantee_name",
                   "grantee_type",
                   "grantee_state" = "state"
            ),
            suffix = c("", "_alt")) %>% 
  left_join(era2_q4_expenditures_by_program_cleaned, 
            by = c("locality" = "grantee_name",
                   "grantee_type",
                   "grantee_state" = "state"
            ),
            suffix = c("", "_alt2")) %>% 
  left_join(era2_q4_expenditures_by_program_cleaned,
            by = c("grantee_name" = "grantee_name_v2",
                   "grantee_type",
                   "grantee_state" = "state"
            ), 
            suffix = c("", "_alt3")
  ) %>% 
  left_join(era2_q4_expenditures_by_program_cleaned, 
            by = c("grantee_name_alt" = "grantee_name_v2",
                   "grantee_type",
                   "grantee_state" = "state"
            ),
            suffix = c("", "_alt4")) %>% 
  left_join(era2_q4_expenditures_by_program_cleaned, 
            by = c("locality" = "grantee_name_v2",
                   "grantee_type",
                   "grantee_state" = "state"
            ),
            suffix = c("", "_alt5")) %>% 
  mutate(
    program = coalesce(program, 
                       program_alt, 
                       program_alt2, 
                       program_alt3, 
                       program_alt4),
    total_assistance_spending = coalesce(total_assistance_spending, 
                                         total_assistance_spending_alt, 
                                         total_assistance_spending_alt2, 
                                         total_assistance_spending_alt3, 
                                         total_assistance_spending_alt4),
    total_spending = coalesce(total_spending, 
                              total_spending_alt,
                              total_spending_alt2, 
                              total_spending_alt3, 
                              total_spending_alt4),
    total_award = coalesce(total_award,
                           total_award_alt, 
                           total_award_alt2, 
                           total_award_alt3, 
                           total_award_alt4)
  ) %>%
  select(-ends_with("_alt"), -ends_with("_alt2")) %>% 
  select(
    grantee_name, 
    grantee_type, 
    grantee_state,
    grantee_id_era2, 
    grantee_id_combined,
    program_id_era2,
    total_spending,
    total_assistance_spending, 
    total_award
  ) %>% 
  filter(!is.na(grantee_id_era2))


#should only be 3 programs, each not reported in era2_q4 treasury file
ERA2_Q4_grantees_w_expenditures %>% 
  filter(is.na(total_spending), 
         !is.na(grantee_id_era2)) %>%
  nrow()


#### GET COMPARISON TO TREASURY AGGS BY PROGRAM ####

phpdf_by_program <- phpdf_ERA2_Q4_cleaned %>%
  group_by(program,
           grantee_id_combined,
           grantee_name,
           grantee_type,
           grantee_state) %>%
  summarize(total_amount = sum(amount_of_payment, na.rm = TRUE)) %>%
  ungroup() %>%
  left_join(ERA2_Q4_grantees_w_expenditures, by =  "grantee_id_combined") %>%
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

bad_programs <- phpdf_by_program %>%
  filter(!grantee_id_combined %in%
           good_programs$grantee_id_combined)

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


bad_programs_state <- phpdf_by_program %>%
  filter(!grantee_id_combined %in%
           good_programs_state$grantee_id_combined)


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


# # save files
write_rds(
  phpdf_by_program,
  str_c(data_path,
        "1_intermediates",
        "phpdfs",
        "5_variable_checks",
        str_c("phpdf_by_program_era2_q4_", Sys.Date(), ".rds"),
        sep = "/"
  ))

toc()
#14.718 sec elapsed
