# Purpose -----------------------------------------------------------------
# Make thresholding decisions and select only counties with 
# complete coverage of grantees with acceptable data.

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(tictoc)
library(janitor)

tic()

# Locally specific part of data folder path already stored in Renviron file
data_path = str_c(Sys.getenv("LOCAL_PATH"), "era-county-level-dataset-data", sep = "/")

# Overall grantees list -----------------------------------------------------------------------

# Grantees for both programs
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

# Grantee goodness summaries ------------------------------------------------------------------

goodness_summary_era1 <- 
  read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                 "era1_grantee_goodness_checks_summary_2025-03-21.csv",
                 sep = "/")) %>% 
  filter(scenario == "County total only")

goodness_summary_era2_q4 <- 
  read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                 "era2_q4_grantee_goodness_checks_summary_2025-03-21.csv",
                 sep = "/")) %>% 
  filter(scenario == "County total only")

goodness_summary_era2_q2 <- 
  read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                 "era2_q2_grantee_goodness_checks_summary_2025-03-21.csv",
                 sep = "/")) %>% 
  filter(scenario == "County total only")

goodness_summary_era2_q1 <- 
  read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                 "era2_q1_grantee_goodness_checks_summary_2025-03-21.csv",
                 sep = "/")) %>% 
  filter(scenario == "County total only")

goodness_summary_era2_reachback <- 
  read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
                 "era2_reachback_grantee_goodness_checks_summary_2025-03-21.csv",
                 sep = "/")) %>% 
  filter(scenario == "County total only")

# Geographic data -----------------------------------------------------------------------------

# Grantee city-county overlaps
city_county_overlap <- 
  read_rds(str_c(data_path, "1_intermediates", "grantees",
                 "grantees_geographies_county_city_overlap_2025-03-14.RDS",
                 sep = "/"))

# For reference -------------------------------------------------------------------------------

# # Variable-level goodness
# variable_checks_era1 <- 
#   read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
#                  "era1_variable_checks_summary_2025-03-21.csv",
#                  sep = "/"))
# 
# variable_checks_era2_q4 <- 
#   read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
#                  "era2_q4_variable_checks_summary_2025-03-21.csv",
#                  sep = "/"))
# 
# variable_checks_era2_q2 <- 
#   read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
#                  "era2_q2_variable_checks_summary_2025-03-21.csv",
#                  sep = "/"))
# 
# variable_checks_era2_q1 <- 
#   read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
#                  "era2_q1_variable_checks_summary_2025-03-21.csv",
#                  sep = "/"))
# 
# variable_checks_era2_reachback <- 
#   read_csv(str_c(data_path, "1_intermediates", "phpdfs", "5_variable_checks",
#                  "era2_reachback_variable_checks_summary_2025-03-21.csv",
#                  sep = "/"))

# Preparation of grantees list ---------------------------------------------------------------

grantees_relevant <- grantees %>% 
  # Fix some errors about grantees which did not participate in ERA2
  # mutate: changed 3 values (1%) of 'grantee_id_era2' (3 new NA)
  mutate(grantee_id_era2 =
           if_else(grantee_id_era2 %in% c("SLT-0209", "SLT-0218", "SLT-2886"), 
                   NA, grantee_id_era2)) %>% 
  # Only include grantees which accepted at least one of ERA1 or ERA2
  # filter: removed 37 rows (8%), 410 rows remaining
  filter(!is.na(grantee_id_era1) | !is.na(grantee_id_era2)) %>% 
  # Remove Territories
  # filter: removed 5 rows (1%), 405 rows remaining
  filter(geographic_level != "Territory") %>% 
  select(grantee_state, grantee_name, grantee_type, geographic_level, contains("grantee_id"))

# Threshold check function -------------------------------------------------------------------

apply_thresholds <- 
  function(data, 
           goodness_threshold = 0.79, 
           era1_spending_threshold_minimum = 0.5,
           era1_spending_threshold_good = 0.8, 
           era2_spending_threshold_minimum = 0.25,
           era2_spending_threshold_good = 0.5) {
    
    # Specify different spending thresholds by program
    program <- unique(data$program)
    output_col <- ifelse(program == "ERA 1", "grantee_id_era1", "grantee_id_era2")
    
    if (length(program) != 1) {
      
      stop("Input data must contain column 'program' with single label for ERA program (ERA 1 or ERA 2)")
      
    } else if (program == "ERA 1") {
      
      spending_threshold_minimum <- era1_spending_threshold_minimum
      spending_threshold_good <- era1_spending_threshold_good
      
    } else if (program == "ERA 2") {
      
      spending_threshold_minimum <- era2_spending_threshold_minimum
      spending_threshold_good <- era2_spending_threshold_good
      
    }
    
    out <- data %>% 
      filter(grantee_type != "Territorial Government") %>% 
      # Threshold 1:
      # Did a grantee meet a threshold of X% of its records having nominal values for 
      # variables needed in this aggregation?
      mutate(threshold_variable_goodness = ok_percent >= goodness_threshold) %>% 
      rename_with(~ "grantee_id", .cols = starts_with("grantee_id_era")) %>% 
      # Threshold 2:
      # Did a grantee likely report complete spending data?
      mutate(threshold_spending_completeness_label =
               case_when(
                     # manually add back those grantees treasury says are slow spenders
             grantee_id == "SLT-0315" & program == "ERA 2" ~ "0_pass_treasury_rescue",
             grantee_id == "ERA-2101070306" & program == "ERA 1" ~ "0_pass_treasury_rescue",
             grantee_id == "SLT-2093" & program == "ERA 2" ~ "0_pass_treasury_rescue",
             grantee_id == "ERA-2101123208" & program == "ERA 1" ~ "0_pass_treasury_rescue",
             grantee_id == "ERA-2101123160" & program == "ERA 1"~ "0_pass_treasury_rescue",
             grantee_id == "ERA-2101112447" & program == "ERA 1" ~ "0_pass_treasury_rescue",
             grantee_id == "SLT-0069" & program == "ERA 2" ~ "0_pass_treasury_rescue",
             grantee_id == "SLT-0167" & program == "ERA 2"~ "0_pass_treasury_rescue",
             grantee_id == "SLT-0313" & program == "ERA 2" ~ "0_pass_treasury_rescue",
             grantee_id == "ERA-2101080905" & program == "ERA 1" ~ "0_pass_treasury_rescue",
             grantee_id == "ERA-2101070455" & program == "ERA 1" ~ "0_pass_treasury_rescue",
                 # No records with payment amounts
                 is.na(percent_of_allocation_spent) ~
                   "1_fail_no_amounts_reported",
                 # Exclude mathematically impossible overspending
                 percent_of_allocation_spent >= 1.1 ~ 
                   "2_fail_overspend",
                 # Exclude very unlikely underspending
                 percent_of_allocation_spent < spending_threshold_minimum ~ 
                   "3_fail_underspend",
                 # Passes if reported spending is high enough % of allocation
                 percent_of_allocation_spent >= spending_threshold_good ~
                   "4_pass_allocation",
                 # Otherwise, passes if reported spending as 
                 # % of aggregate reporting is between 80% and 120%
                 pct_of_spending_in_php_file_quarterly >= 0.8 & 
                   pct_of_spending_in_php_file_quarterly <= 1.2 ~
                   "5_pass_agg_ind_quarterly",
                 pct_of_spending_in_php_file_summation >= 0.8 & 
                   pct_of_spending_in_php_file_summation <= 1.2 ~
                   "6_pass_agg_ind_sum",
                 # Otherwise, passes if sum of state reported spending as 
                 # % of state aggregate reporting is between 80% and 120%
                 state_pct_of_spending_in_php_file_quarterly >= 0.8 & 
                   state_pct_of_spending_in_php_file_quarterly <= 1.2 ~
                   "7_pass_agg_st_quarterly",
                 state_pct_of_spending_in_php_file_summation >= 0.8 & 
                   state_pct_of_spending_in_php_file_summation <= 1.2 ~
                   "8_pass_agg_st_sum",
                 # Grantees not excluded by absolute criteria but not passing any positive criteria
                 percent_of_allocation_spent >= spending_threshold_minimum &
                   percent_of_allocation_spent < spending_threshold_good ~
                   "9_fail_not_rescued",
                 .default = "CHECK")) %>% 
      mutate(threshold_spending_completeness = 
               str_detect(threshold_spending_completeness_label, "pass")) %>% 
      rename(!!sym(output_col):=grantee_id)
    
  }

# Get thresholds for each data source ---------------------------------------------------------

thresholds_era1 <- apply_thresholds(goodness_summary_era1) %>% 
  mutate(data_source = "ERA1 closeout")
#filter: removed 4 rows (1%), 347 rows remaining

thresholds_era2_q4 <- apply_thresholds(goodness_summary_era2_q4) %>% 
  mutate(data_source = "2023 Q4") %>% 
  mutate(priority = 1)
#filter: removed 5 rows (1%), 331 rows remaining

thresholds_era2_q2 <- apply_thresholds(goodness_summary_era2_q2) %>% 
  mutate(data_source = "2023 Q2") %>% 
  mutate(priority = 2)
#filter: removed 3 rows (1%), 279 rows remaining

thresholds_era2_q1 <- apply_thresholds(goodness_summary_era2_q1) %>% 
  mutate(data_source = "2023 Q1") %>% 
  mutate(priority = 3)
#filter: removed 2 rows (1%), 264 rows remaining

thresholds_era2_reachback <- apply_thresholds(goodness_summary_era2_reachback) %>% 
  mutate(data_source = "2023 reachback") %>% 
  mutate(priority = 4)
#filter: removed 5 rows (1%), 359 rows remaining

# Select between ERA2 data sources ------------------------------------------------------------

thresholds_era2_combined <- thresholds_era2_q4 %>% 
  # Join all grantees from previous quarters
  bind_rows(thresholds_era2_q2) %>% 
  bind_rows(thresholds_era2_q1) %>% 
  bind_rows(thresholds_era2_reachback) %>% 
  # If a grantee is present in multiple quarters, 
  # first pick all passing quarters
  mutate(thresholds_both_passing = threshold_variable_goodness & threshold_spending_completeness) %>% 
  #slice_max:  removed 152 rows (12%), 1,081 rows remaining
  slice_max(by = grantee_id_combined,
            order_by = thresholds_both_passing,
            with_ties = TRUE) %>% 
  # then pick the quarter with the highest priority order
  # slice_min: removed 721 rows (67%), 360 rows remaining
  slice_min(by = grantee_id_combined,
            order_by = priority,
            with_ties = TRUE)

# Collate ERA1 and ERA2 thresholds to list of all grantees ------------------------------------

grantees_thresholds_collated <- grantees_relevant %>% 
  left_join(thresholds_era1 %>% 
              select(-c(program, 
                        grantee_name, 
                        grantee_state, 
                        grantee_type, 
                        scenario, 
                        percent_rent_records)) %>% 
              rename_with( ~str_c(., "_era1"), -contains("grantee")),
            by = "grantee_id_combined") %>% 
  left_join(thresholds_era2_combined %>% 
              select(-c(program, 
                        grantee_name, 
                        grantee_state, 
                        grantee_type, 
                        scenario, 
                        percent_rent_records,
                        thresholds_both_passing,
                        priority)) %>% 
              rename_with( ~str_c(., "_era2"), -contains("grantee")),
            by = "grantee_id_combined") %>% 
  rename(grantee_id_era2 = grantee_id_era2.x,
         grantee_id_era1 = grantee_id_era1.x) %>% 
  select(-grantee_id_era1.y, -grantee_id_era2.y)

# Generate cross-program thresholds -----------------------------------------------------------

# 3 thresholds for data coverage, variable quality, and spending completeness
grantees_thresholds_cross_program <- grantees_thresholds_collated %>%  
  # Threshold 1: Grantee has data from all ERA programs it participated in
  mutate(threshold_data_coverage = 
           case_when(!is.na(total_n_era1) & !is.na(total_n_era2) ~ TRUE,
                     is.na(grantee_id_era2) & !is.na(total_n_era1) ~ TRUE,
                     is.na(grantee_id_era1) & !is.na(total_n_era2) ~ TRUE,
                     .default = FALSE)) %>% 
  mutate(threshold_data_coverage_label =
           case_when(threshold_data_coverage == TRUE ~
                       "Passes",
                     !is.na(grantee_id_era1) & is.na(total_n_era1) & 
                       !is.na(grantee_id_era2) & is.na(total_n_era2) ~ 
                       "Both fails",
                     !is.na(grantee_id_era1) & is.na(total_n_era1) ~ 
                       "ERA1 fails",
                     !is.na(grantee_id_era2) & is.na(total_n_era2) ~ 
                       "ERA2 fails",
                     .default = NA)) %>% 
  # Threshold 2: Grantee has 80% or higher share of rows with acceptable quality for 
  # county assignment, payment within geographic jurisdiction, payment amounts, and payment date,
  # for all programs it participated in.
  mutate(threshold_variable_goodness = 
           case_when(threshold_variable_goodness_era1 & threshold_variable_goodness_era2 ~ 
                       TRUE,
                     is.na(grantee_id_era1) & threshold_variable_goodness_era2 ~ 
                       TRUE,
                     is.na(grantee_id_era2) & threshold_variable_goodness_era1 ~ 
                       TRUE,
                     .default = FALSE)) %>% 
  mutate(threshold_variable_goodness_label =
           case_when(threshold_data_coverage == FALSE ~
                       "Missing data",
                     threshold_variable_goodness == TRUE ~
                       "Passes",
                     !is.na(grantee_id_era1) & threshold_variable_goodness_era1 == FALSE & 
                       !is.na(grantee_id_era2) & threshold_variable_goodness_era2 == FALSE ~ 
                       "Both fail",
                     !is.na(grantee_id_era1) & threshold_variable_goodness_era1 == FALSE ~
                       "ERA1 fails",
                     !is.na(grantee_id_era2) & threshold_variable_goodness_era2 == FALSE ~ 
                       "ERA2 fails",
                     .default = NA)) %>% 
  # Threshold 3: Grantee spending amount is within a reasonable percentage of either its full allocation, 
  # OR the amount reported in Treasury aggregate data, for all programs it participated in.
  mutate(threshold_spending_completeness = 
           case_when(
             threshold_spending_completeness_era1 & threshold_spending_completeness_era2 ~ 
                       TRUE,
                     is.na(grantee_id_era1) & threshold_spending_completeness_era2 ~ 
                       TRUE,
                     is.na(grantee_id_era2) & threshold_spending_completeness_era1 ~ 
                       TRUE,
                     .default = FALSE)) %>% 
  mutate(threshold_spending_completeness_label =
           case_when(threshold_data_coverage == FALSE ~
                       "Missing data",
                     threshold_variable_goodness == FALSE ~
                       "Existing data bad quality",
                     threshold_spending_completeness == TRUE ~
                       "Passes",
                     !is.na(grantee_id_era1) & threshold_spending_completeness_era1 == FALSE & 
                       !is.na(grantee_id_era2) & threshold_spending_completeness_era2 == FALSE ~ 
                       "Both fail",
                     !is.na(grantee_id_era1) & threshold_spending_completeness_era1 == FALSE ~
                       "ERA1 fails",
                     !is.na(grantee_id_era2) & threshold_spending_completeness_era2 == FALSE ~ 
                       "ERA2 fails",
                     .default = NA)) %>% 
  # Overall threshold passing
  mutate(threshold_all_passing =
           threshold_data_coverage & 
           threshold_variable_goodness & 
           threshold_spending_completeness)

# grantees_thresholds_cross_program %>% tabyl(threshold_all_passing)
# threshold_all_passing   n   percent
# FALSE 117 0.2888889
# TRUE 288 0.7111111


# Indicate geographic completeness ------------------------------------------------------------

# State grantees not passing thresholds
states_dropping <- grantees_thresholds_cross_program %>% 
  filter(geographic_level == "State") %>% 
  # filter: removed 354 rows (87%), 51 rows remaining
  filter(threshold_all_passing == FALSE) 
#filter: removed 37 rows (73%), 14 rows remaining

# County grantees not passing thresholds
county_dropping <- grantees_thresholds_cross_program %>% 
  filter(geographic_level == "County") %>% 
  # filter: removed 148 rows (37%), 257 rows remaining
  filter(threshold_all_passing == FALSE) 
# filter: removed 176 rows (68%), 81 rows remaining

# City grantees not passing thresholds
city_dropping <- grantees_thresholds_cross_program %>% 
  filter(geographic_level == "City") %>%  
  # filter: removed 308 rows (76%), 97 rows remaining
  filter(threshold_all_passing == FALSE)
# filter: removed 65 rows (67%), 32 rows remaining

# Passing cities that overlap with non-passing counties
# In this case, a city is only marked to be removed if all failing county grantees overlapping
# the city are more than 20% of the city population
city_county_overlap_dropping <- city_county_overlap %>% 
  # filter: removed 88 rows (82%), 19 rows remaining
  filter(!grantee_id_combined_city %in% city_dropping$grantee_id_combined &
           grantee_id_combined_county %in% county_dropping$grantee_id_combined) %>% 
  group_by(grantee_id_combined_city) %>% 
  mutate(city_population_affected = sum(city_to_county_overlap_percentage)) %>% 
  ungroup() %>% 
  # Atlanta (x DeKalb County) and Houston (x Montgomery County) rescued
  filter(city_population_affected > 0.2)
#filter: removed 2 rows (11%), 17 rows remaining

# Passing counties that overlap with non-passing cities
# In this case, a county is only marked to be removed if all failing city grantees within
# the county are more than 20% of the county population
county_city_overlap_dropping <- city_county_overlap %>%   
  #filter: removed 83 rows (78%), 24 rows remaining
  filter(!grantee_id_combined_county %in% county_dropping$grantee_id_combined &
           grantee_id_combined_city %in% city_dropping$grantee_id_combined) %>% 
  group_by(grantee_id_combined_county) %>% 
  mutate(county_population_affected = sum(county_to_city_overlap_percentage)) %>% 
  ungroup() %>% 
  filter(county_population_affected > 0.2)
#filter: removed 7 rows (29%), 17 rows remaining

# Final threshold labels ----------------------------------------------------------------------

grantees_thresholds_final <- grantees_thresholds_cross_program %>% 
  # Mark grantees that have geographic overlaps with non-passing grantees as non-passing
  mutate(threshold_passing_with_complete_geography =
           case_when(grantee_state %in% states_dropping$grantee_state ~ FALSE,
                     grantee_id_combined %in% city_county_overlap_dropping$grantee_id_combined_city ~ FALSE,
                     grantee_id_combined %in% county_city_overlap_dropping$grantee_id_combined_county ~ FALSE,
                     .default = threshold_all_passing)) %>% 
  mutate(threshold_passing_with_complete_geography_label =
           case_when(threshold_all_passing == FALSE ~
                       "Fails by itself",
                     grantee_state %in% states_dropping$grantee_state ~
                       "Fails due to enclosing state failing",
                     grantee_id_combined %in% city_county_overlap_dropping$grantee_id_combined_city |
                       grantee_id_combined %in% county_city_overlap_dropping$grantee_id_combined_county ~
                       "Otherwise fails due to overlapping county/city failing",
                     threshold_all_passing == TRUE ~
                       "Passes",
                     .default = NA))

# grantees_thresholds_final %>% tabyl(threshold_passing_with_complete_geography)
# threshold_passing_with_complete_geography   n   percent
# FALSE 200 0.4938272
# TRUE 205 0.5061728

# Data coverage descriptives ------------------------------------------------------------------

# Failed Threshold 1
grantees_thresholds_final %>% 
  tabyl(threshold_data_coverage_label, grantee_type) %>% 
  adorn_totals("col")

# threshold_data_coverage_label Local Government State/DC Total
# Both fails                5        0     5
# ERA1 fails               46        2    48
# ERA2 fails                6        2     8
# Passes              297       47   344

grantees_thresholds_final %>% 
  filter(threshold_data_coverage_label != "Passes" & grantee_type == "State/DC") %>% 
  arrange(threshold_data_coverage_label) %>% 
  select(grantee_id_combined, grantee_name, threshold_data_coverage_label)

# grantee_id_combined     grantee_name                 threshold_data_coverage_label
# <chr>                   <chr>                        <chr>                        
#   1 ERA-2101080772_SLT-0193 STATE OF OHIO                ERA1 fails                   
# 2 ERA-2101123321_SLT-0388 STATE OF OREGON              ERA1 fails                   
# 3 ERA-2101111878_SLT-0261 STATE OF MARYLAND            ERA2 fails                   
# 4 ERA-2101080798_SLT-0464 COMMONWEALTH OF PENNSYLVANIA ERA2 fails       

# Failed Threshold 2
grantees_thresholds_final %>% 
  tabyl(threshold_variable_goodness_label, grantee_type) %>% 
  adorn_totals("col")

# threshold_variable_goodness_label Local Government State/DC Total
# Both fail                2        0     2
# ERA1 fails                5        0     5
# ERA2 fails                4        2     6
# Missing data               57        4    61
# Passes              286       45   331

grantees_thresholds_final %>% 
  filter(str_detect(threshold_variable_goodness_label, "fail") & grantee_type == "State/DC") %>% 
  arrange(threshold_variable_goodness_label) %>% 
  select(grantee_id_combined, grantee_name, threshold_variable_goodness_label)

# grantee_id_combined     grantee_name     threshold_variable_goodness_label
# <chr>                   <chr>            <chr>                            
#   1 ERA-2101070596_SLT-0165 STATE OF ARIZONA ERA2 fails                       
# 2 ERA-2101112294_SLT-8619 STATE OF INDIANA ERA2 fails     
  
# Failed Threshold 3
grantees_thresholds_final %>% 
  tabyl(threshold_spending_completeness_label, grantee_type) %>% 
  adorn_totals("col")
# 
# threshold_spending_completeness_label Local Government State/DC Total
# Both fail                6        1     7
# ERA1 fails               11        5    16
# ERA2 fails               20        0    20
# Existing data bad quality               11        2    13
# Missing data               57        4    61
# Passes              249       39   288

grantees_thresholds_final %>% 
  filter(str_detect(threshold_spending_completeness_label, "fail") & grantee_type == "State/DC") %>% 
  arrange(threshold_spending_completeness_label) %>% 
  select(grantee_id_combined, grantee_name, threshold_spending_completeness_label)

# grantee_id_combined     grantee_name        threshold_spending_completeness_label
# <chr>                   <chr>               <chr>                                
#   1 ERA-2101070543_SLT-9873 STATE OF IOWA       Both fail                            
# 2 ERA-2101060213_SLT-0073 STATE OF COLORADO   ERA1 fails                           
# 3 ERA-2101123037_SLT-0096 STATE OF MINNESOTA  ERA1 fails                           
# 4 ERA-2101123145_SLT-0343 STATE OF MONTANA    ERA1 fails                           
# 5 ERA-2101111965_SLT-0240 STATE OF NEW JERSEY ERA1 fails                           
# 6 ERA-2101111930_SLT-0341 STATE OF WYOMING    ERA1 fails   

# Failed Threshold 4
grantees_thresholds_final %>% 
  tabyl(threshold_passing_with_complete_geography_label, grantee_type) %>% 
  adorn_totals("col")

# threshold_passing_with_complete_geography_label Local Government State/DC Total
# Fails by itself              105       12   117
# Fails due to enclosing state failing               64        0    64
# Otherwise fails due to overlapping county/city failing               19        0    19
# Passes              166       39   205

# Passing grantees
grantees_thresholds_final %>% 
  tabyl(threshold_passing_with_complete_geography, grantee_type) %>% 
  adorn_totals("col") %>% 
  adorn_percentages("col") %>% 
  adorn_pct_formatting() %>% 
  adorn_ns()

# threshold_passing_with_complete_geography Local Government   State/DC       Total
# FALSE      53.1% (188) 23.5% (12) 49.4% (200)
# TRUE      46.9% (166) 76.5% (39) 50.6% (205)


# Write out data ----------------------------------------------------------

# Selected, geographically filtered, grantees
write_csv(grantees_thresholds_final,
          str_c(data_path, "1_intermediates", "phpdfs", "6_thresholding",
                str_c("county_total_selected_grantees_", Sys.Date(),".csv"),
                sep = "/"))

toc()
#2.602 sec elapsed
