# Purpose -----------------------------------------------------------------
# Final data processing before aggregation

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(tictoc)
library(arrow)

tic()

# Locally specific part of data folder path already stored in Renviron file
data_path <- str_c(Sys.getenv("LOCAL_PATH"), "era-county-level-dataset-data", sep = "/")

# Load data ---------------------------------------------------------------

# Prepared data
data_input <- 
  read_parquet(
    str_c(data_path, "1_intermediates", "phpdfs", "7_pre_aggregation",
          "county_month_data_ready_for_aggregation_2025-04-14.parquet",
    sep = "/"
  ))

# Aggregate -----------------------------------------------------------------------------------

aggregated_initial <- data_input %>% 
  #
  filter(!is.na(county_geoid_coalesced)) %>% 
  #
  filter(!is.na(month_of_payment)) %>% 
  # Should remove 0 rows
  filter(!is.na(amount_of_payment)) %>% 
  # Should remove 0 rows
  filter(!is.na(unique_address_id)) %>% 
  group_by(county_geoid_coalesced, month_of_payment) %>% 
  summarize(sum_assistance_amount = sum(amount_of_payment),
            unique_assisted_addresses = n_distinct(unique_address_id)) %>% 
  ungroup()

# Suppress small counts -----------------------------------------------------------------------

aggregated_suppressed <- aggregated_initial %>% 
  mutate(small_count = unique_assisted_addresses < 11) %>% 
  mutate(sum_assistance_amount =
           if_else(small_count, -99999, sum_assistance_amount)) %>% 
  mutate(unique_assisted_addresses =
           if_else(small_count, -99999, unique_assisted_addresses)) %>% 
  select(-small_count)

# Write out data ----------------------------------------------------------

write_csv(aggregated_suppressed,
          str_c(Sys.getenv("LOCAL_PATH"),
                "era-county-level-dataset",
                "outputs",
                "aggregation_outputs",
                str_c("county_month_aggregated_", Sys.Date(),".csv"),
                sep = "/"))

# write a version for troubleshooting that doesn't include supressed data
write_csv(aggregated_initial,
          str_c(data_path,
                "1_intermediates",
                "phpdfs",
                "8_aggregation",
                str_c("non_supressed_county_month_aggregated_", Sys.Date(),".csv"),
                sep = "/"))

toc()
#17.283 sec elapsed
