# Purpose -----------------------------------------------------------------
# Data quality validation for each variable, plus summaries thereof.

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(janitor)
library(tictoc)
library(arrow)

tic()

# Locally specific part of data folder path already stored in Renviron file
data_path = str_c(Sys.getenv("LOCAL_PATH"), "era-county-level-dataset-data", sep = "/")

# Load PHPDF data ---------------------------------------------------------

# Load deduplicated ERA1 data
era1 <- read_parquet(
  str_c(
    data_path,
    "1_intermediates", 
    "phpdfs", 
    "4_county_imputation",
    "era1_imputed_counties_2025-03-21.parquet",
    sep = "/")
)

# Load deduplicated ERA2_q4 data
era2_q4 <- read_parquet(
  str_c(
    data_path,
    "1_intermediates", 
    "phpdfs", 
    "4_county_imputation", 
    "era2_q4_imputed_counties_2025-03-21.parquet",
    sep = "/"))

# Extract data --------------------------------------------------------------------------------

# Join data with just amount of assistance
amounts <- era1 %>% 
  bind_rows(era2_q4) %>% 
  select(program, amount_of_payment)

# Calculate quantiles -------------------------------------------------------------------------

quantile_999 <- quantile(amounts$amount_of_payment, c(0.999), na.rm = TRUE) %>% 
  unname()

# Write out data ----------------------------------------------------------

write_rds(quantile_999,
          str_c(data_path, 
                "1_intermediates", 
                "phpdfs", 
                "5_variable_checks",
                str_c("amount_of_payment_quantile_999_", Sys.Date(),".rds"),
                sep = "/"))

# 110.514 sec elapsed
toc()

