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
      "phpdf_ERA2_Q1-Q4_23_GSC_Results.parquet",
      sep = "/"
    )
  )

geocodes_ready <- geocodes_raw %>%
  clean_names() %>%
  select(-oid) %>%
  # Removing 7 empty columns of 82 columns total
  remove_empty("cols", quiet = FALSE) %>% 
  rename(development_code = userkeys)

# Payments
payments_raw <-
  read_parquet(
    str_c(
      data_path,
      "0_raw_data",
      "phpdf_ERA2_Q1-Q4_23_Data_Relate_table.parquet",
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
  select(-id, -era_application_number) %>% 
  # Row/file ID variables
  mutate(file_specific_row_id = row_number(), .before = everything()) %>%
  mutate(original_filename = "phpdf_ERA2_Q1-Q4_23_Data_Relate_table.csv", .before = everything())

# Geocodes matching ------------------------------------------------------

geocodes_test_phpdf_nogeocode <- payments_ready %>%
  anti_join(geocodes_ready, by = "development_code")


# anti_join: added no columns
# > rows only in x           0
# > rows only in y  (        0)
# > matched rows    (6,259,004)
# >                 ===========
#   > rows total               0

geocodes_test_geocode_nophpdf <- geocodes_ready %>%
  anti_join(payments_ready, by = "development_code")


# > rows only in x           0
# > rows only in y  (        0)
# > matched rows    (6,259,004)
# >                 ===========
#   > rows total               0

# Write out ---------------------------------------------------------------

write_parquet(geocodes_ready,
          str_c(data_path,
                "1_intermediates",
                "phpdfs",
                "1_geocode",
                str_c("era2_reachback_geocodes_all_", Sys.Date(),".parquet"),
                sep = "/"))

toc()
#130.518 sec elapsed

