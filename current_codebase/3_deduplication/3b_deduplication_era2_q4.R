# Purpose -----------------------------------------------------------------
# Deduplication of each file.

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

# Read in data ------------------------------------------------------------

era2_initial <-
  read_parquet(
    str_c(
      data_path,
      "1_intermediates",
      "phpdfs",
      "2_initial_validation",
      "era2_q4_initial_validation_2025-03-06.parquet",
      sep = "/"
    )
  )


# Grantee/files crosswalk
files <-
  read_csv(
    str_c(
      data_path,
      "1_intermediates",
      "grantees",
      "era2_grantee_files_2025-03-06.csv",
      sep = "/"
    )
  )

# Create dedupe IDs and sizes ---------------------------------------------

dupes_ids <- era2_initial %>%
  # Dupes across grantees
  group_by(
    address_line_1,
    address_line_2,
    address_line_3,
    city_name,
    state_code,
    zip5,
    zip4,
    payee_type,
    type_of_assistance,
    amount_of_payment,
    date_of_payment,
    start_date,
    end_date,
    program
  ) %>%
  mutate(dedupe_id_within_source = cur_group_id(), .before = everything()) %>%
  mutate(
    dupe_size_within_source = n_distinct(grantee_id_era2),
    .before = original_filename
  ) %>%
  ungroup() %>%
  # Dupes within each grantee
  group_by(
    address_line_1,
    address_line_2,
    address_line_3,
    city_name,
    state_code,
    zip5,
    zip4,
    payee_type,
    type_of_assistance,
    amount_of_payment,
    date_of_payment,
    start_date,
    end_date,
    program,
    grantee_id_era2
  ) %>%
  # Note that dedupe_id_within_grantee dupes include within-file as well as
  # across-file dupes, but that dupe_size_within_grantee only count over distinct files
  mutate(dedupe_id_within_grantee = cur_group_id(), .before = everything()) %>%
  mutate(
    dupe_size_within_grantee = n_distinct(file),
    .before = original_filename
  ) %>%
  ungroup() %>%
  # Dupes within each file (conservative)
  # Many dupes are due to NA fields in important variables like address_line_1;
  # to be cautious, NA values in the following variables are replaced with unique values.
  mutate(across(
    c(address_line_1, payee_type, type_of_assistance),
    ~ case_when(is.na(.) ~ as.character(file_specific_row_id), .default = .),
    .names = "{.col}_na_filled"
  )) %>%
  mutate(across(
    c(amount_of_payment),
    ~ case_when(is.na(.) ~ file_specific_row_id, .default = .),
    .names = "{.col}_na_filled"
  )) %>%
  mutate(across(
    c(date_of_payment, start_date),
    ~ case_when(
      is.na(.) ~ as_date("2050-01-01") + file_specific_row_id,
      .default = .
    ),
    .names = "{.col}_na_filled"
  )) %>%
  group_by(
    address_line_1_na_filled,
    address_line_2,
    address_line_3,
    city_name,
    state_code,
    zip5,
    zip4,
    payee_type_na_filled,
    type_of_assistance_na_filled,
    amount_of_payment_na_filled,
    date_of_payment_na_filled,
    start_date_na_filled,
    end_date,
    program,
    grantee_id_era2,
    file
  ) %>%
  mutate(
    dedupe_id_within_file_conservative = cur_group_id(),
    .before = everything()
  ) %>%
  mutate(
    dupe_size_within_file_conservative = n_distinct(file_specific_row_id),
    .before = original_filename
  ) %>%
  ungroup()

# Dupe analyses -----------------------------------------------------------

# Dupes across grantees
dupes_within_source <- dupes_ids %>%
  group_by(
    grantee_id_era2,
    grantee_name,
    grantee_state,
    grantee_type,
    dupe_size_within_source
  ) %>%
  summarize(n = n()) %>%
  mutate(percent = n / sum(n)) %>%
  ungroup() %>%
  select(-n) %>%
  arrange(dupe_size_within_source) %>%
  pivot_wider(
    names_from = dupe_size_within_source,
    names_prefix = "dupe_size_",
    values_from = percent
  ) %>%
  arrange(grantee_state, desc(grantee_type), grantee_name)

dupes_across_grantees <- dupes_within_source %>%
  filter(dupe_size_1 < 1 | is.na(dupe_size_1)) %>%
  distinct(grantee_id_era2) %>%
  left_join(dupes_ids) %>%
  group_by(dedupe_id_within_source) %>%
  mutate(
    grantees_duped_with = str_c(unique(grantee_name), collapse = ""),
    .before = everything()
  ) %>%
  ungroup() %>%
  mutate(grantees_duped_with = str_remove(grantees_duped_with, grantee_name)) %>%
  mutate(grantees_duped_with = na_if(grantees_duped_with, ""))

dupes_across_grantees_summary <- dupes_across_grantees %>%
  group_by(
    grantee_id_era2,
    grantee_name,
    grantee_state,
    grantee_type,
    grantees_duped_with
  ) %>%
  mutate(state_code = str_replace_na(state_code)) %>%
  mutate(
    payment_address_states = str_c(unique(state_code), collapse = "; "),
    .before = everything()
  ) %>%
  group_by(payment_address_states, .add = TRUE) %>%
  summarize(n = n()) %>%
  group_by(grantee_id_era2, grantee_name, grantee_state, grantee_type) %>%
  mutate(percent_duped_with_listed_grantee = n / sum(n)) %>%
  ungroup() %>%
  filter(!is.na(grantees_duped_with)) %>%
  arrange(desc(percent_duped_with_listed_grantee), desc(n))

# Dupes across files within grantees
dupes_within_grantee <- dupes_ids %>%
  group_by(
    grantee_id_era2,
    grantee_name,
    grantee_state,
    grantee_type,
    dupe_size_within_grantee
  ) %>%
  summarize(n = n()) %>%
  mutate(percent = n / sum(n)) %>%
  ungroup() %>%
  select(-n) %>%
  arrange(dupe_size_within_grantee) %>%
  pivot_wider(
    names_from = dupe_size_within_grantee,
    names_prefix = "dupe_size_",
    values_from = percent
  ) %>%
  arrange(
    desc(dupe_size_3),
    desc(dupe_size_2),
    grantee_state,
    desc(grantee_type),
    grantee_name
  )

# List of grantees with file-dupe issues to send to Treasury
dupes_file_issues <- dupes_within_grantee %>%
  left_join(files %>%
    select(grantee_id_era2, contains("file_number")), by = "grantee_id_era2") %>%
  filter(dupe_size_1 != 1 | is.na(dupe_size_1))

# Dupes within files
dupes_within_file_conservative <- dupes_ids %>%
  group_by(
    grantee_id_era2,
    grantee_name,
    grantee_state,
    grantee_type,
    file,
    dupe_size_within_file_conservative
  ) %>%
  summarize(n = n()) %>%
  mutate(percent = n / sum(n)) %>%
  ungroup() %>%
  select(-n) %>%
  arrange(dupe_size_within_file_conservative) %>%
  pivot_wider(
    names_from = dupe_size_within_file_conservative,
    names_prefix = "dupe_size_",
    values_from = percent
  ) %>%
  arrange(grantee_state, desc(grantee_type), grantee_name)

# Write out intermediate products ------------------------------------------
# 
# # Dupe IDs
# write_parquet(dupes_ids,
#           str_c(data_path, "1_intermediates", "phpdfs", "3_deduplication",
#                 str_c("era2_q4_dupe_ids_", Sys.Date(),".parquet"),
#                 sep = "/"))
# 
# 
# # Dupes across grantees
# write_csv(dupes_across_grantees_summary,
#           str_c(data_path, "1_intermediates", "phpdfs","3_deduplication", "reports",
#                 str_c("era2_q4_dupes_across_grantees_", Sys.Date(),".csv"),
#                 sep = "/"))
# 
# # Dupes within grantee
# write_csv(dupes_within_grantee,
#           str_c(data_path, "1_intermediates", "phpdfs",  "3_deduplication","reports",
#                 str_c("era2_q4_dupes_within_grantee_", Sys.Date(),".csv"),
#                 sep = "/"))
# 
# # Dupes across files (+ filenames)
# write_csv(dupes_file_issues,
#           str_c(data_path, "1_intermediates", "phpdfs", "3_deduplication","reports",
#                 str_c("era2_q4_file_dupes_with_filenames_", Sys.Date(),".csv"),
#                 sep = "/"))
# 
# # Dupes within file (conservative)
# write_csv(dupes_within_file_conservative,
#           str_c(data_path, "1_intermediates", "phpdfs",  "3_deduplication","reports",
#                 str_c("era2_q4_dupes_within_file_conservative_", Sys.Date(),".csv"),
#                 sep = "/"))

# Dedupe: across-grantee --------------------------------------------------

# 1) Resolve across-grantee dupes:
# If the same records exist in overlapping jurisdictions,
# only keep the records in the smallest jurisdiction.

fix_step_1 <- dupes_ids %>%
  # Lucas County -> Toledo
  # filter: removed 2,448 rows (<1%), 5,952,453 rows remaining
  filter(!(grantee_id_era2 == "SLT-3742" &
    dupe_size_within_source > 1)) %>%
  # North Carolina -> Wake County, Guildford County
  # filter: removed 2,903 rows (<1%), 5,949,550 rows remaining
  filter(!(grantee_id_era2 == "SLT-0199" &
    dupe_size_within_source > 1)) %>%
  # Arizona -> Yavapai County, Pinal County
  # filter: removed 23 rows (<1%), 5,949,527 rows remaining
  filter(!(grantee_id_era2 == "SLT-0165" &
    dupe_size_within_source > 1)) %>%
  # New Jersey -> Burlington County
  # filter: removed one row (<1%), 5,949,526 rows remaining
  filter(!(grantee_id_era2 == "SLT-0240" &
    dupe_size_within_source > 1))

# Dedupe: across-file -----------------------------------------------------

# 2) Resolve across-file dupes:
# If any record exists identically across more than 1 file,
# drop this record from all but the largest file.

# First, order files by grantee per size
file_sizes <- dupes_ids %>%
  group_by(grantee_id_era2, file) %>%
  summarize(n = n()) %>%
  mutate(size_rank = rank(desc(n), ties.method = "first")) %>%
  ungroup()

# Extract rows without issues
fix_step_2a <- fix_step_1 %>%
  # removed 77,589 rows (1%), 5,871,937 rows remaining
  filter(dupe_size_within_grantee == 1)

# Extract rows to be deduped and dedupe
fix_step_2b <- fix_step_1 %>%
  # filter: removed 5,871,937 rows (99%), 77,589 rows remaining
  filter(dupe_size_within_grantee > 1) %>%
  left_join(file_sizes %>%
    select(-n), by = c("grantee_id_era2", "file")) %>%
  # slice_min: removed 38,941 rows (50%), 38,648 rows remaining
  slice_min(
    order_by = size_rank,
    by = dedupe_id_within_grantee,
    with_ties = TRUE
  ) %>%
  select(-size_rank)

# Join both
fix_step_2c <- fix_step_2a %>%
  bind_rows(fix_step_2b) %>%
  arrange(file_specific_row_id)

# Testing

# Making sure everything was deduped
test_step_2_1 <- fix_step_2c %>%
  group_by(
    address_line_1,
    address_line_2,
    address_line_3,
    city_name,
    state_code,
    zip5,
    zip4,
    payee_type,
    type_of_assistance,
    amount_of_payment,
    date_of_payment,
    start_date,
    end_date,
    program,
    grantee_id_era2
  ) %>%
  mutate(dedupe_id_within_grantee = cur_group_id(), .before = everything()) %>%
  mutate(
    dupe_size_within_grantee = n_distinct(file),
    .before = original_filename
  )

# Everything should have dupe size 1
tabyl(test_step_2_1$dupe_size_within_grantee)

test_step_2_2 <- fix_step_1 %>%
  group_by(
    grantee_id_era1,
    grantee_name,
    grantee_state,
    grantee_type,
    dupe_size_within_grantee
  ) %>%
  summarize(n = n()) %>%
  ungroup() %>%
  arrange(dupe_size_within_grantee) %>%
  pivot_wider(
    names_from = dupe_size_within_grantee,
    names_prefix = "dupe_size_",
    values_from = n
  ) %>%
  arrange(
    desc(dupe_size_3),
    desc(dupe_size_2),
    grantee_state,
    desc(grantee_type),
    grantee_name
  ) %>%
  rowwise() %>%
  mutate(expected_n = sum(dupe_size_2 / 2, dupe_size_3 / 3, na.rm = TRUE)) %>%
  ungroup()

# All rows should show check == FALSE
test_step_2_3 <- fix_step_2b %>%
  summarize(
    .by = c(grantee_id_era1, grantee_name),
    n = n()
  ) %>%
  left_join(test_step_2_2 %>%
    select(grantee_id_era1, expected_n)) %>%
  mutate(check = n != expected_n) %>%
  arrange(desc(check), desc(n))

# Dedupe: within-file -----------------------------------------------------

# 3) Resolve within-file dupes:
# Within each file, drop duplicated rows, but treat NA values for:
# address_line_1, payee_type, amount_of_payment, date_of_payment, type_of_assistance, start_date
# as unique values

# Extract rows without issues
fix_step_3a <- fix_step_2c %>%
  # filter: removed 15,630 rows (<1%), 5,894,955 rows remaining
  filter(dupe_size_within_file_conservative == 1)

# Extract rows to be deduped and dedupe
fix_step_3b <- fix_step_2c %>%
  # filter: removed 5,894,955 rows (>99%), 15,630 rows remaining
  filter(dupe_size_within_file_conservative > 1) %>%
  # slice_min: removed 9,967 rows (64%), 5,663 rows remaining
  slice_min(
    order_by = file_specific_row_id,
    by = dedupe_id_within_file_conservative,
    with_ties = FALSE
  )

# Join both
fix_step_3c <- fix_step_3a %>%
  bind_rows(fix_step_3b) %>%
  arrange(file_specific_row_id) %>%
  select(-contains("_na_filled"))

# Testing

# Long-format list of within-file dupes
test_step_3_1 <- dupes_ids %>%
  group_by(
    grantee_id_era2,
    grantee_name,
    grantee_state,
    grantee_type,
    file,
    dupe_size_within_file_conservative
  ) %>%
  summarize(n = n()) %>%
  ungroup() %>%
  arrange(dupe_size_within_file_conservative) %>%
  filter(dupe_size_within_file_conservative > 1)

# Example of within-file dupes for a chosen grantee, marking which one was kept
test_step_3_2 <- dupes_ids %>%
  filter(grantee_id_era2 == "SLT-0274") %>%
  filter(dupe_size_within_file_conservative > 1) %>%
  mutate(
    kept = file_specific_row_id %in% fix_step_3b$file_specific_row_id,
    .before = address_line_1
  ) %>%
  arrange(
    desc(dupe_size_within_file_conservative),
    dedupe_id_within_file_conservative,
    desc(kept)
  )

# How many of within-file dupes involve $0 payments?
test_step_3_3 <- dupes_ids %>%
  # filter: removed 5,939,189 rows (>99%), 15,712 rows remaining
  filter(dupe_size_within_file_conservative > 1) %>%
  # filter: removed 9,592 rows (61%), 6,120 rows remaining
  filter(amount_of_payment == 0)

# Remaining file dupes ----------------------------------------------------

# Which grantees still have more than 1 file in the dataset?
file_dupes_remaining <- fix_step_3c %>%
  group_by(grantee_id_era2, grantee_name, file) %>%
  summarize(n = n()) %>%
  mutate(percent = n / sum(n)) %>%
  ungroup() %>%
  filter(percent != 1)

# Check remaining dupes just by address
file_dupes_address <- fix_step_3c %>%
  filter(
    grantee_id_era2 %in% file_dupes_remaining$grantee_id_era2 &
      file %in% file_dupes_remaining$file
  ) %>%
  group_by(grantee_id_era2, address_line_1) %>%
  mutate(dupe_id_address = cur_group_id(), .before = everything()) %>%
  mutate(dupe_size_address = n_distinct(file), .before = everything()) %>%
  ungroup()

file_dupes_address_summary <- file_dupes_address %>%
  group_by(grantee_id_era2, grantee_name, file, dupe_size_address) %>%
  summarize(n = n()) %>%
  mutate(percent = n / sum(n)) %>%
  ungroup()

# Check remaining dupes just by address and payment amount
file_dupes_address_amount <- fix_step_3c %>%
  filter(
    grantee_id_era2 %in% file_dupes_remaining$grantee_id_era2 &
      file %in% file_dupes_remaining$file
  ) %>%
  group_by(grantee_id_era2, address_line_1, amount_of_payment) %>%
  mutate(dupe_id_address_amount = cur_group_id(), .before = everything()) %>%
  mutate(
    dupe_size_address_amount = n_distinct(file),
    .before = everything()
  ) %>%
  ungroup()

file_dupes_address_amount_summary <- file_dupes_address_amount %>%
  group_by(grantee_id_era2, grantee_name, file, dupe_size_address_amount) %>%
  summarize(n = n()) %>%
  mutate(percent = n / sum(n)) %>%
  ungroup()

# Check individual grantee
file_dupes_grantee_check <- file_dupes_address_amount %>%
  filter(grantee_id_era2 == "SLT-2001" &
    dupe_size_address_amount > 1) %>%
  arrange(address_line_1, file)

# The upshot is that the only grantee clearly diagnosable with a remaining dupe
# issue is Riverside, where the remaining 10 payments in one file are identical
# to records in the other file except for leading zeroes on unit numbers.
# The other grantees' remaining file dupes do not seem to be due to other
# unaddressed sources of systematic duplication.
# We drop the smaller Riverside file, whose addresses are 100% also found in their
# larger file.
fix_step_4 <- fix_step_3c %>%
  # filter: removed 10 rows (<1%), 5,900,608 rows remaining
  filter(
    !(
      grantee_id_era2 == "SLT-2001" &
        file == "COUNTY OF RIVERSIDE - ERA 2 Quarter 4 2023 - treasury-q4-2023-participant data-rivco-erap2-merged.xlsx"
    )
  ) %>%
  select(-matches("dupe"))

# Dedupe summary ----------------------------------------------------------

# Number/percentage of dropped rows due to deduplication
nrow(era2_initial) - nrow(fix_step_4)
# 54293

(nrow(era2_initial) - nrow(fix_step_4)) / nrow(era2_initial)
# 0.009117364

# Write out deduped data --------------------------------------------------

# write_parquet(fix_step_4,
#           str_c(data_path, "1_intermediates", "phpdfs", "3_deduplication",
#                 str_c("era2_q4_deduplicated_", Sys.Date(),".parquet"),
#                 sep = "/"))


toc()
# 746.333 sec elapsed