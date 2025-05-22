# Purpose -----------------------------------------------------------------
# Impute counties for missing addresses for era2_q1

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tictoc)
library(arrow)
library(tidylog)
library(readxl)

tic()

# Locally specific part of data folder path already stored in Renviron file
data_path <- str_c(Sys.getenv("LOCAL_PATH"),
  "era-county-level-dataset-data",
  sep = "/"
)

# Load deduplicated data ---------------------------------------------------------------
era2_q1_deduped <-
  read_parquet(
    str_c(
      data_path,
      "1_intermediates",
      "phpdfs",
      "3_deduplication",
      "era2_q1_deduplicated_2025-03-12.parquet",
      sep = "/"
    )
  )

# See what we are starting with: 
# 5% of rows are missing a county
era2_q1_deduped %>% filter(is.na(geocode_county_geoid)) %>% nrow()
# filter: removed 3,855,723 rows (96%), 181,275 rows remaining

#----Use grantee geography for counties/single-county cities----
# First, impute for single-county cities ---------------------------------------------------------------
grantees_geographies_cities_tracts <- readRDS(
  str_c(
    data_path,
    "1_intermediates",
    "grantees",
    "grantees_geographies_cities_tracts_2025-03-20.RDS",
    sep = "/"
  )
) %>%
  # grab county
  mutate(imputed_county_geoid = str_extract(geoid_jurisdiction_tract, "^\\d{5}"))

# Determine which counties are single-county cities
single_county_cities <- grantees_geographies_cities_tracts %>%
  count(grantee_id_era2, imputed_county_geoid) %>%
  count(grantee_id_era2) %>%
  filter(n == 1) %>%
  left_join(grantees_geographies_cities_tracts,
    by = "grantee_id_era2"
  ) %>%
  select(-n) %>%
  select(grantee_id_era2, imputed_county_geoid) %>%
  distinct()

# Join the missing counties with the single county cities from the geography
# to get any counties that are included in the geography files
era2_q1_deduped2 <- era2_q1_deduped %>%
  left_join(single_county_cities, by = "grantee_id_era2")

# See how many counties we are getting back after imputing
era2_q1_deduped2 %>% filter(is.na(imputed_county_geoid)) %>% nrow()
# filter: removed 146,743 rows (4%), 3,890,255 rows remaining

# Next, impute for counties ---------------------------------------------------------------
# Read in county geographies
grantees_geographies_counties <- readRDS(
  str_c(
    data_path,
    "1_intermediates",
    "grantees",
    "grantees_geographies_counties_2025-03-20.RDS",
    sep = "/"
  )
)

# Join the grantees that are missing counties with the geography file to get counties for county-level sites
era2_q1_deduped3 <- era2_q1_deduped2 %>%
  left_join(
    grantees_geographies_counties %>%
      rename(imputed_county_geoid = geoid_jurisdiction_county) %>%
      select(grantee_id_era2, imputed_county_geoid),
    by = "grantee_id_era2"
  ) %>%
  mutate(
    imputed_county_geoid =
      coalesce(
        imputed_county_geoid.x,
        imputed_county_geoid.y
      )
  ) %>%
  select(
    -imputed_county_geoid.x,
    -imputed_county_geoid.y
  )

# See how many counties we are getting back after imputing total
era2_q1_deduped3 %>% filter(is.na(imputed_county_geoid)) %>% nrow()
# filter: removed 845,675 rows (21%), 3,191,323 rows remaining

#----Use City + ZIP : county crosswalk for states----
# read in HUD crosswalk
hud_xwalk <- read_excel(str_c(data_path,
                              "0_raw_data",
                              "public_data",
                              "geo_crosswalks",
  "ZIP_COUNTY_032023.xlsx",
  sep = "/"
))

# First, single county zips ---------------------------------------------------------------
# Determine which zip codes are just in one county and can be simply joined to get county info
single_county_zips <-
  hud_xwalk %>%
  count(ZIP, COUNTY) %>%
  count(ZIP) %>%
  filter(n == 1) %>%
  select(-n) %>%
  left_join(hud_xwalk, by = "ZIP") %>%
  select(ZIP, COUNTY, USPS_ZIP_PREF_STATE, USPS_ZIP_PREF_CITY)

# Join the single-county zip grantees that are missing counties with the HUD xwalk 
era2_q1_deduped4 <- era2_q1_deduped3 %>%
  # replace zips that are equal to 0 with NA
  mutate(ZIP = ifelse(zip5 == 0, NA, zip5)) %>%
  # all differences where zip does not match and are within the same state are just variations
  # i.e. "east boston" vs. "boston"
  left_join(single_county_zips,
    by = join_by(
      "ZIP" == "ZIP",
      "state_code" == "USPS_ZIP_PREF_STATE"
    )
  ) %>%
  mutate(imputed_county_geoid = coalesce(
    imputed_county_geoid,
    COUNTY
  )) %>%
  select(-COUNTY, -USPS_ZIP_PREF_CITY, -ZIP)

# See how many counties we are getting back after imputing total
era2_q1_deduped4 %>% filter(is.na(imputed_county_geoid)) %>% nrow()
# filter: removed 3,171,547 rows (79%), 865,451 rows remaining

# Next, multi county zips ---------------------------------------------------------------
# Determine which zip codes straddle multiple counties, then filter to use the county where
# 95% of the zip falls within a county
multi_county_zips <- hud_xwalk %>%
  count(ZIP, COUNTY, USPS_ZIP_PREF_CITY, USPS_ZIP_PREF_STATE) %>%
  count(ZIP, USPS_ZIP_PREF_CITY, USPS_ZIP_PREF_STATE) %>%
  filter(n > 1) %>%
  left_join(hud_xwalk,
    by = c("ZIP", "USPS_ZIP_PREF_CITY", "USPS_ZIP_PREF_STATE")
  ) %>%
  # Filter for counties where 95% of zip is within a county
  filter(RES_RATIO > 0.95)

# Join the multi-county zip grantees that are missing counties with the HUD xwalk 
era2_q1_deduped5 <- era2_q1_deduped4 %>%
  mutate(
    ZIP = ifelse(zip5 == 0, NA, zip5),
    upper_city = toupper(city_name)
  ) %>%
  left_join(
    multi_county_zips,
    by = join_by(
      "ZIP" == "ZIP",
      "state_code" == "USPS_ZIP_PREF_STATE"
    )
  ) %>%
  mutate(imputed_county_geoid = coalesce(
    imputed_county_geoid,
    COUNTY
  )) %>%
  select(
    -ZIP,
    -COUNTY,
    -n,
    -RES_RATIO,
    -BUS_RATIO,
    -OTH_RATIO,
    -TOT_RATIO
  )

# See how many counties we are getting back after imputing total
era2_q1_deduped5 %>% filter(is.na(imputed_county_geoid)) %>% nrow()
# filter: removed 3,655,861 rows (91%), 381,137 rows remaining

# bind back missing counties with full data frame
imputed_counties <- era2_q1_deduped5 %>% 
  mutate(county_geoid_coalesced = coalesce(geocode_county_geoid, imputed_county_geoid),
         state_geoid_coalesced = coalesce(geocode_state2ky, substr(county_geoid_coalesced, 1, 2)),
         # fix CT counties issue
         county_geoid_coalesced = ifelse(grantee_id_combined == "ERA-2101080787_SLT-0446", imputed_county_geoid, county_geoid_coalesced)
  )

# See how many counties were able to be imputed
imputed_counties %>%
  filter(!is.na(imputed_county_geoid) |
    !is.na(geocode_county_geoid)) %>%
  nrow()
# filter: removed 117,503 rows (3%), 3,919,495 rows remaining

# Out of rows that already had a county, 89% also had county imputed
imputed_counties %>% filter(!is.na(geocode_county_geoid), !is.na(imputed_county_geoid)) %>% nrow()
# filter: removed 444,909 rows (11%), 3,592,089 rows remaining

# Check how many geocoded counties do not match the imputed counties:
imputed_counties %>% filter(geocode_county_geoid!=imputed_county_geoid) %>% nrow()
# filter: removed 4,020,678 rows (>99%), 16,320 rows remaining

# Write output ---------------------------------------------------------------
write_parquet(imputed_counties,
  str_c(
    data_path,
    "1_intermediates",
    "phpdfs",
    "4_county_imputation",
    str_c("era2_q1_imputed_counties_", Sys.Date(),".parquet"),
    sep = "/"
))

toc()
# 43.258 sec elapsed
