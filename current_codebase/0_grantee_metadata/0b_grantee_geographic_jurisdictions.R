# Purpose -------------------------------------------------------------------------------------
# Create crosswalks between ERA grantees and Census geographies.

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(janitor)
library(tigris)
library(sf)
library(tictoc)

tic()
options(tigris_use_cache = TRUE)

# Locally specific part of data folder path already stored in Renviron file
data_path = str_c(Sys.getenv("LOCAL_PATH"), "era-county-level-dataset-data", sep = "/")

# All grantees --------------------------------------------------------------------------------

grantees_all <- 
  read_rds(str_c(data_path, "1_intermediates", "grantees",
                 str_c("grantees_combined", "_", "2025-03-06", ".RDS"),
                 sep = "/")) %>% 
  select(grantee_name, grantee_state, locality, geographic_level, 
         grantee_id_era1, grantee_id_era2, grantee_id_combined) %>% 
  # filter: removed 35 rows (8%), 412 rows remaining
  filter(!is.na(grantee_id_era1) | !is.na(grantee_id_era2)) %>% 
  # filter: removed 5 rows (1%), 407 rows remaining
  filter(geographic_level != "Territory")

# Call tigris data ----------------------------------------------------------------------------

states <- states(cb = TRUE, year = 2020)

counties <- counties(cb = TRUE, year = 2020) 

places <- places(cb = TRUE, year = 2020) %>% 
  mutate(county_equivalent = st_equals(geometry, counties$geometry)) %>% 
  mutate(county_equivalent = lengths(county_equivalent) > 0) 

county_subdivisions <- state.abb %>% 
  map_dfr(~ county_subdivisions(cb = TRUE, state = ., year = 2020))

tracts <- tracts(cb = TRUE, year = 2020)

# Assign state GEOIDs -------------------------------------------------------------------------

grantees_states <- grantees_all %>% 
  filter(geographic_level == "State") %>% 
  inner_join(states %>% 
               st_drop_geometry() %>% 
               mutate(locality = str_to_upper(NAME)) %>% 
               select(locality, geoid_jurisdiction_state = GEOID))

# Assign county GEOIDs ------------------------------------------------------------------------

grantees_counties <- grantees_all %>% 
  filter(geographic_level == "County") %>% 
  inner_join(counties %>% 
               st_drop_geometry() %>% 
               mutate(locality = str_to_upper(NAMELSAD)) %>% 
               mutate(locality = str_remove_all(locality, "[\\.\\,']")) %>% 
               select(locality, grantee_state = STATE_NAME, geoid_jurisdiction_county = GEOID),
             by = c("locality", "grantee_state"))

# Assign city GEOIDs: Places ------------------------------------------------------------------

# For cities, we first use Census's 'Place' geography, since that matches incorporated local
# non-county jurisdictions in all cases of ERAP grantees except for the 3 New York state 'towns'
# (See: https://www2.census.gov/geo/pdfs/reference/GARM/Ch9GARM.pdf, p. 9-2)

grantees_cities_places <- grantees_all %>% 
  filter(geographic_level == "City") %>% 
  inner_join(places %>% 
               st_drop_geometry() %>% 
               mutate(locality = str_to_upper(NAMELSAD)) %>% 
               mutate(locality = str_remove_all(locality, "[\\.\\,']")) %>% 
               select(locality, grantee_state = STATE_NAME, geoid_jurisdiction_place = GEOID),
             by = c("locality", "grantee_state"))

# Assign city GEOIDs: MCDs ------------------------------------------------------------------

# For the 3 NY 'town' grantees, we need to bring in county subdivisions. For the other city 
# grantees as well, a county subdivision code is still desired if possible, since the geocoding
# to Place level is for some reason much less complete than the geocoding to county subdivisions.
# However, because of the complex interplay between county subdivisions and incoporated places,
# only some are available as MCDs, and even then, may not include all parts of the city.
# So the MCD codes are used as backup. Note that multiple MCDs may pertain to a single grantee.

grantees_cities_mcds <- grantees_all %>% 
  filter(geographic_level == "City") %>% 
  inner_join(county_subdivisions %>% 
               st_drop_geometry() %>% 
               mutate(locality = str_to_upper(NAMELSAD)) %>% 
               mutate(locality = str_remove_all(locality, "[\\.\\,']")) %>% 
               select(locality, grantee_state = STATE_NAME, geoid_jurisdiction_mcd = GEOID),
             by = c("locality", "grantee_state"))

# Assign city GEOIDs: Tracts ------------------------------------------------------------------

# As another layer of fallback, get tracts for cities in Places and MCDs file

tracts_places <- places %>% 
  filter(GEOID %in% grantees_cities_places$geoid_jurisdiction_place) %>% 
  st_join(tracts %>% 
            select(GEOID, NAMELSADCO, STATE_NAME), 
          join = st_relate, 
          pattern = "2********",
          left = TRUE,
          suffix = c("_place", "_tract"))

tracts_mcds <- county_subdivisions %>% 
  filter(GEOID %in% c("3605934000", "3610338000", "3605956000")) %>% 
  st_join(tracts %>% 
            select(GEOID, NAMELSADCO, STATE_NAME), 
          join = st_relate, 
          pattern = "2********",
          left = TRUE,
          suffix = c("_mcd", "_tract"))

grantees_cities_tracts_places <- grantees_cities_places %>% 
  left_join(tracts_places %>% 
              st_drop_geometry() %>% 
              select(geoid_jurisdiction_place = GEOID_place, 
                     geoid_jurisdiction_tract = GEOID_tract),
            by = "geoid_jurisdiction_place") %>% 
  select(-geoid_jurisdiction_place)

grantees_cities_tracts_mcds <- grantees_cities_mcds %>% 
  filter(geoid_jurisdiction_mcd %in% c("3605934000", "3610338000", "3605956000")) %>% 
  left_join(tracts_mcds %>% 
              st_drop_geometry() %>% 
              select(geoid_jurisdiction_mcd = GEOID_mcd, 
                     geoid_jurisdiction_tract = GEOID_tract),
            by = "geoid_jurisdiction_mcd") %>% 
  select(-geoid_jurisdiction_mcd)

grantees_cities_tracts_all <- grantees_cities_tracts_places %>% 
  bind_rows(grantees_cities_tracts_mcds)

# City-county crosswalk -----------------------------------------------------------------------

# Read in city/county population crosswalk
place_county_poulation_overlap <- 
  read_csv(str_c(Sys.getenv("LOCAL_PATH"),
                 "era-county-level-dataset",
                 "public_data",
                 "hud_crosswalks",
                 "geocorr2022_2507107313.csv",
                 sep = "/"))[-1,] %>% 
  type_convert() %>% 
  clean_names() %>% 
  mutate(city_geoid = str_c(state, place)) %>% 
  select(county_geoid = county, 
         county_name, 
         city_geoid, 
         city_name = place_name, 
         overlap_population = pop20, 
         county_to_city_overlap_percentage = afact, 
         city_to_county_overlap_percentage = afact2)

mcd_county_poulation_overlap <- 
  read_csv(str_c(Sys.getenv("LOCAL_PATH"),
                 "era-county-level-dataset",
                 "public_data",
                 "hud_crosswalks",
                 "geocorr2022_2507101846.csv",
                 sep = "/"))[-1,] %>% 
  type_convert() %>% 
  clean_names() %>% 
  mutate(city_geoid = str_c(county, cousub20)) %>% 
  select(county_geoid = county, 
         county_name, 
         city_geoid, 
         city_name = mcd_name, 
         overlap_population = pop20, 
         county_to_city_overlap_percentage = afact, 
         city_to_county_overlap_percentage = afact2) %>% 
  mutate(county_geoid = as.character(county_geoid)) %>% 
  filter(str_detect(city_name, "Oyster|Islip|Hempstead"))

city_county_poulation_overlap <- place_county_poulation_overlap %>% 
  bind_rows(mcd_county_poulation_overlap)

# Extract geometry for city grantees
city_geographies <- places %>% 
  select(geoid = GEOID) %>% 
  inner_join(grantees_cities_places %>% 
               select(grantee_name, grantee_state, grantee_id_combined, 
                      grantee_id_era1, grantee_id_era2, geoid_jurisdiction_place),
             by = c(geoid = "geoid_jurisdiction_place")) %>% 
  bind_rows(county_subdivisions %>% 
              filter(str_detect(NAME, "Islip|Hempstead|Oyster Bay")) %>% 
              select(geoid = GEOID) %>% 
              inner_join(grantees_cities_mcds %>% 
                           select(grantee_name, grantee_state, grantee_id_combined, 
                                  grantee_id_era1, grantee_id_era2, geoid_jurisdiction_mcd),
                         by = c(geoid = "geoid_jurisdiction_mcd"))) 

# Which geographic counties are city grantees located in?
city_county_crosswalk <- city_geographies %>% 
  rename(city_geoid = geoid) %>% 
  st_join(counties %>% 
            select(county_geoid = GEOID, county_name = NAMELSAD),
          join = st_relate, 
          pattern = "2********",
          left = TRUE) %>% 
  st_drop_geometry() %>% 
  left_join(city_county_poulation_overlap %>% 
              select(contains("geoid"), contains("percentage")),
            by = c("city_geoid", "county_geoid"))

# County-city overlap -----------------------------------------------------------------------

# Which county grantees have overlapping territory with city grantees and vice versa?
# (The 3 NY towns are not included but their counties were not grantees)
county_city_overlap <- counties %>% 
  # filter: removed 2,976 rows (92%), 259 rows remaining
  filter(GEOID %in% grantees_counties$geoid_jurisdiction_county) %>% 
  st_join(places %>% 
            filter(GEOID %in% grantees_cities_places$geoid_jurisdiction_place),
          join = st_relate, 
          pattern = "2********",
          left = FALSE,
          suffix = c("_county", "_city")) %>% 
  st_drop_geometry() %>% 
  select(geoid_jurisdiction_county = GEOID_county, 
         name_county = NAMELSAD_county, 
         geoid_jurisdiction_place = GEOID_city, 
         name_city = NAMELSAD_city) %>% 
  left_join(grantees_counties %>% 
              select(grantee_id_combined_county = grantee_id_combined,
                     geoid_jurisdiction_county),
            by = "geoid_jurisdiction_county") %>% 
  left_join(grantees_cities_places %>% 
              select(grantee_id_combined_city = grantee_id_combined,
                     geoid_jurisdiction_place),
            by = "geoid_jurisdiction_place") %>% 
  left_join(city_county_poulation_overlap %>% 
              select(contains("geoid"), contains("percentage")),
            by = c(geoid_jurisdiction_place = "city_geoid", 
                   geoid_jurisdiction_county = "county_geoid"))

# Export --------------------------------------------------------------------------------------

# write_rds(grantees_states,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_geographies_states", "_", Sys.Date(), ".RDS"),
#                 sep = "/"))
#
# write_rds(grantees_counties,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_geographies_counties", "_", Sys.Date(), ".RDS"),
#                 sep = "/"))
#
# write_rds(grantees_cities_places,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_geographies_cities_places", "_", Sys.Date(), ".RDS"),
#                 sep = "/"))
#
# write_rds(grantees_cities_mcds,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_geographies_cities_mcds", "_", Sys.Date(), ".RDS"),
#                 sep = "/"))
#
# write_rds(grantees_cities_tracts_all,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_geographies_cities_tracts", "_", Sys.Date(), ".RDS"),
#                 sep = "/"))
#
# write_rds(city_county_crosswalk,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_city_geographic_county_crosswalk", "_", Sys.Date(), ".RDS"),
#                 sep = "/"))
#
# write_rds(county_city_overlap,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_geographies_county_city_overlap", "_", Sys.Date(), ".RDS"),
#                 sep = "/"))

toc()

# 76.287 sec elapsed

