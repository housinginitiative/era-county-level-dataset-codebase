# Purpose -----------------------------------------------------------------
# Read in and clean NLIHC program crosswalk data.

# Preliminaries -----------------------------------------------------------

library(tidyverse)
library(tidylog)
library(janitor)
library(readxl)
library(skimr)
library(tictoc)

tic()
# Locally specific part of data folder path already stored in Renviron file
data_path = str_c(Sys.getenv("LOCAL_PATH"), "era-county-level-dataset-data", sep = "/")

# ERA1 grantees -------------------------------------------------------------------------------

grantees_era1_raw <- 
  read_excel(str_c(data_path, "0_raw_data", "public_data", "nlihc_program_crosswalks",
                   "ERA1 UniqueProgram IDs to Grantees 4 27 23.xlsx",
                   sep = "/")) %>% 
  clean_names() 

grantees_era1_clean <- grantees_era1_raw %>% 
  rename(grantee_id_era1 = emergency_rental_application_name,
         grantee_name = recipient_name,
         grantee_state = state,
         grantee_type = recipient_type) %>% 
  # Adjust some data to fix discrepancies
  mutate(grantee_state = 
           case_when(grantee_state == "Virgin Islands" ~ "U.S. Virgin Islands",
                     grantee_state == "Northern Mariana Isl" ~ "Northern Mariana Islands",
                     .default = grantee_state)) %>% 
  mutate(grantee_name = 
           case_when(grantee_name == "Government of the United States Virgin Islands" ~
                       "Government of the Virgin Islands",
                     grantee_name == "City of Salt Lake City" ~
                       "Salt Lake City",
                     grantee_name == "Dona Ana County" ~
                       "Doña Ana County",
                     .default = grantee_name)) %>% 
  mutate(locality =
           case_when(locality == "Dona Ana County" ~
                       "Doña Ana County",
                     locality == "Venture County" ~
                       "Ventura County",
                     locality == "Lexington-Fayette Urban County Government" ~
                       "Fayette County",
                     locality == "Louisville/Jefferson County Metro Government" ~
                       "Jefferson County",
                     .default = locality)) %>% 
  mutate(geographic_level =
           case_when(grantee_name == "Dallas County" ~ "County",
                     grantee_name == "Town of Oyster Bay" ~ "City",
                     # County-equivalent cities should be labeled as counties
                     grantee_name == "Municipality of Anchorage" ~ "County",
                     grantee_name == "City and County of San Francisco" ~ "County",
                     grantee_name == "City and County of Denver" ~ "County",
                     grantee_name == "City of New Orleans" ~ "County",
                     grantee_name == "City of Baltimore" ~ "County",
                     grantee_name == "City of St. Louis" ~ "County",
                     grantee_name == "City of Philadelphia" ~ "County",
                     .default = geographic_level)) %>% 
  mutate(grantee_type = 
           case_when(is.na(grantee_type) & geographic_level %in% c("County", "City") ~
                       "Local Government",
                     is.na(grantee_type) & geographic_level == "State" ~
                       "State/DC",
                     .default = grantee_type)) %>% 
  # Align locality names to Census Bureau standards
  mutate(locality = str_remove(locality, " County")) %>% 
  mutate(locality = str_remove(locality, "State of ")) %>% 
  mutate(locality = 
           case_when(grantee_name == "Municipality of Anchorage" ~ "Anchorage Municipality",
                     grantee_name == "City and County of San Francisco" ~ "San Francisco County",
                     grantee_name == "City and County of Denver" ~ "Denver County",
                     grantee_name == "City of New Orleans" ~ "Orleans Parish",
                     grantee_name == "City of Baltimore" ~ "Baltimore city",
                     grantee_name == "City of St. Louis" ~ "St. Louis city",
                     grantee_name == "City of Philadelphia" ~ "Philadelphia County",
                     grantee_name == "Town of Gilbert" ~ "Gilbert town",
                     grantee_name == "Town of Islip" ~ "Islip town",
                     grantee_name == "Town of Hempstead" ~ "Hempstead town",
                     grantee_name == "Town of Oyster Bay" ~ "Oyster Bay town",
                     grantee_name == "City of Boise" ~ "Boise City city",
                     grantee_name == "City of Saint Paul" ~ "St. Paul city",
                     grantee_name == "City of Indianapolis" ~ "Indianapolis city (balance)",
                     grantee_name == "Benton County" ~ "Benton County",
                     grantee_name == "Genesee County" ~ "Genesee County",
                     grantee_name == "City of Augusta" ~ "Richmond County",
                     geographic_level == "City" ~ str_c(locality, " City"),
                     geographic_level == "County" & grantee_state != "Louisiana" ~ str_c(locality, " County"),
                     geographic_level == "State" ~ grantee_state,
                     is.na(locality) ~ grantee_name,
                     .default = locality)) %>% 
  # String standardization
  mutate(grantee_name = str_to_upper(grantee_name)) %>% 
  mutate(grantee_name = str_remove_all(grantee_name, "[\\.\\,']")) %>% 
  mutate(locality = str_to_upper(locality)) %>% 
  mutate(locality = str_remove_all(locality, "[\\.\\,']")) 

# ERA2 grantees -------------------------------------------------------------------------------

grantees_era2_raw <- 
  read_excel(str_c(data_path, "0_raw_data", "public_data", "nlihc_program_crosswalks",
                   "ERA2 UniqueProgram IDs to Grantees 4 27 23.xlsx",
                   sep = "/")) %>% 
  clean_names() 

grantees_era2_clean <- grantees_era2_raw %>% 
  rename(grantee_id_era2 = slt_application_name_era2,
         grantee_name = recipient_name,
         grantee_state = state,
         grantee_name_alt = recipient_name_unclean_era2,
         grantee_type = recipient_type) %>% 
  # Adjust some data to fix discrepancies
  mutate(grantee_state = 
           case_when(grantee_state == "Virgin Islands" ~ "U.S. Virgin Islands",
                     grantee_state == "Northern Mariana Isl" ~ "Northern Mariana Islands",
                     .default = grantee_state)) %>% 
  mutate(grantee_name = 
           case_when(grantee_name == "Government of United States Virgin Islands" ~
                       "Government of the Virgin Islands",
                     grantee_name == "City of Salt Lake City" ~
                       "Salt Lake City",
                     grantee_name == "Dona Ana County" ~
                       "Doña Ana County",
                     .default = grantee_name)) %>% 
  mutate(locality =
           case_when(locality == "Dona Ana County" ~
                       "Doña Ana County",
                     locality == "Lexington-Fayette Urban County Government" ~
                       "Fayette County",
                     locality == "Louisville/Jefferson County Metro Government" ~
                       "Jefferson County",
                     .default = locality)) %>% 
  mutate(geographic_level =
           case_when(grantee_name == "Dallas County" ~ "County",
                     grantee_name == "Town of Oyster Bay" ~ "City",
                     # County-equivalent cities should be labeled as counties
                     grantee_name == "Municipality of Anchorage" ~ "County",
                     grantee_name == "City and County of San Francisco" ~ "County",
                     grantee_name == "City and County of Denver" ~ "County",
                     grantee_name == "City of New Orleans" ~ "County",
                     grantee_name == "City of Baltimore" ~ "County",
                     grantee_name == "City of St. Louis" ~ "County",
                     grantee_name == "City of Philadelphia" ~ "County",
                     grantee_name == "City of Modesto" ~ "City", 
                     grantee_name == "City of Fontana" ~ "City",
                     .default = geographic_level)) %>% 
  mutate(grantee_type = 
           case_when(grantee_name == "City of Baton Rouge" ~ 
                       "Local Government",
                     grantee_name == "City of Salt Lake City" ~ 
                       "Local Government",
                     grantee_name == "Government of the Virgin Islands" ~ 
                       "Territorial Government",
                     is.na(grantee_type) & geographic_level %in% c("County", "City") ~
                       "Local Government",
                     is.na(grantee_type) & geographic_level == "State" ~
                       "State/DC",
                     .default = grantee_type)) %>% 
  # Fix empty grantee IDs
  mutate(grantee_id_era2 = 
           case_when(grantee_name_alt == "Hialeah city" ~ "SLT-11854", 
                     grantee_name == "State of Nebraska" ~ "SLT-0069", 
                     .default = grantee_id_era2)) %>% 
  # Changing some names around to match the name used in the ERA1 crosswalk, 
  # preserving ERA2 names as alt names as applicable
  mutate(grantee_name_alt =
           case_when(grantee_name == "St. Johns County" ~ grantee_name,
                     grantee_name == "Commonwealth of Massachusetts" ~ grantee_name,
                     grantee_name == "Nashville-Davidson Metropolitan Government" ~ grantee_name,
                     .default = grantee_name_alt)) %>% 
  mutate(grantee_name = 
           case_when(grantee_name == "St. Johns County" ~ "Saint Johns County",
                     grantee_name == "Commonwealth of Massachusetts" ~ "State of Massachusetts",
                     grantee_name == "Nashville-Davidson Metropolitan Government" ~ 
                       "Nashville and Davidson County",
                     .default = grantee_name)) %>% 
  # Align locality names to Census Bureau standards
  mutate(locality = str_remove(locality, " County")) %>% 
  mutate(locality = str_remove(locality, "State of ")) %>% 
  mutate(locality = 
           case_when(grantee_name == "Municipality of Anchorage" ~ "Anchorage Municipality",
                     grantee_name == "City and County of San Francisco" ~ "San Francisco County",
                     grantee_name == "City and County of Denver" ~ "Denver County",
                     grantee_name == "City of New Orleans" ~ "Orleans Parish",
                     grantee_name == "City of Baltimore" ~ "Baltimore city",
                     grantee_name == "City of St. Louis" ~ "St. Louis city",
                     grantee_name == "City of Philadelphia" ~ "Philadelphia County",
                     grantee_name == "Town of Gilbert" ~ "Gilbert town",
                     grantee_name == "Town of Islip" ~ "Islip town",
                     grantee_name == "Town of Hempstead" ~ "Hempstead town",
                     grantee_name == "Town of Oyster Bay" ~ "Oyster Bay town",
                     grantee_name == "City of Boise" ~ "Boise City city",
                     grantee_name == "City of Saint Paul" ~ "St. Paul city",
                     grantee_name == "City of Indianapolis" ~ "Indianapolis city (balance)",
                     grantee_name == "Benton County" ~ "Benton County",
                     grantee_name == "Genesee County" ~ "Genesee County",
                     grantee_name == "City of Augusta" ~ "Richmond County",
                     geographic_level == "City" ~ str_c(locality, " City"),
                     geographic_level == "County" & grantee_state != "Louisiana" ~ str_c(locality, " County"),
                     geographic_level == "State" ~ grantee_state,
                     is.na(locality) ~ grantee_name,
                     .default = locality)) %>% 
  # String standardization
  mutate(across(contains("grantee_name"), ~ str_to_upper(.))) %>% 
  mutate(across(contains("grantee_name"), ~ str_remove_all(., "[\\.\\,']"))) %>% 
  mutate(locality = str_to_upper(locality)) %>% 
  mutate(locality = str_remove_all(locality, "[\\.\\,']")) 

# Joined ERA1/ERA2 --------------------------------------------------------

# Joined list of all NLIHC programs for ERA1/ERA2 with grantee IDs for either program
grantees_combined <- grantees_era1_clean %>% 
  select(grantee_state, grantee_type, grantee_name, locality, geographic_level, 
         grantee_id_era1, program_id_era1) %>% 
  full_join(grantees_era2_clean %>% 
              select(grantee_state, grantee_type, grantee_name, locality, geographic_level,
                     grantee_name_alt, grantee_id_era2, program_id_era2),
            by = c("grantee_state", "grantee_type", "grantee_name", "locality", "geographic_level")) %>% 
  relocate(grantee_state, grantee_type, grantee_name, grantee_name_alt,
           grantee_id_era1, grantee_id_era2, program_id_era1, program_id_era2,
           locality, geographic_level) %>% 
  arrange(grantee_state, desc(grantee_type)) %>% 
  # Generate combined ID
  mutate(grantee_id_combined = 
           case_when(is.na(grantee_id_era1) & is.na(grantee_id_era2) ~ 
                       NA,
                     !is.na(grantee_id_era1) & !is.na(grantee_id_era2) ~
                       str_c(grantee_id_era1, grantee_id_era2, sep = "_"),
                     .default = coalesce(grantee_id_era1, grantee_id_era2)),
         .after = grantee_id_era2) 

# Export --------------------------------------------------------------------------------------

# # Write out combined read and cleaned crosswalk
# write_rds(grantees_combined,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_combined", "_", Sys.Date(), ".RDS"),
#                 sep = "/"))
# 
# write_csv(grantees_combined,
#           str_c(data_path, "1_intermediates", "grantees",
#                 str_c("grantees_combined", "_", Sys.Date(), ".csv"),
#                 sep = "/"))

toc()
#11.948 sec elapsed
# Testing -----------------------------------------------------------------

# # Test for anomalies in program IDs between ERA1 and ERA2
# # 376 grantees that have the same program ID across ERA1 and ERA2
# # 32 grantees that had an ERA1 program but not an ERA2 program
# # 7 grantees that had an ERA2 program but not an ERA1 program
# # 4 grantees with no program ID for either ERA1/ERA2
# # 8 others:
# #   Placer County, CA: Had own ERA1, joined state ERA2
# #   Port St Lucie, FL: Joined with County of St Lucie for ERA1, own program for ERA2
# #   Wichita, KS: Own program for ERA1; for ERA2, Sedgwick County, KS joined it
# #       (which did not have an ERA1 program) and there is a new program ID for the collaboration
# #   City of St Louis, County of St Louis, MO: For ERA1, city and county had separate
# #       programs; for ERA2, city and county had same program
# #   Lincoln, NE: For ERA1, had same program as Lancaster County, NE; for ERA2, own program
# #   Las Vegas, NV: For ERA1, had same program as Clark County, Henderson, North LV;
# #       for ERA2, own program
# #   City of Durham: For ERA1, same program as Durham County; for ERA2, own program
# #
# # The upshot is that for matching to the NLIHC program characteristics database we
# # will need to match by both grantee ID and program, since both will be needed
# # to correctly specify the relevant program ID
# program_id_check <- grantees_combined %>%
#   mutate(id_difference =
#            case_when(program_id_era1 == program_id_era2 ~ "Pass",
#                      !is.na(program_id_era1) & is.na(program_id_era2) ~
#                        "ERA1 program but not ERA2 program",
#                      !is.na(program_id_era2) & is.na(program_id_era1) ~
#                        "ERA2 program but not ERA1 program",
#                      is.na(program_id_era2) & is.na(program_id_era1) ~
#                        "No program ID for either ERA1/ERA2",
#                      .default = "Check"),
#          .before = everything()) 
# 
# tabyl(program_id_check$id_difference)

