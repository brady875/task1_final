# Script to read and process data for ARP data analysis

library(pins)
library(ggplot2)
library(readxl)
library(dplyr)
library(stringr)
library(tidyr)
library(qs)


## SETUP ----
# Set the OneDrive environment variable directly in the script
Sys.setenv(OneDrive = "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation")

# Verify the OneDrive environment variable
one_drive_path <- Sys.getenv("OneDrive")
print(paste("OneDrive path:", one_drive_path))

# Read processed data
data_path <- file.path(Sys.getenv("OneDrive"),
                      "ACF FVPS",
                       "Data", "Quantitative"
)


# Load data path
# Read processed data
# data_path <- file.path(Sys.getenv("OneDrive"),
#                       "ACF Office of Family Violence Prevention and Services - General",
#                      "Data", "Quantitative"
# )


# Set up versioning
board <- board_folder(file.path(data_path,
                                "Outputs"), versioned = TRUE)





# Load helper functions
# source(
#   file.path(
#     here::here(),
#     "data_analysis",
#     "ARP",
#     "arp_data_analysis_helper_functions.R")
# )


setwd("~/Desktop/git/fvpsa-data-analysis")
source(file.path("data_analysis", "ARP", "arp_data_analysis_helper_functions.R"))



## READ DATA ----
# Load 2023 Data
sheets <- list("WideFormat", "OriginalFormat", "ServiceOutcome", "Subawardee")
fn_23 <- list.files(file.path(data_path, "Processed Data/States and Tribes/"), pattern = ".xlsx", full.names = TRUE)[1]
dat <- lapply(sheets, function(sheetname) {
  read_xlsx(fn_23, sheet = sheetname)
})
names(dat) <- sheets

# Load 2024 Data
fn_24 <- list.files(file.path(data_path, "Processed Data/States and Tribes 2024/"), pattern = ".xlsx", full.names = TRUE)[1]
dat_24 <- lapply(sheets, function(sheetname) {
  read_xlsx(fn_24, sheet = sheetname)
})
names(dat_24) <- sheets

# Load Crosswalks
xw_path_23 <- file.path(data_path, "Lookup Tables", "Data_Element_Crosswalk.xlsx")
xw_path_24 <- file.path(data_path, "Lookup Tables", "Data_Element_Crosswalk_2024_Updates.xlsx")
xw <- read_xlsx(xw_path_23, sheet = "crosswalk")
xw_24 <- read_xlsx(xw_path_24, sheet = "crosswalk")

# Get pretty names of PPR columns
sub_cols <- submission_cols_23(xw_path_23, widedata = dat$WideFormat, originaldata = dat$OriginalFormat)
sub_cols_24 <- submission_cols_24(xw_path_24, widedata = dat_24$WideFormat, originaldata = dat_24$OriginalFormat)

## PROCESS FY23 DATA ----
wide_23 <- dat$WideFormat |> 
  mutate(across(sub_cols$all$pretty, ~as.character(.)))|> 
  pivot_longer(sub_cols$all$pretty, names_to = "metric", values_to = "value") |> 
  mutate(new_value = ifelse(is.na(value) | value == "0", 0, 1)) |> 
  left_join(
    dat$OriginalFormat |> distinct("Program Acronym" = ProgAcronym, EIN, "Grantee Name" = GranteeName, "Grant Type" = GranteeTypeTxt, "State" = PostalCode, Year = Fy, CodeTxt),
    by = c("Program Acronym", "EIN", "Grantee Name", "Grant Type", "State", "Year")
  ) |> 
  group_by(EIN, `Grant Type`, Year, `Program Acronym`, State) |> 
  mutate(ppr_completeness = case_when(sum(new_value) == 0 ~ "Empty", TRUE ~ "Complete")) |> 
  select(-new_value) |> 
  pivot_wider(names_from = "metric", values_from = "value") |> 
  mutate(across(sub_cols$numeric$pretty, ~as.numeric(.))) |> 
  mutate(`Program Acronym` = case_when(
    `Program Acronym` == "FVC6" ~ "ARP Act",
    `Program Acronym` == "FVPS" ~ "Core FVPSA",
    `Program Acronym` == "FVC3" ~ "CARES Act",
    TRUE ~ `Program Acronym`
  )) |> 
  
  ungroup()

wide_23 <- wide_23 |> filter(Year == 2023)

sub23 <- dat$Subawardee |> 
  mutate(
    CultSpec2 = ifelse(is.na(CultSpec2), "", CultSpec2),
    CultSpec3 = ifelse(is.na(CultSpec3), "", CultSpec3),
    cult_cat = paste(CultSpec2, CultSpec3, sep = " and "),
    cult_cat = sub(" and $", "", cult_cat),
    cult_cat = ifelse(cult_cat == "", "None", cult_cat)
  ) |> 
  mutate(ProgAcronym = case_when(
    ProgAcronym == "FVC6" ~ "ARP Act",
    ProgAcronym == "FVPS" ~ "Core FVPSA",
    ProgAcronym == "FVC3" ~ "CARES Act",
    TRUE ~ ProgAcronym
  ), csus = ifelse(cult_cat == "None", FALSE, TRUE)) |> 
  rename(
    "Rural Designation"=`Subawardee List - Classification of urban, rural, suburban or frontier`,
    "Shelter Type"=`Subawardee List - Type of Subawardee`
  ) |>  
  filter(Fy == 2023)  



## PROCESS FY24 DATA ----
wide_24 <- dat_24$WideFormat |> 
  mutate(across(sub_cols_24$all$pretty, ~as.character(.)))|> 
  pivot_longer(sub_cols_24$all$pretty, names_to = "metric", values_to = "value") |> 
  mutate(new_value = ifelse(is.na(value) | value == "0", 0, 1)) |> 
  left_join(
    dat_24$OriginalFormat |> distinct("Program Acronym" = ProgAcronym, EIN, "Grantee Name" = GranteeName, "Grant Type" = GranteeTypeTxt, "State" = PostalCode, Year = Fy, CodeTxt),
    by = c("Program Acronym", "EIN", "Grantee Name", "Grant Type", "State", "Year")
  ) |> 
  group_by(EIN, `Grant Type`, Year, `Program Acronym`, State) |> 
  mutate(ppr_completeness = case_when(sum(new_value) == 0 ~ "Empty", TRUE ~ "Complete")) |> 
  select(-new_value) |> 
  pivot_wider(names_from = "metric", values_from = "value") |> 
  mutate(across(sub_cols_24$numeric$pretty, ~as.numeric(.))) |> 
  mutate(`Program Acronym` = case_when(
    `Program Acronym` == "FVC6" ~ "ARP Act",
    `Program Acronym` == "FVPS" ~ "Core FVPSA",
    `Program Acronym` == "FVC3" ~ "CARES Act",
    `Program Acronym` == "FTC6" ~ "ARP COVID-19 Testing Supplemental",
    `Program Acronym` == "FSC6" ~ "ARP Support Survivors of Sexual Assualt",
    
    TRUE ~ `Program Acronym`
  )) |> 
  ungroup()

wide_24 <- wide_24 |> filter(Year == 2024)

sub24 <- dat_24$Subawardee |> 
  mutate(
    CultSpec2 = ifelse(is.na(CultSpec2), "", CultSpec2),
    CultSpec3 = ifelse(is.na(CultSpec3), "", CultSpec3),
    cult_cat = paste(CultSpec2, CultSpec3, sep = " and "),
    cult_cat = sub(" and $", "", cult_cat),
    cult_cat = ifelse(cult_cat == "", "None", cult_cat)
  ) |> 
  mutate(ProgAcronym = case_when(
    ProgAcronym == "FVC6" ~ "ARP Act",
    ProgAcronym == "FVPS" ~ "Core FVPSA",
    ProgAcronym == "FVC3" ~ "CARES Act",
    ProgAcronym == "FTC6" ~ "ARP COVID-19 Testing Supplemental",
    ProgAcronym == "FSC6" ~ "ARP Support Survivors of Sexual Assualt",
    TRUE ~ ProgAcronym
  ), csus = ifelse(cult_cat == "None", FALSE, TRUE)) |> 
  rename(
    "Rural Designation"=`Subawardee List - Classification of urban, rural, suburban or frontier`,
    "Shelter Type"=`Subawardee List - Type of Subawardee`
  ) |>  
  filter(Fy == 2024)  


## SAVE ----
dat <- list(
  xw      = xw,
  xw_24   = xw_24,
  wide_23 = wide_23,
  wide_24 = wide_24,
  sub23   = sub23,
  sub24   = sub24
)

# build a description from the actual files
desc <- paste0(
  basename(fn_23),  ", ",
  basename(fn_24),  ", ",
  basename(xw_path_23), ", ",
  basename(xw_path_24)
)

board |> pin_write(
  dat,
  name        = "data_for_R",
  type        = "qs",
  description = desc
)



