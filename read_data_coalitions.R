# Script to read and process Coalitions data

# Authored on: 03/26/2024
# Created by: Noelle Horvath
#             nhorvath@mitre.org

library(pins)
library(ggplot2)
library(readxl)
library(dplyr)
library(stringr)
library(tidyr)
library(qs)

## SETUP ----

# Global Variables
# onedrive_path <- gsub("\\\\","/", gsub("OneDrive - ","", Sys.getenv("OneDrive")))
# data_folder <- file.path(onedrive_path, 
#                          "ACF Office of Family Violence Prevention and Services - General",
#                          "Data", "Quantitative"
# )
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

data_folder <- "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative"


# Set up versioning
board <- board_folder(file.path(data_folder,
                                "Outputs"), versioned = TRUE)

##### Data through 2023 #####
## READ DATA ----

# Raw PPR data
# raw_data_path <- list.files(
#  file.path(
#     data_folder,
#     "Raw Data/Coalitions/"
#   ),
#   pattern = ".xlsx",
#   full.names = TRUE
# )[1]

raw_data_path <- list.files(
  "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative/Raw Data/Coalitions/",
  pattern = ".xlsx",
  full.names = TRUE
)[1]


sheets <- list("Search Criteria", "Screen-1", "Screen-2", "Screen-3", "Screen-4", "Screen-5", "Screen-6", "Screen-7")

dat <- lapply(sheets, function(sheetname) {
  read_xlsx(raw_data_path,
            sheet = sheetname
  )
})

names(dat) <- sheets

# Processed PPR Data

processed_data_path <- list.files(
  "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative/Processed Data/Coalitions/",
  pattern = ".xlsx",
  full.names = TRUE
)[1]

# processed_data_path <- list.files(
#   file.path(
#     data_folder,
#     "Processed Data/Coalitions/"
#   ),
#   pattern = ".xlsx",
#   full.names = TRUE
# )[1]



sheets_processed <- list(
  "I. Cover Page", 
  "II. FVPSA Funds", 
  "III. Coalition Members", 
  "IV. Narrative Questions", 
  "V. Summary of Activities", 
  "VI. Other Topics", 
  "VII. Training", 
  "Section IV Narr Long Format", 
  "Section V SoA Long Format"
)

dat_processed <- lapply(sheets_processed, function(sheetname) {
  read_xlsx(processed_data_path,
            sheet = sheetname
  )
})

names(dat_processed) <- sheets_processed


## LIGHT PROCESSING ----

# Funding Data
# Group by Fy, RptEin, ProgAcronym and select record with latest SubmitDate
funding_data <- dat$`Screen-2` |>
  group_by(
    Fy, 
    RptEin, 
    ProgAcronym) |>
  arrange(desc(SubmitDate), .by_group = TRUE) |>
  slice(1)

# Event Data
# Group by Fy, RptEin, ProgAcronym and select record with latest SubmitDate
event_data <- dat$`Screen-7` |>
  group_by(
    Fy, 
    RptEin, 
    ProgAcronym) |>
  arrange(desc(SubmitDate), .by_group = TRUE) |>
  slice(1)

# Training Data
# Group by Fy, RptEin, ProgAcronym and select record with latest SubmitDate
tta_data <- dat$`Screen-6` |>
  group_by(
    Fy, 
    RptEin, 
    ProgAcronym) |>
  arrange(desc(SubmitDate), .by_group = TRUE) |>
  slice(1) 

# Organization Data
# Group by Fy, RptEin, ProgAcronym and select record with latest SubmitDate
org_data <- dat$`Screen-3` |>
  group_by(
    Fy, 
    RptEin, 
    ProgAcronym) |>
  arrange(desc(SubmitDate), .by_group = TRUE) |>
  slice(1)

# Priority Area Data
pa_data <- dat_processed$`Section V SoA Long Format`

##### Data from 2024 onwards #####
## READ DATA ----

# Raw PPR data
raw_data_path_24 <- list.files(
  "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative/Raw Data/Coalitions 2024/",
  pattern = ".xlsx",
  full.names = TRUE
)[1]

# raw_data_path_24 <- list.files(
#   file.path(
#     data_folder,
#     "Raw Data/Coalitions 2024/"
#   ),
#   pattern = ".xlsx",
#   full.names = TRUE
# )[1]


sheets_24 <- list("Search Criteria", "Screen-1", "Screen-2", "Screen-3", "Screen-4", "Screen-5", "Screen-6", "Screen-7")

dat_24 <- lapply(sheets_24, function(sheetname) {
  read_xlsx(raw_data_path_24,
            sheet = sheetname
  )
})

names(dat_24) <- sheets_24

# Processed PPR Data
processed_data_path_24 <- list.files(
  "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative/Processed Data/Coalitions 2024/",
  pattern = ".xlsx",
  full.names = TRUE
)[1]

# processed_data_path_24 <- list.files(
#   file.path(
#     data_folder,
#     "Processed Data/Coalitions 2024/"
#   ),
#   pattern = ".xlsx",
#   full.names = TRUE
# )[1]


sheets_processed_24 <- list(
  "I. Cover Page", 
  "II. FVPSA Funds", 
  "III. Coalition Members", 
  "IV. Narrative Questions", 
  "V. Summary of Activities", 
  "VI. Other Topics", 
  "VII. Training", 
  "Section IV Narr Long Format", 
  "Section V SoA Long Format"
)

dat_processed_24 <- lapply(sheets_processed_24, function(sheetname) {
  read_xlsx(processed_data_path_24,
            sheet = sheetname
  )
})

names(dat_processed_24) <- sheets_processed_24


## LIGHT PROCESSING ----

# Funding Data
# Group by Fy, RptEin, ProgAcronym and select record with latest SubmitDate
funding_data_24 <- dat_24$`Screen-2` |>
  group_by(
    Fy, 
    RptEin, 
    ProgAcronym) |>
  arrange(desc(SubmitDate), .by_group = TRUE) |>
  slice(1)

# Event Data
# Group by Fy, RptEin, ProgAcronym and select record with latest SubmitDate
event_data_24 <- dat_24$`Screen-7` |>
  group_by(
    Fy, 
    RptEin, 
    ProgAcronym) |>
  arrange(desc(SubmitDate), .by_group = TRUE) |>
  slice(1)

# Training Data
# Group by Fy, RptEin, ProgAcronym and select record with latest SubmitDate
tta_data_24 <- dat_24$`Screen-6` |>
  group_by(
    Fy, 
    RptEin, 
    ProgAcronym) |>
  arrange(desc(SubmitDate), .by_group = TRUE) |>
  slice(1) 

# Organization Data
# Group by Fy, RptEin, ProgAcronym and select record with latest SubmitDate
org_data_24 <- dat_24$`Screen-3` |>
  group_by(
    Fy, 
    RptEin, 
    ProgAcronym) |>
  arrange(desc(SubmitDate), .by_group = TRUE) |>
  slice(1)

# Priority Area Data
pa_data_24 <- dat_processed_24$`Section V SoA Long Format`

## SAVE ----
# Save to board
dat_final <- list(funding_data = funding_data, event_data = event_data, tta_data = tta_data, org_data = org_data, pa_data = pa_data,
                  funding_data_24 = funding_data_24, event_data_24 = event_data_24, tta_data_24 = tta_data_24, org_data_24 = org_data_24, pa_data_24 = pa_data_24)
board |> pin_write(dat_final, name = "data_for_R_coalitions", type = "qs", description = paste0(basename(raw_data_path), ", ", basename(processed_data_path), ", ", basename(raw_data_path_24), ", ", basename(processed_data_path_24)))



