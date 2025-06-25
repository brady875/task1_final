# Authored on: 04/04/2025
# Created by: Paul Ursino
# Funding referenced: Core FVPSA funding for 2023 and 2024

onedrive_path <- "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation"
# for Paul
data_folder <- "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative/Outputs"

onedrive_path <- gsub("\\\\","/", gsub("OneDrive - ","", Sys.getenv("OneDrive")))
data_folder <- file.path(onedrive_path, 
                         "ACF Office of Family Violence Prevention and Services - General",
                         "Data", "Quantitative", "Outputs")


# GLOBAL VARIABLES
# File path of analysis Rmd file
RMD_FP <- file.path(here::here(), "data_analysis", "Core", "Subawardee Report", "Core_subawardees_23_24.Rmd")

# Folder name where versioned file will get saved to
OUTPUT_DIR <- file.path(data_folder, "Core", gsub(".Rmd", "", basename(RMD_FP)))

# Date and time to append to rendered file
TIMESTAMP <- strftime(Sys.time(), format = "%Y-%m-%d_%H%M%S")

# Path to output file
OUTPUT_FILE <- file.path(OUTPUT_DIR, paste0(basename(OUTPUT_DIR), "_rendered_", TIMESTAMP, ".docx"))

if (!dir.exists(OUTPUT_DIR)) {
  dir.create(OUTPUT_DIR, recursive = TRUE)
}

# Run
rmarkdown::render(
  input = RMD_FP,
  output_format = "word_document",
  output_file = OUTPUT_FILE,
  params = list(report_link = gsub(onedrive_path, "", OUTPUT_FILE))
)
