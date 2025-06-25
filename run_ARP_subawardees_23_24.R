# ARP Subawardees 2023 and 2024 

# Authored on: 6/20/2024
# Created by: Paul Ursino (pursino@mitre.org)

 onedrive_path <- "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation"
 data_folder <- "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative/Outputs"


onedrive_path <- gsub("\\\\","/", gsub("OneDrive - ","", Sys.getenv("OneDrive")))
data_folder <- file.path(onedrive_path, 
                         "ACF Office of Family Violence Prevention and Services - General",
                         "Data", "Quantitative", "Outputs")


# GLOBAL VARIABLES --> UPDATE APPROPRIATELY
# File path of analysis Rmd file
# e.g. file.path(here::here(), "data_analysis", "ARP", "Briefing Reports", "states_and_tribes_arp_briefing_report.Rmd") 
RMD_FP <- file.path(here::here(), "data_analysis", "ARP", "Subawardee Report", "subawardees_arp_markdown_23_24.Rmd")

# Folder name where versioned file will get saved to
OUTPUT_DIR <- file.path(data_folder, "ARP", gsub(".Rmd", "", basename(RMD_FP)))

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

