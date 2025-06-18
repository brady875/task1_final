#!/bin/bash

while getopts ":p:c:s:e:" opt; do
  case $opt in
    p) PPR="$OPTARG"
    ;;
    c) COALITION="$OPTARG"
    ;;
    s) SECONDPPR="$OPTARG"
    ;;
    e) SECONDCOALITION="$OPTARG"
    ;;
    \?) echo "Invalid option -$OPTARG" >&2
    exit 1
    ;;
  esac
done


var=$(date +"%FORMAT_STRING")
now=$(date +"%m%d%Y")

repo_dir=$PWD

# Go to Raw Data directory
cd -- "$OneDrive"
pwd
cd "YourDataDirectory/Raw Data"

root_dir=$PWD

# Identify 2023 States and Tribes and Coalitions Excel files from your raw data directory
ppr_name=$(find States\ and\ Tribes -maxdepth 1 -type f -name "*.xlsx")
coalition_name=$(find Coalitions -maxdepth 1 -type f -name "*.xlsx")

# Identify 2024 States and Tribes and Coalitions Excel files from your raw data directory
secondppr_name=$(find "YourDataDirectory/Raw Data/States and Tribes 2024" -maxdepth 1 -type f -name "*.xlsx")
secondcoalition_name=$(find "YourDataDirectory/Coalitions 2024" -maxdepth 1 -type f -name "*.xlsx")


ppr_name_basename="${ppr_name%.xlsx}"
dir="${ppr_name_basename%/}"
ppr_name_no_ext="${dir##*/}"

coalition_name_basename="${coalition_name%.xlsx}"
dir="${coalition_name_basename%/}"
coalition_name_no_ext="${dir##*/}"

secondppr_name_basename="${secondppr_name%.xlsx}"
dir="${secondppr_name_basename%/}"
secondppr_name_no_ext="${dir##*/}"

secondcoalition_name_basename="${secondcoalition_name%.xlsx}"
dir="${secondcoalition_name_basename%/}"
secondcoalition_name_no_ext="${dir##*/}"

rename_ppr="${ppr_name_no_ext}_Archived_${now}.xlsx"
rename_coalition="${coalition_name_no_ext}_Archived_${now}.xlsx"
rename_secondppr="${secondppr_name_no_ext}_Archived_${now}.xlsx"
rename_secondcoalition="${secondcoalition_name_no_ext}_Archived_${now}.xlsx"

new_ppr="fvps_sf-ppr_state_ver__6_(fy__2018_to_2021)_${now}.xlsx"
new_coalition="fvpsa_performance_progress_report_ver_1_(fy_2001_to_2024)_${now}.xlsx"
new_secondppr="fvps_sf-ppr_state_ver__8_(fy__2024_to_2027)_${now}.xlsx"
new_secondcoalition="fvpsa_performance_progress_report_ver_2_(fy_2024_to_2027)_${now}.xlsx"

legacy_ppr="States and Tribes/Archive/${rename_ppr}"
legacy_coalition="Coalitions/Archive/${rename_coalition}"
legacy_secondppr="States and Tribes 2024/Archive/${rename_secondppr}"
legacy_secondcoalition="Coalitions 2024/Archive/${rename_secondcoalition}"


# Rename the file
echo "Moving current PPR version to ${legacy_ppr}..."
$(mv "$ppr_name" "$legacy_ppr")

# Rename the file
echo "Moving current coalition PPR version to ${legacy_coalition}..."
$(mv "$coalition_name" "$legacy_coalition")

# Rename the file
echo "Moving current second PPR version to ${legacy_secondppr}..."
$(mv "$secondppr_name" "$legacy_secondppr")

# Rename the file
echo "Moving current second coalition PPR version to ${legacy_secondcoalition}..."
$(mv "$secondcoalition_name" "$legacy_secondcoalition")

# Back to root directory
cd "${root_dir}"

echo "DEBUG: Checking file paths before copying..."
echo "PPR: ${PPR}"
echo "COALITION: ${COALITION}"
echo "SECONDPPR: ${SECONDPPR}"
echo "SECONDCOALITION: ${SECONDCOALITION}"

# Rename to proper format
echo "Copying ${PPR} to Raw Data directory and renaming..."
cp "${PPR}" "States and Tribes/${new_ppr}"

echo "Copying ${COALITION} to Raw Data directory and renaming..."
cp "${COALITION}" "Coalitions/${new_coalition}"

echo "Copying ${SECONDPPR} to Raw Data directory and renaming..."
cp "${SECONDPPR}" "States and Tribes 2024/${new_secondppr}" 

echo "Copying ${SECONDCOALITION} to Raw Data directory and renaming..."
cp "${SECONDCOALITION}" "Coalitions 2024/${new_secondcoalition}" 


# Run data processing
echo "Running data processing..."



cd "${repo_dir}"
cd ScriptFiles/Processing\ Scripts

# For 2023 data processing
python -u process_PPR_data.py -f -pc

# for 2024 data processing
python -u process_PPR_data.py -ps2024 --new_states_OLDC_filename="${secondppr_name}" -pc2024 --new_coalitions_OLDC_filename="${secondcoalition_name}"

deactivate

root_dir=$PWD

# Run R processing for briefing reports
cd "${repo_dir}"
Rscript data_analysis/read_data.R
Rscript data_analysis/read_data_coalitions.R

