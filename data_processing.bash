#!/bin/bash

# Set the OneDrive environment variable directly in the script - for Paul only
#export OneDrive="/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation"

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
cd "ACF FVPS/Data/Quantitative/Raw Data"

root_dir=$PWD

ppr_name=$(find States\ and\ Tribes -maxdepth 1 -type f -name "*.xlsx")
coalition_name=$(find Coalitions -maxdepth 1 -type f -name "*.xlsx")
#secondppr_name=$(find States\ and\ Tribes\ 2024 -maxdepth 1 -type f -name "*.xlsx")
secondppr_name=$(find "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative/Raw Data/States and Tribes 2024" -maxdepth 1 -type f -name "*.xlsx")
echo "DEBUG: secondppr_name=${secondppr_name}"

#secondcoalition_name=$(find Coalitions\ 2024 -maxdepth 1 -type f -name "*.xlsx")
secondcoalition_name=$(find "/Users/pursino/Library/CloudStorage/OneDrive-TheMITRECorporation/ACF FVPS/Data/Quantitative/Raw Data/Coalitions 2024" -maxdepth 1 -type f -name "*.xlsx")
echo "DEBUG: new_secondcoalition=${new_secondcoalition}"


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

echo "DEBUG: Attempting to read ${new_states_OLDC_filename}"


echo "DEBUG: Found PPR file -> ${ppr_name}"
echo "DEBUG: Found Coalition file -> ${coalition_name}"
echo "DEBUG: Found Second PPR file -> ${secondppr_name}"
echo "DEBUG: Found Second Coalition file -> ${secondcoalition_name}"


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
cp "${SECONDPPR}" "States and Tribes 2024/${new_secondppr}" || echo "ERROR: Failed to copy ${SECONDPPR}"

echo "Copying ${SECONDCOALITION} to Raw Data directory and renaming..."
cp "${SECONDCOALITION}" "Coalitions 2024/${new_secondcoalition}" || echo "ERROR: Failed to copy ${SECONDCOALITION}"


# Run data processing
echo "Running data processing..."

echo "DEBUG: Running Python script with secondppr_name=${secondppr_name}"
echo "DEBUG: Running Python script with new_states_OLDC_filename=${secondppr_name}"
#echo "DEBUG: Running Python script with new_coalitions_OLDC_filename=${new_coalitions_OLDC_filename}"
echo "DEBUG: Running Python script with new_coalitions_OLDC_filename=${new_secondcoalition}"



cd "${repo_dir}"
#. /Users/pursino/Desktop/git/fvpsa-data-analysis/fvpsa_env/bin/activate  
. /Users/pursino/Desktop/git/fvpsa-data-analysis/ScriptFiles/Processing\ Scripts/fvpsa_env/bin/activate
cd ScriptFiles/Processing\ Scripts

# For 2023 data processing
python -u process_PPR_data.py -f -pc

# for 2024 data processing
python -u process_PPR_data.py -ps2024 --new_states_OLDC_filename="${secondppr_name}" -pc2024 --new_coalitions_OLDC_filename="${secondcoalition_name}"
#python -u process_PPR_data.py -ps2024 --new_states_OLDC_filename="${secondppr_name}" -spf2024 "${repo_dir}/Processed Data/States and Tribes 2024/HistoricalPPR_${now}_processed.xlsx" -pc2024 --new_coalitions_OLDC_filename="${secondcoalition_name}" -cf2024 "${repo_dir}/Processed Data/Coalitions 2024/coalitions_processed_${now}.xlsx"

deactivate

root_dir=$PWD

# Run R processing for briefing reports
cd "${repo_dir}"
Rscript data_analysis/read_data.R
Rscript data_analysis/read_data_coalitions.R

