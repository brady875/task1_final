# NOTICE

# This (software/technical data) was produced for the U. S. Government under Contract Number 75FCMC18D0047,
# and is subject to Federal Acquisition Regulation Clause 52.227-14, Rights in Data-General. No other use other than
# that granted to the U. S. Government, or to those acting on behalf of the U. S. Government under that Clause is
# authorized without the express written permission of The MITRE Corporation.

# For further information, please contact The MITRE Corporation, Contracts Management Office, 7515 Colshire Drive,
# McLean, VA  22102-7539, (703) 983-6000.

# 2021 The MITRE Corporation.

# import statements
import pandas as pd
import numpy as np
import html
from datetime import date, datetime
from openpyxl.utils.dataframe import dataframe_to_rows
import processing_functions as pf
import coalitions_processing_functions as cpf
import shutil
import os
import time
import argparse
from dateutil.parser import parse
from functools import reduce
import glob



# Define data path
default_data_path = os.path.join(os.environ['OneDrive'], 'Your_Root_Directory', 'Your_Data_Folder')

def get_parser():
    parser = argparse.ArgumentParser(
        description="Process grantee PPR data and save as new file.",
    )
    # === Arguments for 2018-2021 States & Tribes  ===
    # Replace file paths accordingly
    parser.add_argument("--process_formula", "-f", action='store_true')

    parser.add_argument(
        "--formula_OLDC_data_filename",
        "-o",
        default=glob.glob(os.path.join(default_data_path,"Folder that contains the raw OLDC extracted data/States and Tribes/fvps_sf-ppr_state_ver__6_(fy__2018_to_2021)*.xlsx"))[0],
        help='File path of raw formula OLDC data. Default is "Folder that contains the raw OLDC extracted data/States and Tribes/fvps_sf-ppr_state_ver__6_(fy__2018_to_2021).xlsx"',
    )

    parser.add_argument(
        "--processed_data_filename",
        "-p",
        default=glob.glob(os.path.join(default_data_path, "Insert folder name where the processed data will be stored/States and Tribes/HistoricalPPR*.xlsx"))[0],
        help='File path of previously processed data. Default is "Processed Data/States and Tribes/HistoricalPPR.xlsx"',
    )

    # === Arguments for 2001-2023 Coalitions  ===
    parser.add_argument(
        "--process_coalitions",
        "-pc",
        action='store_true',
        help="True or False, whether or not to process coalitions data.",
    )

    parser.add_argument(
        "--processed_coalitions_data_filename",
        "-cf",
        default=glob.glob(os.path.join(default_data_path, "Insert folder name where the processed data will be stored/Coalitions/coalitions_processed*.xlsx"))[0]
    )

    parser.add_argument(
        "--coalitions_OLDC_filename",
        "-c",
        default=glob.glob(os.path.join(default_data_path, "Folder that contains the raw OLDC extracted data/Coalitions/fvpsa_performance_progress_report_ver_1_(fy_2001_to_2024)*.xlsx"))[0],
        help='File path of raw coalitions OLDC data. Default is "Raw Data/Coalitions/fvpsa_performance_progress_report_ver_1_(fy_2001_to_2024).xlsx"',
    )

    parser.add_argument(
        "--coalitions_names_filename",
        "-cn",
        default=os.path.join(default_data_path, "Lookup Tables/coalition_names.csv"), 
        help='File path of raw coalitions OLDC data. Default is "Lookup Tables/coalition_names.csv"',
    )

    parser.add_argument(
        "--crosswalk_filename",
        "-l",
        default=os.path.join(default_data_path, "Lookup Tables/Data_Element_Crosswalk.xlsx"),
        help='File path of lookup table for reference in processing. Default is "Lookup Tables/Data_Element_Crosswalk.xlsx"',
    )

    # === Arguments for 2024 Coalitions  ===
    parser.add_argument(
        "--process_new_coalitions",
        "-pc2024",
        action='store_true',
        help="True or False, whether or not to process new 2024 coalitions data.",
    )

    parser.add_argument(
        "--new_coalitions_OLDC_filename",
        "-c2024",
        default=glob.glob(os.path.join(default_data_path, "Folder that contains the raw OLDC extracted data/Coalitions 2024/fvpsa_performance_progress_report_ver_2_(fy_2024_to_2027)*.xlsx"))[0], 
        help="File path of raw coalitions PPR data for 2024.",
    )

    parser.add_argument(
        "--processed_new_coalitions_data_filename",
        "-cf2024",
        default=glob.glob(os.path.join(default_data_path, "Insert folder name where the processed data will be stored/Coalitions 2024/coalitions_processed*.xlsx"))[0],
        help="File path for processed coalitions PPR data for 2024.",
    )

    # ===  Arguments for 2024 States & Tribes  ===

    parser.add_argument(
        "--process_new_states",
        "-ps2024",
        action="store_true",
        help="True or False, whether or not to process new 2024 States and Tribes data.",
    )

    parser.add_argument(
        "--new_states_OLDC_filename",
        "-s2024",
        default=glob.glob(os.path.join(default_data_path, "Folder that contains the raw OLDC extracted data/States and Tribes 2024/fvps_sf-ppr_state_ver__8_(fy__2024_to_2027)*.xlsx"))[0],
        help="File path of raw States and Tribes PPR data for 2024.",
    )

    parser.add_argument(
        "--processed_new_states_data_filename",
        "-spf2024",
        default=glob.glob(os.path.join(default_data_path, "Insert folder name where the processed data will be stored/States and Tribes 2024/HistoricalPPR*.xlsx"))[0],
        help="File path for processed States and Tribes PPR data for 2024.",
    )

    # === Crosswalk for 2024 States & Tribes and Coalitions === 
    parser.add_argument(
        "--crosswalk_filename_2024",
        "-l2024",
        default=os.path.join(default_data_path, "Lookup Tables/Data_Element_Crosswalk_2024_Updates.xlsx"), 
        help="File path for the 2024 crosswalk file for coalitions data.",
    )

    return parser


def main(
    process_formula,                    # To process States & Tribes data (2018-2021)
    formula_OLDC_data_filename,         # Raw OLDC data path for States & Tribes (2018-2021)
    processed_data_filename,            # Path to the existing processed file to back up and overwrite with new States & Tribes output
    process_coalitions,                 # To process 2023 Coalitions data
    coalitions_OLDC_filename,           # Raw OLDC data path for 2023 Coalitions data
    processed_coalitions_data_filename, # Output path to save processed 2023 Coalitions data
    coalitions_names_filename,          # CSV containing name mappings to normalize coalition names during cleaning
    crosswalk_filename,                 # Crosswalk for 2023 data
    process_new_coalitions,             # To process 2024 Coalitions data
    new_coalitions_OLDC_filename,      # Raw OLDC data path for 2024 Coalitions data
    processed_new_coalitions_data_filename,  # Output path to save proessed 2024 Coalitions data
    process_new_states,                # To process 2024 States & Tribes data
    new_states_OLDC_filename,          # Raw OLDC data path for 2024 States & Tribes data
    processed_new_states_data_filename,  # Output path to save processed 2024 States & Tribes data
    crosswalk_filename_2024            # Crosswalk for 2024 data
):
    string_date = datetime.today().strftime('%m%d%Y_%H%M%S')
    if process_formula:
        t1 = time.time()

        oldc_pull_splits = formula_OLDC_data_filename.split("_")
        oldc_pull_date = oldc_pull_splits[len(oldc_pull_splits)-1].replace(".xlsx", "")


        # INITIALIZE GLOBAL VARIABLES
        # ==================================================================================================================
        all_states = sorted(
            "PA MS PR LA NM AZ FL AK OK HI KS DE IN ND MT WA RI KY TN OH IA WV ID GA WI MD NE VT ME VA TX CA UT NC NJ NV "
            "MI MN OR NY DC SD WY CO MA IL CT AR MO NH SC AL".split()
        )

        # Read in data files
        print("Reading in data files...")
        (
            raw_data,
            lookup_data_based,
            subawardee_lookup,
            field_names_conversion,
        ) = pf.read_data(formula_OLDC_data_filename, crosswalk_filename)
        print("Reading in data files - COMPLETE")

        # Get columns needed to join on for processing
        first_43_cols = list(raw_data["Screen-1"].columns[0:43])
        first_43_cols.remove("Screen-Name")

        # Light processing on raw data
        raw_data = pf.process_raw_data(raw_data)

        # Process lookup data
        lookup_data_based.Element = lookup_data_based.Element.str.upper()
        lookup_data_based["Meta Name Description"] = lookup_data_based[
            "Meta Name Description"
        ].str.upper()
        field_names_conversion.Element = field_names_conversion.Element.str.upper()
        field_names_conversion["Meta Name Description"] = field_names_conversion[
            "Meta Name Description"
        ].str.upper()
        field_names_conversion.dropna(
            subset=["Meta Name Description", "Note"], how="all", inplace=True
        )

        # Make copy of old processed data and put in Archive If there already exists a processed data file,
        # append _Archived_<timestamp> to the name to store as a legacy file create historical data backup before we
        # overwrite it (backup HistoricalPPR.xlsx regardless if input file was a backup)
        backup_file_name = (
            f"{processed_data_filename.replace('.xlsx', '')}_Archived_{string_date}.xlsx"
        )

        # Create Archive directory if it doesn't exist
        if os.path.exists(processed_data_filename):
            if not os.path.exists(
                os.path.join(os.path.dirname(processed_data_filename), "Archive")
            ):
                print("Creating Archive directory to store backup processed data in...")
                os.mkdir(
                    os.path.join(os.path.dirname(processed_data_filename), "Archive")
                )

            backup_file_path = os.path.join(
                os.path.dirname(backup_file_name),
                "Archive",
                os.path.basename(backup_file_name),
            )

            print("Saving current processed file to " + backup_file_path + "...")

            # Create backup file from existing historical file
            shutil.copy(processed_data_filename, backup_file_path)

        # GRANTEE DATA
        # ==================================================================================================================
        print("Processing 2023 OLDC data...")

        new_processed_data_filename = f"{os.path.dirname(processed_data_filename)}/HistoricalPPR_{oldc_pull_date}_processed_{string_date}.xlsx"

        # Join screens 1 and screens 3 for grantee data
        processed_data = raw_data["Screen-1"].merge(
            raw_data["Screen-3"], on=first_43_cols
        )

        # Make sure data types match
        date_columns = processed_data.select_dtypes(include=["datetime"])
        processed_data[date_columns.columns] = date_columns.map(
            lambda x: x.date()
        ).fillna("")
        text_columns = processed_data.select_dtypes(include=["object"])
        processed_data[text_columns.columns] = text_columns.map(
            lambda x: html.unescape(str(x))
        )

        # Remove brackets and spaces from EIN for ease of use
        processed_data["old_EIN"] = processed_data.EIN
        processed_data["EIN"] = processed_data.RptEin.apply(pf.parse_ein)

        # Filter out rows that have been returned for edits
        processed_data_filtered = processed_data.loc[
            ~processed_data.CodeTxt.isin(["Submission Returned by CO"])
        ]

        # If a grantee has multiple rows, only keep the last RevSeqNumber
        processed_data_filtered = processed_data_filtered.loc[
            processed_data_filtered.groupby(["Fy", "EIN", "ProgAcronym"])[
                "RevSeqNumber"
            ].transform("max")
            == processed_data_filtered.RevSeqNumber
        ]

        # Exclude any grantees listed as "other"
        processed_data_filtered = processed_data_filtered.loc[
            processed_data_filtered.GranteeTypeTxt != "Other"
        ]

        # If state grantee has two EINs for same program and year, 
        # choose submission with the latest submit date, otherwise, choose the first row
        grouped_states = processed_data_filtered[processed_data_filtered['GranteeTypeTxt']=='State'].groupby(['PostalCode', 'Fy', 'ProgAcronym'])
        # Get max submit date for each grouped state, merge back onto state data to get rest of data for each state
        # If submit date is the same for a duplicate row, max() chooses the first one based on row rank
        max_states = grouped_states.SubmitDate.max().reset_index().merge(
            processed_data_filtered[processed_data_filtered['GranteeTypeTxt']=='State'], 
            how="left", 
            on=['PostalCode', 'Fy', 'ProgAcronym', 'SubmitDate']
            )
        # Add tribes back in
        processed_data_filtered = pd.concat([max_states, processed_data_filtered[processed_data_filtered.GranteeTypeTxt == "Tribe"]])


        # Find all the versions of the H-02 column that exist and replace with the correct column name
        replacements = {
            "H-02 What does the FVPSA grant allow you to do that you wouldn¿t be able to do without this "
            "funding?": [
                "H-03 Describe any efforts supported in whole or in part by your FVPSA grant to meet the "
                "needs of underserved populations in your community, including populations underserved "
                "because of ethnic, racial, cultural or language diversity, sexual orientation or gender "
                "identity or geographic isolation. Describe any ongoing challenges."
            ]
        }
        processed_data_filtered = pf.replace_duplicate_columns(
            df=processed_data_filtered, replacements=replacements
        )

        # Convert all nans to empty
        processed_data_filtered = processed_data_filtered.replace("nan", np.nan)

        # Remove unnecessary columns
        processed_data_filtered.drop(
            columns=["Screen-Name_x", "Screen-Name_y", "old_EIN"]
        )

        print("Saving processed data to sheet: OriginalFormat...")
        # Save processed data in original format
        workbook = pf.save_to_final_workbook(
            df_to_save=processed_data_filtered, sheet_name="OriginalFormat"
        )

        # SERVICE OUTCOME DATA (SECTION G)
        # ==================================================================================================================
        service_outcome_data = pf.service_outcome_transform(
            processed_data_filtered, field_names_conversion
        )

        print("Saving service outcome data to sheet: ServiceOutcome...")
        # Save service outcome data
        workbook = pf.save_to_final_workbook(
            df_to_save=service_outcome_data,
            sheet_name="ServiceOutcome",
            historical_workbook=workbook,
        )

        # SUBAWARDEE DATA
        # ==================================================================================================================
        # Split out states and tribes
        tribes_processed_data = processed_data_filtered[
            processed_data_filtered.GranteeTypeTxt == "Tribe"
        ]
        states_processed_data = processed_data_filtered[
            processed_data_filtered.GranteeTypeTxt == "State"
        ]

        # Aggregate state data for subawardees
        print("Processing subawardee data...")
        receipt_ids_to_keep = states_processed_data[
            "Rpt-Receipt-Id"
        ]  # Only want subawardees that are in processed data
        final_subawardee = pf.process_subawardee_data(
            raw_data, subawardee_lookup, receipt_ids_to_keep
        )

        # Put clean subawardee data in the historicalPPR
        # Note: this data has only been edited to use characters like " instead of &quot;
        print("Saving processed subawardee data to sheet: Subawardee")
        workbook = pf.save_to_final_workbook(
            df_to_save=final_subawardee,
            sheet_name="Subawardee",
            historical_workbook=workbook,
        )
        print("Processing subawardee data - COMPLETE")

        # LONG FORMAT DATA
        # ==================================================================================================================
        print("Transforming the data to long format...")
        # Add total funding amounts by state and year to state data
        states_processed_data = pf.calculate_total_funds(
            subawardee_df=final_subawardee,
            state_df=states_processed_data,
            cols_to_merge=first_43_cols,
        )

        # Convert to long format for later merge on lookup table
        states_long_data = states_processed_data.melt(
            id_vars=["GranteeTypeTxt", "Fy", "ProgAcronym", "PostalCode", "EIN"]
        )
        tribes_long_data = tribes_processed_data.melt(
            id_vars=["GranteeTypeTxt", "Fy", "ProgAcronym", "PostalCode", "EIN"]
        )

        all_long_data = pd.concat([states_long_data, tribes_long_data])

        # Join on lookup tab of lookup table and subset to relevant columns
        joined_long_data = pf.join_on_meta_name_desc(all_long_data, lookup_data_based, year=2023)

        # Create and append historical long format data
        historical_long_data = pf.process_long_data(
            raw_data, joined_long_data, backup_file_path
        )

        # Save
        print(f"Saving long format data to sheet: {str(date.today())}")
        workbook = pf.save_to_final_workbook(
            df_to_save=historical_long_data,
            sheet_name=str(date.today()),
            historical_workbook=workbook,
        )
        print("Transforming the data to long format - COMPLETE")

        # WIDE FORMAT DATA
        # ==================================================================================================================
        print("Transforming the data to wide format...")

        # Join on the crosswalk tab of the lookup table to get the final, clean column names
        # The cleaned up column names are in the Label field of the crosswalk sheet
        historical_wide_data = (
            joined_long_data.merge(field_names_conversion, how="left", on="Element")
            .dropna(subset=["Label"])[
                [
                    "Grant Type",
                    "Year",
                    "Program Acronym",
                    "State",
                    "EIN",
                    "Label",
                    "Value",
                ]
            ]
            .pivot(
                values="Value",
                columns="Label",
                index=["Grant Type", "Year", "Program Acronym", "State", "EIN"],
            )
            .reset_index()
        )

        # Add sums for gender and shelter/non-shelter
        genders = ["Men", "Women", "Children", "Not Specified"]
        historical_wide_data = pf.calculate_gender_totals(historical_wide_data, genders)

        # Get grantee names from original file
        historical_wide_data.loc[:, "Grantee Name"] = historical_wide_data.EIN.apply(
            lambda x: pf.lookup_name_from_ein(EIN=x, df=processed_data_filtered)
        )

        # Save
        print("Saving wide format data to sheet: WideFormat")
        workbook = pf.save_to_final_workbook(
            df_to_save=historical_wide_data,
            sheet_name="WideFormat",
            historical_workbook=workbook,
        )
        print("Transforming the data to wide format - COMPLETE")

        # METADATA
        # ==================================================================================================================
        print("Creating Metadata sheet...")
        # Create sheet with metadata information, including the number of states & tribes reporting each year,
        # the timestamp of the last data processing, and the list of missing states for each year
        workbook = pf.create_metadata_sheet(
            workbook, historical_wide_data, all_states, True, True
        )
        ws = workbook["Metadata"]
        max_year = int(historical_wide_data.Year.max())

        # CodeTxt: ["Submitted", "Submission Accepted by CO", "Submission in Review by CO", "Submission Returned by CO"]
        # Create table of counts for each code for each year (split on states and tribes)
        codetxt_table = pf.create_codetxt_table(processed_data)

        print("Saving meta data to sheet: Metadata")
        for row_index, row in enumerate(
            dataframe_to_rows(codetxt_table, index=False, header=False), 1
        ):
            for col_index, item in enumerate(
                row, 6 + (max_year - 2018)
            ):  # Leave space for the table of missing grantees
                ws.cell(row_index, col_index, item)
        print("Creating Metadata sheet - COMPLETE")

        # SAVE FINAL WORKBOOK
        # ==================================================================================================================
        print("Saving workbook...")
        workbook.save(new_processed_data_filename)
        os.remove(processed_data_filename)  # Only remove current version if save was successful
        print("Processing 2023 OLDC data - COMPLETE")
        print(time.time() - t1)



    # New States & Tribes Processing
    # ==================================================================================================================

     # INITIALIZE GLOBAL VARIABLES
        # ==================================================================================================================
    string_date = datetime.today().strftime('%m%d%Y_%H%M%S')
    if process_new_states:
        t1 = time.time()
        print("Processing new 2024 States and Tribes data...")

        # Use new arguments for file paths and crosswalk
        new_raw_data_filename = new_states_OLDC_filename
        new_processed_data_filename = processed_new_states_data_filename
        crosswalk_file = crosswalk_filename_2024  
        print("Using crosswalk file:", crosswalk_file)
        print(f"DEBUG: new_states_OLDC_filename = {args.new_states_OLDC_filename}")


        
        #Extract the pull date from the filename
        oldc_pull_splits = os.path.basename(new_raw_data_filename).split("_")
        oldc_pull_date = oldc_pull_splits[-1].replace(".xlsx", "")
        print(f"Extracted oldc_pull_date: {oldc_pull_date}")  # confirm extraction
        
        
        all_states = sorted(
            "PA MS PR LA NM AZ FL AK OK HI KS DE IN ND MT WA RI KY TN OH IA WV ID GA WI MD NE VT ME VA TX CA UT NC NJ NV "
            "MI MN OR NY DC SD WY CO MA IL CT AR MO NH SC AL".split()
        )

        # Read in data files
        print("Reading in 2024 States and Tribes data files...")
        (
            raw_data,
            lookup_data_based,
            subawardee_lookup,
            field_names_conversion,
        ) = pf.read_data(new_raw_data_filename, crosswalk_file)
        print("Reading in data files - COMPLETE")

        # Get columns needed to join on for processing
        first_43_cols = list(raw_data["Screen-1"].columns[0:43])
        first_43_cols.remove("Screen-Name")

        # Light processing on raw data
        raw_data = pf.process_raw_data(raw_data)

        # Process lookup data
        lookup_data_based.Element = lookup_data_based.Element.str.upper()
        lookup_data_based["Meta Name Description"] = lookup_data_based[
            "Meta Name Description"
        ].str.upper()
        field_names_conversion.Element = field_names_conversion.Element.str.upper()
        field_names_conversion["Meta Name Description"] = field_names_conversion[
            "Meta Name Description"
        ].str.upper()
        field_names_conversion.dropna(
            subset=["Meta Name Description", "Note"], how="all", inplace=True
        )

        # Make copy of old processed data and put in Archive If there already exists a processed data file,
        # append _Archived_<timestamp> to the name to store as a legacy file create historical data backup before we
        # overwrite it (backup HistoricalPPR.xlsx regardless if input file was a backup)
        backup_file_name = (
            f"{new_processed_data_filename.replace('.xlsx', '')}_Archived_{string_date}.xlsx"
        )


        # Create Archive directory if it doesn't exist
        if os.path.exists(processed_new_states_data_filename):
            if not os.path.exists(
                os.path.join(os.path.dirname(processed_new_states_data_filename), "Archive")
            ):
                print("Creating Archive directory to store backup processed data in...")
                os.mkdir(
                    os.path.join(os.path.dirname(processed_new_states_data_filename), "Archive")
                )

            backup_file_path = os.path.join(
                os.path.dirname(backup_file_name),
                "Archive",
                os.path.basename(backup_file_name),
            )

            print("Saving current processed file to " + backup_file_path + "...")

            # Create backup file from existing historical file
            shutil.copy(processed_new_states_data_filename, backup_file_path)

        # GRANTEE DATA
        # ==================================================================================================================
        print("Processing 2024 States OLDC data...")

        new_processed_data_filename = f"{os.path.dirname(new_processed_data_filename)}/HistoricalPPR_{oldc_pull_date}_processed_{string_date}.xlsx"

        # Join screens 1 and screens 3 for grantee data
        processed_data = raw_data["Screen-1"].merge(
            raw_data["Screen-3"], on=first_43_cols
        )

        # Make sure data types match
        date_columns = processed_data.select_dtypes(include=["datetime"])
        processed_data[date_columns.columns] = date_columns.map(
            lambda x: x.date()
        ).fillna("")
        text_columns = processed_data.select_dtypes(include=["object"])
        processed_data[text_columns.columns] = text_columns.map(
            lambda x: html.unescape(str(x))
        )

        # Remove brackets and spaces from EIN for ease of use
        processed_data["old_EIN"] = processed_data.EIN
        processed_data["EIN"] = processed_data.RptEin.apply(pf.parse_ein)

        # Filter out rows that have been returned for edits
        processed_data_filtered = processed_data.loc[
            ~processed_data.CodeTxt.isin(["Submission Returned by CO"])
        ]

        # If a grantee has multiple rows, only keep the last RevSeqNumber
        processed_data_filtered = processed_data_filtered.loc[
            processed_data_filtered.groupby(["Fy", "EIN", "ProgAcronym"])[
                "RevSeqNumber"
            ].transform("max")
            == processed_data_filtered.RevSeqNumber
        ]

        # Exclude any grantees listed as "other"
        processed_data_filtered = processed_data_filtered.loc[
            processed_data_filtered.GranteeTypeTxt != "Other"
        ]

        # If state grantee has two EINs for same program and year, 
        # choose submission with the latest submit date, otherwise, choose the first row
        grouped_states = processed_data_filtered[processed_data_filtered['GranteeTypeTxt']=='State'].groupby(['PostalCode', 'Fy', 'ProgAcronym'])
        # Get max submit date for each grouped state, merge back onto state data to get rest of data for each state
        # If submit date is the same for a duplicate row, max() chooses the first one based on row rank
        max_states = grouped_states.SubmitDate.max().reset_index().merge(
            processed_data_filtered[processed_data_filtered['GranteeTypeTxt']=='State'], 
            how="left", 
            on=['PostalCode', 'Fy', 'ProgAcronym', 'SubmitDate']
            )
        # Add tribes back in
        processed_data_filtered = pd.concat([max_states, processed_data_filtered[processed_data_filtered.GranteeTypeTxt == "Tribe"]])


        # Convert all nans to empty
        processed_data_filtered = processed_data_filtered.replace("nan", np.nan)

        # Remove unnecessary columns
        processed_data_filtered.drop(
            columns=["Screen-Name_x", "Screen-Name_y", "old_EIN"]
        )

        print("Saving processed data to sheet: OriginalFormat...")
        # Save processed data in original format
        workbook = pf.save_to_final_workbook(
            df_to_save=processed_data_filtered, sheet_name="OriginalFormat"
        )

        # SERVICE OUTCOME DATA (SECTION G)
        # ==================================================================================================================
        service_outcome_data = pf.service_outcome_transform(
            processed_data_filtered, field_names_conversion
        )

        print("Saving service outcome data to sheet: ServiceOutcome...")
        # Save service outcome data
        workbook = pf.save_to_final_workbook(
            df_to_save=service_outcome_data,
            sheet_name="ServiceOutcome",
            historical_workbook=workbook,
        )

        # SUBAWARDEE DATA
        # ==================================================================================================================
        # Split out states and tribes
        tribes_processed_data = processed_data_filtered[
            processed_data_filtered.GranteeTypeTxt == "Tribe"
        ]
        states_processed_data = processed_data_filtered[
            processed_data_filtered.GranteeTypeTxt == "State"
        ]

        # Aggregate state data for subawardees
        print("Processing subawardee data...")
        receipt_ids_to_keep = states_processed_data[
            "Rpt-Receipt-Id"
        ]  # Only want subawardees that are in processed data
        final_subawardee = pf.process_subawardee_data(
            raw_data, subawardee_lookup, receipt_ids_to_keep
        )

        # Put clean subawardee data in the historicalPPR
        # Note: this data has only been edited to use characters like " instead of &quot;
        print("Saving processed subawardee data to sheet: Subawardee")
        workbook = pf.save_to_final_workbook(
            df_to_save=final_subawardee,
            sheet_name="Subawardee",
            historical_workbook=workbook,
        )
        print("Processing subawardee data - COMPLETE")

        # LONG FORMAT DATA
        # ==================================================================================================================
        print("Transforming the data to long format...")
        # Add total funding amounts by state and year to state data
        states_processed_data = pf.calculate_total_funds(
            subawardee_df=final_subawardee,
            state_df=states_processed_data,
            cols_to_merge=first_43_cols,
        )

        # Convert to long format for later merge on lookup table
        states_long_data = states_processed_data.melt(
            id_vars=["GranteeTypeTxt", "Fy", "ProgAcronym", "PostalCode", "EIN"]
        )
        tribes_long_data = tribes_processed_data.melt(
            id_vars=["GranteeTypeTxt", "Fy", "ProgAcronym", "PostalCode", "EIN"]
        )

        all_long_data = pd.concat([states_long_data, tribes_long_data])

        # Join on lookup tab of lookup table and subset to relevant columns
        joined_long_data = pf.join_on_meta_name_desc(all_long_data, lookup_data_based, year=2024)

        # Create and append historical long format data
        historical_long_data = pf.process_long_data(
            raw_data, joined_long_data, backup_file_path
        )

        # Save
        print(f"Saving long format data to sheet: {str(date.today())}")
        workbook = pf.save_to_final_workbook(
            df_to_save=historical_long_data,
            sheet_name=str(date.today()),
            historical_workbook=workbook,
        )
        print("Transforming the data to long format - COMPLETE")

        # WIDE FORMAT DATA
        # ==================================================================================================================
        print("Transforming the data to wide format...")
        
       # Join on the crosswalk tab of the lookup table to get the final, clean column names
        # The cleaned up column names are in the Label field of the crosswalk sheet
        historical_wide_data = (
            joined_long_data.merge(field_names_conversion, how="left", on="Element")
            .dropna(subset=["Label"])[
                [
                    "Grant Type",
                    "Year",
                    "Program Acronym",
                    "State",
                    "EIN",
                    "Label",
                    "Value",
                ]
            ]
            .pivot(
                values="Value",
                columns="Label",
                index=["Grant Type", "Year", "Program Acronym", "State", "EIN"],
            )
            .reset_index()
        )


        # Add sums for gender and shelter/non-shelter
        genders = ["Men", "Women", "Children", "Not Specified"]
        historical_wide_data = pf.calculate_gender_totals(historical_wide_data, genders)

        # Get grantee names from original file
        historical_wide_data.loc[:, "Grantee Name"] = historical_wide_data.EIN.apply(
            lambda x: pf.lookup_name_from_ein(EIN=x, df=processed_data_filtered)
        )

        # Save
        print("Saving wide format data to sheet: WideFormat")
        workbook = pf.save_to_final_workbook(
            df_to_save=historical_wide_data,
            sheet_name="WideFormat",
            historical_workbook=workbook,
        )
        print("Transforming the data to wide format - COMPLETE")

        # METADATA
        # ==================================================================================================================
        print("Creating Metadata sheet...")
        # Create sheet with metadata information, including the number of states & tribes reporting each year,
        # the timestamp of the last data processing, and the list of missing states for each year
        workbook = pf.create_metadata_sheet(
            workbook, historical_wide_data, all_states, True, True
        )
        ws = workbook["Metadata"]
        max_year = int(historical_wide_data.Year.max())

        # CodeTxt: ["Submitted", "Submission Accepted by CO", "Submission in Review by CO", "Submission Returned by CO"]
        # Create table of counts for each code for each year (split on states and tribes)
        codetxt_table = pf.create_codetxt_table(processed_data)

        print("Saving meta data to sheet: Metadata")
        for row_index, row in enumerate(
            dataframe_to_rows(codetxt_table, index=False, header=False), 1
        ):
            for col_index, item in enumerate(
                row, 6 + (max_year - 2018)
            ):  # Leave space for the table of missing grantees
                ws.cell(row_index, col_index, item)
        print("Creating Metadata sheet - COMPLETE")

        # SAVE FINAL WORKBOOK
        # ==================================================================================================================
        print("Saving workbook...")
        workbook.save(new_processed_data_filename)
        os.remove(processed_new_states_data_filename) 
        print("Processing 2024 States OLDC data - COMPLETE")
        print(time.time() - t1)

    # COALITIONS PROCESSING
    # ==================================================================================================================
    if process_coalitions:
        print("Processing coalitions data...")

        oldc_pull_splits = coalitions_OLDC_filename.split("_")
        oldc_pull_date = oldc_pull_splits[len(oldc_pull_splits)-1].replace(".xlsx", "")

        new_coalitions_processed_data_filename = cpf.copy_old_data(string_date,
        processed_coalitions_data_filename = processed_coalitions_data_filename,
        oldc_pull_date=oldc_pull_date)

        # Set up ground truth of submissions to identify missing
        cs_df = cpf.get_ground_truth_submissions(target_year="before_2024")

        # Read coalitions data and crosswalk
        print("Reading in coalitions data...")
        (coal_dat, coal_xw, coalition_names) = cpf.read_coalitions_data(
            coalitions_OLDC_filename, crosswalk_filename, coalitions_names_filename
        )
        print("Reading in coalitions data - COMPLETE")

        # The list of sheet names in the raw data and their proper section names
        # as seen in the OLDC PPR
        screen_names = {
            "Screen-1": "I. Cover Page",
            "Screen-2": "II. FVPSA Funds",
            "Screen-3": "III. Coalition Members",
            "Screen-4": "IV. Narrative Questions",
            "Screen-5": "V. Summary of Activities",
            "Screen-6": "VI. Other Topics",
            "Screen-7": "VII. Training",
        }

        # Light processing on coalitions data
        coal_dat_processed = pf.process_raw_data(coal_dat, coalitions=True)
        coal_dat_processed = dict(
            (k, coal_dat_processed[k]) for k in screen_names.keys()
        )

        # Columns to join on across all screens, should be identifiers
        join_cols = (
            coal_dat["Screen-1"]
            .columns[1:41]
            .drop(
                [
                    "Screen-Name",
                    "Row-Iteration",
                    "Screen-Iteration",
                    "RevSeqNumber",
                    "SubmitDate",
                    "PostalCode",
                    "Fy",
                    "ProgAcronym",
                    "ProgramName",
                    "DunsId9",
                    "RptEin"
                ]
            )
        )
        # Going to use State, Year, and Program Abbr as renamed columns
        join_cols = list(join_cols) + ["State", "Year", "Program Abbr", "EIN", "Program Name", "DUNS"]

        # Standardize submissions by row iteration, review sequence number, and submit date
        (coal_dat_processed, new_join_cols) = cpf.standardize_submissions(coal_dat_processed, join_cols, coalition_names)

        # Fix duplicate columns in Section V. Summary of Activities
        soa_sheetName = [
            k for k, v in screen_names.items() if v == "V. Summary of Activities"
        ][0]
        soa = coal_dat_processed[soa_sheetName]

        # Rename duplicated columns
        soa = soa.rename(
            columns=
            {
                "Types of Activities,FVPSA Summary of Activities,R19C2": "Types of Activities,FVPSA Summary of Activities,R9C2",
                "Types of Activities,FVPSA Summary of Activities,R19C2.1": "Types of Activities,FVPSA Summary of Activities,R19C2",
                "Number of People Reached &lt;BR&gt;(Training /TA only),FVPSA Underserved and culturally-specific populations Summary of Activities,RvC3": "Number of People Reached &lt;BR&gt;(Training /TA only),FVPSA Underserved and culturally-specific populations Summary of Activities,R33C3",
                "Number of People Reached &lt;BR&gt;(Training /TA only),FVPSA Underserved and culturally-specific populations Summary of Activities,RvC3.1": "Number of People Reached &lt;BR&gt;(Training /TA only),FVPSA Underserved and culturally-specific populations Summary of Activities,R31C3",
            }
        )
        coal_dat_processed[soa_sheetName] = soa

        # Process all sheets
        (coal_dat_processed, new_join_cols) = cpf.process_sheets(
            coal_dat_processed, coal_xw, screen_names, cs_df, soa_sheetName, new_join_cols, coalition_names, "2023"
        )

        # Save processed sheets
        workbook = None
        for screen in coal_dat_processed.keys():
            workbook = pf.save_to_final_workbook(
                df_to_save=coal_dat_processed[screen],
                sheet_name=screen_names[screen],
                historical_workbook=workbook,
            )


        var_cols = new_join_cols.copy()

        # Create Section IV. long format
        narr_sheetName = [
            k for k, v in screen_names.items() if v == "IV. Narrative Questions"
        ][0]
        narr = coal_dat_processed[narr_sheetName]
        narr_long = cpf.sectionIV_long_format(narr, var_cols + ["Rpt-Receipt-Id"], coal_xw)

        # Save long format of Section IV. Narrative Questions
        workbook = pf.save_to_final_workbook(
            df_to_save=narr_long,
            sheet_name="Section IV Narr Long Format",
            historical_workbook=workbook,
        )

        # Create Section V. long format
        soa = coal_dat_processed[soa_sheetName]
        soa_long = cpf.sectionV_long_format(soa, var_cols + ["Rpt-Receipt-Id"])

        # Save long format of Section V. Summary of Activities
        workbook = pf.save_to_final_workbook(
            df_to_save=soa_long,
            sheet_name="Section V SoA Long Format",
            historical_workbook=workbook,
        )

        # SAVE FINAL WORKBOOK
        # ==================================================================================================================
        print("Saving coalitions workbook...")
        print(new_coalitions_processed_data_filename)
        workbook.save(new_coalitions_processed_data_filename)
        os.remove(processed_coalitions_data_filename)  # Only remove current version if save was successful
        print("Processing coalitions OLDC data - COMPLETE")


    # NEW COALITIONS PROCESSING
    # ==================================================================================================================
    if process_new_coalitions:
        print("Processing new 2024 coalitions data...")

    # Extract the pull date from the filename
        oldc_pull_splits = new_coalitions_OLDC_filename.split("_")
        oldc_pull_date = oldc_pull_splits[len(oldc_pull_splits) - 1].replace(".xlsx", "")

    # Create a processed filename with a timestamp
        new_processed_new_coalitions_data_filename = cpf.copy_old_data(
            string_date,
            processed_coalitions_data_filename=processed_new_coalitions_data_filename,
            oldc_pull_date=oldc_pull_date,
        )

    # Set up ground truth of submissions to identify missing (reuse logic)
        cs_df = cpf.get_ground_truth_submissions(target_year="after_2024")
       

    # Read the new 2024 coalitions data and the new crosswalk
        print("Reading in new 2024 coalitions data...")
        (coal_dat, coal_xw, coalition_names) = cpf.read_coalitions_data(
            new_coalitions_OLDC_filename,  # new 2024 raw coalitions file
            crosswalk_filename_2024,       # 2024 crosswalk file
            coalitions_names_filename,     # Reuse coalition names lookup table 
        )
        print("Reading in new 2024 coalitions data - COMPLETE")



        # The list of sheet names in the raw data and their proper section names
        # as seen in the OLDC PPR
        screen_names = {
            "Screen-1": "I. Cover Page",
            "Screen-2": "II. FVPSA Funds",
            "Screen-3": "III. Coalition Members",
            "Screen-4": "IV. Narrative Questions",
            "Screen-5": "V. Summary of Activities",
            "Screen-6": "VI. Other Topics",
            "Screen-7": "VII. Training",
        }

        # Light processing on coalitions data
        coal_dat_processed = pf.process_raw_data(coal_dat, coalitions=True)
        coal_dat_processed = dict(
            (k, coal_dat_processed[k]) for k in screen_names.keys()
        )

        print("Finished Light Processing of New Coalitions Data")

        # Columns to join on across all screens, should be identifiers
        join_cols = (
            coal_dat["Screen-1"]
            .columns[1:41]
            .drop(
                [
                    "Screen-Name",
                    "Row-Iteration",
                    "Screen-Iteration",
                    "RevSeqNumber",
                    "SubmitDate",
                    "PostalCode",
                    "Fy",
                    "ProgAcronym",
                    "ProgramName",
                    "UEI[Unique Entity Identifier]",
                    "RptEin"
                ]
            )
        )
        # Going to use State, Year, and Program Abbr as renamed columns
        join_cols = list(join_cols) + ["State", "Year", "Program Abbr", "EIN", "Program Name", "UEI"]

        standardize_submissions_col_mapping={
        "PostalCode": "State",
        "Fy": "Year",
        "ProgAcronym": "Program Abbr",
        "RptEin": "EIN",
        "ProgramName": "Program Name",
        "UEI[Unique Entity Identifier]":"UEI"
    }

        # Standardize submissions by row iteration, review sequence number, and submit date
        (coal_dat_processed, new_join_cols) = cpf.standardize_submissions(coal_dat_processed, join_cols, coalition_names, standardize_submissions_col_mapping)

        print("Finished Standardize Submissions for New Coalitions Data")

        # Fix duplicate columns in Section V. Summary of Activities
        soa_sheetName = [
            k for k, v in screen_names.items() if v == "V. Summary of Activities"
        ][0]
        soa = coal_dat_processed[soa_sheetName]

        # Rename duplicated columns
        soa = soa.rename(
            columns=
            {
                "Types of Activities,FVPSA Summary of Activities,R19C2": "Types of Activities,FVPSA Summary of Activities,R9C2",
                "Types of Activities,FVPSA Summary of Activities,R19C2.1": "Types of Activities,FVPSA Summary of Activities,R19C2",
                "Number of People Reached &lt;BR&gt;(Training /TA only),FVPSA Underserved and culturally-specific populations Summary of Activities,RvC3": "Number of People Reached &lt;BR&gt;(Training /TA only),FVPSA Underserved and culturally-specific populations Summary of Activities,R33C3",
                "Number of People Reached &lt;BR&gt;(Training /TA only),FVPSA Underserved and culturally-specific populations Summary of Activities,RvC3.1": "Number of People Reached &lt;BR&gt;(Training /TA only),FVPSA Underserved and culturally-specific populations Summary of Activities,R31C3",
            }
        )
        coal_dat_processed[soa_sheetName] = soa
        print("Finished Removing Duplicates")

        # Process all sheets
        (coal_dat_processed, new_join_cols) = cpf.process_sheets(
            coal_dat_processed, coal_xw, screen_names, cs_df, soa_sheetName, new_join_cols, coalition_names, "2024"
        )

        # Save processed sheets
        workbook = None
        for screen in coal_dat_processed.keys():
            workbook = pf.save_to_final_workbook(
                df_to_save=coal_dat_processed[screen],
                sheet_name=screen_names[screen],
                historical_workbook=workbook,
            )


        var_cols = new_join_cols.copy()

        # Create Section IV. long format
        narr_sheetName = [
            k for k, v in screen_names.items() if v == "IV. Narrative Questions"
        ][0]
        narr = coal_dat_processed[narr_sheetName]
        narr_long = cpf.sectionIV_long_format(narr, var_cols + ["Rpt-Receipt-Id"], coal_xw)

        # Save long format of Section IV. Narrative Questions
        workbook = pf.save_to_final_workbook(
            df_to_save=narr_long,
            sheet_name="Section IV Narr Long Format",
            historical_workbook=workbook,
        )

        # Create Section V. long format
        soa = coal_dat_processed[soa_sheetName]
        soa_long = cpf.sectionV_long_format(soa, var_cols + ["Rpt-Receipt-Id"])

        # Save long format of Section V. Summary of Activities
        workbook = pf.save_to_final_workbook(
            df_to_save=soa_long,
            sheet_name="Section V SoA Long Format",
            historical_workbook=workbook,
        )

        # SAVE FINAL WORKBOOK
        # ==================================================================================================================
       
        print("Saving new 2024 coalitions workbook...")
        print(new_processed_new_coalitions_data_filename)
        workbook.save(new_processed_new_coalitions_data_filename)
        # Remove the old processed version if the save was successful
        os.remove(processed_new_coalitions_data_filename)
        print("Processing new 2024 coalitions OLDC data - COMPLETE")



if __name__ == "__main__":
    args = get_parser().parse_args()
    main(**vars(args))
