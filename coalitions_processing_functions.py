import pandas as pd
import os
import shutil
import numpy as np
import pandas as pd
from datetime import date, datetime


def copy_old_data(string_date,
    processed_coalitions_data_filename,
    oldc_pull_date):
    """Copy old coalitions data over to a Archive file

    This function copies the existing processed coalitions data file,
    if it exists, into a Archive directory that is created automatically if
    it doesn't already exist. The copied data file is appended with the
    date provided by <string_date>.

    :param string_date: The current date to be appended to the copied
        processed coalitions data file
    :type string_date: <str>
    :param processed_coalitions_data_filename: The last processed coalitions file path
    :type processed_coalitions_data_filename: <str>
    :oldc_pull_date: The date the raw coalitions data was pulled from OLDC in
        <%Y%m%d> format
    :type oldc_pull_date: <str>

    :return: The relative path of the processed coalitions data file
    :rtype: <str>
    """

    # Create Processed Data directory if doesn't exist
    if not os.path.exists(os.path.dirname(processed_coalitions_data_filename)):
        os.makedirs(os.path.dirname(processed_coalitions_data_filename))

    backup_coalition_name = (
        f"{processed_coalitions_data_filename.replace('.xlsx', '')}_Archived_{string_date}.xlsx"
    )

    # Create Archive directory if it doesn't exist
    if os.path.exists(processed_coalitions_data_filename):
        if not os.path.exists(
            os.path.join(os.path.dirname(processed_coalitions_data_filename), "Archive")
        ):
            print("Creating Archive directory to store backup processed data in...")
            os.mkdir(
                os.path.join(
                    os.path.dirname(processed_coalitions_data_filename), "Archive"
                )
            )

        backup_file_path = os.path.join(
            os.path.dirname(backup_coalition_name),
            "Archive",
            os.path.basename(backup_coalition_name),
        )

        print("Saving current processed coalitions file to " + backup_file_path + "...")

        # Create backup file from existing historical file
        shutil.copy(processed_coalitions_data_filename, backup_file_path)
        print("Saving current processed coalitions file - COMPLETE")

    return os.path.join(os.path.dirname(processed_coalitions_data_filename), f"coalitions_processed_{oldc_pull_date}_processed_{string_date}.xlsx")


def read_coalitions_data(filepath_raw, crosswalk_filename, coalitions_names_filename):
    """Read in raw coalitions data

    This function reads in the raw coalitions data (all sheets) and
    the crosswalk file and returns as pandas data frames.

    :param filepath_raw: File path to the raw OLDC data (this is what will
        be processed)
    :type filepath_raw: <str>
    :param filepath_crosswalk: File path to the lookup data to be used for
        processing
    :type filepath_crosswalk: <str>
    :param coalitions_names_filename: File path to the full coalition names
    :type coalitions_names_filename: <str>

    :return: Data frames corresponding to the given sheets, except for the
        raw data, which is returned as a dictionary of data frames
        corresponding to the relevant sheets
    :rtype: <pd.DataFrame>; raw_data: <Dict<pd.DataFrame>>
    """
    raw_data = pd.read_excel(filepath_raw, sheet_name=None, parse_dates=True)

    xw = pd.read_excel(crosswalk_filename, sheet_name="coalitions")

    coal_names = pd.read_csv(coalitions_names_filename)

    return raw_data, xw, coal_names


def get_ground_truth_submissions(target_year=None):
    """Create data frame of expected submissions

    This function creates a data frame corresponding to every state-program-year
    combination that's expected from the raw OLDC data. Since coalitions don't
    always submit their PPRs, this data frame is used to identify which ones
    don't have any submission at all.

    NOTE: This must be updated with additional programs/years over time

    :return: Data frame corresponding to every state-program-year combination
        expected from the coalitions data
    :rtype: <pd.DataFrame>
    """
    coalition_states = [
        "AL",
        "AK",
        "AZ",
        "AR",
        "CA",
        "CO",
        "CT",
        "DE",
        "DC",
        "FL",
        "GA",
        "HI",
        "ID",
        "IL",
        "IN",
        "IA",
        "KS",
        "KY",
        "LA",
        "ME",
        "MD",
        "MA",
        "MI",
        "MN",
        "MS",
        "MO",
        "MT",
        "NE",
        "NV",
        "NH",
        "NJ",
        "NM",
        "NY",
        "NC",
        "ND",
        "OH",
        "OK",
        "OR",
        "PA",
        "RI",
        "SC",
        "SD",
        "TN",
        "TX",
        "UT",
        "VT",
        "VA",
        "WA",
        "WV",
        "WI",
        "WY",
        "PR",
        "AS",
        "MP",
        "GU",
        "VI",
    ]

    # NOTE: Years and programs should be added for each new fiscal year
    years = {
        "Year": ["2018", "2019", "2020", "2021", "2021", "2022", "2022", "2023", "2023", "2024", "2024"],
        "Coal Program Abbr": [
            "Core FVPSA",
            "Core FVPSA",
            "Core FVPSA",
            "Core FVPSA",
            "CARES Act",
            "Core FVPSA",
            "ARP Act",
            "Core FVPSA",
            "ARP Act",
            "Core FVPSA",
            "ARP Act"
        ],
    }

    # Filter years based on the target_year input
    years_df = pd.DataFrame(data=years)
    if target_year == "after_2024":  # Only return 2024
        years_df = years_df[years_df["Year"] >= "2024"]
    elif target_year == "before_2024":  # Return all years except 2024
        years_df = years_df[years_df["Year"] < "2024"]

    # Build the combinations of State-Year
    cs_df = pd.DataFrame(
        data={
            "State": np.sort(coalition_states * len(years_df["Year"].unique())),
            "Year": list(years_df["Year"].unique()) * len(coalition_states),
        }
    )
    cs_df = cs_df.merge(years_df, how="left", on="Year")

    return cs_df


def standardize_submissions(
    coal_dat_processed,
    id_cols,
    coalition_names,
    col_mapping={
        "PostalCode": "State",
        "Fy": "Year",
        "ProgAcronym": "Program Abbr",
        "RptEin": "EIN",
        "DunsId9": "DUNS",
        "ProgramName": "Program Name",
    },
):
    """Standardize coalition submissions by year, program, and name

    This function standardizes the coalition submissions so that only the
    minimum row iteration, maximum review sequence number, and maximum submit date are used
    for each coalition, year, program.

    :param coal_dat_processed: Dictionary of coalition sheets to be processed
    :type coal_dat_processed: <Dict(<pd.DataFrame>)>
    :param id_cols: Columns that serve as identifying variables for all sheets
    :type id_cols: <list>
    :param coalition_names: The full coalition names
    :type coalition_names: <pd.DataFrame>
    :param col_mapping: New names to give to choice columns listed in id_cols
    :type col_mapping: <Dict>

    :return: Standardized coalition submissions
    :rtype: <Dict(<pd.DataFrame>)>
    """

    for screen in coal_dat_processed.keys():
        df = coal_dat_processed[screen]

        if "EIN" in df.columns:
            df = df.drop(["EIN"], axis="columns")

        df = df.rename(columns=col_mapping)

        merge_cols = id_cols.copy()

        df_submitdate_col = "SubmitDate"
        df_revseq_col = "RevSeqNumber"

        # Use max review sequence number
        if df_revseq_col in df.columns:
            df_maxRev = (
                df.groupby(merge_cols, dropna=False)[df_revseq_col].max().reset_index()
            )

            merge_cols.append(df_revseq_col)
            df = df.merge(df_maxRev, how="right", on=merge_cols)

        # Convert date to datetime, if exists, and use max
        if df_submitdate_col in df.columns:
            df[df_submitdate_col] = [
                datetime.strptime(x, "%m/%d/%Y") for x in df[df_submitdate_col]
            ]
            df_maxDate = (
                df.groupby(merge_cols, dropna=False)[df_submitdate_col]
                .max()
                .reset_index()
            )

            merge_cols.append(df_submitdate_col)
            df = df.merge(df_maxDate, how="right", on=merge_cols)

        # Use min row iteration
        df_min = df.groupby(merge_cols, dropna=False)["Row-Iteration"].min().reset_index()
        merge_cols.append("Row-Iteration")
        df = df.merge(df_min, how="right", on=merge_cols)

        df = df.merge(coalition_names, how="left", on="State")
        grant_name_cols = [
            x for x in df.columns if "granteename" in x.replace(" ", "").lower()
        ]
        df = df.drop(grant_name_cols, axis="columns")

        if screen == "Screen-1":
            coal1 = df.copy()
        else:
            intersect_columns = list(set(coal1.columns).intersection(set(df.columns)))
            intersect_columns = [
                i
                for i in intersect_columns
                if i not in ["Screen-Name", "Row-Iteration", "Screen-Iteration"]
            ]
            df = df.merge(coal1, how="left", on=intersect_columns)

        coal_dat_processed[screen] = df

    # Add the unique Screen-1 names to identifyer columns, since they were added to every sheet
    join_cols = id_cols + [c for c in coal1.columns if c not in intersect_columns]
    join_cols = [c for c in join_cols if c not in ["Screen-Name", "Row-Iteration", "Screen-Iteration"]]

    return coal_dat_processed, join_cols


def process_sheets(
    coal_dat_processed,
    coal_xw,
    screen_names,
    cs_df,
    soa_sheetName,
    join_cols,
    coalition_names,
    ppr_year="2024" 
):
    """Process coalition sheets
    
    This function processes the data in each coalition sheet. It maps the raw 
    coalition column names to more readable header names, identifies missing submissions,
    and adjusts for year-specific narrative questions.

    :param coal_dat_processed: Dictionary of coalition sheets to be processed
    :type coal_dat_processed: <Dict(<pd.DataFrame>)>
    :param coal_xw: Crosswalk file (dynamically loaded)
    :type coal_xw: <pd.DataFrame>
    :param screen_names: Names of the coalition sheets and their section headers
    :type screen_names: <Dict>
    :param cs_df: List of expected coalition submissions for available years and programs
    :type cs_df: <pd.DataFrame>
    :param soa_sheetName: Name of Summary of Activities sheet
    :type soa_sheetName: <Str>
    :param join_cols: Unique identifier columns to join sheets on
    :type join_cols: <List>
    :param coalition_names: The full coalition names
    :type coalition_names: <pd.DataFrame>
    :param ppr_year: The PPR year being processed (default is "2023")
    :type ppr_year: <Str>

    :return: Processed coalition sheets and updated join columns
    :rtype: <Dict(<pd.DataFrame>), <List>
    """


    new_join_cols = join_cols
    for screen in screen_names.keys():
        df = coal_dat_processed[screen]

        # Update join columns dynamically based on availability
        these_join_cols = [col for col in join_cols if col in df.columns]
        new_join_cols = list(set(these_join_cols).intersection(set(new_join_cols)))

        # Get intersecting crosswalk column names
        set_coal_xw = set(coal_xw["Meta Name Description"])
        set_df_columns = set(df.columns)

        intersect_columns = list(set_coal_xw.intersection(set_df_columns))

        intersect_labels = list(
            coal_xw.loc[
                coal_xw["Meta Name Description"].isin(intersect_columns), "Label"
            ].reset_index(drop=True)
        )
        intersect_columns = list(
            coal_xw.loc[
                coal_xw["Meta Name Description"].isin(intersect_columns),
                "Meta Name Description",
            ].reset_index(drop=True)
        )
        rename_map = dict(zip(intersect_columns, intersect_labels))

        # Rename columns based on crosswalk file
        df = df.rename(columns=rename_map)

        # Process some of Section V.
        if screen == soa_sheetName:
            # Remove 'select' and 'none' from Involvement strings
            involve_cols = [f for f in df.columns if "Involvement" in f]
            for col in involve_cols:
                df.loc[
                    (df.astype({col: str})[col].str.lower() == "none")
                    | (df.astype({col: str})[col].str.lower() == "select"),
                    col,
                ] = np.nan

            # Make training columns numeric
            train_cols = [f for f in df.columns if "Trained" in f]
            for col in train_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Evaluate missing submissions for each program and year
        df["Missing"] = False
        df.loc[df["Program Abbr"] == "SDVC", "Program Abbr"] = "Core FVPSA"
        df.loc[df["Program Abbr"] == "SDC6", "Program Abbr"] = "ARP Act"
        df.loc[df["Program Abbr"] == "SDC3", "Program Abbr"] = "CARES Act"
        df.Year = df.Year.astype(str)
        df = df.drop_duplicates(subset=these_join_cols)
        df = (
            pd.merge(
                df,
                cs_df,
                how="right",
                left_on=["State", "Year", "Program Abbr"],
                right_on=["State", "Year", "Coal Program Abbr"],
            )
            .drop(columns=["Program Abbr"])
            .rename(columns={"Coal Program Abbr": "Program Abbr"})
            .drop_duplicates(subset=these_join_cols, ignore_index=True)
        )
        df.loc[df["Missing"] != False, "Missing"] = True

        # Add coalition name to missing coalitions
        df = df.drop("CoalitionName", axis="columns", errors="ignore")
        df = df.merge(coalition_names, how="left", on="State")

        # Assign to processed data
        coal_dat_processed[screen] = df

    new_join_cols = new_join_cols + ["Missing", "CoalitionName"]

    # Merge Screen IV and Screen V with year-specific logic
    screen5 = coal_dat_processed["Screen-5"]
    screen4 = coal_dat_processed["Screen-4"]

    # Define narrative questions based on PPR year
    if ppr_year == "2024":
        narrative_prefix = ["1. ", "2. ", "3. ", "4. ", "5. ", "6. ", "7. "]  # Extra narrative questions in 2024
    elif ppr_year == "2023":
        narrative_prefix = ["1. ", "2. ", "3. ", "4. ", "5. "]

    # Filter Screen IV based on year-specific narrative questions
    narr_questions = [c for c in screen4.columns for e in narrative_prefix if e in c]
    screen4 = screen4[new_join_cols + narr_questions]

    # Merge Screen IV and V
    coal_dat_processed["Screen-5"] = screen5.merge(screen4, how="left", on=new_join_cols)

    return coal_dat_processed, new_join_cols


def sectionIV_long_format(narr, var_cols, xw):
    """
    Transform Section IV into long format.

    :param narr: Section IV data (raw data)
    :param var_cols: Columns to keep as identifiers
    :param xw: Crosswalk DataFrame (contains Meta Name Description and Label)
    :return: Long format Section IV
    """


    rename_map = dict(zip(xw["Meta Name Description"], xw["Label"]))

    narr = narr.rename(columns=rename_map)

    # Include all narrative questions from 1 through 7
    narr_cols = [label for label in xw["Label"] if any(sub in label for sub in ["1. ", "2. ", "3. ", "4. ", "5. ", "6. ", "7. "])]

    narr_sub = narr[var_cols + narr_cols]
    coal_long_narr = narr_sub.melt(
        id_vars=var_cols,
        value_vars=narr_cols,
        value_name="Response",
        var_name="Narrative Question",
    ).drop_duplicates(subset=var_cols + ["Response"])

    return coal_long_narr




def sectionV_long_format(soa, var_cols):
    """Convert Section V to long format

    This function converts Section V. Summary of Activities in the coalitions
    data to long format.

    :param soa: Summary of Activities sheet
    :type soa: <pd.DataFrame>
    :param var_cols: Unique identifier columns
    :type var_cols: <List>

    :return: Summary of Activities sheet in long format
    :rtype: <pd.DataFrame>

    """

    # Involvement
    involve_cols = [f for f in soa.columns if "Involvement" in f]
    soa_sub = soa[var_cols + involve_cols]
    coal_long_involve = soa_sub.melt(
        id_vars=var_cols,
        value_vars=involve_cols,
        value_name="Level of Involvement",
        var_name="Priority Area",
    ).drop_duplicates(subset=var_cols + ["Priority Area"])
    coal_long_involve["Priority Area"] = (
        coal_long_involve["Priority Area"]
        .map(lambda x: x.replace("Level of Involvement - ", ""))
        .reset_index(drop=True)
    )

    # Short responses
    short_cols = [f for f in soa.columns if "Short Response" in f]
    soa_sub = soa[var_cols + short_cols]
    coal_long_short = soa_sub.melt(
        id_vars=var_cols,
        value_vars=short_cols,
        value_name="Short Response",
        var_name="Priority Area",
    ).drop_duplicates(subset=var_cols + ["Priority Area"])
    coal_long_short["Priority Area"] = (
        coal_long_short["Priority Area"]
        .map(
            lambda x: x.replace(
                "Short Response (Involved and Highly Involved only) - ", ""
            )
        )
        .reset_index(drop=True)
    )

    # Types of activities
    types_cols = [f for f in soa.columns if "Types of Activities" in f]
    soa_sub = soa[var_cols + types_cols]
    coal_long_types = soa_sub.melt(
        id_vars=var_cols,
        value_vars=types_cols,
        value_name="Types of Activities",
        var_name="Priority Area",
    ).drop_duplicates(subset=var_cols + ["Priority Area"])
    coal_long_types["Priority Area"] = (
        coal_long_types["Priority Area"]
        .map(lambda x: x.replace("Types of Activities - ", ""))
        .reset_index(drop=True)
    )

    # Number of people trained
    trained_cols = [f for f in soa.columns if "Number of People Trained" in f]
    soa_sub = soa[var_cols + trained_cols]
    coal_long_trained = soa_sub.melt(
        id_vars=var_cols,
        value_vars=trained_cols,
        value_name="Number of People Trained",
        var_name="Priority Area",
    ).drop_duplicates(subset=var_cols + ["Priority Area"])
    coal_long_trained["Priority Area"] = (
        coal_long_trained["Priority Area"]
        .map(lambda x: x.replace("Number of People Trained - ", ""))
        .reset_index(drop=True)
    )

    soa_long = coal_long_involve.merge(
        coal_long_types, on=var_cols + ["Priority Area"], how="outer"
    )
    soa_long = soa_long.merge(coal_long_short, how="outer").merge(
        coal_long_trained, on=var_cols + ["Priority Area"], how="outer"
    )

    # Split Types of Activities into separate rows
    split_types = [str(x).split("|") for x in soa_long["Types of Activities"]]
    split_types = list(map(lambda x: [s.strip() for s in x], split_types))
    soa_long["Types of Activities"] = split_types
    soa_long = soa_long.explode("Types of Activities")
    soa_long["Types of Activities"] = soa_long["Types of Activities"].replace(
        "nan", np.nan
    )

    return soa_long
