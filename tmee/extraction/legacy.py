"""
Development of functions related to extraction of legacy data
Functions here could also end up being part of utility
"""

import pandas as pd
import re
from difflib import get_close_matches


def check_year_row(array):
    """
    Checks if a numpy array contains years (all entries) between min and max
    :param array: numpy array (object type) without NaN's
    :return: A message if any entry in the array is not an year
    """
    # minimum and maximum year for checking
    min_y = 1950
    max_y = 2050
    # try to convert to integer
    try:
        array_int = array.astype("int")
    except ValueError:
        print("This years row contains entries that are not numbers")
    else:
        if ((array_int < min_y) | (array_int > max_y)).any():
            print("This row contains numbers that are not years")


def checkout_merged_df(merged_df):
    """
    Checks notes and data merge operation
    Perform small cleaning operations
    Add sex, age and unit columns for later use in SDMX
    :param merged_df: dataframe merged from parsed data and notes (parse_legacy function)
    :return: Warning messages eventually.
    :return: Operates directly to merged_df (statement return is left empty)
    """

    # validate matching (non-nulls in noteID and obs_note must coincide)
    if merged_df.noteId.notnull().sum() != merged_df.obs_note.notnull().sum():
        print("Information lost when notes joined into data")
    # validate empty data (typically marked as '-' in TMEE legacy)
    if merged_df.value.notnull().sum() != len(merged_df):
        print("There are empty values not marked with '-'")

    # drop column noteId (used for matching purposes only)
    merged_df.drop(columns=["noteId"], inplace=True)
    # Drop rows with empty values (marked as '-' in TMEE legacy)
    merged_df.drop(index=merged_df[merged_df.value == "-"].index, inplace=True)

    # create columns unit, age and sex (filled with most used values)
    std_unit = "NUMBER"
    merged_df["unit"] = std_unit
    std_sex_age = "_T"
    merged_df["age"] = std_sex_age
    merged_df["sex"] = std_sex_age

    # return (introduced changes directly to merged_df)
    return


def parse_legacy(excel_file, sheet_number, country_list):
    """
    Parse legacy data from an excel sheet
    :param excel_file: path to excel file to extract info
    :param sheet_number: an integer identifying the sheet to parse
    :country_list: list of countries to fuzzy match against legacy data
    :return: a pandas dataframe with the raw parsed legacy data
    """

    # read excel sheet into pandas dataframe
    excel_sheet_df = pd.read_excel(
        excel_file, sheet_name=sheet_number, dtype=str, header=None
    )
    # Number of rows in the spreadsheet
    n_row_sheet = len(excel_sheet_df)
    # Column offset (number of columns before entries begin)
    col_off = 3

    # empty list to fill with temporary parsed data
    tempData = []
    tempNote = []
    # regex pattern to retrieve notes (just a number)!
    note_pattern = r"^\d+$"
    # indicator regex pattern (e.g: 4.1.7)
    ind_pattern = r"\d+\.\d+.\d+.*"

    for row in range(n_row_sheet):

        # get 1st column - pd.iat(): pandas fastest access to single entry
        col0 = str(excel_sheet_df.iat[row, 0]).strip()
        # get 2nd column
        col1 = str(excel_sheet_df.iat[row, 1]).strip()
        # get 3rd column (str not used, NaN's are kept!)
        col2 = excel_sheet_df.iat[row, 2]

        if re.match(ind_pattern, col1):
            # data for a new indicator begins
            indicator = col1
            # retrieve row with years
            years = excel_sheet_df.iloc[row + 2, :].dropna().values
            # validate row: years
            check_year_row(years)
            # get years length
            n_years = len(years)

        elif get_close_matches(col1, country_list):
            # close matching: there is country data along the row
            country = col1.lower()
            # loop on columns (years) and get data
            for col in range(n_years):
                tempData.append(
                    {
                        "country": country,
                        "indicator": indicator,
                        "year": years[col].strip(),
                        "value": excel_sheet_df.iat[row, col + col_off].strip(),
                        "noteId": col2,
                    }
                )

        elif re.match(note_pattern, col0):
            # there are notes
            tempNote.append({"indicator": indicator, "noteId": col0, "obs_note": col1})

    # loop spreadsheet rows ended: all indicators retrieved
    # create pandas df for data and notes separately
    sheet_data_df = pd.DataFrame.from_dict(tempData)
    sheet_note_df = pd.DataFrame.from_dict(tempNote)

    # join notes into data
    merged_df = sheet_data_df.merge(
        sheet_note_df,
        on=["indicator", "noteId"],
        how="left",
        sort=False,
        validate="m:1",
    )

    # return merged dataframe after small operations (see checkout function)
    checkout_merged_df(merged_df)
    return merged_df

