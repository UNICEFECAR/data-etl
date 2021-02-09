"""
Utility functions that operate directly on our data-dictionary in Excel using pandas
Typically retrieve queries, useful for our ETL purposes
To improve: implement extraction as an abstract class following James' refactoring
"""

import requests
import pandas as pd
import numpy as np
import re
import warnings
from pandas_datareader import wb
from difflib import SequenceMatcher
from heapq import nlargest


def get_API_code_address_etc(excel_data_dict):
    """ filters all indicators that are extracted by API
        :param excel_data_dict: path/file to our excel data dictionary in repo
        :return: pandas dataframe (df) with code, url endpoint for requests, data source name, etc
    """
    # read snapshots table from excel data-dictionary
    snapshot_df = pd.read_excel(excel_data_dict, sheet_name="Snapshot")
    # read sources table from excel data-dictionary
    sources_df = pd.read_excel(excel_data_dict, sheet_name="Source")
    # join snapshot and source based on Source_Id
    # 'left' preserves key order from snapshots
    snap_source_df = snapshot_df.merge(
        sources_df, on="Source_Id", how="left", sort=False
    )
    # read indicators table from excel data-dictionary
    indicators_df = pd.read_excel(excel_data_dict, sheet_name="Indicator")
    # join snap_source and indicators based on Indicator_Id
    snap_source_ind_df = snap_source_df.merge(
        indicators_df, on="Indicator_Id", how="left", sort=False
    )
    # Finally read Value_type table from excel data-dictionary
    val_df = pd.read_excel(excel_data_dict, sheet_name="Value_type")
    # join snap_source_ind and Value_type based on Value_Id
    snap_source_ind_val_df = snap_source_ind_df.merge(
        val_df, on="Value_Id", how="left", sort=False
    )
    # get list of API extractions: indicator codes, url endpoints, etc
    logic_API = snap_source_ind_val_df.Type_x == "API"

    api_code_addr_etc_df = snap_source_ind_val_df[logic_API][
        [
            "Theme",
            "Code_y",
            "Name",
            "Address",
            "Name_y",
            "Comments_y",
            "Content_type",
            "Units_y",
            "Disaggregation",
            "Freq_Coll",
            "Nature",
        ]
    ]

    api_code_addr_etc_df.rename(
        columns={
            "Name": "Indicator_name",
            "Code_y": "Code",
            "Name_y": "Data_Source",
            "Comments_y": "Obs_Footnote",
            "Units_y": "Units",
        },
        inplace=True,
    )
    # return dataframe (note its df index will correspond to that of snapshots)
    return api_code_addr_etc_df


# function for api request as proposed by Daniele
# errors are printed and don't stop program execution
def api_request(address, params=None, headers=None):
    """
    TODO: Look at the error handerling here
    """
    try:
        response = requests.get(address, params=params, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as error:
        print(f"Other error occurred: {error}")
    # return response object (check with James notes below)
    # response will not be defined for the following errors:
    # "no connection adapters found", "Invalid URL: No schema supplied"
    return response


def data_reader(address, country_codes=None, start_period=None, end_period=None):
    """
    Uses pandas data reader to download World Bank indicators (data and metadata)
    :param address: from data dictionary, string containing World Bank Indicator code/s
    :param country_codes: TMEE countries to call (note data reader default just 'US', 'CA' and 'MX')
    :param start_period: First year of the data series (note data reader default is 2003)
    :param end_period: Last year of the data series, inclusive (note data reader default is 2005)
    :return: pandas dataframe with data/metadata and error flag
    World Bank sex disaggregation is compilated using different indicator codes with 'FE' and 'MA'
    Dev note: address must contain either one indicator (no sex) or three indicators (total, 'FE' and 'MA')
    Transformations must be done to flaten data reader output
    Simple error handling prints messages and don't stop program execution
    Error message: when any of the provided indicator codes is wrong
    """

    # initialize output
    data_df = None
    data_error = False

    # get indicator code/s from data dictionary
    wb_codes = re.findall(r"'(.+?)'", address)

    # add country_call if present
    country_call = None
    if country_codes:
        country_call = list(country_codes.values())

    # pandas data reader data call
    # handle warnings as errors forces all indicator codes to be correct!
    warnings.filterwarnings("error")
    try:
        # pandas data reader country calls (ISO-alpha 3)
        data_df = wb.download(
            indicator=wb_codes, country=country_call, start=start_period, end=end_period
        )
    except Exception as error:
        print(f"Error occurred: {error}")
        data_error = True

    if not data_error:

        # flatten output
        data_df.reset_index(inplace=True)

        # set warnings back to default for metadata call
        warnings.resetwarnings()
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        # call for metadata using first element in wb_codes list
        metadata_df = wb.search(string=wb_codes[0], field="id")
        # metadata will download and cache all indicators the first time is called
        # depending on your connection this can take some time
        # subsequent searches should be faster on the chached copy

        # filter metadata_df (wb.search does fuzzy matching)
        filter_id = metadata_df.id == wb_codes[0]
        metadata_row = metadata_df[filter_id]

        # data source: collection
        collection = metadata_row.source.values[0].strip(" .")
        # data source: topic
        topic = metadata_row.topics.values[0].strip(" .")
        # data source: organization
        organization = metadata_row.sourceOrganization.values[0].decode().strip(" .")

        # acknowledge World Bank data compilation
        wb_collection = f"Compiled in World Bank collection: {collection} ({topic})."
        # full metadata to publish in data_source
        data_source = f"{organization}. {wb_collection}"
        data_df["source"] = data_source

        # add country codes from pandas data reader and avoid posterior country mappings
        country_df = wb.get_countries()
        country_df.rename(columns={"name": "country"}, inplace=True)
        # add iso3 codes to data_df from country_df
        data_df = data_df.merge(
            country_df[["country", "iso3c"]], on="country", how="left", sort=False
        )

        # boolean sex_female array
        sex_female = np.array(["FE" in elem.split(".") for elem in wb_codes])
        # boolean sex_male array
        sex_male = np.array(["MA" in elem.split(".") for elem in wb_codes])
        # boolean sex_total array
        sex_total = ~(sex_female | sex_male)

        # accepted disaggregation: total or (total and female and male)
        if len(wb_codes) == 1 and sex_total.any():
            # add sex dimension all populated with total
            data_df["sex"] = "total"
            # rename column with indicator name as value
            data_df.rename(columns={wb_codes[0]: "value"}, inplace=True)

        elif (
            len(wb_codes) == 3
            and sex_total.any()
            and sex_female.any()
            and sex_male.any()
        ):
            # rename columns using sex disaggregation
            data_df.rename(
                columns={wb_codes[np.where(sex_total)[0][0]]: "value_total"},
                inplace=True,
            )
            data_df.rename(
                columns={wb_codes[np.where(sex_female)[0][0]]: "value_female"},
                inplace=True,
            )
            data_df.rename(
                columns={wb_codes[np.where(sex_male)[0][0]]: "value_male"}, inplace=True
            )

            # wide to long transformation: SDMX has sex as a dimension
            data_df_long = pd.wide_to_long(
                data_df,
                stubnames="value",
                i=["country", "year"],
                j="sex",
                sep="_",
                suffix=r"\w+",
            )

            # overwrite output variable with long format flatten
            data_df = data_df_long.reset_index()

        else:
            # flag data_error to avoid posterior mapping of raw data
            data_error = True
            print("Verify correct sex disaggregation in pandas data reader call")

    return data_df, data_error


def get_close_match_indexes(word, possibilities, n=3, cutoff=0.6):
    """Use SequenceMatcher to return a list of indexes of the best "good enough" matches
    :param word: string
    :param possibilities: list of strings
    :param n: (optional integer) maximum number of close matches to return
    :param cutoff: (optional) Threshold score to ignore possibilities
    """
    result = []
    s = SequenceMatcher()
    s.set_seq2(word)
    for idx, x in enumerate(possibilities):
        s.set_seq1(x)
        if s.ratio() >= cutoff:
            result.append((s.ratio(), idx))

    # Move the best scorers to head of list
    result = nlargest(n, result)

    # Strip scores for the best n matches
    return [x for score, x in result]


def get_codelist_API_legacy(data_dict_df, path_legacy):
    """
    wraps all indicators codes and names taken from data dictionary and
    adds to these the indicator codes and names from legacy data
    :param data_dict_df: df from data dictionary query (get_API_code_address_etc function output)
    :path_legacy: path/file to legacy indicators meta data (age, sex, code, units)
    returns df with indicator code and name (typicall SDMX codelist)
    """

    # import csv into pandas
    legacy_meta_data = pd.read_csv(path_legacy, dtype=str)

    # search for sex and age totals
    sex_and_age_t = (legacy_meta_data.age == "_T") & (legacy_meta_data.sex == "_T")

    # TMEE legacy indicator pattern to be deleted (e.g: 4.1.7)
    ind_pattern = r"\d+\.\d+.\d+."

    # make new data frame with legacy code and name only
    legacy_code_name_df = pd.concat(
        [
            legacy_meta_data.code[sex_and_age_t].str.strip(),
            legacy_meta_data.indicator[sex_and_age_t]
            .replace(ind_pattern, "", regex=True)
            .str.strip(),
        ],
        axis=1,
    )
    # rename df columns to match data_dict_df
    legacy_code_name_df.rename(
        columns={"code": "Code", "indicator": "Indicator_name"}, inplace=True
    )

    # search indicators not listed under sex and age totals
    missed_codes = np.setdiff1d(
        legacy_meta_data.code.unique(), legacy_code_name_df.Code.unique()
    )

    # add missed_codes in missed_legacy (with empty indicator names if code is repated)
    missed_legacy = [
        {
            "Code": code,
            "Indicator_name": legacy_meta_data.indicator[legacy_meta_data.code == code]
            .replace(ind_pattern, "", regex=True)
            .str.strip()
            .values[0],
        }
        if ((legacy_meta_data.code == code).sum() == 1)
        else {"Code": code, "Indicator_name": ""}
        for code in missed_codes
    ]

    # return concatenation of data_dict_df and legacy_code_name_df (missed_legacy appended)
    return pd.concat(
        [
            data_dict_df[["Code", "Indicator_name"]],
            legacy_code_name_df.append(missed_legacy),
        ]
    )


def get_units_codelist(etl_out_file):
    """
    wraps all unique unit codes from etl_out_file 'csv' file
    flags if there is any entry without units specified
    :param etl_out_file: path/file to 'csv' with all transformed data
    returns df with units code and empty name/label (typicall SDMX codelist)
    """

    etl_out_df = pd.read_csv(etl_out_file, dtype=str)

    # get unique codes
    unit_codes_array = etl_out_df.UNIT_MEASURE.unique()
    # empty array for code labels (filled with NaN's)
    unit_label_array = np.empty(len(unit_codes_array))
    unit_label_array[:] = np.NaN
    # prepare dataframe output
    cl_units_df = pd.DataFrame(
        {"Code": unit_codes_array, "Unit_label": unit_label_array}
    )

    # flag empty
    empty_flag = etl_out_df.UNIT_MEASURE.isnull().any()

    return cl_units_df, empty_flag


# function from stackoverflow by MaxU
def append_df_to_excel(
    filename,
    df,
    sheet_name="Sheet1",
    startrow=None,
    truncate_sheet=False,
    **to_excel_kwargs,
):
    """
    Append a DataFrame [df] to existing Excel file [filename]
    into [sheet_name] Sheet.
    If [filename] doesn't exist, then this function will create it.

    Parameters:
      filename : File path or existing ExcelWriter
                 (Example: '/path/to/file.xlsx')
      df : dataframe to save to workbook
      sheet_name : Name of sheet which will contain DataFrame.
                   (default: 'Sheet1')
      startrow : upper left cell row to dump data frame.
                 Per default (startrow=None) calculate the last row
                 in the existing DF and write to the next row...
      truncate_sheet : truncate (remove and recreate) [sheet_name]
                       before writing DataFrame to Excel file
      to_excel_kwargs : arguments which will be passed to `DataFrame.to_excel()`
                        [can be dictionary]

    Returns: None
    """
    from openpyxl import load_workbook

    # ignore [engine] parameter if it was passed
    if "engine" in to_excel_kwargs:
        to_excel_kwargs.pop("engine")

    writer = pd.ExcelWriter(filename, engine="openpyxl")

    # Python 2.x: define [FileNotFoundError] exception if it doesn't exist
    try:
        FileNotFoundError
    except NameError:
        FileNotFoundError
        raise IOError

    try:
        # try to open an existing workbook
        writer.book = load_workbook(filename)

        # get the last row in the existing Excel sheet
        # if it was not specified explicitly
        if startrow is None and sheet_name in writer.book.sheetnames:
            startrow = writer.book[sheet_name].max_row

        # truncate sheet
        if truncate_sheet and sheet_name in writer.book.sheetnames:
            # index of [sheet_name] sheet
            idx = writer.book.sheetnames.index(sheet_name)
            # remove [sheet_name]
            writer.book.remove(writer.book.worksheets[idx])
            # create an empty sheet [sheet_name] using old index
            writer.book.create_sheet(sheet_name, idx)

        # copy existing sheets
        writer.sheets = {ws.title: ws for ws in writer.book.worksheets}
    except FileNotFoundError:
        # file does not exist yet, we will create it
        pass

    # would this below overwrite startrow defined above? (commented by beto S.)
    if startrow is None:
        startrow = 0

    # write out the new sheet
    df.to_excel(writer, sheet_name, startrow=startrow, **to_excel_kwargs)

    # save the workbook
    writer.save()
