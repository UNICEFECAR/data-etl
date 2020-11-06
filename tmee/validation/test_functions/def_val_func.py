import math
import numbers
import numpy as np
import pandas as pd
import re
from collections.abc import Iterable


def is_missing(data):
    """
    Check missing value in TMEE Excel Data Collection
    Criteria: empty or string '-'
    :param data: cell value or Series value
    :return: test (scalar or array: boolean)
    """
    is_empty = pd.isnull(data)
    is_dashed = data == "-"
    test = is_empty | is_dashed
    return test


def is_partial(data):
    """
    Check flag partial value in TMEE Excel Data Collection
    Criteria: string 'pa'
    :param data: pd Series type (scalar accepted if len(Series) == 1)
    :return: test (boolean Series)
    """
    test = data.str.strip().str.lower() == "pa"
    return test


# Definition of TMEE validation functions


def valid_empty_data(col_year, data_rows):
    """
    Validate empty data for the year column being collected
    Test Number 1: Availability Warning
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param col_year: column with data collection for the year
    :param data_rows: mask rows that should contain data entry
    :return: test (boolean array, passed test: True)
    Pandera Check method: Series level
    """
    # computes function is_missing on the whole column
    col_year_missing = is_missing(col_year)
    # account missing data in data_rows only
    test = ~data_rows | ~col_year_missing
    return test


def valid_data_compilation(df, col_val, data_rows):
    """
    Validate availability of data between consecutive years
    Test Number 2 & 3: Availability Warning
    This function groups two tests proposed separately in Reference Doc.
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param df: dataframe
    :param col_val: column index for the data collection year
    :param data_rows: mask rows that should contain data entry
    :return: test (boolean array, passed test: True)
    Pandera Check method: DataFrame level
    """
    # valid_empty_data for two consecutive years
    col_year_T = df.iloc[:, col_val]
    test_year_T = valid_empty_data(col_year_T, data_rows)
    col_year_T_1 = df.iloc[:, col_val - 2]
    test_year_T_1 = valid_empty_data(col_year_T_1, data_rows)
    test = test_year_T == test_year_T_1
    return test


def valid_flag_compilation(df, col_val, data_rows):
    """
    Validate compilation of flags between consecutive years
    Test Number 4: Availability Alert
    Alerts when there's flag for year T-1 but not for year T
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param df: dataframe
    :param col_val: column index for the data collection year
    :param data_rows: mask rows that should contain data entry
    :return: test (boolean array, passed test: True)
    Pandera Check method: DataFrame level
    """
    col_flag_T_1 = df.iloc[:, col_val - 1]
    col_flag_T = df.iloc[:, col_val + 1]
    # Get False if flag T-1 not missing
    empty_flags_T_1 = ~valid_empty_data(col_flag_T_1, data_rows)
    # Get False if flag T is missing
    not_empty_flags_T = valid_empty_data(col_flag_T, data_rows)
    # test OR
    test = empty_flags_T_1 | not_empty_flags_T
    return test


def valid_flag_correspondence(df, col_val, data_rows):
    """
    Validate correspondance of flags between consecutive years
    Test Number 5: Availability Alert
    Alerts when flags activated for both years (T-1 and T) but are different
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param df: dataframe
    :param col_val: column index for the data collection year
    :param data_rows: mask rows that should contain data entry
    :return: test (boolean array, passed test: True)
    Pandera Check method: DataFrame level
    """
    col_flag_T_1 = df.iloc[:, col_val - 1]
    col_flag_T = df.iloc[:, col_val + 1]
    # check differences between flags
    flag_diff = col_flag_T_1 == col_flag_T
    # search for both nan's
    both_flags_nan = col_flag_T_1.isnull() & col_flag_T.isnull()
    # if both entries are NaN's, equality is forced True
    flag_diff = flag_diff | both_flags_nan
    # validate missing flags for the consecutive years
    not_missing_flags_T_1 = valid_empty_data(col_flag_T_1, data_rows)
    not_missing_flags_T = valid_empty_data(col_flag_T, data_rows)
    both_missing = ~(not_missing_flags_T_1 & not_missing_flags_T)
    # test: flag_diff OR both_missing
    test = flag_diff | both_missing
    return test


def valid_partial_disaggregation(df, col_val, df_rows):
    """
    Validate consistency on flags for the year being collected
    Test Number 6: Consistency Error on missing or partial disaggregation
    Flag 'pa' Error: missing or partial disaggregation while total not flagged partial
    Note: This function MUST be triggerd ONLY IF total == sum(disaggregation)
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param df: dataframe
    :param col_val: column index for the data collection year
    :param df_rows: dictionary with pointers
    Dev Note: the ideal df_rows should integrate all different disaggregations
    For the moment I try the following:
        --> df_rows['total']: pointer to total
        --> (ORDERED DICT PYTHON >>> 3.6) dict values [1,2] point start/end disaggregation
    Dev Note: this idea holds by assuming disaggregation always take consecutive rows!
    :return: test (boolean, passed test: True)
    Pandera Check method: DataFrame level
    """
    # pointers from df_rows: disaggregation start/end bounds
    disag_bounds = list(df_rows.values())[1:]
    rows_disag = range(disag_bounds[0], disag_bounds[1] + 1)

    # check missing values for disaggregation
    disag_values = df.iloc[rows_disag, col_val]
    missing_values = is_missing(disag_values)

    # retrieve flags for disaggregation
    disag_flags = df.iloc[rows_disag, col_val + 1]

    # initialize test True
    test = True

    # test checks for missing values or partial flags in disaggregation
    if missing_values.any() | ("pa" in disag_flags.values):
        #  pointer from df_rows: total
        row_total = df_rows["total"]
        # check total flag
        total_flag = df.iloc[row_total, col_val + 1]
        if total_flag != "pa":
            test = False

    return test


def valid_partial_total(df, col_val, df_rows):
    """
    Dev Note: function complementary to valid_partial_disaggregation, any ideas to reuse code?
    Validate consistency on flags for the year being collected
    Test Number 7: Consistency Error on partial total
    Flag 'pa' Error: disaggregation fully provided and total flagged partial
    Note: This function MUST be triggerd ONLY IF total == sum(disaggregation)
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param df: dataframe
    :param col_val: column index for the data collection year
    :param df_rows: dictionary with pointers
    Dev Note: the ideal df_rows should integrate all different disaggregations
    For the moment I try the following:
        --> df_rows['total']: pointer to total
        --> (ORDERED DICT PYTHON >>> 3.6) dict values [1,2] point start/end disaggregation
    Dev Note: this idea holds by assuming disaggregation always take consecutive rows!
    :return: test (boolean, passed test: True)
    Pandera Check method: DataFrame level
    """
    #  pointer from df_rows: total
    row_total = df_rows["total"]

    # retrieve total flag
    total_flag = df.iloc[row_total, col_val + 1]

    # initialize test True
    test = True

    # test checks for total flagged as partial
    if total_flag == "pa":
        # pointers from df_rows: disaggregation start/end bounds
        disag_bounds = list(df_rows.values())[1:]
        rows_disag = range(disag_bounds[0], disag_bounds[1] + 1)

        # check missing values for disaggregation
        disag_values = df.iloc[rows_disag, col_val]
        missing_values = is_missing(disag_values)

        # retrieve flags for disaggregation
        disag_flags = df.iloc[rows_disag, col_val + 1]

        # check fully provided disaggregation (all entries AND no partial flags reported)
        if ~missing_values.all() & ("pa" not in disag_flags.values):
            test = False

    return test


def valid_p_or_e_flag(col_flag_year, df_rows):
    """
    Validate consistency on flags for the year being collected
    Test Number 8: Consistency Alert on provisional or estimated flags
    Alerts when total contains 'p' or 'e' flags while not disaggregation
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param col_flag_year: column with flags for the year data collection
    :param df_rows: dictionary with pointers
    Dev Note: the ideal df_rows should integrate all different disaggregations
    For the moment I try the following:
        --> df_rows['total']: pointer to total
        --> (ORDERED DICT PYTHON >>> 3.6) dict values [1,2] point start/end disaggregation
    Dev Note: this idea holds by assuming disaggregation always take consecutive rows!
    :return: test (boolean, passed test: True)
    Pandera Check method: Series level
    """
    # pointers from df_rows: total and disaggregations
    row_total = df_rows["total"]

    # disaggregation start/end bounds
    disag_bounds = list(df_rows.values())[1:]
    rows_disag = range(disag_bounds[0], disag_bounds[1] + 1)

    # flag for total
    total_flag = col_flag_year.iloc[row_total]
    # flags for disaggregation
    disag_flags = col_flag_year.iloc[rows_disag]

    # initialize test True
    test = True

    # check total flag
    total_p_or_e = total_flag in ["p", "e"]
    if total_p_or_e:
        # check disaggregation flags
        disag_p_or_e = total_flag in disag_flags.values
        if not disag_p_or_e:
            test = False

    return test


def valid_sex_disagg(df, col_val, df_rows):
    """
    Validate a sex disaggregation (total == sex1 + sex2)
    Test Number 9: Consistency Error
    Logic of the test developed by TMEE team 
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param df: dataframe
    :param col_val: column index for the data collection year
    :param df_rows: dictionary with pointers
    :param df_rows['total']: pointer to total
    :param df_rows['sex1']: pointer to first gender disaggregation
    :param df_rows['sex2']: pointer to second gender disaggregation
    :return: test (boolean, passed test: True)
    Pandera Check method: DataFrame level
    """
    # pointers from df_rows: total, sex1 and sex2
    row_total = df_rows["total"]
    row_sex1 = df_rows["sex1"]
    row_sex2 = df_rows["sex2"]

    # check missing values
    all_values = df.iloc[[row_total, row_sex1, row_sex2], col_val]
    missing_values = is_missing(all_values)
    # pd.to_numeric traces not allowed data entries
    num_values = pd.to_numeric(all_values, errors="coerce")
    nan_values = pd.isnull(num_values)
    # nan_values allowed are missing_values only
    not_allowed = (missing_values != nan_values).any()

    if missing_values.all():
        # skip test: all missing data
        return True
    elif missing_values.iloc[0] | not_allowed:
        # fail test: total missing, any sex not
        # fail test: not allowed data entry types
        return False
    elif missing_values[1:].all():
        # skip test: valid total and all sex missing
        return True

    # sum numeric values from sex
    sum_sex = num_values[~nan_values][1:].sum()
    test = math.isclose(num_values.iloc[0], sum_sex)

    # if math.isclose fails, validate condition: sum_sex less than total
    if (not test) & (sum_sex < num_values.iloc[0]):
        # check partial flags in sex
        sex_flags = df.iloc[[row_sex1, row_sex2], col_val + 1]
        partial_flags = is_partial(sex_flags)
        # accept only complementary flags 'partial' to missing_values
        if (~partial_flags == missing_values[1:]).all():
            test = True

    return test


def valid_disaggregation(df, col_val, df_rows):
    """
    Validate disaggregation (total == sum(disaggregation))
    Test Numbers 9, 10, ... : Consistency Errors
    Logic of the test developed by TMEE team
    Reference Doc: Transmonee - Manual for Data Validation -  24 June
    Reference Author: Flavio Bianconi
    :param df: dataframe
    :param col_val: column index for the data collection year
    :param df_rows: dictionary with pointers
    Dev Note: the ideal df_rows should integrate all different disaggregations
    For the moment I try the following:
        --> df_rows['total']: pointer to total
        --> (ORDERED DICT PYTHON >>> 3.6) dict values [1,2] point start/end disaggregation
    Dev Note: this idea holds by assuming disaggregation always take consecutive rows!
    :return: test (boolean, passed test: True)
    Pandera Check method: DataFrame level
    """
    # total pointer from df_rows
    all_rows = [df_rows["total"]]

    # disaggregation start/end bounds
    disag_bounds = list(df_rows.values())[1:]
    rows_disag = range(disag_bounds[0], disag_bounds[1] + 1)
    all_rows.extend(rows_disag)

    # check missing values
    all_values = df.iloc[all_rows, col_val]
    missing_values = is_missing(all_values)

    # pd.to_numeric traces not allowed data entries
    num_values = pd.to_numeric(all_values, errors="coerce")
    nan_values = pd.isnull(num_values)
    # nan_values allowed are missing_values only
    not_allowed = (missing_values != nan_values).any()

    if missing_values.all():
        # skip test: all missing data
        return True
    elif missing_values.iloc[0] | not_allowed:
        # fail test: total missing, any disaggregation not
        # fail test: not allowed data entry types
        return False
    elif missing_values[1:].all():
        # skip test: valid total and all disaggregation missing
        return True

    # sum numeric values from disaggregation
    sum_disag = num_values[~nan_values][1:].sum()
    total = num_values.iloc[0]
    test = math.isclose(total, sum_disag)

    # if math.isclose fails, validate condition: sum_disag less than total
    if (not test) & (sum_disag < total):
        # check partial flags in disaggregation
        disag_flags = df.iloc[rows_disag, col_val + 1]
        partial_flags = is_partial(disag_flags)
        # accept only complementary flags 'partial' to missing_values
        if (~partial_flags == missing_values[1:]).all():
            test = True

    return test


# utils functions
def get_totals_by_sex(formulas, row_offset):
    """
    Get totals and sex disaggregation from formulas
    Formulas Doc: Transmonee - Manual for Data Validation -  24 June
    :param formula: concatenation of formulas type str
    :param row_offset: number of rows skiped by pandas excel reader
    :return totals_by_sex: dictionaries list, keys: 'total', 'sex1', 'sex2'
    """
    # formulas concatenated with commas
    total_sex_formulas = formulas.split(",")
    # split symbol not equal
    split_neq_formulas = [x.split("≠") for x in total_sex_formulas]
    totals_in_formulas = [x[0] for x in split_neq_formulas]
    both_sex_formulas = [x[1].split("+") for x in split_neq_formulas]
    # substract: int() - (row_offset + 2) --> go from Excel skiprows to pandas index
    totals_by_sex = [
        {
            "total": int(rel[0]) - (row_offset + 2),
            "sex1": int(rel[1][0]) - (row_offset + 2),
            "sex2": int(rel[1][1]) - (row_offset + 2),
        }
        for rel in zip(totals_in_formulas, both_sex_formulas)
    ]
    return totals_by_sex


def get_totals_and_disag(formulas, row_offset):
    """
    Get totals and disaggregation from formulas
    Note: most documented formulas are "to complete once the form is finalized"
    Formulas Doc: Transmonee - Manual for Data Validation -  24 June
    :param formula: concatenation of formulas type str
    :param row_offset: number of rows skiped by pandas excel reader
    :return totals_and_disag: list of dictionaries, keys: 'total', 'lower', 'upper'
    Dev Note: assumes data entered in consecutive rows 'lower' and 'upper' (both inclusive)
    """
    # formulas concatenated with commas
    total_disag_formulas = formulas.split(",")

    # check list last element: are formulas completed?
    if "complete" in total_disag_formulas[-1]:
        # eliminate label from list
        label = total_disag_formulas.pop()
        print(f"Formulas: {label.strip()}")

    # split symbol not equal
    split_neq_formulas = [x.split("≠") for x in total_disag_formulas]
    totals_in_formulas = [x[0] for x in split_neq_formulas]
    disag_bounds = [re.findall(r"\d+", x[1]) for x in split_neq_formulas]
    # substract: int() - (row_offset + 2) --> go from Excel skiprows to pandas index
    totals_and_disag = [
        {
            "total": int(rel[0]) - (row_offset + 2),
            "lower": int(rel[1][0]) - (row_offset + 2),
            "upper": int(rel[1][1]) - (row_offset + 2),
        }
        for rel in zip(totals_in_formulas, disag_bounds)
    ]
    return totals_and_disag


def is_entry_row(col_codes):
    """
    Determine data collection rows from code criteria (if code then data)
    Reference: Transmonee - Manual for Data Validation -  24 June
    :param col_codes: column containing codes from TMEE collection template
    :return data_rows: mask all rows where data should be entered
    """
    # if not code then NaN (pd.to_numeric)
    code_nan_col = pd.to_numeric(col_codes, errors="coerce")
    return code_nan_col.notnull()


# Dev Zone: functions below are not being used


def sum_if_num(cell_val1, cell_val2):
    """
    Adds two variables if both numeric
    :param cell_val1: cell value 1
    :param cell_val2: cell value 2
    :return: the sum or boolean False
    """
    is_num1 = isinstance(cell_val1, numbers.Number)
    is_num2 = isinstance(cell_val2, numbers.Number)
    if is_num1 & is_num2:
        return cell_val1 + cell_val2
    else:
        return False


def is_num(cell_val):
    """
    Check if scalar variable is numeric
    :param cell_val: cell value (scalar, 0-dim)
    :return: boolean
    """
    return isinstance(cell_val, numbers.Number)
