"""
Development of functions related to prepare the information required
to transform legacy data into TMEE SDMX structure (mapping)
The functions here could also end up being part of utility
The objective of these functions here is to easy the manual data entry process for:
_ Assigning a value for the sex disaggregation of an indicator
_ Assigning a value for the age disaggregation of an indicator
_ Assigning a value for the unit of an indicator
_ Match an indicator name to an indicator code (for those already loaded into SDMX)
_ Match indicator names (between those established as legacy against those 'new' or 'retained')
"""

import pandas as pd
import numpy as np
import re
from difflib import SequenceMatcher
from heapq import nlargest
from fuzzywuzzy import fuzz


def process_indicator_names(excel_file):
    """
    Loop legacy content and retrieve indicators information
    :param excel_file: path to excel legacy file
    :return: dataframe with indicators name and metadata (sex, age, unit)
    """

    # read excel content sheet (first one) into pandas dataframe
    excel_sheet_df = pd.read_excel(excel_file, sheet_name=0, dtype=str, header=None)
    # Number of rows in the content spreadsheet
    n_row_sheet = len(excel_sheet_df)

    # empty list to fill with indicators information
    tempInfo = []
    # indicator name regex pattern (e.g: 4.1.7)
    ind_pattern = r"\d+\.\d+.\d+.*"

    # Dev Check: strictly the name used for indicators in the content page don't coincide
    # exactly to those in the data spreadsheets (fuzzy match possible though)

    for row in range(n_row_sheet):

        # get 2nd column only - pd.iat(): pandas fastest access to single entry
        col1 = str(excel_sheet_df.iat[row, 1]).strip()

        if re.match(ind_pattern, col1):
            # indicator name
            indicator = col1
            # retrieve sex disaggregation from indicator name
            sex = get_indicator_sex(indicator)
            # retrieve age disaggregation from indicator name
            age = get_indicator_age(indicator)
            # retrieve units type from indicator name
            unit = get_indicator_units(indicator)

            tempInfo.append(
                {"indicator": indicator, "sex": sex, "age": age, "unit": unit,}
            )

    # loop spreadsheet rows ended: indicators content retrieved
    # return pandas df
    return pd.DataFrame.from_dict(tempInfo)


def get_indicator_sex(indicator_name):
    """
    Uses indicator name to infer either male or female sex
    :param indicator_name: indicator name (type string)
    :return sex: states either male or female (type string), or total (type NaN)
    """

    f = fuzz.token_set_ratio("girls", indicator_name)
    f1 = fuzz.partial_ratio("of women at", indicator_name)
    f2 = fuzz.token_set_ratio("Female", indicator_name)
    m = fuzz.token_set_ratio("boys", indicator_name)
    m1 = fuzz.partial_ratio("of men at", indicator_name)
    m2 = fuzz.token_set_ratio("male", indicator_name)

    # partial ratio matching tolerance
    match_tol = 75

    # possible string outputs
    switcher = {0: "F", 1: "M"}

    # no near match with any sex assigns a None type
    if max([f, f1, f2, m, m1, m2]) < match_tol:
        sex = np.nan
    else:
        max_sex = np.argmax([max([f, f1, f2]), max([m, m1, m2])])
        sex = switcher[max_sex]

    return sex


def get_indicator_age(indicator_name):
    """
    Uses indicator name to infer age groups
    :param indicator_name: indicator name (type string)
    :return age: string indicating the age group, or total (type NaN)
    """

    # erase the indicator code from name
    indicator_name = re.sub(r"\d+\.\d+.\d+.", "", indicator_name)

    a02 = fuzz.partial_ratio("aged 0-2 years", indicator_name)
    a36 = fuzz.partial_ratio("aged 3-6 years", indicator_name)
    a717 = fuzz.partial_ratio("aged 7-17 years", indicator_name)
    a79 = fuzz.token_set_ratio("aged 7-9 years", indicator_name)
    a1017 = fuzz.token_set_ratio("aged 10-17 years", indicator_name)
    a18 = fuzz.token_set_ratio("aged 18 years and", indicator_name)
    a017 = fuzz.token_set_ratio("aged 0-17", indicator_name)
    a01 = fuzz.token_set_ratio("under 1 year", indicator_name)
    a04 = fuzz.token_set_ratio("aged 0-4 years", indicator_name)
    a59 = fuzz.token_set_ratio("aged 5-9 years", indicator_name)
    a014 = fuzz.token_set_ratio("aged 0-14 years", indicator_name)
    a1417 = fuzz.token_set_ratio("aged 14-17 years", indicator_name)
    a1517 = fuzz.token_set_ratio("aged 15-17 years", indicator_name)
    a1519 = fuzz.token_set_ratio("aged 15-19 years", indicator_name)
    a1819 = fuzz.token_set_ratio("aged 18-19 years", indicator_name)
    a1859 = fuzz.token_set_ratio("aged 18-59 years", indicator_name)
    a60 = fuzz.token_set_ratio("aged 60 years over", indicator_name)
    a1544 = fuzz.token_set_ratio("aged 15-44 years", indicator_name)
    a1564 = fuzz.partial_ratio("aged 15-64 years", indicator_name)
    a65 = fuzz.token_set_ratio("aged 65 years over", indicator_name)
    aT = fuzz.token_set_ratio("ratio", indicator_name)
    a020 = fuzz.partial_ratio("under age 20", indicator_name)

    # possible string outputs (check this codes with Daniele to match standards)
    switcher = {
        0: "Y0T2",
        1: "Y3T6",
        2: "Y7T17",
        3: "Y7T9",
        4: "Y10T17",
        5: "Y_GE18",
        6: "Y0T17",
        7: "Y0",
        8: "Y0T4",
        9: "Y5T9",
        10: "Y0T14",
        11: "Y14T17",
        12: "Y15T17",
        13: "Y15T19",
        14: "Y18T19",
        15: "Y18T59",
        16: "Y_GE60",
        17: "Y15T44",
        18: "Y15T64",
        19: "Y_GE65",
        20: "_T",
        21: "Y0T19",
    }

    # partial ratio matching tolerance
    match_tol = 75

    # no near match with any age group assigns a None type
    if (
        max(
            [
                a02,
                a36,
                a717,
                a79,
                a1017,
                a18,
                a017,
                a01,
                a04,
                a59,
                a014,
                a1417,
                a1517,
                a1519,
                a1819,
                a1859,
                a60,
                a1544,
                a1564,
                a65,
                aT,
                a020,
            ]
        )
        < match_tol
    ):
        age = np.nan
    else:
        max_age = np.argmax(
            [
                a02,
                a36,
                a717,
                a79,
                a1017,
                a18,
                a017,
                a01,
                a04,
                a59,
                a014,
                a1417,
                a1517,
                a1519,
                a1819,
                a1859,
                a60,
                a1544,
                a1564,
                a65,
                aT,
                a020,
            ]
        )
        age = switcher[max_age]

    return age


def get_indicator_units(indicator_name):
    """
    Uses indicator name to get percentages in indicator units
    :param indicator_name: indicator name (type string)
    :return unit: states percentage (type string), or a NaN type
    """

    p = fuzz.partial_ratio("percentage", indicator_name)
    p1 = fuzz.partial_ratio("%", indicator_name)

    # partial ratio matching tolerance
    match_tol = 75

    # no near match with percentage assigns a None type
    if max([p, p1]) < match_tol:
        unit = np.nan
    else:
        unit = "PCNT"

    return unit


def process_indicator_codes(content_csv, legacy_indicators):
    """
    Loop legacy content and match indicator codes already stored in TMEE warehouse
    :param content_csv: path to csv with indicators name and metadata (sex, age, unit)
    :param legacy_indicators: dictionary with indicators code and names from TMEE warehouse
    :return: dataframe with codes added where indicators match
    """

    content_df = pd.read_csv(content_csv, dtype=str)
    # Number of rows in the indicators content dataframe
    n_row_df = len(content_df)
    # add empty code column to dataframe
    content_df["code"] = ""

    # list of legacy indicator names in TMEE warehouse
    ind_names_tmee = list(legacy_indicators.values())
    # list of legacy indicator codes in TMEE warehouse
    ind_codes_tmee = list(legacy_indicators.keys())

    # indicator name regex pattern (e.g: 4.1.7)
    ind_pattern = r"\d+\.\d+.\d+."
    # tolerance for matching indicators name
    tol_match = 0.92

    for row in range(n_row_df):

        # get indicator entry and erase legacy code
        indicator = re.sub(ind_pattern, "", content_df.iat[row, 0])
        best_match_idx = get_close_match_indexes(
            indicator, ind_names_tmee, n=1, cutoff=tol_match
        )
        if best_match_idx:
            # match indicator with code in TMEE warehouse
            content_df.iat[row, 4] = ind_codes_tmee[best_match_idx[0]]

    return content_df


def match_legacy_and_new(content_csv, new_or_retained_nsi, tol_match):
    """
    Loop legacy content and match indicator names (TMEE 'new' or 'retained' with NSI source)
    :param content_csv: path to csv with indicators name and metadata (sex, age, unit)
    :param new_or_retained: indicator names list (marked as TMEE 'new' or 'retained' with NSI source)
    :param tol_match: tolerance for matching indicators name
    :return: dataframe matching indicator names
    """

    # content dataframe
    content_df = pd.read_csv(content_csv, dtype=str)
    # initialize dataframe to return
    nsi_matched = pd.DataFrame(new_or_retained_nsi, columns=["NSI new or retained"])
    # add empty code column to return dataframe
    nsi_matched["legacy match"] = ""

    # indicator name regex pattern (e.g: 4.1.7)
    ind_pattern = r"\d+\.\d+.\d+."
    # erase pattern from content_df column
    content_df.indicator.replace(
        to_replace=ind_pattern, value="", regex=True, inplace=True
    )

    for irow, indicator in enumerate(new_or_retained_nsi):

        best_match_idx = get_close_match_indexes(
            indicator, content_df.indicator.values, n=1, cutoff=tol_match
        )

        if best_match_idx:
            # match NSI indicator with legacy indicator
            nsi_matched.iat[irow, 1] = content_df.iat[best_match_idx[0], 0]

    return nsi_matched


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


def match_country_name(name, country_list):
    """
    function that takes a name and return the best match to a list of country names
    :param name: string with a country name
    :param country_list: list of strings with country names
    """
    # initialize max_socre
    max_score = 0
    # initialize best_match
    best_match = ""
    # Iterating over all names in the country list
    for name2 in country_list:
        # Finding fuzzy match score1
        score1 = fuzz.partial_ratio(name, re.sub("republic", "", name2.lower()))
        # fuzzy match score2
        score2 = SequenceMatcher(
            None, name, re.sub("republic", "", name2.lower())
        ).ratio()
        score = max([score1, score2])
        # Checking if we have a better score
        if score > max_score:
            best_match = name2
            max_score = score
    return best_match
