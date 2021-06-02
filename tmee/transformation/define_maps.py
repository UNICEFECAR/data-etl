"""
This file contains information similar to SDMX data structure definitions (DSD)
These are stored variables type: dictionary
There will be our destination DSD, TransMonEE and the dataflows mapping
We also place a variable type dictionary with code mappings
"""

# country code mapping (ISO 2/3 letters and M49) is writen in country_map.py
# country code mapping (legacy country name to ISO 3 code) is writen in country_names_map.py
# seasons mapping and season_str is writen in seasons_map.py

# py files above are wrap together in one py file: codes_2_map
from .codes_2_map import (
    country_map,
    country_map_49,
    country_names_map,
    country_web_map,
    seasons_map,
    season_str,
)

import pandas as pd
import numpy as np

# path to file with legacy indicators meta data (age, sex, code, units)
path_legacy = "./data_in/legacy_data/content_legacy_codes_v3.csv"
# import csv into pandas
legacy_meta_data = pd.read_csv(path_legacy, dtype=str)


# NOTE: change comments, I separate dsd_dictionary and dflow_dictionary
# Development NOTES: there could be a more complicated relation in the future
# so far, all dflow_dictionary keys relate to the only one dsd_dictonary key

# the destination format (similar to a SDMX Data Structure Definition)
dsd_destination = {
    "TMEE": [
        {"id": "Dataflow", "type": "string"},
        {"id": "REF_AREA", "type": "enum", "role": "dim"},
        {"id": "INDICATOR", "type": "string", "role": "dim"},
        {"id": "SEX", "type": "string", "role": "dim"},
        {"id": "AGE", "type": "string", "role": "dim"},
        {"id": "RESIDENCE", "type": "string", "role": "dim"},
        {"id": "WEALTH_QUINTILE", "type": "string", "role": "dim"},
        {"id": "TIME_PERIOD", "type": "string", "role": "time"},
        {"id": "OBS_VALUE", "type": "string"},
        {"id": "COVERAGE_TIME", "type": "string"},
        {"id": "UNIT_MEASURE", "type": "string"},
        {"id": "OBS_FOOTNOTE", "type": "string"},
        {"id": "FREQ", "type": "string"},
        {"id": "DATA_SOURCE", "type": "string"},
        {"id": "UNIT_MULTIPLIER", "type": "string"},
        {"id": "OBS_STATUS", "type": "string"},
    ]
}

# Development NOTE 2: explain what are the ingredients of the column map dictionary below
# in particular: a) what are columns and constants
#                b) for constants: empty value means retrieved from data dictionary, the opposite: taken from dataflow directly
#                c) how dataflow mapping order is defined: name in DSD (keys), name in dataflow ('value')

# We store a mapping that relates columns in the dataflows to those in the destination DSD
# So far all dataflows mapping refer to one destination DSD (TMEE)
# The dataflows are the different DSD from where we extract indicators using API
dflow_col_map = {
    "DM": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "CME": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        # data structure modification as of June 2021
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        # added reference period June 2021
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": "REF_PERIOD"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        # added country notes June 2021
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": "COUNTRY_NOTES"},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        # data structure modification as of June 2021
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib", "value": ""},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "NUTRITION": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "MNCH": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "HIV_AIDS": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "SERIES_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "IMMUNISATION": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "const", "role": "dim", "value": ""},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "ECD": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "PT": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "GENDER": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "EDU_FINANCE": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "const", "role": "dim", "value": ""},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "SDG4": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "LOCATION"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "EDU_NON_FINANCE": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "LOCATION"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "LEGACY": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "country"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "indicator"},
        "SEX": {"type": "col", "role": "dim", "value": "sex"},
        "AGE": {"type": "col", "role": "dim", "value": "age"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "year"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "value"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "season"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "unit"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "obs_note"},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib", "value": ""},
        "OBS_STATUS": {"type": "const", "role": "attrib", "value": ""},
    },
    "WASH_SCHOOLS": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "const", "role": "dim", "value": ""},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "col",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    # WHO: indicators WHS_PBR and WHS9_CDR
    "csv-str-1": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "COUNTRY (CODE)"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "const", "role": "dim", "value": ""},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "YEAR (CODE)"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "Numeric"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "const", "role": "attrib", "value": ""},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "Comments"},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "GHO (URL)"},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib", "value": ""},
        "OBS_STATUS": {"type": "const", "role": "attrib", "value": ""},
    },
    # WHO: indicator SDGSUICIDE (noted struct mod WHO: 12 march 2021, age groups droped)
    "csv-str-2": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "COUNTRY (CODE)"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "SEX (CODE)"},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "YEAR (CODE)"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "Numeric"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "const", "role": "attrib", "value": ""},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "Comments"},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "GHO (URL)"},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib", "value": ""},
        "OBS_STATUS": {"type": "const", "role": "attrib", "value": ""},
    },
    # WHO: indicator SDGPM25
    "csv-str-3": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "COUNTRY (CODE)"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "const", "role": "dim", "value": ""},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {
            "type": "col",
            "role": "dim",
            "value": "RESIDENCEAREATYPE (CODE)",
        },
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "YEAR (CODE)"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "Numeric"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "const", "role": "attrib", "value": ""},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "Comments"},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "GHO (URL)"},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib", "value": ""},
        "OBS_STATUS": {"type": "const", "role": "attrib", "value": ""},
    },
    "DF_SDG_GLH": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {
            "type": "col",
            "role": "dim",
            "value": "INCOME_WEALTH_QUANTILE",
        },
        "RESIDENCE": {"type": "col", "role": "dim", "value": "URBANISATION"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "TIME_COVERAGE"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "COMMENT_OBS"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "SOURCE_DETAIL"},
        "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "NATURE"},
    },
    # pandas data reader: indicators compiled by World Bank
    "pandas data reader": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "iso3c"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "sex"},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "year"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "value"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "const", "role": "attrib", "value": ""},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "source"},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib", "value": ""},
        "OBS_STATUS": {"type": "const", "role": "attrib", "value": ""},
    },
    # ILO uses different dataflows per indicator - Unusual, why not to leverage SDMX DSDs???
    "DF_SDG_ALL_SDG_0861_SEX_RT": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "INDICATOR_NOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "SOURCE_NOTE"},
        "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    # bring unit multiplier from Helix (Daniele updated as of april 2021)
    "UNPD_DEMOGRAPHY": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "COVERAGE_TIME": {"type": "col", "role": "attrib", "value": "COVERAGE_TIME"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "OBS_FOOTNOTE"},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ_COLL"},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "DATA_SOURCE"},
        "UNIT_MULTIPLIER": {
            "type": "const",
            "role": "attrib",
            "value": "UNIT_MULTIPLIER",
        },
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    # web: web scraping (WHO Immunization only as of 26 Feb 2021)
    "web": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "country"},
        "INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "const", "role": "dim", "value": ""},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "year"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "value"},
        "COVERAGE_TIME": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MEASURE": {"type": "const", "role": "attrib", "value": ""},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "col", "role": "attrib", "value": "source"},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib", "value": ""},
        "OBS_STATUS": {"type": "const", "role": "attrib", "value": ""},
    },
}

# Code mappings are intended to normalize data entries in our destination DSD
# We must know beforehand if the extraction dataflow contains a different code to that of our destination one
# The mapping order is very important
# mapping order is written as actual_value:destination_value
# Also, for some extraction dataflows, we get codes and label descriptions as entries and we will keep only codes
# This last case is denoted as 'code:description':true in the code mapping

# Interesting discussion here: when to apply the code mapping?
# The columns name must be addressed properly (either destination or actual dataflow name)
# At this point it is done with dataflow name

code_mapping = {
    "DM": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "SEX": {"code:description": True},
        "AGE": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
    },
    "CME": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "SEX": {"code:description": True},
        "WEALTH_QUINTILE": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
    },
    "NUTRITION": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "SEX": {"code:description": True},
        "AGE": {"code:description": True},
        "WEALTH_QUINTILE": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
        "FREQ_COLL": {"code:description": True},
    },
    "MNCH": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "SEX": {"code:description": True},
        "AGE": {"code:description": True},
        "WEALTH_QUINTILE": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "DATA_SOURCE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
    },
    "HIV_AIDS": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "AGE": {"code:description": True},
        "SEX": {"code:description": True},
        "WEALTH_QUINTILE": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "DATA_SOURCE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
        "FREQ_COLL": {"code:description": True},
    },
    "IMMUNISATION": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "AGE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
        "FREQ_COLL": {"code:description": True},
    },
    "ECD": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "SEX": {"code:description": True},
        "AGE": {"code:description": True},
        "WEALTH_QUINTILE": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
        "DATA_SOURCE": {"code:description": True},
        "FREQ_COLL": {"code:description": True},
    },
    "PT": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "SEX": {"code:description": True},
        "AGE": {"code:description": True},
        "WEALTH_QUINTILE": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
        "DATA_SOURCE": {"code:description": True},
        "FREQ_COLL": {"code:description": True},
    },
    "GENDER": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "SEX": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "DATA_SOURCE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
        "FREQ_COLL": {"code:description": True},
    },
    "EDU_FINANCE": {
        "REF_AREA": country_map,
        "UNIT_MEASURE": {"GDP": "GDP_PERC"},
        "FREQ": {"A": "1"},
        "OBS_STATUS": {"Z": ""},
        # simple trick to work out millions not properly set for UNIT_MULT
        # works only since PPP_CONST corresponds to one indicator only
        "UNIT_MULT": {"depends": "UNIT_MEASURE", "map": {"PPP_CONST": "6"}},
    },
    "SDG4": {
        "REF_AREA": country_map,
        "UNIT_MEASURE": {"PER": "PS", "PT": "PCNT", "NB": "NUMBER"},
        "LOCATION": {"RUR": "R", "URB": "U", "_Z": "_T"},
        "WEALTH_QUINTILE": {"_Z": "_T"},
        "FREQ": {"A": "1"},
        "OBS_STATUS": {"Z": ""},
    },
    "EDU_NON_FINANCE": {
        "REF_AREA": country_map,
        "UNIT_MEASURE": {"PER": "PS", "PT": "PCNT", "NB": "NUMBER"},
        "AGE": {
            "UNDER1_AGE": "UNDER1_SCHOOL_ENTRY",
            "SCH_AGE_GROUP": "SCHOOL_AGE",
            "TH_ENTRY_AGE": "SCHOOL_ENTRY_AGE",
        },
        "LOCATION": {"RUR": "R", "URB": "U", "_Z": "_T"},
        "WEALTH_QUINTILE": {"_Z": "_T"},
        "FREQ": {"A": "1"},
        "OBS_STATUS": {"Z": ""},
    },
    "LEGACY": {
        "country": country_names_map,
        "unit": {
            "depends": "indicator",
            "map": dict(zip(legacy_meta_data.indicator, legacy_meta_data.unit)),
        },
        "age": {
            "depends": "indicator",
            "map": dict(zip(legacy_meta_data.indicator, legacy_meta_data.age)),
        },
        "sex": {
            "depends": "indicator",
            "map": dict(zip(legacy_meta_data.indicator, legacy_meta_data.sex)),
        },
        # it is important to place "indicator" here (previous mapping dependece) !!!
        "indicator": dict(zip(legacy_meta_data.indicator, legacy_meta_data.code)),
        # below is a trick to fill season if year column contains seasons strings
        "season": {"depends": "year", "map": dict(zip(season_str, season_str))},
        # it is important to place "year" after "season" (mapping dependence) !!!
        "year": seasons_map,
        # legacy data: integer to float conversion may result from pd.read_excel
        # ensure integers for units "PS" (persons) and "YR" (years)
        "value": {"depends": "unit", "map": {"PS": "to_int", "YR": "to_int"}},
    },
    "WASH_SCHOOLS": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
        "FREQ_COLL": {"code:description": True},
    },
    "csv-str-2": {
        "SEX (CODE)": {"FMLE": "F", "MLE": "M", "BTSX": "_T"},
        # WHO: indicator SDGSUICIDE (noted struct mod WHO: 12 march 2021, age groups droped)
        # "AGEGROUP (CODE)": {
        #     # empty values for age --> "_T"
        #     np.nan: "_T",
        #     "YEARS05-09": "Y5T9",
        #     "YEARS10-14": "Y10T14",
        #     "YEARS15-19": "Y15T19",
        #     "YEARS20-24": "Y20T24",
        #     "YEARS25-29": "Y25T29",
        #     "YEARS10-19": "Y10T19",
        #     "YEARS20-29": "Y20T29",
        #     "YEARS30-39": "Y30T39",
        #     "YEARS40-49": "Y40T49",
        #     "YEARS50-59": "Y50T59",
        #     "YEARS60-69": "Y60T69",
        #     "YEARS70-79": "Y70T79",
        #     "YEARS80PLUS": "Y_GE80",
        #     "YEARS15-29": "Y15T29",
        #     "YEARS30-49": "Y30T49",
        # },
    },
    "csv-str-3": {"RESIDENCEAREATYPE (CODE)": {"RUR": "R", "TOTL": "_T", "URB": "U"},},
    "DF_SDG_GLH": {
        "REF_AREA": country_map_49,
        "UNIT_MEASURE": {
            "PER_1000_POP": "RATE_1000",
            "PT": "PCNT",
            "PER_100000_POP": "RATE_100000",
            "PER_1000_UNINFECTED_POP": "RATE_1000",
            "IX": "IDX",
        },
        "NATURE": {"C": "A", "CA": "AD", "_X": "A", "M": "MD"},
        "FREQ": {"A": "1"},
    },
    "pandas data reader": {"sex": {"female": "F", "male": "M", "total": "_T"}},
    "DF_SDG_ALL_SDG_0861_SEX_RT": {
        "SEX": {"SEX_F": "F", "SEX_M": "M", "SEX_T": "_T"},
        "UNIT_MEASURE": {"PT": "PCNT"},
        "FREQ": {"A": "1"},
    },
    "UNPD_DEMOGRAPHY": {
        "REF_AREA": {"code:description": True},
        "INDICATOR": {"code:description": True},
        "SEX": {"code:description": True},
        "AGE": {"code:description": True},
        "RESIDENCE": {"code:description": True},
        "UNIT_MULTIPLIER": {"code:description": True},
        "UNIT_MEASURE": {"code:description": True},
        "OBS_STATUS": {"code:description": True},
        "FREQ_COLL": {"code:description": True},
    },
    "web": {"country": country_web_map},
}

# constants added at the dataflow level
# we need to do this as an input from the data dictionary
# at the dataflow level, there is a "compulsary" information to be added
# these are the constants (e.g: SEX, AGE, WEALTH_QUINTILE, RESIDENCE) defined as Dimensions
# there MUST be an entry for any variable defined as a Dimension in the SDMX destination DSD
# depending on the dataflow extraction source we could not have some of these in the dataframe
# we use then info from the constants input below to fill these entries!

# Development NOTE: discuss advantages/disadvatages of doing this at the indicator or dataflow level
# Development NOTE 2: if keeping dataflow level: you could enter it at dflow_col_map 'value' (using extra code)

# Is it decanting towards indicator level?

# last recall: data dictionary already have (INDICATOR, DATA_SOURCE, OBS_FOOTNOTE)
# last recall Jan 21: data dictionary additions (UNIT_MEASURE, FREQ, OBS_STATUS)
# last recall Mar 26: data dictionary additions (UNIT_MULTIPLIER) --> for UNPD and NET MIGRATION
# last recall: data dictionary don't have information related to LEGACY indicators

dflow_const = {
    "DM": {"WEALTH_QUINTILE": "_T"},
    # data structure modification as of June 2021 (wealth quintile included in Helix)
    "CME": {"AGE": "_T", "RESIDENCE": "_T"},
    "IMMUNISATION": {"SEX": "_T", "WEALTH_QUINTILE": "_T", "RESIDENCE": "_T"},
    "GENDER": {"AGE": "_T", "WEALTH_QUINTILE": "_T"},
    "EDU_FINANCE": {
        "SEX": "_T",
        "AGE": "_T",
        "WEALTH_QUINTILE": "_T",
        "RESIDENCE": "_T",
    },
    "LEGACY": {
        "WEALTH_QUINTILE": "_T",
        "RESIDENCE": "_T",
        "DATA_SOURCE": "TMEE Legacy DB",
    },
    "WASH_SCHOOLS": {"SEX": "_T", "AGE": "_T", "WEALTH_QUINTILE": "_T"},
    "csv-str-1": {
        "SEX": "_T",
        "AGE": "_T",
        "WEALTH_QUINTILE": "_T",
        "RESIDENCE": "_T",
    },
    # WHO: indicator SDGSUICIDE (noted struct mod WHO: 12 march 2021, age groups droped)
    "csv-str-2": {"AGE": "_T", "WEALTH_QUINTILE": "_T", "RESIDENCE": "_T"},
    "csv-str-3": {"SEX": "_T", "AGE": "_T", "WEALTH_QUINTILE": "_T"},
    "pandas data reader": {"AGE": "_T", "WEALTH_QUINTILE": "_T", "RESIDENCE": "_T"},
    "DF_SDG_ALL_SDG_0861_SEX_RT": {
        "AGE": "_T",
        "WEALTH_QUINTILE": "_T",
        "RESIDENCE": "_T",
    },
    "UNPD_DEMOGRAPHY": {"WEALTH_QUINTILE": "_T"},
    "web": {"SEX": "_T", "AGE": "_T", "WEALTH_QUINTILE": "_T", "RESIDENCE": "_T"},
}

