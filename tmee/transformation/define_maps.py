"""
This file contains information similar to SDMX data structure definitions (DSD)
These are stored variables type: dictionary
There will be our destination DSD, TransMonEE and the dataflows mapping
We also place a variable type dictionary with code mappings
"""

# import country code mapping (ISO 2/3 letters)
from transformation.country_map import country_map

# import country code mapping (country name to ISO 3 code)
from transformation.country_names_map import country_names_map

import pandas as pd

# path to file with legacy indicators meta data (age, sex, code, units)
path_legacy = "./legacy_data/content_legacy_codes_v2.csv"
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
        {"id": "UNICEF_INDICATOR", "type": "string", "role": "dim"},
        {"id": "SEX", "type": "string", "role": "dim"},
        {"id": "AGE", "type": "string", "role": "dim"},
        {"id": "RESIDENCE", "type": "string", "role": "dim"},
        {"id": "WEALTH_QUINTILE", "type": "string", "role": "dim"},
        {"id": "TIME_PERIOD", "type": "string", "role": "time"},
        {"id": "OBS_VALUE", "type": "string"},
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
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
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
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MULTIPLIER": {"type": "const", "role": "attrib", "value": ""},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "NUTRITION": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
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
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
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
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
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
    "IMMUNISATION": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "const", "role": "dim", "value": ""},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
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
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
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
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
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
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "INDICATOR"},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "RESIDENCE"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
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
        "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "const", "role": "dim", "value": ""},
        "AGE": {"type": "const", "role": "dim", "value": ""},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "SDG4": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "LOCATION"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "EDU_NON_FINANCE": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "REF_AREA"},
        "UNICEF_INDICATOR": {"type": "const", "role": "dim", "value": ""},
        "SEX": {"type": "col", "role": "dim", "value": "SEX"},
        "AGE": {"type": "col", "role": "dim", "value": "AGE"},
        "WEALTH_QUINTILE": {"type": "col", "role": "dim", "value": "WEALTH_QUINTILE"},
        "RESIDENCE": {"type": "col", "role": "dim", "value": "LOCATION"},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "TIME_PERIOD"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "OBS_VALUE"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "UNIT_MEASURE"},
        "OBS_FOOTNOTE": {"type": "const", "role": "attrib", "value": ""},
        "FREQ": {"type": "col", "role": "attrib", "value": "FREQ"},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
        "UNIT_MULTIPLIER": {"type": "col", "role": "attrib", "value": "UNIT_MULT"},
        "OBS_STATUS": {"type": "col", "role": "attrib", "value": "OBS_STATUS"},
    },
    "LEGACY": {
        "REF_AREA": {"type": "col", "role": "dim", "value": "country"},
        "UNICEF_INDICATOR": {"type": "col", "role": "dim", "value": "indicator"},
        "SEX": {"type": "col", "role": "dim", "value": "sex"},
        "AGE": {"type": "col", "role": "dim", "value": "age"},
        "WEALTH_QUINTILE": {"type": "const", "role": "dim", "value": ""},
        "RESIDENCE": {"type": "const", "role": "dim", "value": ""},
        "TIME_PERIOD": {"type": "col", "role": "time", "value": "year"},
        "OBS_VALUE": {"type": "col", "role": "obs", "value": "value"},
        "UNIT_MEASURE": {"type": "col", "role": "attrib", "value": "unit"},
        "OBS_FOOTNOTE": {"type": "col", "role": "attrib", "value": "obs_note"},
        "FREQ": {"type": "const", "role": "attrib", "value": ""},
        "DATA_SOURCE": {"type": "const", "role": "attrib", "value": ""},
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
    "EDU_FINANCE": {"REF_AREA": country_map, "UNIT_MEASURE": {"GDP": "GDP_PERC"},},
    "SDG4": {
        "REF_AREA": country_map,
        "UNIT_MEASURE": {"PER": "PS", "PT": "PCNT", "NB": "NUMBER"},
        "AGE": {"UNDER1_AGE": "M023"},
        "LOCATION": {"RUR": "R", "URB": "U", "_Z": "_T"},
        "WEALTH_QUINTILE": {"_Z": "_T"},
    },
    "EDU_NON_FINANCE": {
        "REF_AREA": country_map,
        "UNIT_MEASURE": {"PER": "PS", "PT": "PCNT", "NB": "NUMBER"},
        "AGE": {"UNDER1_AGE": "M023"},
        "LOCATION": {"RUR": "R", "URB": "U", "_Z": "_T"},
        "WEALTH_QUINTILE": {"_Z": "_T"},
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
        # it is very important to put it last since the previous depend on this column !!!
        "indicator": dict(zip(legacy_meta_data.indicator, legacy_meta_data.code)),
    },
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

# last recall: data dictionary already have (UNICEF_INDICATOR, DATA_SOURCE, OBS_FOOTNOTE)
# last recall: data dictionary don't have information related to LEGACY indicators

dflow_const = {
    "DM": {"WEALTH_QUINTILE": "_T"},
    "CME": {"AGE": "_T", "WEALTH_QUINTILE": "_T", "RESIDENCE": "_T"},
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
}

