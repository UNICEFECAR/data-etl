#!/usr/bin/env python
# coding: utf-8

# #### TransMonEE Indicators - API (Helix and UIS) sources populated in Data Dictionary - LEGACY DATA ETL
# In this notebook, I will loop along these indicators for extraction and transformation.
#
# **Numbers (updated with dictionary v8):**
# * Total in WH: 598 indicators
# * Helix sources (70 indicators)
# * UIS sources (149 indicators)
# * WB sources (55 indicators)
# * UN-SDG sources (76 indicators)
# * WHO sources (4 indicators)
# * WHO immunization web (4 indicators)
# * ILO source (1 indicator)
# * UNDP source (1 indicator)
# * CD2030 source (16 indicators)
# * EUROSTAT source (26 indicators)
# * OECD source (20 indicators)
# * Legacy Excel file (323 indicators - 176 in SDMX)

# #### Imports

# In[ ]:


from utils import (
    get_API_code_address_etc,
    api_request,
    get_codelist_API_legacy,
    get_units_codelist,
    data_reader,
    estat_reader,
    oecd_reader,
    undp_reader,
    web_scrape,
    calculate_indicator,
)
from sdmx.sdmx_struc import SdmxJsonStruct
from extraction import legacy
from extraction.wrap_api_address import wrap_api_address
from transformation.destination import Destination
from transformation.dataflow import Dataflow, define_maps
from data_in.legacy_data.prepare_mapping import (
    match_country_name,
    match_indicator_names,
)
import os
import re
import filecmp
import pandas as pd
import numpy as np
import sys
import shutil
from time import time


# #### TransMonEE countries list - Country ISO codes
# ##### Countries list is taken from dataflow TransMonEE in UNICEF Warehouse (requested by Eduard)

# In[ ]:


# start execution time of the ETL
start_time = time()
# open file to write terminal output
py_terminal_file = "etl_terminal.out"
f_t_out = open(py_terminal_file, "w")
sys.stdout = f_t_out


# In[ ]:


# UNICEF’s REST API data endpoint for TransMonEE Dataflow
url_endpoint = (
    "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data/ECARO,TRANSMONEE,1.0/"
)


# In[ ]:


# address and parameters for dataflow structure request
api_address = url_endpoint + "all"
api_params = {"format": "sdmx-json", "detail": "structureOnly"}
# API dataflow structure request
d_flow_struc = api_request(api_address, api_params)


# ##### Country ISO-alpha codes (2, 3 letters) and M49 code (UNSD)

# In[ ]:


# instantiate class SdmxJsonStruct with TMEE dataflow
TmeeSdmxStruc = SdmxJsonStruct(d_flow_struc.json())
# TransMonEE three-letters country codes are taken from its dataflow
country_codes_3 = TmeeSdmxStruc.get_all_country_codes()
# Modified to accomodate country list from ECARO
country_list = [
    "ALB",
    "ARM",
    "AZE",
    "BLR",
    "BIH",
    "BGR",
    "HRV",
    "CZE",
    "EST",
    "GEO",
    "HUN",
    "KAZ",
    "KGZ",
    "LVA",
    "LTU",
    "MNE",
    "MKD",
    "POL",
    "MDA",
    "ROU",
    "RUS",
    "SRB",
    "SVK",
    "SVN",
    "TJK",
    "TUR",
    "TKM",
    "UKR",
    "UZB",
    "AND",
    "AUT",
    "BEL",
    "CYP",
    "DNK",
    "FIN",
    "FRA",
    "DEU",
    "GRC",
    "VAT",
    "ISL",
    "IRL",
    "ITA",
    "LIE",
    "LUX",
    "MLT",
    "MCO",
    "NLD",
    "NOR",
    "PRT",
    "SMR",
    "ESP",
    "SWE",
    "CHE",
    "GBR",
    "XKX",
]
# exclude vatican and kosovo from WHO API (produce empty file)
who_exclude = ["VAT", "XKX"]
# exclude kosovo (iso code error) + vatican (indicator id weird error) from WB reader
wb_exclude = who_exclude

# remane country code dictionary with dummie keys
country_codes_3 = {v: v for v in country_list}


# In[ ]:


# country codes equivalence from excel file in repo root
country_codes_file = "./data_in/all_countrynames_list.xlsx"
country_codes_df = pd.read_excel(country_codes_file)


# In[ ]:


# country M49 codes from UNSD (reference: https://unstats.un.org/unsd/methodology/m49/)
m49_codes_file = "./data_in/UNSD_Methodology.csv"
m49_codes_df = pd.read_csv(m49_codes_file, dtype=str)


# In[ ]:


# add country M49 codes to country_codes_df
country_codes_m49_df = country_codes_df.merge(
    m49_codes_df[["M49 Code", "ISO-alpha3 Code"]].rename(
        columns={"ISO-alpha3 Code": "CountryIso3"}
    ),
    on="CountryIso3",
    how="left",
    sort=False,
)


# In[ ]:


# map TMEE country_codes (three-letters/two-letters equivalence)
country_codes_2 = [
    country_codes_df.CountryIso2[country_codes_df.CountryIso3 == elem].values
    for elem in country_codes_3.values()
]
# country names are repeated in the list, and I want the uniques only
# numpy unique sorts the array, I take an extra step to retrieve the original order
uni_sort, sort_ind = np.unique(np.concatenate(country_codes_2), return_index=True)
country_codes_2 = uni_sort[np.argsort(sort_ind)]


# In[ ]:


# map TMEE contry_codes (three-letters/M49 equivalence)
country_codes_m49 = [
    m49_codes_df["M49 Code"][m49_codes_df["ISO-alpha3 Code"] == elem].values
    for elem in country_codes_3.values()
]


# In[ ]:


# country codes mapping dictionary (two-letters/three-letters)
country_map = {k: v for k, v in zip(country_codes_2, country_codes_3.values())}
# country codes mapping dictionary (M49, removing zeros to the left / ISO three-letters)
country_map_m49 = {
    k: v
    for k, v in zip(
        np.concatenate(country_codes_m49).astype(int).astype(str),
        country_codes_3.values(),
    )
}


# In[ ]:


# write dictionaries in py file to use it during transformations
path_file = "./transformation/country_map.py"
f = open(path_file, "w")
f.write("country_map = " + repr(country_map) + "\n")
f.write("country_map_49 = " + repr(country_map_m49) + "\n")
f.close()


# ##### Country names as defined in Legacy Data
# Required to identify rows with data by the Excel Legacy parser.

# In[ ]:


# list of countries as reported in legacy data
legacy_country_list = [
    "albania",
    "armenia",
    "azerbaijan",
    "belarus",
    "bosnia and herzegovina",
    "bulgaria",
    "croatia",
    "czech republic",
    "estonia",
    "georgia",
    "hungary",
    "kazakhstan",
    "kyrgyzstan",
    "latvia",
    "lithuania",
    "moldova",
    "montenegro",
    "poland",
    "romania",
    "russian federation",
    "serbia",
    "slovakia",
    "slovenia",
    "tajikistan",
    "north macedonia",
    "turkey",
    "turkmenistan",
    "ukraine",
    "uzbekistan",
]


# In[ ]:


# match country names (legacy data) with country names used in TMEE
# build dictionary with country names (legacy data) and country codes in TMEE
# matching not OK with country_codes_3 dummie keys: ECARO country update
legacy_country_codes_3 = {}
for name in legacy_country_list:
    match = match_country_name(name, list(country_codes_3.keys()))
    legacy_country_codes_3[name] = country_codes_3[match]


# In[ ]:


# write legacy_country_codes_3 dictionary in py file to use during transformations
path_file = "./transformation/country_names_map.py"
f = open(path_file, "w")
f.write("country_names_map = " + repr(legacy_country_codes_3) + "\n")
f.close()


# #### Map seasons (Education Legacy Data) to reference year
# Use first year as SDMX `TIME_PERIOD`, retain season as `COVERAGE_TIME` attribute

# In[ ]:


# year range
year_ini = 1950
year_end = 2050
# seasons list
season_str = [
    str(a) + "/" + str(b)[2:]
    for a, b in zip(range(year_ini, year_end + 1), range(year_ini + 1, year_end + 2))
]
# season to year mapping dictionary
seasons_map = {k: str(v) for k, v in zip(season_str, range(year_ini, year_end + 1))}
# write dictionary in py file to use it during transformations
path_file = "./transformation/seasons_map.py"
f = open(path_file, "w")
f.write("seasons_map = " + repr(seasons_map) + "\n")
f.write("season_str = " + repr(season_str) + "\n")
f.close()


# #### Legacy data Extraction
# ##### Source file

# In[ ]:


# path to legacy excel file
source_path_nsi = "./data_in/legacy_data/"
source_file = "TM-2020-EN-December.xlsx"
full_path = source_path_nsi + source_file


# ##### Raw data destination

# In[ ]:


# raw data destination path
raw_path = "./data_out/data_raw/"


# ##### Parse all legacy indicators from Excel `source_file`
# There's one spreadsheet with contents and 6 spreadsheets containing data.
#
# The loop calls `parse_legacy` function for different spreadsheets.
#
# **Dev improvement**: `parse_legacy` could get the number of sheets directly from excel file and loop inside.

# In[ ]:


n_sheets = 6
# Initialize legacy dataframe as None type
legacy_df = None
# legacy data filename to write
legacy_file_write = "legacy_data"

# Skip extraction if legacy already parsed and writen
flag_parsed = os.path.exists(f"{raw_path}{legacy_file_write}.csv")

if flag_parsed:
    print(f"Legacy data already parsed and writen")
else:
    for i in range(1, n_sheets + 1):
        print(f"Parsing Spreadsheet: {i}")
        df = legacy.parse_legacy(full_path, i, legacy_country_list)
        legacy_df = pd.concat([legacy_df, df])

    # write legacy raw data (all indicators) to csv file
    legacy_df.to_csv(f"{raw_path}{legacy_file_write}.csv", index=False)


# **Warning Messages**: Education legacy indicators specify seasons instead of years, e.g: 2005/06
#
# SDMX accepts only a year as time dimension. Seasons are kept in `COVERAGE_TIME` attribute as suggested by *Daniele*.
# ##### Transformation of legacy data into an SDMX structure
# It is performed on `legacy_df` dataframe, and placed in this [**Section**](#Transformation-of-Legacy-Indicators-into-an-SDMX-structure).
#
#
#

# #### TransMonEE UIS API Key

# In[ ]:


uis_key = "9d48382df9ad408ca538352a4186791b"


# #### Read and Query Data Dictionary

# In[ ]:


# path to excel data dictionary in repo
data_dict_file = "./data_in/data_dictionary/indicator_dictionary_TM_v8.xlsx"


# In[ ]:


# get indicators that are extracted by API (code, address and more in pandas dataframe)
api_code_addr_df = get_API_code_address_etc(data_dict_file)


# #### Extract and Transform Indicators from dataframe `api_code_addr_df`

# ##### API or Web Scrape: extraction parameters

# In[ ]:


# parameters: API-SDMX request dataflow from UN-SDG
sdg_api_params = {"startPeriod": str(year_ini), "endPeriod": str(year_end)}
# parameters: API-SDMX request dataflow from Helix
helix_api_params = {**sdg_api_params, "locale": "en"}
# parameters: API-SDMX request dataflow from UIS
uis_api_params = {**helix_api_params, "subscription-key": uis_key}
# parameters: SDMX-JSON API request dataflow from OECD
oecd_api_params = {**sdg_api_params, "dimensionAtObservation": "AllDimensions"}


# In[ ]:


# parameters: API request from WHO (TM countries, all years available)
country_call_who = "COUNTRY:" + ";COUNTRY:".join(
    set(country_codes_3.values()).difference(who_exclude)
)
year_filter_who = ";YEAR:*"
who_api_params = {
    "format": "csv",
    "profile": "verbose",
    "filter": country_call_who + year_filter_who,
}


# In[ ]:


# parameters: web scrape WHO immunization (TM countries)
who_web_params = {
    "ir[c][]": list(country_codes_3.values()),
    "commit": "Ok+with+the+selection",
}


# ##### API Extraction: headers

# In[ ]:


# API headers (compress response)
compress_header = {"Accept-Encoding": "gzip"}
# API headers for SDMX type API sources (desired format)
sdmx_headers = {
    **compress_header,
    "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
}
# API headers for ESTAT-SDMX type API sources (desired format)
estat_headers = {
    **compress_header,
    "Accept": "application/vnd.sdmx.structurespecificdata+xml;version=2.1",
}


# ##### Transformation: map raw data into dataflow TransMonEE in UNICEF Warehouse

# In[ ]:


# transformed data destination path
trans_path = "./data_out/data_transformed/"
# name of dataflow TransMonEE in UNICEF warehouse
dataflow_out = "ECARO:TRANSMONEE(1.0)"


# In[ ]:


# TMEE DSD (data structure definition)
dest_dsd = Destination("TMEE")


# ##### Loop on dataframe `api_code_addr_df`

# In[ ]:


# actual loop (EXTRACT AND TRANSFORM)
for index, row in api_code_addr_df.iterrows():

    # sanity check on first four: strip strings leading and ending spaces
    ext_type = row["Extraction_type"].strip()
    url_endpoint = row["Address"].strip()
    indicator_code = row["Code"].strip()
    indicator_source = row["Data_Source"].strip()
    # line below doesn't handle possible error: "Units" blank entry
    indicator_units = row["Units"].strip()
    indicator_freq = row["Freq_Coll"]
    indicator_u_mult = row["Unit_Mult"]
    # ensure FREQ (years) astype int
    if not np.isnan(indicator_freq):
        indicator_freq = int(indicator_freq)
    # ensure UNIT_MULTIPLIER astype int
    if not np.isnan(indicator_u_mult):
        indicator_u_mult = int(indicator_u_mult)
    # indicator nature (e.g: all observations are estimated)
    indicator_nature = row["Nature"]
    indicator_notes = row["Obs_Footnote"]

    # get source_key from indicator_source
    pattern = "(.*?):"
    source_key = re.findall(pattern, indicator_source)[0].strip()
    # type of extraction response (json, sdmx, etc) from data dictionary
    source_format = row["Content_type"].strip()

    print(f"Dealing with indicator: {indicator_code}")

    # wrap API addresses if Extraction is not Calculation
    api_address = (
        wrap_api_address(
            source_key,
            url_endpoint,
            indicator_code,
            country_codes_3,
            country_codes_m49_df,
        )
        if ext_type.lower() != "calculation"
        else url_endpoint
    )
    # oecd and estat requires api calls to dsd: if failed returns api_address None
    if not api_address:
        print(f"Indicator {indicator_code} extraction skipped")
        continue

    # wrap API parameters & headers
    api_headers = sdmx_headers
    if source_key.lower() == "helix":
        api_params = helix_api_params
    elif source_key.lower() == "uis":
        api_params = uis_api_params
    elif source_key.lower() == "sdg" or source_key.lower() == "ilo":
        # 'sdg' and 'ilo' rest sdmx use same parameters
        api_params = sdg_api_params
    elif source_key.lower() == "who":
        api_params = who_api_params
        api_headers = compress_header
    elif source_format == "web":
        api_params = who_web_params

    # Skip extraction if indicator already downloaded
    flag_download = os.path.exists(f"{raw_path}{indicator_code}.csv")
    # This skip would need extra info to be executed for update purposes!
    # File names could include the year of execution?
    if flag_download:
        print(f"Indicator {indicator_code} skipped (already downloaded)")

    elif ext_type.lower() == "calculation":
        denominator = (
            row["Denominator"].strip()
            if not pd.isnull(row["Denominator"])
            else row["Denominator"]
        )
        transf_params = (
            row["Transf_Param"].strip()
            if not pd.isnull(row["Transf_Param"])
            else row["Transf_Param"]
        )
        flag_download = calculate_indicator(
            url_endpoint,
            transf_params,
            row["Numerator"].strip(),
            denominator,
            {"path": raw_path, "code": indicator_code, "units": indicator_units},
        )

    elif source_format == "pandas data reader":
        # this could be optimized
        country_call_wb = country_codes_3.copy()
        for k in wb_exclude:
            del country_call_wb[k]

        # raw data is extracted as a pandas df directly
        data_raw, data_error = data_reader(
            api_address, country_call_wb, year_ini, year_end
        )
        # if data_reader satisfactory
        if not data_error:
            # write data_raw to raw file
            raw_file = f"{raw_path}{indicator_code}.csv"
            data_raw.to_csv(raw_file, index=False)
            print(f"Indicator {indicator_code} succesfully downloaded")
            flag_download = True

    elif source_format == "web":
        # raw data is web scraped
        # update parameters: indicator code (all "HT_" stripped: 26/Feb/2021)
        ind_dict = {"ir[i][]": indicator_code.strip("HT_")}
        api_params.update(ind_dict)
        # request html page (static extraction)
        indicator_raw = api_request(api_address, api_params)
        # if requests satisfactory
        if indicator_raw.status_code == 200:
            print(f"Indicator {indicator_code} HTML succesfully accessed for scraping")
            # raw data is scraped into pandas df
            data_raw = web_scrape(indicator_raw)
            # write data_raw to raw file
            raw_file = f"{raw_path}{indicator_code}.csv"
            data_raw.to_csv(raw_file, index=False)
            print(f"Indicator {indicator_code} succesfully scraped")
            flag_download = True

    elif source_key.lower() == "estat":
        flag_download = estat_reader(
            api_address, raw_path, indicator_code, sdg_api_params, estat_headers
        )

    elif source_key.lower() == "oecd":
        flag_download = oecd_reader(
            api_address, raw_path, indicator_code, oecd_api_params, compress_header
        )

    elif source_key.lower() == "undp":
        flag_download = undp_reader(
            api_address, raw_path, indicator_code, headers=compress_header
        )

    else:
        # request indicator raw data
        indicator_raw = api_request(api_address, api_params, api_headers)
        # if requests satisfactory
        if indicator_raw.status_code == 200:
            # raw file path
            raw_file = f"{raw_path}{indicator_code}.csv"
            with open(raw_file, "wb") as f:
                f.write(indicator_raw.content)
            print(f"Indicator {indicator_code} succesfully downloaded")
            flag_download = True

    # Transform raw_data if it hasn't occured before
    flag_transform = os.path.exists(f"{trans_path}{indicator_code}.csv")

    if flag_transform:
        print(f"Transformation for {indicator_code} skipped (already done)")
    elif flag_download:
        # build dataframe with indicator raw data
        data_raw = pd.read_csv(f"{raw_path}{indicator_code}.csv", dtype=str)

        # retain only codes from csv headers
        raw_columns = data_raw.columns.values
        rename_dict = {k: v.split(":")[0] for k, v in zip(raw_columns, raw_columns)}
        data_raw.rename(columns=rename_dict, inplace=True)

        # get dataflow from data raw anchor [0,0] if source_format is SDMX
        if source_format.lower() == "sdmx":
            text = data_raw.iloc[0, 0]
            pattern = r":(.+?)\("
            dataflow_key = re.findall(pattern, text)[0]
        else:
            # use source_format as dataflow_key (e.g: WHO api DSD change within indicator calls)
            dataflow_key = source_format

        print(f"Transform indicator: {indicator_code}, from dataflow: {dataflow_key}")

        # instantiate dataflow class with the actual one
        dflow_actual = Dataflow(dataflow_key)

        if dflow_actual.cod_map:
            # map the codes - normalization - works 'inplace'
            dflow_actual.map_codes(data_raw)

        # pre-view transformation duplicates
        if dflow_actual.check_duplicates(data_raw):
            print(f"Indicator {indicator_code} will generate duplicates")
            # MNCH_SAB: only indicator with duplicates as of 26/Feb/2021
            # rebuild dataframe (can't think of a generalization for duplicates)
            # customization 26/Feb/2021 for MNCH_SAB: remove duplication
            data_raw = dflow_actual.rem_dupli_source(data_raw)

        # "metadata" from data dictionary: dataflow constants
        # any of these below won't be used if they are dataflow columns
        # Development NOTE: data dictionary info may be overwriten after
        constants = {
            "INDICATOR": indicator_code,
            "UNIT_MEASURE": indicator_units,
            "OBS_FOOTNOTE": indicator_notes,
            "FREQ": indicator_freq,
            "DATA_SOURCE": indicator_source,
            "OBS_STATUS": indicator_nature,
            "UNIT_MULTIPLIER": indicator_u_mult,
        }

        # map the columns
        data_map = dflow_actual.map_dataframe(data_raw, constants)

        # save transformed indicator info independently (through pandas)
        data_trans = pd.DataFrame(columns=dest_dsd.get_csv_columns(), dtype=str)
        data_trans = data_trans.append(data_map)
        # destination Dataflow: corresponding UNICEF Warehouse DSD name
        data_trans["Dataflow"] = dataflow_out

        # good point to raise analysis on non-numerics (NaN, etc)
        # e.g: drop nan values if present
        data_trans.dropna(subset=["OBS_VALUE"], inplace=True)
        # check non-numeric data in observations
        filter_non_num = pd.to_numeric(data_trans.OBS_VALUE, errors="coerce").isnull()
        # eliminate non-numeric observations if units not BINARY ('YES/NO' must be kept)
        if (filter_non_num.sum() > 0) and (indicator_units != "BINARY"):
            not_num_series = data_trans.OBS_VALUE[filter_non_num]
            if not_num_series.str.contains("<|>").all():
                print(
                    f"Non-numeric observations accepted in {indicator_code}:\n{not_num_series.unique()}"
                )
            else:
                print(
                    f"Non-numeric observations in {indicator_code} discarded:\n{not_num_series.unique()}"
                )
                data_trans.drop(data_trans[filter_non_num].index, inplace=True)

        # check country iso not mapped (trick: eliminate country data from non-queryable source: WHO estimates app)
        filter_non_iso = data_trans.REF_AREA.str.len() > 3
        if filter_non_iso.any():
            print(f"AREA_REF not in ECARO discarded from {indicator_code}")
            data_trans.drop(data_trans[filter_non_iso].index, inplace=True)

        # save file
        data_trans.to_csv(f"{trans_path}{indicator_code}.csv", index=False)


# #### Transformation of Legacy Indicators into an SDMX structure
# For this purpose we need some indicators *metadata* that allows the mappings.
#
# **Dev note**: data dictionary is not leveraged for legacy data so far. *Metadata* is prepared in a separated csv file `content_legacy_codes_v3`, located in `legacy_data` folder (and read by `define_maps.py` in `transformation` folder)

# In[ ]:


# build dataframe with legacy raw data
data_raw = pd.read_csv(f"{raw_path}{legacy_file_write}.csv", dtype=str)

# check indicator names match: legacy metadata file and raw data
not_matching = match_indicator_names(
    define_maps.legacy_meta_data.indicator, data_raw.indicator
)

if not_matching:
    print(f"Correct file '{define_maps.path_legacy}' using mappings: {not_matching}")


# In[ ]:


# Transform raw_data if it hasn't occured before
flag_transform = os.path.exists(f"{trans_path}{legacy_file_write}.csv")

if flag_transform:
    print(f"Transformation for legacy data skipped (already done)")
else:
    # dataflow to process is legacy data
    dataflow_key = "LEGACY"
    # instantiate dataflow class with the actual key (LEGACY)
    dflow_actual = Dataflow(dataflow_key)

    # pre-view duplicates in legacy data
    if dflow_actual.check_duplicates(data_raw):
        print(f"Legacy data contains duplicates")

    # map the codes - normalization - from legacy dataframe
    dflow_actual.map_codes(data_raw)

    # initialize constants empty (no data from dictionary for legacy)
    constants = {}
    # map the columns
    data_map = dflow_actual.map_dataframe(data_raw, constants)

    # save transformed indicator info independently (through pandas)
    data_trans = pd.DataFrame(columns=dest_dsd.get_csv_columns(), dtype=str)
    data_trans = data_trans.append(data_map)
    # destination Dataflow: TMEE DSD in UNICEF Warehouse
    data_trans["Dataflow"] = dataflow_out

    # drop nan values if present
    data_trans.dropna(subset=["OBS_VALUE"], inplace=True)
    # check non-numerics in legacy data observations
    filter_non_num = pd.to_numeric(data_trans.OBS_VALUE, errors="coerce").isnull()
    # eliminate non-numerics
    if filter_non_num.sum() > 0:
        not_num_array = data_trans.OBS_VALUE[filter_non_num].unique()
        print(f"Non-numeric observations discarded in legacy data:\n{not_num_array}")
        data_trans.drop(data_trans[filter_non_num].index, inplace=True)

    # save file
    data_trans.to_csv(f"{trans_path}{legacy_file_write}.csv", index=False)


# #### Data to Upload - Build only one CSV with all data transformed
# Could be done with Linux command `sed` for faster performance.

# In[ ]:


# all csv files with data transformed (that are not TMEE out file)
files_trans = [
    file
    for file in os.listdir(trans_path)
    if (file.endswith(".csv") and file.find("TMEE") < 0)
]

# pandas concat
dest_dsd_df = pd.concat(
    [pd.read_csv(f"{trans_path}{f}", dtype=str) for f in files_trans]
)


# In[ ]:


# save file if not present to avoid re-writing
etl_out_file = "TMEE_ETL_out"

if f"{etl_out_file}.csv" not in [file for file in os.listdir(trans_path)]:
    dest_dsd_df.to_csv(f"{trans_path}{etl_out_file}.csv", index=False)
else:
    print(f"{etl_out_file} file not re-written, please first delete it to update.")


# #### File output ETL
# csv lines counted for the first SDMX upload: header + 235927
#
# csv lines counted for the second SDMX upload: header + 241548
#
# csv lines counted for the third SDMX upload: header + 285646
#
# csv lines counted for the fourth SDMX upload: header + 948460 (632664: UNPD indicator only; web scrape excluded)
#
# csv lines counted for the fourth SDMX upload: header + 1555490 (web scrape + sitan feb 21 included)
#
# last round june 21 not updated --> new idea --> commit TMEE_out.csv to repo? (consult James: file size for github)

# #### Work on output for Daniele codelist of indicators

# In[ ]:


# codelists destination path
cl_path = "./data_out/codelists/"


# In[ ]:


# path to meta data used for legacy indicators transformation
path_legacy = define_maps.path_legacy
# data dictionary + legacy meta data: indicator codelists pre-print (some manual input for legacy still required)
ind_codes_file = "CL_TMEE_INDICATORS_pre_print"
pre_cl_indicators_df = get_codelist_API_legacy(api_code_addr_df, path_legacy)
pre_cl_indicators_df.to_csv(f"{cl_path}{ind_codes_file}.csv", index=False)


# In[ ]:


# after some manual input (legacy indicator names), indicators codelist file renamed
ind_codes_final_file = "CL_TMEE_INDICATORS"
if f"{ind_codes_final_file}.csv" in [file for file in os.listdir(cl_path)]:
    cl_indicators_df = pd.read_csv(f"{cl_path}{ind_codes_final_file}.csv", dtype=str)
    # check there aren't empty entries in code and name
    is_empty_code = cl_indicators_df.Code.isnull().any()
    is_empty_name = cl_indicators_df.Indicator_name.isnull().any()
    if len(cl_indicators_df) < len(pre_cl_indicators_df):
        print(f"Check missing indicators in {ind_codes_final_file}.csv file.")
    elif is_empty_code or is_empty_name:
        print(f"Check empty codes or names in {ind_codes_final_file}.csv file.")
else:
    print(f"Please produce indicators codelist {ind_codes_final_file} file.")


# In[ ]:


# produce units codelist pre-print from etl_out_file
units_codes_file = "CL_TMEE_UNITS_pre_print"
cl_units_df, empty_flag = get_units_codelist(f"{trans_path}{etl_out_file}.csv")
if not empty_flag:
    cl_units_df.to_csv(f"{cl_path}{units_codes_file}.csv", index=False)
else:
    print(
        f"{units_codes_file} not written given observations without unit code specified."
    )


# In[ ]:


# spot-checks on codes (sex, residence, wealth, etc)
codes_in_sex = dest_dsd_df["SEX"].unique()
print(f"Codes for SEX: {codes_in_sex}")
codes_in_res = dest_dsd_df["RESIDENCE"].unique()
print(f"Codes for Residence: {codes_in_res}")
codes_in_wealth = dest_dsd_df["WEALTH_QUINTILE"].unique()
print(f"Codes for Wealth: {codes_in_wealth}")
print(f"Codes for Freq: {dest_dsd_df['FREQ'].unique()}")
print(f"Codes for Unit Mult: {dest_dsd_df['UNIT_MULTIPLIER'].unique()}")
print(f"Codes for Obs Status: {dest_dsd_df['OBS_STATUS'].unique()}")
# dimensions can't be empty for SDMX --> check sex, res and wealth
if pd.isnull(np.concatenate((codes_in_sex, codes_in_res, codes_in_wealth))).sum() > 0:
    print("Check above for empty codes in sex, residence or wealth quintile")


# In[ ]:


# codelist age groups is checked w.r.t CL_AGE in SDMX warehouse (too many to spot-check only)
# UNICEF’s REST API endpoint for codelists
url_endpoint = "https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/codelist/"
codelist = "UNICEF/CL_AGE"
# address and parameters for codelist request
api_address = url_endpoint + codelist
api_params = {"format": "sdmx-json"}
# API codelist request
age_cl_json = api_request(api_address, api_params).json()
# Age Codelist from SDMX-JSON
age_cl_dict = {
    elem["id"]: elem["name"] for elem in age_cl_json["data"]["codelists"][0]["codes"]
}
# Check if any age group in ETL output is EMPTY or NOT in age_cl_dict
if dest_dsd_df["AGE"].isnull().sum() > 0:
    print("Check empty age codes")
else:
    age_check_list = np.setdiff1d(dest_dsd_df["AGE"], list(age_cl_dict.keys()))
    if len(age_check_list) > 0:
        print(f"Array of code/s not in {codelist}:\n{age_check_list}")
    else:
        print(f"Age-groups codelist {codelist} checked.")


# In[ ]:


# age_groups codelist pre-print
age_groups_file = "CL_TMEE_AGE_pre_print"
# make dataframe column with unique codes in etl_out_file
cl_age_df = pd.DataFrame(dest_dsd_df["AGE"].unique(), columns=["code"])
# make list with description from UNICEF CL_AGE
cl_age_label = [
    age_cl_dict[code] if code in age_cl_dict else np.nan for code in cl_age_df.code
]
cl_age_df["label"] = cl_age_label
cl_age_df.to_csv(f"{cl_path}{age_groups_file}.csv", index=False)


# In[ ]:


# report on data raw differences among ETL rounds
files_raw = [file for file in os.listdir(raw_path) if file.endswith(".csv")]
raw_path_prev = "./data_raw/b_up_jun_10/"
match, mismatch, errors = filecmp.cmpfiles(
    raw_path, raw_path_prev, files_raw, shallow=False
)
print("Deep comparison")
print("Mismatch:", mismatch)
print("Errors:", errors)


# In[ ]:


# finish execution time of ETL
total_sec = time() - start_time
print(f"{round(total_sec/60,2)} minutes execution")
# close and move terminal output file
f_t_out.close()
shutil.move(py_terminal_file, f"{trans_path}{py_terminal_file}")

