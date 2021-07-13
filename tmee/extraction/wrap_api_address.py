"""
first prototype, this version could change a lot!
To begin, there's only one function:
it retrieves API address depending on the API source to call
Dev Note: original idea comes from trying to specify only url endpoint in data dictionary
To modify: James insight about data dictionary, address should be complete not only API endpoint
To implement: print the final url used by requests into data dictionary --> allows users retrieve raw data
To improve: wrap_api_address could become an abstract class (similar to James' extraction refactoring)
To improve note: code repetition is very high therefore there would be a clever way to wrap it all
"""

import numpy as np
import re
import pandas as pd
import requests
import sys

from utils import api_request
from sdmx.sdmx_struc import SdmxJsonStruct
import pandasdmx as pdsdmx
import xmltodict


def wrap_api_address(
    source_key, url_endpoint, indicator_code, country_codes=None, country_map_df=None
):
    """
    :param source_key: identifies the source
    :param url_endpoint: provides api end point from data dictionary
    :param indicator: indicator code
    :param country_code: TMEE countries to filter in the call
    :param country_map_df: dataframe with country names and code mappings (2/3 letters and M49)

    TODO: Make use of .format method or F strings to format a string mask of what the call should look like
    """

    # separate how API addresses are built:
    # source_key: helix (reads dataflows DSD)
    # brings disaggregation in sex, age, residence and wealth if present
    # sometimes would like to keep the query as it is --> flag is query!
    # CDDEM could be brought under this case!
    if source_key.lower() == "helix":

        # split url_endpoint
        url_split = url_endpoint.split("/")

        # flag if query!
        if len(url_split) > 9:

            # use query and add countries
            if country_codes:
                # Join string of all TMEE country codes (3 letters) for SDMX requests
                country_call_3 = "+".join(country_codes.values())

                # query split
                query_split = url_split[9].split(".")

                # place country call at first dimension
                query_split[0] = country_call_3
                # rebuild query with country call
                query_with_geo = ".".join(query_split)

                # rebuild api_adress using query_with_geo
                api_address = "/".join(url_split[:-1]) + "/" + query_with_geo
            else:
                api_address = url_endpoint

        else:

            # first get dataflow number of dimensions
            dsd_api_address = url_endpoint + "all"
            # parameters: API request dataflow structure only
            dsd_api_params = {"format": "sdmx-json", "detail": "structureOnly"}
            data_flow_struc = SdmxJsonStruct(
                api_request(dsd_api_address, dsd_api_params).json()
            )
            # search num_dims and totals "_T" (not in TMEE destination dimensions)
            num_dims, dim_t = data_flow_struc.get_sdmx_dims()

            # build dimension points for sdmx API call
            # (num_dims - 3): 3 dimensions not considered (REF_AREA, INDICATOR, TIME)
            dim_points = "." * (num_dims - 3)
            # position '_T' in dim_points according to dim_t
            for i, t_pos in enumerate(dim_t, 1):
                t_ind = t_pos - 2 + (i - 1) * 2
                dim_points = dim_points[:t_ind] + "_T" + dim_points[t_ind:]

            # wrap api_address
            if country_codes:
                # Join string of all TMEE country codes (3 letters) for SDMX requests
                country_call_3 = "+".join(country_codes.values())

                api_address = (
                    url_endpoint + country_call_3 + "." + indicator_code + dim_points
                )
            else:
                api_address = url_endpoint + "." + indicator_code + dim_points

    # source_key: UIS (no dataflow DSD read, uses url_endpoint directly)
    elif source_key.lower() == "uis":

        # wrap api_address
        if country_codes:
            # already know UIS has two-letter country codes (first map)
            country_codes_2 = [
                country_map_df.CountryIso2[country_map_df.CountryIso3 == elem].values
                for elem in country_codes.values()
            ]
            # country names are repeated in the list, and I want the unique codes only
            # Note: numpy unique sorts the array, but this doesn't modify the API call
            country_codes_2 = np.unique(np.concatenate(country_codes_2))
            # Join string of all TMEE country codes (2 letters) for SDMX requests
            country_call_2 = "+".join(country_codes_2)

            api_address = url_endpoint + country_call_2
        else:
            # just keep url_endpoint (no country_codes)
            api_address = url_endpoint

    # source_key: SDG (no dataflow DSD read, uses url_endpoint directly)
    elif source_key.lower() == "sdg":

        # wrap api_address
        if country_codes:
            # already know SDG has m49 country codes (first map)
            country_codes_m49 = [
                country_map_df["M49 Code"][country_map_df.CountryIso3 == elem].values
                for elem in country_codes.values()
            ]
            # country names are repeated in the list, and I want the unique codes only
            # eliminate M49 missmatch countries (kosovo eg): unique not nan's
            all_countries = np.concatenate(country_codes_m49)
            # pandas not null supports mix of string with floats (numpy doesn't)
            null_match = pd.isnull(all_countries)
            # Note: numpy unique sorts the array, but this doesn't modify the API call
            country_codes_m49 = np.unique(all_countries[~null_match])
            # Join country_codes_m49 for SDMX requests (removing zeros to the left)
            country_call_m49 = "+".join(country_codes_m49.astype(int).astype(str))
            # split url_endpoint
            url_split = url_endpoint.split("/")
            # get the sdmx dimensions call from url_split
            sdmx_dim_call = url_split[-1]
            # split the sdmx dimensions call
            sdmx_dims_list = sdmx_dim_call.split(".")
            # place country call in SDG REF_AREA (4th dimension)
            sdmx_dims_list[3] = country_call_m49

            # build api_address with country_call
            api_address = "/".join(url_split[:-1]) + "/" + ".".join(sdmx_dims_list)
        else:
            # just keep url_endpoint (no country_codes)
            api_address = url_endpoint

    # source_key: ILO - different dataflows per indicator, unusual, why not to leverage SDMX DSDs???
    elif source_key.lower() == "ilo":

        # split url_endpoint
        url_split = url_endpoint.split("/")
        # get position of rest
        rest_position = url_split.index("rest") + 1
        # split dataflow reference (rest_position + 1) with commas and re-join with slash
        flow_ref = "/".join(url_split[rest_position + 1].split(","))

        # build base url from url_endpoint
        url_base = "/".join(url_split[:rest_position])
        # build api_address for datastructure call
        dsd_api_address = f"{url_base}/datastructure/{flow_ref}"
        # API headers: valid for structural metadata queries only
        api_headers = {
            "Accept": "application/vnd.sdmx.structure+json;version=1.0",
            "Accept-Encoding": "gzip",
        }
        # do the datastructure call
        data_flow_struc = SdmxJsonStruct(
            api_request(dsd_api_address, headers=api_headers).json()
        )

        # search num_dims and dim_num_dict (dimension key positions)
        num_dims, dim_num_dict = data_flow_struc.get_ilo_dims()

        # build dimensions key for sdmx API data query
        dim_key = ["" for i in range(num_dims)]

        # fill dimensions key for data query (only country call implemented so far)
        if country_codes:
            # Join string of all TMEE country codes (3 letters) for SDMX requests
            country_call_3 = "+".join(country_codes.values())
            dim_key[dim_num_dict["REF_AREA"]] = country_call_3

        api_address = url_endpoint + ".".join(dim_key)

    # source_key: ESTAT - different dataflows per indicator groups by Eurostat
    elif source_key.lower() == "estat":

        # wrap api_address (similar to UIS iso2 with two particularities)
        if country_codes:
            # first map two-letter country codes
            country_codes_2 = [
                country_map_df.CountryIso2[country_map_df.CountryIso3 == elem].values
                for elem in country_codes.values()
            ]
            # country names are repeated in the list, and I want the unique codes only
            # Note: numpy unique sorts the array, but this doesn't modify the API call
            country_codes_2 = np.unique(np.concatenate(country_codes_2))

            # replace iso2 eurostat particulars
            estat_only = {"GR": "EL", "GB": "UK"}
            country_call = [estat_only.get(i, i) for i in country_codes_2]

            # EUROSTAT datastructure --> query countries reported only (if not breaks)
            # pandasdmx with Cache HTTP response: instantiate a Request object with data provider ESTAT
            Estat = pdsdmx.Request(source_key.upper(), backend="memory")

            # split url_endpoint
            url_split = url_endpoint.split("/")
            # get position of dataflow
            dflow_position = url_split.index("data") + 1
            dflow_name = url_split[dflow_position]

            # dsd name
            dsd_name = f"DSD_{dflow_name}"
            # url endpoint for estat datastructure calls
            # estat_url_dsd = (
            # "https://ec.europa.eu/eurostat/SDMX/diss-web/rest/datastructure/ESTAT/"
            # )

            # handle errors from pdsdmx dsd request
            try:
                print(
                    f"Querying EUROSTAT metadata: please wait, long response times are reported"
                )
                # until I could pass timeout into pdsdmx I do an intermediate step
                # check_Estat = api_request(f"{estat_url_dsd}{dsd_name}", timeout=15)
                # if not hasattr(check_Estat, "status_code"):
                # return None
                Dsd_estat = Estat.datastructure(dsd_name)
                # If the response was successful, no Exception will be raised
                Dsd_estat.response.raise_for_status()
            except requests.exceptions.HTTPError as http_err:
                print(f"HTTP error occurred: {http_err}")
                return None
            except Exception as error:
                print(f"Other error occurred: {error}")
                return None

            # filter estat/dsd/geo in ecaro
            geo_query = [
                country
                for country in pdsdmx.to_pandas(Dsd_estat.codelist["CL_GEO"]).index
                if country in country_call
            ]

            api_address = url_endpoint + "+".join(geo_query)
        else:
            # just keep url_endpoint (no country_codes)
            api_address = url_endpoint

    # source_key: CDDEM - from UNICEF web services
    elif source_key.lower() == "cddem":

        # wrap api_address (iso3)
        if country_codes:
            # Join string of all TMEE country codes (3 letters) for SDMX requests
            country_call_3 = "+".join(country_codes.values())

            # split url_endpoint
            url_split = url_endpoint.split("/")
            # get position of query to split
            query_pos = url_split.index("data") + 2
            query_split = url_split[query_pos].split(".")

            # place country call at dimension 3
            query_split[2] = country_call_3
            # rebuild query with country call
            query_with_geo = ".".join(query_split)

            # rebuild api_adress using query_with_geo
            api_address = "/".join(url_split[:-1]) + "/" + query_with_geo
        else:
            # just keep url_endpoint (no country_codes)
            api_address = url_endpoint

    # source_key: OECD - different dataflows per indicator groups
    elif source_key.lower() == "oecd":

        # wrap api_address (iso3 but not all ecaro)
        if country_codes:
            # get dataflow from url
            url_split = url_endpoint.split("/")
            # get dataflow position
            dflow_position = url_split.index("data") + 1
            dflow_name = url_split[dflow_position]

            # build api_address for datastructure call (hardcoded here as uses SDMX-ML service)
            url_base = "http://stats.oecd.org/restsdmx/sdmx.ashx"
            dsd_api_address = f"{url_base}/GetDataStructure/{dflow_name}"

            # do the datastructure call and parse xmltodict
            dsd_struc = xmltodict.parse(api_request(dsd_api_address).text)

            # get position of country codes in codelists
            dsd_codelists = dsd_struc["message:Structure"]["message:CodeLists"][
                "CodeList"
            ]
            cou_pos = [
                k for k, item in enumerate(dsd_codelists) if "_COU" in item["@id"]
            ]

            # cou_pos: one element list or substring matching has failed
            cou_pos = cou_pos[0] if len(cou_pos) == 1 else None

            # if exit: '_COU' in item['@id'] is not exclusive and must be revised
            if cou_pos is None:
                sys.exit(f"Identify COU Codelist for indicator {indicator_code} - OECD")
            else:
                # filter oecd/dsd/geo in ecaro
                cou_query = [
                    country
                    for country in [
                        item["@value"] for item in dsd_codelists[cou_pos]["Code"]
                    ]
                    if country in country_codes.values()
                ]
                query_split = url_split[dflow_position + 1].split(".")
                # place cou_query at cou_pos (assumes codelist position matches dimensions)
                query_split[cou_pos] = "+".join(cou_query)
                # rebuild api_adress using query_with_geo
                api_address = "/".join(url_split[:-1]) + "/" + ".".join(query_split)

        else:
            # just keep url_endpoint (no country_codes)
            api_address = url_endpoint

    elif source_key.lower() == "undp":

        # wrap api_address (iso3 let's see if all ecaro)
        if country_codes:
            # Join string of all TMEE country codes (3 letters) for SDMX requests
            country_call_3 = ",".join(country_codes.values())

            # rebuild api_adress using query_with_geo
            api_address = url_endpoint + "country_code=" + country_call_3
        else:
            # just keep url_endpoint (no country_codes)
            api_address = url_endpoint

    # rest of the source_keys: just keep url_endpoint
    else:
        api_address = url_endpoint

    return api_address
