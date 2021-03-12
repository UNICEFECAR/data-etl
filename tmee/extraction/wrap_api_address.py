"""
first prototype, this version could change a lot!
To begin, there's only one function:
it retrieves API address depending on the API source to call
Dev Note: original idea comes from trying to specify only url endpoint in data dictionary
To modify: James insight about data dictionary, address should be complete not only API endpoint
To implement: print the final url used by requests into data dictionary --> allows users retrieve raw data
To improve: wrap_api_address could become an abstract class (similar to James' extraction refactoring)
"""

import numpy as np
import re
import pandas as pd

from utils import api_request
from sdmx.sdmx_struc import SdmxJsonStruct


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
    if source_key.lower() == "helix":

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

    # rest of the source_keys: just keep url_endpoint
    else:
        api_address = url_endpoint

    return api_address
