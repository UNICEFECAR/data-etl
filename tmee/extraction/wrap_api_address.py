"""
first prototype, this version could change a lot!
To begin, there's only one function:
it retrieves API address depending on the API source to call
"""
import numpy as np

from ..utils import api_request
from ..sdmx.sdmx_struc import SdmxJsonStruct


def wrap_api_address(
    source_key, url_endpoint, indicator_code, country_codes=None, country_map_df=None
):
    """
    :param source_key: identifies the source
    :param url_endpoint: provides api end point from data dictionary
    :param indicator: indicator code
    :param country_code: TMEE countries to filter in the call
    :param country_map_df: dataframe with country names and code mappings (2/3 letters)

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
        num_dims = data_flow_struc.get_sdmx_dims()

        # wrap api_address
        if country_codes:
            # Join string of all TMEE country codes (3 letters) for SDMX requests
            country_call_3 = "+".join(country_codes.values())

            api_address = (
                url_endpoint
                + country_call_3
                + "."
                + indicator_code
                + "." * (num_dims - 2)
            )
        else:
            api_address = url_endpoint + "." + indicator_code + "." * (num_dims - 2)

    # source_key: UIS (no dataflow DSD read, uses url_endpoint directly)
    else:

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
            api_address = url_endpoint

    return api_address

