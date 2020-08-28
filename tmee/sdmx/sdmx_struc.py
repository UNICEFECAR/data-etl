
# functions extracting basic information about an sdmx structure from a sdmx-json file
# could be a good candidate to create a class sdmx_struct_json and these functions as methods


def get_sdmx_dim(sdmx_json_struc):
    """ get number of dimensions from sdmx_json_struc file """
    n_dim = len(sdmx_json_struc['structure']['dimensions']['observation'])
    return n_dim


def get_all_country_codes(sdmx_json_struc):
    """ Parse sdmx_json_struc file and get all country codes in a sdmx dataflow
    :param sdmx_json_struc: sdmx JSON file with dataflow structure
    :return: dictionary, (keys) all country names,(values) all country codes from sdmx dataflow
    """

    country_code = {}
    for elem in sdmx_json_struc['structure']['dimensions']['observation'][0]['values']:
        country_code[elem['name']] = elem['id']
    
    # return dictionary sorted by keys
    return {k:v for k,v in sorted(country_code.items())}


from difflib import get_close_matches

def match_country_list(country_list,sdmx_json_struc):
    """ Parse sdmx_json_struc file to match a country list to all country codes in sdmx dataflow
    :param country_list: country names list
    :param sdmx_json_struc: sdmx JSON file with dataflow structure
    :return: dictionary, (keys) country names,(values) country codes that matches country list
    """

    country_code = {}
    country_discard = {}

    for elem in sdmx_json_struc['structure']['dimensions']['observation'][0]['values']:
        if elem['name'].lower() in country_list:
            country_code[elem['name'].lower()] = elem['id']
        else:
            country_discard[elem['name'].lower()] = elem['id']

    # check countries not found
    not_found = [k for k in country_list if k not in country_code]
    # uses previous knowledge to avoid comparing string 'republic' (function can be generalized)
    for elem in not_found:
        matched_country = get_close_matches(elem[:5], country_discard,1)[0]
        # add the matched_country in dictionary (note: key retained from country_list)
        country_code[elem] = country_discard[matched_country]

    # return dictionary ordered with country list
    ordered_dict = {k:country_code[k] for k in country_list}
    return ordered_dict
