# functions extracting basic information about an sdmx structure from a sdmx-json file
# could be a good candidate to create a class sdmx_struct_json and these functions as methods
from difflib import get_close_matches


class SdmxJsonStruct:

    sdmx_json = None

    def __init__(self, sdmx_json_struc: dict):
        self.sdmx_json = sdmx_json_struc

    def get_sdmx_dims(self):
        """ 
        get number of dimensions from sdmx_json_struc file 
        """
        n_dim = len(self.sdmx_json["structure"]["dimensions"]["observation"])
        return n_dim

    def get_all_country_codes(self):
        """
        Parse sdmx_json_struc file and get all country codes in a sdmx dataflow
        
        :return: dictionary, (keys) all country names,(values) all country codes from sdmx dataflow
        """

        country_code = {}
        for elem in self.sdmx_json["structure"]["dimensions"]["observation"][0][
            "values"
        ]:
            country_code[elem["name"]] = elem["id"]

        # return dictionary sorted by keys
        return {k: v for k, v in sorted(country_code.items())}

    def match_country_list(self, country_list):
        """ 
        Parse sdmx_json_struc file to match a country list to all country codes in sdmx dataflow
        
        :param country_list: country names list
        :return: dictionary, (keys) country names,(values) country codes that matches country list
        """

        country_code = {}
        country_discard = {}

        for elem in self.sdmx_json["structure"]["dimensions"]["observation"][0][
            "values"
        ]:
            if elem["name"].lower() in country_list:
                country_code[elem["name"].lower()] = elem["id"]
            else:
                country_discard[elem["name"].lower()] = elem["id"]

        # check countries not found
        not_found = [k for k in country_list if k not in country_code]
        # uses previous knowledge to avoid comparing string 'republic' (function can be generalized)
        for elem in not_found:
            matched_country = get_close_matches(elem[:5], country_discard, 1)[0]
            # add the matched_country in dictionary (note: key retained from country_list)
            country_code[elem] = country_discard[matched_country]

        # return dictionary ordered with country list
        ordered_dict = {k: country_code[k] for k in country_list}
        return ordered_dict

    def get_legacy_indicators(self):
        """
        Parse sdmx_json_struc and retrieve indicators with parent 'SP' or 'PT'
        
        :return: dictionary, (keys) indicators code,(values) indicators name
        """

        legacy_indicators = {}
        for elem in self.sdmx_json["structure"]["dimensions"]["observation"][1][
            "values"
        ]:
            if elem["inDataset"]:
                if (elem["parent"] == "PT") | (elem["parent"] == "SP"):
                    legacy_indicators[elem["id"]] = elem["name"]

        # return dictionary
        return legacy_indicators
