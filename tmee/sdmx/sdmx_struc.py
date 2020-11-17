# functions extracting basic information about an sdmx structure from a sdmx-json file
# could be a good candidate to create a class sdmx_struct_json and these functions as methods
from difflib import get_close_matches


class SdmxJsonStruct:

    sdmx_json = None

    def __init__(self, sdmx_json_struc: dict):
        self.sdmx_json = sdmx_json_struc

    def get_sdmx_dims(self):
        """ 
        get (n_dim) number of dimensions from sdmx_json_struc file
        searches for totals in sdmx dims not in: {SEX, AGE, RESIDENCE, WEALTH_QUINTILE}
        duplicates when conforming to TMEE dataflow are prevented from the API call
        Dev Note: it is a nice approach but must be tested (e.g: the "_Z" case)
        :return dim_total: dictionary with key position 
        """

        # this is the list of dimension dictionaries
        dim_list = self.sdmx_json["structure"]["dimensions"]["observation"]
        # number of dimensions in sdmx structure
        n_dim = len(dim_list)
        # dimensions id
        dim_id = [dim["id"] for dim in dim_list]
        # TMEE dimensions list
        tmee_dim_list = ["SEX", "AGE", "RESIDENCE", "WEALTH_QUINTILE"]
        # dimensions not in tmee_dim_list
        dim_not_tmee = []
        for dim in dim_id:
            if not get_close_matches(dim, tmee_dim_list, 1):
                dim_not_tmee.append(dim)

        not_tmee_with_t = []
        # retrieve din_not_tmee key position that contains totals "_T"
        for dim in dim_not_tmee:
            dim_ind = dim_id.index(dim)
            # access dimension dictionary
            dim_dict = dim_list[dim_ind]
            # search for totals in values
            for val in dim_dict["values"]:
                if "_T" == val["id"].strip():
                    not_tmee_with_t.append(dim_ind + 1)

        return n_dim, not_tmee_with_t

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
