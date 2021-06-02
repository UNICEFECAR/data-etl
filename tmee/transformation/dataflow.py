from . import define_maps
import pandas as pd
import numpy as np


class Dataflow:
    """
    dataflows: similar to data structure definitions (DSD)
    dataflows: are the different DSD from where we extract indicators raw data
    column mapping: relation between columns in the dataflow to the destination DSD
    code mapping: normalization of the dataflow entries to match destination DSD
    """

    # column and code mappings: known before hand and stored in define_maps
    def __init__(self, key):
        self.col_map = define_maps.dflow_col_map[key]
        self.cod_map = None
        if key in define_maps.code_mapping:
            self.cod_map = define_maps.code_mapping[key]
        self.const = None
        if key in define_maps.dflow_const:
            self.const = define_maps.dflow_const[key]

    def map_df_row(self, row, constants):
        """
        Maps a row
        :param row: the row to map
        :param constants: the constants
        :return: A mapped row (in variable type dictionary)
        """

        ret = {}
        for c in self.col_map:
            if self.col_map[c]["type"] == "const" and c in constants:
                ret[c] = constants[c]
            elif self.col_map[c]["type"] == "col":
                ret[c] = row[self.col_map[c]["value"]]
        return ret

    def map_dataframe(self, dataframe, constants):
        """
        Maps the columns starting from a dataframe
        :param dataframe: The dataframe to map
        :param constants: the constants retrieved from data dictionary
        Development Note: (all constants must come from data dictionary)
        Testing fase Note: I'm retrieving some constants now from define_maps file
        :return: The mapped columns (a list of dictionaries)
        """

        # testing fase: retrieve some constants from define_maps file
        # Here there could be a "big" discussion:
        # either entering constants at the dataflow level or at the indicator level
        # at the indicator level there could be lot's of redundance (not nice from the data entry point)
        # at the dataflow level, there could a generalization that doesn't apply at some indicators case
        if self.const:
            constants.update(self.const)

        ret = []

        # Development Note: would we prefer to operate on the whole dataframe rather than by rows?
        # advantage: faster
        # disadvantage: rethink the flow of method map_df_row
        for r in range(len(dataframe)):
            ret.append(self.map_df_row(dataframe.iloc[r], constants))

        return ret

    # code mapping (normalization of the content)
    # operates directly to dataframe
    # check the above! (Now return statement is left empty)
    def map_codes(self, dataframe):

        deps = "depends"
        # multi index dependence (two-index for UIS BDSS in Helix: not tested)
        bi_deps = "bi_depends"
        maps = "map"

        for col in self.cod_map:

            if deps in self.cod_map[col]:
                # the mapping depends on a source column entry (legacy data transformations)
                for m in self.cod_map[col][maps]:
                    # get indexes where source column entries are == m
                    source_col = self.cod_map[col][deps]
                    indexes = dataframe[source_col] == m
                    # apply mapping on that indexes only
                    # mapping of observation values is an exception
                    if col == "value":
                        # ensure integers where corresponds
                        # see code_mapping['LEGACY'] in define_maps.py
                        # astype: int - str (avoid posterior float cast)
                        dataframe[col][indexes] = (
                            pd.to_numeric(dataframe[col][indexes])
                            .round()
                            .astype(int)
                            .astype(str)
                        )
                    else:
                        dataframe[col][indexes] = self.cod_map[col][maps][m]

            elif bi_deps in self.cod_map[col]:
                # the mapping depends on entries of two columns (BDSS UIS in Helix)
                for m, n in self.cod_map[col][maps]:
                    # get indexes where entries in two columns are m and n
                    source_col1 = self.cod_map[col][bi_deps][0]
                    source_col2 = self.cod_map[col][bi_deps][1]
                    indexes = dataframe[source_col1] == m & dataframe[source_col2] == n
                    # apply mapping on indexes only
                    dataframe[col][indexes] = self.cod_map[col][maps][m, n]

            else:
                # simpler case: apply the mapping straightforward to a column
                for m in self.cod_map[col]:
                    # any column entry not NaN: split "code:description" for strings only
                    if m == "code:description" and dataframe[col].notnull().any():
                        log_ind = dataframe[col].notnull()
                        dataframe.loc[log_ind, col] = dataframe.loc[log_ind, col].apply(
                            lambda x: x.split(":")[0]
                        )
                    else:
                        dataframe[col].replace(m, self.cod_map[col][m], inplace=True)

        return

    # logic borrowed from Daniele (it is used for check on duplicates)
    # duplicates are checked in SDMX only on columns that are dimensions, right?
    def get_dim_cols(self):
        """
        Gets the list of columns marked as Dimensions in the Dataflow (note that time is a Dimension)
        Uses the column mapper for a dataflow (dictionary type)
        :return: A list with dataflow names of dimensions type column
        """
        cols = []
        for c in self.col_map:
            if self.col_map[c]["type"] == "col":
                if (
                    self.col_map[c]["role"] == "dim"
                    or self.col_map[c]["role"] == "time"
                ):
                    cols.append(self.col_map[c]["value"])
        return cols

    # function to test on duplicates (proposed by Daniele)
    # preview duplicates in destination DSD (not conforming disaggregation)
    def check_duplicates(self, dataframe):
        """
        :param dataframe: dataframe to check duplicates (dataflow must correspond!)
        :return: boolean (duplicates YES/NO)
        """
        dim_cols = self.get_dim_cols()
        return dataframe.duplicated(subset=dim_cols, keep=False).any()

    # function to remove duplicates from data source
    # customized per indicator: can't think of a generalization so far
    # it requires to know the dimension (can't be more that one) that brings the duplicities
    # works on new column of MNCH dataflow --> data source priority
    # the idea is clear: use data source priority and first occurrence for cases not solved by data source priority
    def rem_dupli_source(
        self, dataframe, target_source="DATA_SOURCE_PRIORITY",
    ):
        """
        :param dataframe: dataframe to remove duplicates (pre-requisite: already know there are!)
        :param target_dim: uses a targeted column to remove duplicates (pre-requisite: know the column)
        :return non_dupli_df: dataframe without duplicates
        """
        dim_cols = self.get_dim_cols()
        logic_dupli = dataframe.duplicated(subset=dim_cols, keep=False)
        # initialize output dataframe: first keep entries without any duplicates
        non_dupli_df = dataframe[~logic_dupli]
        # all duplicates df
        dupli_df = dataframe[logic_dupli]

        # use target column to prioritize from duplicates
        target_prior = dupli_df[target_source] == "1"
        # add source to output
        non_dupli_df = pd.concat([non_dupli_df, dupli_df[target_prior]])

        # ideally target_source should be specified for all multiple indexes available
        # Note to Daniele this doesn't happend
        if target_prior.sum() != len(dupli_df.groupby(dim_cols).size()):
            # eliminate entries already prioritized using country-year combination
            countries_prior = dupli_df[target_prior].REF_AREA
            years_prior = dupli_df[target_prior].TIME_PERIOD
            country_year_prior = np.logical_or.reduce(
                [
                    (dupli_df.REF_AREA == country) & (dupli_df.TIME_PERIOD == year)
                    for country, year in zip(countries_prior, years_prior)
                ]
            )
            # update remaining from duplicates
            dupli_df = dupli_df[~country_year_prior]
            # retain first occurrence among duplicates only
            logic_dupli = dupli_df.duplicated(subset=dim_cols)
            non_dupli_2add_df = dupli_df[~logic_dupli]
            # add source to output
            non_dupli_df = pd.concat([non_dupli_df, non_dupli_2add_df])

        # check-out duplicates before return!
        if self.check_duplicates(non_dupli_df):
            print("Not enough target sources to remove all duplicates!")
        else:
            print("Duplicates eliminated.")

        return non_dupli_df

