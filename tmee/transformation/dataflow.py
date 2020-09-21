from transformation import define_maps


class dataflow:
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
        maps = "map"

        for col in self.cod_map:

            if deps in self.cod_map[col]:
                # the mapping depends on a source column entry (legacy data transformations)
                for m in self.cod_map[col][maps]:
                    # get indexes where source column entries are == m
                    source_col = self.cod_map[col][deps]
                    indexes = dataframe[source_col] == m
                    # apply mapping on that indexes only
                    dataframe[col][indexes] = self.cod_map[col][maps][m]

            else:
                # simpler case: apply the mapping straightforward to a column
                for m in self.cod_map[col]:
                    # any() condition below: split will get an error if any NaN in column
                    if m == "code:description" and dataframe[col].notnull().any():
                        dataframe[col] = dataframe[col].apply(lambda x: x.split(":")[0])
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
    # it is leave here for future development (validation step proposed by James)
    def check_duplicates(self, dataframe):
        """
        :param dataframe: dataframe to check duplicates (dataflow must correspond!)
        :return: boolean (duplicates YES/NO)
        """
        dim_cols = self.get_dim_cols()
        return dataframe.duplicated(subset=dim_cols, keep=False).any()
