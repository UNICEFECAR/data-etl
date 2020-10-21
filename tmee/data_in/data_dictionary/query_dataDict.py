import pandas as pd

# Functions that operate directly from our data-dictionary in Excel using pandas
# Typically retrieve queries, useful for our ETL purposes


def get_API_code_address_etc(excel_data_dict):
    """ filters all indicators that are extracted by API
        :param excel_data_dict: path/file to our excel data dictionary in repo
        :return: pandas dataframe (df) with code, address for API requests, data source name and observation footnotes
    """
    # read snapshots table from excel data-dictionary
    snapshot_df = pd.read_excel(excel_data_dict, sheet_name="Snapshot")
    # read sources table from excel data-dictionary
    sources_df = pd.read_excel(excel_data_dict, sheet_name="Source")
    # join snapshot and source based on Source_Id
    # 'left' preserves key order from snapshots
    snap_source_df = snapshot_df.merge(
        sources_df, on="Source_Id", how="left", sort=False
    )
    # read indicators table from excel data-dictionary
    indicators_df = pd.read_excel(excel_data_dict, sheet_name="Indicator")
    # join snap_source and indicators based on Indicator_Id
    snap_source_ind_df = snap_source_df.merge(
        indicators_df, on="Indicator_Id", how="left", sort=False
    )
    # get list of indicator codes, address with API extraction and other info
    logic_API = snap_source_ind_df.Type == "API"
    api_code_addr_etc_df = snap_source_ind_df[logic_API][
        ["Code_y", "Address", "Name_y", "Comments_y"]
    ]
    api_code_addr_etc_df.rename(
        columns={
            "Code_y": "Code",
            "Name_y": "Data_Source",
            "Comments_y": "Obs_Footnote",
        },
        inplace=True,
    )
    # return dataframe (note its df index will correspond to that of snapshots)
    return api_code_addr_etc_df

