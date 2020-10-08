import pandas as pd


def cleanse_who_api_num_data(
    raw_data, raw_data_iso_3_col, country_df, country_df_iso3_col, non_dim_cols
):
    """Cleanse raw data from WHO API

    Retrieve only the latest observations for each country-dimension group. NB Dimensions make are required to uniquely identify a row of the dataset and therefore their values define groups, e.g. male vs female in a country.
    Discard countries that aren't in the CRBA final list of countries

    Parameters:
    raw_data (obj): Return of function 'extract_sdg_api_data'. Should be a pandas dataframe.
    raw_data_iso_2_col (str): Name of the column in 'raw data' frame that contains the ISO2 country codes.
    country_df (obj): Dataframe containing the final CRBA list of countries
    country_df_iso2_col (str): Column in 'country_df', which contains the country iso2 codes.
    non_dim_cols (list): List of columns, which should be excluded from the group-by-statement (that is, if a column is not indicated here, then the maximum value for each of its values will be calculated)

    Returns:
    obj: Returns pandas dataframe, which only contains the countries meant to be in CRBA and the latest observed value for the indiator.

   """

    # Sometimes, there are duplicates in the dataset. Remove them
    raw_data = raw_data.drop_duplicates()

    # Group by to obtain the latest available value per group, where group is 'col_list_gb'
    # Create list of all columns to group by
    col_list = raw_data.columns.to_list()  # list of all columns in the dataframe
    non_dim_cols_tuple = tuple(
        non_dim_cols
    )  # parameters must be passed as list, but the following command requires a tuple
    col_list_gb = [
        e for e in col_list if e not in non_dim_cols_tuple
    ]  # exclude timePeriodStart and value, because these one'saren't used for the groupby statement

    # Some of the columns contain values which Python interprets as lists (e.g. [8.1]). These make the groupby statement malfunction. Convert them to string
    raw_data[col_list_gb] = raw_data[col_list_gb].astype(str)

    # Retreive the latest available data for each group, where group is 'col_list_gb'
    grouped_data = raw_data[
        raw_data["YEAR"] == raw_data.groupby(col_list_gb)["YEAR"].transform("max")
    ]

    # Discard countries that aren't part of the final CRBA master list
    grouped_data_iso_filt = grouped_data.merge(
        right=country_df,
        how="right",
        left_on=raw_data_iso_3_col,
        right_on=country_df_iso3_col,
        indicator=True,
        validate="many_to_one",
    )

    # return result
    return grouped_data_iso_filt.sort_values(by=country_df_iso3_col, axis=0)


def cleanse_who_api_cat_data(
    raw_data, raw_data_iso_3_col, country_df, country_df_iso3_col, non_dim_cols
):
    """TO DO

    To do

    Parameters:
    raw_data (obj): Return of function 'extract_sdg_api_data'. Should be a pandas dataframe.
    raw_data_iso_2_col (str): Name of the column in 'raw data' frame that contains the ISO2 country codes.
    country_df (obj): Dataframe containing the final CRBA list of countries
    country_df_iso2_col (str): Column in 'country_df', which contains the country iso2 codes.
    non_dim_cols (list): List of columns, which should be excluded from the group-by-statement (that is, if a column is not indicated here, then the maximum value for each of its values will be calculated)

    Returns:
    obj: Returns pandas dataframe, which only contains the countries meant to be in CRBA and the latest observed value for the indiator.

   """

    # To do
