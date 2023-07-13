import pandas as pd

def cleanse_sdg_api_data(raw_data, country_list_full, country_list_full_name_col, country_list_full_iso2_col, country_df, country_df_iso2_col, non_dim_cols):
    """Cleanse raw data from SDG API

    Retrieve only the latest observations for each country-dimension group. NB Dimensions make are part of the composite primary key of the dataset and therefore their values define groups, e.g. male vs femeale in a country.
    Discard countries that aren't in the CRBA final list of countries

    Parameters:
    raw_data (obj): Return of function 'extract_sdg_api_data'. Should be a pandas dataframe.
    country_list_full (obj): Dataframe containing the full list of countries with all their country name variations, should be in long format
    country_list_full_name_col (str): Column in 'country_list_full', which contains the country names (this column should be the primary key of the dataframe as well)
    country_list_full_iso2_col (str): Column in 'country_list_full', which contains the country iso2 codes.
    country_df (obj): Dataframe containing the final CRBA list of countries
    country_df_iso2_col (str): Column in 'country_df', which contains the country iso2 codes.
    non_dim_cols (list): List of columns, which should be excluded from the group by statement (that is, if a column is not indicated here, then the maximum value for each of its values will be calculated)

    Returns:
    obj: Returns pandas dataframe, which only contains the countries meant to be in CRBA and the latest observed value for the indiator.

   """

    # Group by to obtain the latest available value per group, where group is 'col_list_gb'
        # Create list of all column to group by
    col_list = raw_data.columns.to_list() # list of all columns in the dataframe
    non_dim_cols_tuple = tuple(non_dim_cols) # parameters must be passed as list, but the following command requires a tuple
    col_list_gb = [e for e in col_list if e not in non_dim_cols_tuple] # exclude timePeriodStart and value, because these one'saren't used for the groupby statement

        # Some of the columns contain values which Python interprets as lists (e.g. [8.1]). These make the groupby statement malfunction. Convert them to string
    raw_data[col_list_gb] =  raw_data[col_list_gb].astype(str)

        # It may be that the time period column is not of type numerical
    raw_data['timePeriodStart'] = pd.to_numeric(raw_data['timePeriodStart'])

        # Retreive the latest available data for each group, where group is 'col_list_gb'
    grouped_data = raw_data[raw_data['timePeriodStart'] == raw_data.groupby(col_list_gb)['timePeriodStart'].transform('max')]

    # Discard rows of countries that are not in the master country list
        # The raw data only contains country names. Assign ISO codes to these country names. Use country full list to make sure each country name variation is captured
    grouped_data_iso = grouped_data.merge(
        right = country_list_full,
        how = 'left',
        left_on = 'geoAreaName',
        right_on = country_list_full_name_col,
        validate = 'many_to_one')

        # Discard countries that aren't part of the final CRBA master list
    grouped_data_iso_filt = grouped_data_iso.merge(
        right = country_df,
        how = 'right',
        left_on = country_list_full_iso2_col,
        right_on = country_df_iso2_col,
        indicator = True,
        validate = 'many_to_one')

    # Note of Michael to Michael: "Validation: Number of rows should be 195"
    # --> This is incorrect, the number can also diverge (because the left table may contain one, several or no values for a country-key)

    # return result
    return(grouped_data_iso_filt.sort_values(by = country_df_iso2_col, axis = 0))
