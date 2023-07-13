import pandas as pd
import numpy as np
import math

def cleanse_un_treaty_data(raw_data,
                           raw_data_country_col,
                           country_list_full,
                           country_list_full_name_col,
                           country_list_iso2_col,
                           country_crba_list,
                           country_crba_list_iso2_col,
                           raw_data_ind_col
                          ):
    """Cleanse raw data from UN treaty html pages

    Add country ISO and ISO 3 codes and add all countries (and only those) to the dataframe, which are in the final CRBA country list.
    Furthermore, encode the target variable into numbers, so that the cleansed data can be given to the scaler function

    Parameters:
    raw_data (obj): Return of function 'extract_un_treaty_data'. Should be a pandas dataframe.
    raw_data_country_col (str): Name of the column in 'raw data' frame that contains the country name.
    country_list_full (obj): pandas dataframe which contains all possible county name variations and the iso2 and iso3 country codes
    country_list_full_name_col (str): Column in country_list_full which contains country names
    country_list_iso2_col (str): Column in country_list_full which contains iso2 codes
    country_crba_list (obj): Dataframe containing the final CRBA list of countries
    country_crba_list_iso2_col (str): Column in 'country_df', which contains the country iso2 codes.
    raw_data_ind_col (str): Column in raw_data which contains the target variable.

    Returns:
    obj: Returns pandas dataframe, which only contains the countries meant to be in CRBA and the latest observed value for the indiator.

   """

    # Obtain ISO2 code of countries and see which countries aren't listed in the raw data
        # The raw data only contains country names. Assign ISO codes to these country names. Use country full list to make sure each country name variation is captured
    grouped_data_iso = raw_data.merge(
        right = country_list_full,
        how = 'left',
        left_on = raw_data_country_col,
        right_on = country_list_full_name_col,
        validate = 'many_to_one')

        # Discard countries that aren't part of the final CRBA master list
    grouped_data_iso_filt = grouped_data_iso.merge(
        right = country_crba_list,
        how = 'right',
        left_on = country_list_iso2_col,
        right_on = country_crba_list_iso2_col,
        indicator = True,
        validate = 'many_to_one')

    # Encode target variable: 1 if not ratified, 2 if ratified. The value 0 (missing) data is assumed to not exist. Either a country ratified, or it didn't
    grouped_data_iso_filt['raw_data_enc'] = grouped_data_iso_filt[raw_data_ind_col].apply(
        lambda x: 1 if pd.isna(x) else 2)

    # Return dataframe
    return(grouped_data_iso_filt)
