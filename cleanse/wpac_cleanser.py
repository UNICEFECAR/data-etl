import numpy as np
import pandas as pd

def cleanse_wpac_data(
    wpac_cleansed_base,
    target_variable,
    raw_data_values,
    encoded_values
):
    """ Cleanse wpac data by discarding unnecesary columns and encoding raw data into numbers ready for 'scaler' function

    Discard unnecessary columns and encode the raw data into the encoding system which is required for the scaler function.
    Function requires the the Pandas dataframe which holds ALL WPAC data.

    Parameters:
    wpac_cleansed_base (obj): DataFrame that contains all WPAC raw data
    target_variable (str): Column in wpac_cleansed, which is meant to be retrieved
    raw_data_values (list): The (unique) values that are in the raw data from WPAC. These values will be encoded into the values of encoded_values. Values must be given in right order.
    encoded_values (list): Encoded values, into which raw_data_values are to be converted. Must be given in corresponding, right order and have same length as raw_data_values.

    Return: Pandas DataFrame
    """

    # Include test to assert that raw_data_values and encoded_values have same length
    assert len(raw_data_values) == len(encoded_values), 'The list of raw data values and encoded values have different length. They must be equal length to guaruantee correspondence.'

    # Discard columns which aren' required
    cleansed_data = wpac_cleansed_base[[
    'country',
    'iso2',
    'iso3',
    target_variable
    ]]

    # Convert raw data into number encoded variables
        # Crate empty list for loop
    conditions = []

        # Loop to create conditions list
    for i in range(len(raw_data_values)):
        conditions += [cleansed_data[target_variable] == raw_data_values[i]]

    # Encode the raw into the encoded_values with the conditions given above
    cleansed_data['raw_data_enc'] = np.select(conditions, encoded_values)

    # Return cleansed dataframe
    return(cleansed_data)
