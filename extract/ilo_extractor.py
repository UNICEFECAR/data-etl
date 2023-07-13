import pandas as pd
import json
import requests

def extract_ilo_api_data(api_call_url):
    """Extract raw data from the ILO API

    The API can be called without an API key.
    The calls can be done with simple https requests, in which a few parameters must be specified.
    To understand the structure and build the URL corresponding to your query, please

    * Visit the data explorer https://www.ilo.org/shinyapps/bulkexplorer28/
    * There, you can browse the indicator your are looking for and by clicking on the "i"-button (top right corner), you can retrieve the indicator code
    * With the indicator code, visit the query builder: https://ilostat.ilo.org/data/sdmx-query-builder/
    * Insert the code, specify the other parameters and get the https URL

    Parameters:
    api_call_url (str): URL request for the ILO API. You can build a data query and retrieve the URL as decribed above.

    Returns:
    obj: Returns pandas dataframe

   """

    # Extract data
    raw_data = pd.read_csv(filepath_or_buffer = api_call_url)

    # Provide log for user to display what dimensions might be in the dataset
    print('The following columns are present in the datasets, and this is the number of unique values they have. ')
    for col in raw_data:
        print('The column {} has {} unique values.'.format(col, raw_data[col].nunique()))

    # Return data as dataframe
    return(raw_data)
