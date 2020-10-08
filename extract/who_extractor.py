
import pandas as pd
import json
import requests

def extract_who_api_data(api_call_url):
    """Extract raw data from the WHO Athena API

    The WHO has two web services (APIs) through which data can be pulled: GHO OData and Athena.
    This function pulls data from the Athena web service, because it supports csv format.
    The documentation of the API and how to build a query is here: https://www.who.int/data/gho/info/athena-api, but
    the short version is follow these steps:

    * Take the link http://apps.who.int/gho/athena/api/GHO/<indicator_code>.csv
    * To get the indicicator visit: https://apps.who.int/gho/athena/api/GHO
    * There, search for your indicator (usually in the tags <Display>)
    * Then look what the corresponding indicator code is, which is in the <Code Label>

    Parameters:
    api_call_url (str): URL request for the WHO API. You can build your URL following the steps above

    Returns:
    obj: Returns pandas dataframe

   """
    # Extract data
    raw_data = pd.read_csv(filepath_or_buffer = api_call_url)

    # Give information for user to see what possible dimensions are in the raw data
    print('The following columns are present in the datasets, and this is the number of unique values they have. ')
    for col in raw_data:
        print('The column {} has {} unique values.'.format(col, raw_data[col].nunique()))

    # Return result
    return(raw_data)
