import pandas as pd
import json
import requests


def extract_sdg_api_data(series_code):
    """Extract raw data for a series from the SDG API (UN STATS)

    Extract data on the specifies series as json and flatten it out into a pandas dataframe
    To retrieve the data from the API of the SDG indicators, you must proceed as follows:

    * Find the seriesCode of the indicator for which you want to retrieve data: I recommend visiting https://unstats.un.org/sdgs/indicators/database/ and browse the indicator you want and the series code will be indicated there
    * Visit https://unstats.un.org/SDGAPI/swagger/#!/Series/V1SdgSeriesDataGet
    * Expand the tab GET /v1/sdg/Series/Data
    * Type in the seriesCode, this will provide you with the link to the JSON
    * Important: The results will be on shwon on various pages if the dataset is too large. That is why it is a good idea to set a pageSize value large enough to accomodate all data in just one page to not have to iterate over various pages


    Parameters:
    series (string): Code of the series, which can be found by browsing for the relevant series here:https://unstats.un.org/sdgs/indicators/database?indicator=16.2.2

    Returns:
    obj: Returns pandas dataframe

   """

    # Define URL to extract data in json format
    url = "https://unstats.un.org/SDGAPI/v1/sdg/Series/Data?seriesCode={}&pageSize=999999999".format(
        series_code
    )

    # Extract data and convert to pandas dataframe
    raw_data = pd.json_normalize(requests.get(url).json()["data"])

    # Log functionality for user
    print(
        "The following columns are present in the datasets, and this is the number of unique values they have. "
    )
    for col in raw_data:
        print(
            "The column {} has {} unique values.".format(
                col, raw_data[col].astype(str).nunique()
            )
        )

    # Return pandas dataframe
    return raw_data
