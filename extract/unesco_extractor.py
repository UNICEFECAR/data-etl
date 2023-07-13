import pandas as pd


def extract_unesco_api_data(api_call_url, subs_key="460ab272abdd43c892bb59c218c22c09"):
    """Extract raw data from the UNESCO API

    The API can be called with a API key, which you must generate by creating an account.
    The calls can be done with simple https requests, in which many parameter must be specified (e.g. indicator code, sexes, age, ...). To understand the structure and build the URL corresponding to your query, please check the query builder https://apiportal.uis.unesco.org/query-builder
    URLs must be returning cs files (i.e. they should contain ''&format=csv-sdmx' contain in their URL)

    Parameters:
    api_call_url (str): URL request for the UNESCO API. You can build a data query and retrieve the URL for it with the query-builder: https://apiportal.uis.unesco.org/query-builder.
    subs_key (str): Your API key. You must create one. Visit https://apiportal.uis.unesco.org/getting-started for more info. The default value is the subscription key created by me (Michael Gramlich) in August 2020.

    Returns:
    obj: Returns pandas dataframe

   """

    # Define URL with Michael Gramlichs subscription key
    if subs_key == "460ab272abdd43c892bb59c218c22c09":
        url = api_call_url
    else:
        print(
            "If you would like to use another subscription key, please build your query including that subscription key here: https://apiportal.uis.unesco.org/query-builder"
        )

    # Extract data
    raw_data = pd.read_csv(filepath_or_buffer=url, sep=",",)

    # Provide log for users to see what possible dimensions might be in the dataset
    print(
        "The following columns are present in the datasets, and this is the number of unique values they have. "
    )
    for col in raw_data:
        print(
            "The column {} has {} unique values.".format(col, raw_data[col].nunique())
        )

    # Return dataframe
    return raw_data
