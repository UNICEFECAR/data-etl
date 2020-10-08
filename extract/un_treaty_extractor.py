import pandas as pd
import json
import requests
import urllib
import bs4 as bs

def extract_un_treaties_data(html_url):
    """Extract raw data from html pages of UN treaties

    This function makes a get request against the specified url, finds the table that contains the data (which is
    typically whether a country signed a particular convention or not), converts it from html format into a pandas
    dataframe and returns it.

    Parameters:
    html_url (str): URL of the UN website (starts with treaties.un.org/), for example https://treaties.un.org/pages/ViewDetails.aspx?src=TREATY&mtdsg_no=XXV-4&chapter=25&clang=_en

    Returns:
    obj: Returns pandas dataframe

   """

    # http get request
    source = urllib.request.urlopen(html_url)

    # soupify
    soup = bs.BeautifulSoup(source, features = 'lxml')

    # Extract the target table as attribute
    target_table = str(soup.find_all('table', {'class' : 'table table-striped table-bordered table-hover table-condensed'}))

    # Create dataframe with the data
    raw_data = pd.read_html(io = target_table,
                       header = 0)[0] # return is a list of DFs, specify [0] to get actual DF
    # Return result
    return(raw_data)
