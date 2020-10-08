import pandas as pd
import json
import requests
import urllib
import os
import bs4 as bs
import selenium
from selenium import webdriver


def extract_ilo_normlex_data(html_url):
    """Extract raw data from ILO NOMRLEX

    This function makes a get request against the specified url, finds the table that contains the data (which is
    typically whether a country signed a particular convention or not), converts it from html format into a pandas
    dataframe and returns it. It uses selenium instead of the standard urllib request, because the ILO NORMLEX
    website have a structure, which ir prone to bugs and which can't be opened and get with normal urllib.

    For the function to run, you must install selenium, use firefox as browser and download the geckodriver.exe.
    The driver is part of the repo and it must be put in the root directory. If you want to put it somewhere else,
    then you must specifiy it in the code below.

    Note that for each function call, Selenium will open a Firefox browser window to open the webpage from which
    to scrape. 

    Parameters:
    html_url (str): URL of the ILO NORMLEX website, for example 'https://www.ilo.org/dyn/normlex/en/f?p=NORMLEXPUB:11300:0::NO:11300:P11300_INSTRUMENT_ID:312226:NO'

    Returns:
    obj: Returns pandas dataframe

   """

    # Define current work directory
    cwd = os.getcwd()

    # Open the targete html. Must be done with selenium, because it doesnt work with normal URL request
    driver = webdriver.Firefox(executable_path=cwd + "\\geckodriver.exe")

    # Get response
    response = driver.get(html_url)

    # Retrieve the actual html
    html = driver.page_source

    # Soupify
    soup = bs.BeautifulSoup(html)

    # Extract the target table as attribute
    target_table = str(
        soup.find_all("table", {"cellspacing": "0", "class": "horizontalLine"})
    )

    # Create dataframe with the data
    raw_data = pd.read_html(io=target_table, header=0)[
        0
    ]  # return is a list of DFs, specify [0] to get actual DF

    # Return result
    return raw_data
