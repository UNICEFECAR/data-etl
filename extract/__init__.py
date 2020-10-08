from io import StringIO
from abc import ABC, abstractmethod, abstractproperty

import pandas as pd
import requests


class Extractor(ABC):
    """Abstract base class for extractor classes
    """

    type = None
    headers = {}

    @classmethod
    def content_type(cls):
        return cls.type

    @classmethod
    @abstractmethod
    def data(cls, url):
        """Performs the actual extraction of the data from the provided URL 

        Args:
            url (string): URL to fetch the data from

        Returns:
            DataFrame: DataFrame contianing the data
        """
        raise NotImplementedError

    @classmethod
    def api_request(cls, address, params=None, headers=None):
        try:
            response = requests.get(address, params=params, headers=headers)
            # If the response was successful, no Exception will be raised
            response.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as error:
            print(f"Other error occurred: {error}")
        # return response object
        return response

    @classmethod
    def extract(cls, url):

        raw_data = cls.data(url)

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


class CSVExtractor(Extractor):

    type = "csv"

    @classmethod
    def data(cls, url):

        csv_data = cls.api_request(url, headers=cls.headers).text

        raw_data = pd.read_csv(StringIO(csv_data), sep=",")

        return raw_data


class JSONExtractor(Extractor):

    type = "json"

    @classmethod
    def data(cls, url):
        # Extract data and convert to pandas dataframe
        raw_data = pd.json_normalize(requests.get(url).json()["data"])

        return raw_data


class SDMXExtractor(CSVExtractor):

    type = "sdmx-csv"

    headers = {
        "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
        "Accept-Encoding": "gzip",
    }
