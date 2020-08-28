
import requests

# function for api request as proposed by Daniele
# errors are printed and don't stop program execution
def api_request(address, params=None, headers=None):
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