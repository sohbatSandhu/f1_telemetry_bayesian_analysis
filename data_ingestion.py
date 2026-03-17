# api
from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
import json

# data manipulation
import pandas as pd

# OpenF1 API url
BASE_URL = "https://api.openf1.org/v1"

def request_openf1_data(endpoint, **params) -> pd.DataFrame:
    """
    Request data from OpenF1 API using a endpoint and the respective parameters
    passed 

    Args:
        endpoint (str): API endpoint resource path

    Returns:
        pd.DataFrame: Data retrieved
    """
    query = urlencode(params, doseq=True)
    url = f"{BASE_URL}/{endpoint}?{query}" # base url w/ endpoint and query
    data = None
    
    print("Requesting:", url)
    try:
        with urlopen(url) as response:
            # For successful responses (e.g., 200)
            response_status = response.getcode() # Or response.status
            data = json.loads(response.read().decode("utf-8"))
            print(f"Success! Response code: {response_status}")
            return pd.DataFrame(data)
    except HTTPError as e:
        # For HTTP errors (e.g., 404, 500)
        response_status = e.code
        print(f"HTTP Error! Response code: {response_status}")
    except URLError as e:
        # For other URL-related errors (e.g., connection issues)
        print(f"URL Error! Reason: {e.reason}")
    
    return None