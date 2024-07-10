import pandas as pd
import requests
from io import StringIO


def curl(path, query_string) -> pd.DataFrame:
    url = "http://127.0.0.1:25510/v2/" + path
    query_string['use_csv'] = 'true'
    headers = {"Accept": "application/json"}
    response = requests.get(url, headers=headers, params=query_string)
    # print(response.request.url)
    if response.status_code != 200:
        print(response.url)
        raise Exception("Error: Response code " + str(response.status_code) + ": " + response.content.decode('utf-8'))

    # print(response.headers)
    if 'Next-page' in response.headers and response.headers['Next-page'] != 'null':
        print("Has next page!")

    csv = response.content.decode('utf-8')
    df = pd.read_csv(StringIO(csv))
    return df
