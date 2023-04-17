import json
import sys
sys.path.append('../')

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from utils.url_utils import get_json_from_url

import pandas as pd


class SupermetricsAPI:
    # base_url = "https://api.supermetrics.com/enterprise/v2/query/data/json?json="
    base_url = "https://aws1-api-default.supermetrics.com/enterprise/v2/query/data/json?json="

    def __init__(self, url, adapter=None, offset=100000):
        """
        :param url: supermetrics url query
        :param spark: spark session
        :param adapter: HTTP adapter for request sessions.
                        See SupermetricsAPI._build_session for more details.
                        Defaults to None
        """
        self.url = url
        self.adapter = adapter
        self.offset = offset

        self._build_session()

    def _build_session(self):
        """
        Builds session with retries.
        Builds the default session if self.adapter is not None.
        """

        if self.adapter is not None:
            return

        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1
        )

        self.adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session = requests.Session()
        self.session.mount("https://", self.adapter)

    def get_request(self, url: str, **r_kwargs):
        """
        Generic method to get any URL.

        :param url: url or key of self.url.
        :param **r_kwargs: kwargs for requests

        return requests.response
        """

        self.last_request = self.session.get(url, **r_kwargs)

        return self.last_request

    def build_url(self, dt_start, dt_end):
        url = get_json_from_url(self.url)
        url['start_date'] = str(dt_start)
        url['end_date'] = str(dt_end)
        url['offset_start'] = 0
        url['offset_end'] = self.offset

        return self.base_url + json.dumps(url)

    def extract(self, dt_start, dt_end, r_kwargs={}):
        """
        Extract paginated requests from url from dt_start to dt_end.

        :param dt_start: start date of the extraction.
        :param dt_end: end date of the extractions.
        :param r_kwargs: kwargs for requests.get.

        :return: list[response]
        """

        url = self.build_url(dt_start, dt_end)
        r = self.get_request(url, **r_kwargs)
        r_list = [r]

        while r.json()['meta']['paginate']['next'] is not None:
            url = r.json()['meta']['paginate']['next']
            r = self.get_request(url, **r_kwargs)
            r_list.append(r)

        return r_list

    def extract_json(self, *args, **kwargs):

        r_list = self.extract(*args, **kwargs)
        j_list = [r.json() for r in r_list]

        return j_list

    def extract_data(self, *args, **kwargs):

        j_list = self.extract_json(*args, **kwargs)

        if len(j_list[0]['data']) <= 1:
            return pd.DataFrame()
        else:
            columns = j_list[0]['data'][0]
            df = pd.DataFrame(j_list[0]['data'][1:], columns=columns)

        df_list = [df]

        for j in j_list[1:]:
            if len(j['data']) <= 1:
                continue

            df = pd.DataFrame(j['data'], columns=columns)
            df_list.append(df)

        df = pd.concat(df_list)

        return df


if __name__ == '__main__':
    import os
    from datetime import datetime

    URL = os.environ['URL']

    dt_start = datetime(2022, 1, 1).date()
    dt_end = datetime(2022, 1, 2).date()

    api = SupermetricsAPI(URL, offset=10)
    df = api.extract_data(dt_start, dt_end, r_kwargs={'timeout': 360})
    df.to_csv('test1.csv', index=False)

