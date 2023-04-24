import json
import urllib.parse
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

def get_json_from_url(url: str):
    """
    Get JSON parameters as a dictionary

    :param url: url to extract the parameters
    :return: dict
    """

    decoded = urllib.parse.unquote(url)
    query = urllib.parse.urlparse(decoded).query

    if query.startswith('json='):
        query = query[5:]
        if query:
            return json.loads(query)
        else:
            return {}
    else:
        raise ValueError(f"URL does not contain a JSON")


class BaseAPI:

    endpoints = {
        'cat_facts': 'https://catfact.ninja/fact',
        'random_user': 'https://randomuser.me/api/'
    }

    def __init__(self, adapter=None):
        """
        :param spark: spark session
        :param adapter: HTTP adapter for request sessions.
                        See SupermetricsAPI._build_session for more details.
                        Defaults to None
        """
        self.adapter = adapter

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
            backoff_factor=5,
            raise_on_status=False
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

        if url in self.endpoints.keys():
            url = self.endpoints[url]

        self.last_request = self.session.get(url, **r_kwargs)

        return self.last_request

    def get_json(self, *args, **kwargs):
        """
        Generic method to get any JSON from url.

        :param args: arguments for self.get_request.
        :param **r_kwargs: kwargs for get_request.

        return requests.response.json()
        """

        return self.get_request(*args, **kwargs).json()

if __name__ == '__main__':

    api = BaseAPI()

    r_kwargs = {'timeout': 30}

    print(api.get_json('random_user', **r_kwargs))

