from random import randint
import requests
from requests.adapters import HTTPAdapter, Retry

def get_random_user_agent():
    user_agents = [
        # Chrome
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/<version> Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/<version> Safari/537.36",
        # Firefox
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:<version>) Gecko/20100101 Firefox/<version>",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:<version>) Gecko/20100101 Firefox/<version>",
        # Safari
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/<version> Safari/605.1.15",
        # Microsoft Edge
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/<version> Safari/537.36 Edg/<version>"
    ]
    return user_agents[randint(0, len(user_agents)-1)]


class RandomUser:

    base_url = "https://randomuser.me/api"

    def __init__(self, http_adapter=None):
        self.build_adapter(http_adapter)

    def build_adapter(self, http_adapter):
        self.session = requests.Session()

        if http_adapter is not None:
            self.session.mount(self.base_url, http_adapter)
            return

        retries = Retry(
            total=5, backoff_factor=1,
            status_forcelist=[502, 503, 504, 429]
        )
        self.session.mount(self.base_url, HTTPAdapter(max_retries=retries))

    @staticmethod
    def build_headers(h={}):
        if "User-Agent" not in h.keys():
            h["User-Agent"] = get_random_user_agent()
        return h

    def get(self, **kwargs):
        if "headers" not in kwargs.keys():
            kwargs["headers"] = {}
        kwargs["headers"] = self.build_headers(kwargs["headers"])
        self.last_response = self.session.get(self.base_url, **kwargs)
        return self.last_response

    def get_n(self, n=10):
        return self.get(params={"results": n})

def get_n(n, timeout=30):

    ru = RandomUser()
    r = ru.get(params={"results": n}, timeout=timeout)

    if r.status_code >= 400:
        raise Exception(f"Status code = {r.status_code}\n m={r.text}")

    return r.json()