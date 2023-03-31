import urllib.parse
import json

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