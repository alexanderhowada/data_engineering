import boto3
import json
from clima_tempo.engine import ClimaTempoAPI

from requests import Response

from utils.aws_utils import write_to_bucket

def forecast72(event):
    token = event['token']
    city_ids = event['city_ids']
    r_kwargs = {} if 'r_kwargs' not in event.keys() else event['r_kwargs']

    api = ClimaTempoAPI(token, **r_kwargs)
    json_list = api.forecast_72(city_ids)

    if api.last_request.status_code == 200:
        return json_list
    else:
        return api.last_request
def main(event, context):
    """Requests the forecast of the last 75h. and writes it into the bucket.

    Args:
        event: dictionary. Ex:
        {
            'token': Clima tempo API token,
            'city_ids': list with clima tempo city ids,
            'r_kwargs': kwargs for requests.get,
            'bucket': AWS bucket write the JSON,
            'path_file': the file in the bucket to write the data,
        }

    Returns:
        dict with the 'statusCode' and 'body'.
    """
    f = forecast72(event)
    if isinstance(f, Response):
        return {
            'statusCode': f.status_code,
            'body': f.text
        }

    body = json.dumps(f, indent=4)
    write_to_bucket(body, event['bucket'], event['path_file'])
    return {
        'statusCode': 200,
        'body': body
    }