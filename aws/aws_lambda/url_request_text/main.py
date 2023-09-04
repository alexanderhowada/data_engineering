import boto3
from utils.url_utils import BaseAPI


def write_to_bucket(s, bucket_name, path_file):
    cli = boto3.resource('s3')
    bucket = cli.Bucket(bucket_name)
    bucket.put_object(Body=s, Key=path_file)

def main(event, context):
    url = event['url']
    r_kwargs = event['r_kwargs']

    api = BaseAPI()
    r = api.get_request(url, **r_kwargs)

    if 'bucket' in event.keys():
        write_to_bucket(r.text, event['bucket'], event['path_file'])
        return {'statusCode': r.status_code}

    return {
        'statusCode': r.status_code,
        'body': r.text
    }
