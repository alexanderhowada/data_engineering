import json
import unittest

import boto3

from aws.aws_lambda.url_request_text.main import main


def delete_from_bucket(bucket_name, path_file):
    cli = boto3.resource('s3')
    cli.Object(bucket_name, path_file).delete()

def read_from_bucket(bucket_name, path_file):
    cli = boto3.resource('s3')
    obj = cli.Object(bucket_name, path_file)
    return obj.get()['Body'].read()

class TestMain(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.uri = "https://random-data-api.com/api/v2/users"

    def test_request(self):
        event = {
            'url': self.uri,
            'r_kwargs': {
                'params': {
                    'size': 2,
                    'response_type': 'json'
                }
            },
            'bucket': 'ahow-delta-lake',
            'path_file': 'delta-lake/raw/random_data_api/test.json'
        }
        r = main(event, None)

        obj = read_from_bucket(event['bucket'], event['path_file'])
        obj = json.loads(obj)

        self.assertEqual(r["statusCode"], 200)
        self.assertEqual(len(obj), event['r_kwargs']['params']['size'])

        delete_from_bucket(event['bucket'], event['path_file'])


if __name__ == '__main__':
    unittest.main()

