import unittest

from aws.aws_lambda.clima_tempo.main import main

TOKEN = 'fa37b0a07b45cccb0a9057facd4d022e'
event = {
    'token': TOKEN,
    'city_ids': [3477],
    'bucket': 'ahow-delta-lake',
    'path_file': 'raw/clima_tempo/forecast_72/test.csv',
}

r = main(event, None)
print(r['statusCode'])
print(r['body'])