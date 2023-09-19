import os
import unittest

from aws.aws_lambda.clima_tempo.main import main



TOKEN = os.environ['CLIMATEMPO_TOKEN']
event = {
    'token': TOKEN,
    'city_ids': [3477],
    'bucket': 'ahow-delta-lake',
    'path_file': 'raw/clima_tempo/forecast_72/test2.csv',
}

r = main(event, None)
print(r['statusCode'])
print(r['body'])