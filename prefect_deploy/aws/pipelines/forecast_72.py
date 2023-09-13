import os

import boto3
from prefect import task, flow

from prefect_deploy.aws.base_block import AwsBaseBlock


def get_aws_block():
    return AwsBaseBlock.load('base-aws')

@flow(
    timeout_seconds=60*5,
    retries=1,
    log_prints=True
)
def raw_forecast_72(bucket_name, file_path):
    block = get_aws_block()
    event = {
        'token': block.get_secret('clima_tempo', 'TOKEN'),
        'city_ids': [3477],
        'bucket': bucket_name,
        'path_file': file_path,
    }

    r = block.invoke_lambda(
        'clima_tempo_forecast_72', event
    )
    return bucket_name, file_path


@flow(
    log_prints=True,
    timeout_seconds=60*15
)
def clima_tempo_forecast_72(bucket_name, file_path):
    raw_forecast_72(bucket_name, file_path)


if __name__ == '__main__':
    raw_forecast_72('ahow-delta-lake', 'raw/clima_tempo/forecast_72/test2.csv')