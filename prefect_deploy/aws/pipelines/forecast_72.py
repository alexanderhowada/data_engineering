import os
from datetime import datetime, timedelta

from pytz import timezone
import boto3
import prefect
from prefect import task, flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_deploy.aws.base_block import AwsBaseBlock


def get_aws_block():
    return AwsBaseBlock.load('base-aws')

@flow(timeout_seconds=60*5, retries=1, log_prints=True)
def raw_forecast_72(bucket_name, file_path):
    file_path += str(datetime.now()) + '.json'
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
    if r["StatusCode"] >= 400:
        raise Exception(f"Status code {r['StatusCode']}")

    return bucket_name, file_path


@flow(log_prints=True, timeout_seconds=60*15)
def clima_tempo_forecast_72(bucket_name, file_path):
    now = datetime.now().replace(tzinfo=timezone("UTC"))
    d = prefect.context.get_run_context().flow_run.dict()
    diff_time = abs(d['start_time'] - now)
    print(d['start_time'], now)

    if diff_time > timedelta(seconds=3600*3):
        raise Exception(f"Too late {diff_time}. Will not run.")

    raw_forecast_72(bucket_name, file_path)


def deploy():
    parameters = {
        'file_path': 'raw/clima_tempo/forecast_72/forecast_72_',
        'bucket_name': 'ahow-delta-lake'
    }
    dep = Deployment.build_from_flow(
        flow=clima_tempo_forecast_72,
        name=f"clima_tempo_forecast_72",
        work_pool_name="default-agent-pool",
        work_queue_name="default",
        schedule=CronSchedule(
            cron="0 20 * * *",
            timezone="America/Sao_Paulo"
        ),
        parameters=parameters
    )
    dep.apply()

if __name__ == '__main__':
    deploy()