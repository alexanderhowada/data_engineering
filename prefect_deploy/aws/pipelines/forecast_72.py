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
    file_path = file_path.replace(' ', '_').replace(':', '-')
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

@flow(timeout_seconds=60*15, retries=0, log_prints=True)
def raw_etl(raw_path, target_tb_path, checkpoint_location):
    block = get_aws_block()
    job_driver = {
        'sparkSubmit': {
            'entryPoint': 'data_engineering/aws/aws_emr_serverless/forecast_72/raw.py',
            'entryPointArguments': [raw_path, target_tb_path, checkpoint_location]
        }
    }
    r = block.invoke_emr(
        'forecast_72_raw_etl', job_driver, execution_timeout=15
    )

    if r["jobRun"]["state"] != "SUCCESS":
        raise Exception(f"Job status {r['jobRun']['state']}")
    return r

@flow(log_prints=True, timeout_seconds=60*15)
def clima_tempo_forecast_72(param: dict):
    """Runs forecast pipeline if schedule is less than 3h late.

    Args:
        param: ex
            {
                'raw_forecast_72': {
                    'file_path': 'raw/clima_tempo/forecast_72/forecast_72_',
                    'bucket_name': 'ahow-delta-lake',
                },
                'raw_etl': {
                    'raw_path': f'{prefix}/raw/clima_tempo/forecast_72/',
                    'target_tb_path': f'{prefix}/delta-lake/clima_tempo/forecast_72/',
                    'checkpoint_location': f'{prefix}/raw/clima_tempo/forecast_72/checkpoint',
                }
            }
    """

    now = datetime.now().replace(tzinfo=timezone("UTC"))
    d = prefect.context.get_run_context().flow_run.dict()
    diff_time = abs(d['start_time'] - now)

    print(f"Prefect scheduled time: {d}")
    print(f"Agent execution time: {now}")

    if diff_time > timedelta(seconds=3600*3):
        raise Exception(f"Too late {diff_time}. Will not run.")

    raw_forecast_72(**param['raw_forecast_72'])
    raw_etl(**param['raw_etl'])


def deploy(test=False):
    prefix = "s3://ahow-delta-lake"
    parameters = {
        "param": {
            "raw_forecast_72": {
                "file_path": "raw/clima_tempo/forecast_72/forecast_72_",
                "bucket_name": "ahow-delta-lake",
            },
            "raw_etl": {
                "raw_path": f"{prefix}/raw/clima_tempo/forecast_72/",
                "target_tb_path": f"{prefix}/delta-lake/clima_tempo/forecast_72/",
                "checkpoint_location": f"{prefix}/raw/clima_tempo/forecast_72/checkpoint",
            }
        }
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

    if test:
        clima_tempo_forecast_72(**parameters)
    else:
        dep.apply()

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        deploy(test=True)
    else:
        deploy(test=False)

