import os
from datetime import datetime

from prefect import task, flow
from prefect.deployments import Deployment
from prefect import variables

@task(name="write", log_prints=True)
def write_file():

    with open(f"{variables.get('data_path')}/hello_world.txt", "a") as f:
        f.write(f"{datetime.now()} hello_world\n")

    return "Ok?"

@flow
def write_flow():
    write_file()
    return True

def deploy():
    deploy = Deployment.build_from_flow(
        flow=write_flow,
        name="write-hello"
    )
    deploy.apply()

if __name__ == "__main__":
    deploy()