from prefect import variables, flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

from pipeline_rng import pipeline_rng


if __name__ == "__main__":
    # help(Deployment)

    path = variables.get("quickstart_data_path")
    print(path)

    for i in range(1, 3):
        dep = Deployment.build_from_flow(
            flow=pipeline_rng,
            name=f"pipeline_rng_{i}",
            work_pool_name="default-agent-pool",
            work_queue_name="default",
            path=variables.get("quickstart_data_path"),
            schedule=CronSchedule(
                cron="*/5 * * * *",
                timezone="America/Sao_Paulo"
            ),
            parameters={"file_name": f"{path}/pipeline_rng/data_{i}.csv"}
        )
        dep.apply()
