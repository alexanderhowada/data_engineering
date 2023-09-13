from random import randint
from datetime import datetime

from prefect import flow, task
from prefect import variables


@task
def get_rn():
    return randint(0, 1024)

@task
def is_even(n):
    n = abs(n)
    if n == 0 or n%2 == 0:
        return "even"
    else:
        return "odd"

@task(log_prints=True)
def write_line(file_name: str, m: str):
    with open(file_name, "a") as f:
        f.write(m)
        f.write("\n")

@flow(name="pipeline_rng")
def pipeline_rng(file_name):
    rn = get_rn()
    parity = is_even(rn)

    m = f"{datetime.now()},{rn},{parity}"
    write_line(file_name, m)


if __name__ == "__main__":
    data_path = variables.get("quickstart_data_path")
    data_path += f"/pipeline_rng/data.csv"

    pipeline_rng(data_path)

