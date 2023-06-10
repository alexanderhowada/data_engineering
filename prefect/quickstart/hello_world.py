from prefect import flow


@flow(log_prints=True)
def hello():
    print("Hello world!")


if __name__ == "__main__":
    hello()

