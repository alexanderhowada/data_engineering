import os
import sys

from sqlalchemy import create_engine
import pandas as pd
from google.cloud import storage


class OracleSQL:

    def __init__(self, host: str, port: str, username: str, password: str, database: str):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

        self.engine = self.get_conn_str()

    def get_conn_str(self):
        return f"oracle+cx_oracle://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

    @property
    def engine(self):
        return self._engine

    @engine.setter
    def engine(self, conn_str):
        self._engine = create_engine(conn_str)

    def get_df(self, q, **kwargs):
        df = pd.read_sql_query(q, self.engine, **kwargs)
        return df


def df_to_bucket(bucket, f, df):
    cli = storage.Client()
    bucket = cli.get_bucket(bucket)
    bucket.blob(f).upload_from_string(df.to_csv(index=False))


if __name__ == '__main__':
    from dotenv import load_dotenv

    load_dotenv()
    HOST = os.environ['HOST_IP']
    PORT = os.environ['PORT']
    USERNAME = os.environ['USERNAME']
    PASSWORD = os.environ['PASSWORD']
    DATABASE = os.environ['DATABASE']

    bucket = sys.argv[1]
    gcs_file = sys.argv[2]
    query = sys.argv[3]

    oracle = OracleSQL(
        HOST, PORT, USERNAME,
        PASSWORD, DATABASE
    )
    df = oracle.get_df(query)

    df_to_bucket(bucket, gcs_file, df)

