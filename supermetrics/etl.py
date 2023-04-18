import sys
sys.path.append('../')

from datetime import datetime, timedelta
from typing import List

from pyspark.sql import SparkSession

from engine import SupermetricsAPI
from utils.delta_utils import merge
from utils.date_utils import gen_date_range
from utils.spark_utils import pandas_to_spark_string


def transform_string(s_list: List[str], replace_f: List):
    """
    Transform each string of s_list by appling each function in replace_f.
    :param s_list: list of strings.
    :param replace_f: list of functions. Each function should transform a string and return the transformed one.
    :return: List[str]
    """

    new_list = []

    for s in s_list:
        for f in replace_f:
            s = f(s)
        new_list.append(s)

    return new_list

def get_df(api: SupermetricsAPI,
           spark_session: SparkSession,
           dt_start: datetime, dt_end: datetime,
           r_kwargs={}, replace_f=[]):
    """
    Get a single DataFrame from SM.
    Useful for prototyping replace_f functions.

    :param spark_session: spark session.
    :param url: URL from Supermetrics.
    :param dt_start: start date of the extraction.
    :param dt_end: end date of the extraction.
    :param r_kwargs: requests.get kwargs.
    :param replace_f: list of functions to transform columns.
    :return: PySpark DataFrame
    """

    df = api.extract_data(dt_start.strftime('%d/%m/%Y'), dt_end.strftime('%d/%m/%Y'), r_kwargs=r_kwargs)
    df = pandas_to_spark_string(df, spark_session=spark_session, col_f=replace_f)
    return df


def default(api: SupermetricsAPI,
            spark_session: SparkSession, tb: str, pk: List[str], partition: List[str],
            dt_start: datetime, dt_end: datetime,
            dt_delta: timedelta, subtract_ed: timedelta = timedelta(days=1),
            r_kwargs={}, replace_f=[]):
    """
    Default SM ETL.
    Extracts data from dt_start, up to dt_end in intervals of dt_delta.
    Merges data after each date interval.

    :param api: SupermetricsAPI instance.
    :param spark_session: spark session.
    :param tb: delta table to merge.
    :param pk: Primary keys to merge.
    :param partition: Columns to partition.
    :param dt_start: start date of the extraction.
    :param dt_end: end date of the extraction.
    :param dt_delta: interval of the extraction.
    :param subtract_ed: interval to subtract of each extraction. Defaults to timedelta(days=1) (ideal for daily ETL).
    :param r_kwargs: requests.get kwargs.
    :param replace_f: list of functions to transform the columns.
    """

    print('**************************************')
    print('*********** Starting SM ETL **********')
    print('**************************************')

    date_range = gen_date_range(dt_start, dt_end, dt_delta, subtract_ed=subtract_ed, format='%d/%m/%Y')

    pk = transform_string(pk, replace_f)
    partition = transform_string(partition, replace_f)

    for dr in date_range:

        print(f"from {dr['dt_start']} to {dr['dt_end']}")

        print("extracting")
        df = api.extract_data(dr['dt_start'], dr['dt_end'], r_kwargs=r_kwargs)
        df = pandas_to_spark_string(df, spark_session=spark_session, col_f=replace_f)

        print("merging")
        merge(df, tb, pk, spark_session=spark_session, partition=partition)

        print('\n')

