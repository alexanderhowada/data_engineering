import sys
sys.path.append('../')

from datetime import datetime
from typing import List

import pandas as pd
import pyspark

from engine import ClimaTempoAPI
from utils.delta_utils import merge
from utils.spark_utils import pandas_to_spark_string

def cities_etl(api: ClimaTempoAPI, tb: str,
               spark: pyspark.sql.SparkSession,
               **r_kwargs) -> pyspark.sql.DataFrame:
    """
    ETL for cities.

    :param api: ClimaTempoAPI.
    :param tb: delta table to merge.
    :param spark: spark session.
    :param r_kwargs: kwargs for requests.get.
    :return: pyspark.sql.DataFrame.
    """

    j = api.get_json('list_cities', **r_kwargs)

    df = pd.DataFrame(j)
    df = pandas_to_spark_string(df, spark)

    merge(df, tb, ['id'], spark)

    return df


def forecast_72h_etl(api: ClimaTempoAPI, tb: str,
                     spark: pyspark.sql.SparkSession,
                     city_ids: List[int], **r_kwargs) -> pyspark.sql.DataFrame:
    """
    ETL for 72h forecast.
    This ETL generates a table where forecast data is constantly appended.

    :param api:
    :param tb:
    :param spark:
    :param city_ids:
    :param r_kwargs:
    :return: spyaprk.sql.DataFrame
    """

    # Get forecast and cast it to a pandas DataFrame
    j_list = api.forecast_72(city_ids, **r_kwargs)

    df_list = []
    for j in j_list:
        df = pd.DataFrame(j['data'])
        del j['data']

        for k, v in j.items():
            df[k] = v

        df_list.append(df)

    # Add date columns (will be used for partitioning).
    d = datetime.now()
    df['dt_processed'] = str(d)
    df['dt_processed_m'] = str(d.date().replace(day=1))

    # Transform into a Spark DataFrame and append it to the table.
    df = pd.concat(df_list)
    df = pandas_to_spark_string(df, spark)

    df.write.mode('append').format('delta').partitionBy('dt_processed_m').saveAsTable(tb)

    return df






