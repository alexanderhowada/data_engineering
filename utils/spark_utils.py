from typing import List

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType

def pandas_to_spark(df: pd.DataFrame, spark_session: SparkSession) -> DataFrame:
    """
    Converts pandas to spark.

    :param df: pandas dataframe
    :param spark_session: PySpark session
    :return: PySpark DataFrame
    """
    spark = spark_session
    df = spark.createDataFrame(df)

    return df

def pandas_to_spark_string(df: pd.DataFrame, spark_session: SparkSession, col_f: List = []) -> DataFrame:
    """
    Converts a Pandas DataFrame to PySpark and converts the datatypes to string.
    :param df: Pandas DataFrame.
    :param col_f: List with functions to convert column names.
    :return: pyspark.DataFrame.
    """

    spark = spark_session

    df = df.astype(str)
    df = spark.createDataFrame(df)

    for c in df.columns:
        df = df.withColumn(c, df[c].cast(StringType()))
        for f in col_f:
            df = df.withColumnRenamed(c, f(c))

    df = df.select(*[F.when((df[c] == 'nan') | (df[c] == '<NA>') | (df[c] == 'None'), None).otherwise(df[c]).alias(c) for c in df.columns])

    return df
