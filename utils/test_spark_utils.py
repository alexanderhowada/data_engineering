import unittest

import pandas as pd
from numpy import nan
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.pip_utils import configure_spark_with_delta_pip

from utils.spark_utils import pandas_to_spark_string

class PandasToSparkString(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        builder = SparkSession.builder \
            .appName('test_delta_utils') \
            .config('spark.sql.warehouse.dir', 'pyspark_tables') \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config('spark.databricks.delta.retentionDurationCheck.enabled', False) \
            .config('spark.databricks.delta.schema.autoMerge.enabled', True) \
            .config('spark.databricks.delta.checkLatestSchemaOnRead', True) \
            .config("spark.log.level", "ERROR") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config('delta.enableChangeDataFeed', True)

        cls.spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
        cls.sc = cls.spark.sparkContext
        cls.sc.setLogLevel("FATAL")

        cls.pdf1 = pd.DataFrame(
            [
                [1, 'Alice', 23],
                [2, 'Bob', 12],
                [3, 'Yoda', None],
                [4, 'God', pd.NA],
                [5, 'Unknown Universe', nan]
            ],
            columns=['id', 'name', 'age']
        )

    def test_simple_transformation(self):

        
        df = pandas_to_spark_string(self.pdf1, spark_session=self.spark)

        for _, t in df.dtypes:
            self.assertEqual(t, 'string')

    def test_simple_transformation(self):
        df = pandas_to_spark_string(self.pdf1, spark_session=self.spark)

        null_count = df.select(F.sum(F.when(df['age'].isNull(), 1).otherwise(0).alias('age'))).collect()[0][0]

        self.assertEqual(null_count, 3)