import unittest

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DateType
from pyspark.sql.utils import AnalysisException
from delta.pip_utils import configure_spark_with_delta_pip
import pyspark.sql.functions as F

from delta_utils import table_exists, generate_where_clause, merge


class BaseTest(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(BaseTest, self).__init__(*args, **kwargs)

        self.init_spark()
        self.load_dfs()


    def init_spark(self):

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

        self.spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
        self.sc = self.spark.sparkContext
        self.sc.setLogLevel("FATAL")

    def load_dfs(self):

        self.df1 = self.spark.createDataFrame(
            [
                [1, 'bob'],
                [2, 'alice'],
                [3, 'charlie'],
                [4, 'richard'],
                [5, 'qwer']
            ],
            schema=['id', 'name']
        )

        self.df2 = self.spark.createDataFrame(
            [
                [1, 'BOB'],
                [3, 'CHARLIE JR.'],
                [5, 'QWER']
            ],
            schema=['id', 'name']
        )

        self.df3 = self.spark.createDataFrame(
            [
                [1, 'Billy', "Once I was bob, then BOB. But now I'm Billy!"],
                [6, 'Gandalf', "You shall not pass!"],
            ],
            schema=['id', 'name', 'quote']
        )

        self.df4 = self.spark.createDataFrame(
            [
                [7, 'Yoda', "Do. Or do not. There is no try."]
            ],
            schema=['id', 'name', 'quote']
        )

        s = StructType([
            StructField('id', LongType(), False),
            StructField('name', StringType(), True)
        ])
        self.df_empty1 = self.spark.createDataFrame([], schema=s)

        self.df_dt = self.spark.createDataFrame(
            [(1, '2022-01-01'), (2, '2022-01-02'), (3, '2022-01-03')],
            schema=['id', 'dt']
        )
        self.df_dt = self.df_dt.withColumn('date', F.to_date(self.df_dt['dt'], 'yyyy-MM-dd'))

        self.df_ts = self.spark.createDataFrame(
            [(1, '2022-03-01 12:23:49'),(2, '2022-03-02 13:01:32'),(3, '2022-03-03 14:00:00'),(4, '2022-03-04 15:00:00'),(5, '2022-03-05 16:00:00')],
            schema=['id', 'ts']
        )
        self.df_ts = self.df_ts.withColumn('ts', F.to_timestamp(self.df_ts['ts'], 'yyyy-MM-dd HH:mm:ss'))


class TestMerge(BaseTest):

    database = 'test_merge'
    test_table = f'{database}.test'

    def setUp(self):
        self.spark.sql(f'CREATE DATABASE IF NOT EXISTS {self.database}')

    def tearDown(self):
        self.spark.sql(f'DROP DATABASE IF EXISTS {self.database} CASCADE')

    def get_count(self):
        df = self.spark.read.format('delta').table(self.test_table)
        return df.count()

    def count_distinct(self):
        df = self.spark.read.format('delta').table(self.test_table)
        cd = df.select(*[F.countDistinct(df[c]).alias(c) for c in df.columns])
        return cd.toPandas().to_dict(orient='records')[0]

    def test_create_table(self):
        merge(self.df1, self.test_table, pk=['id'], spark_session=self.spark)

        df = self.spark.read.format('delta').table(self.test_table)

        self.assertEqual(df.exceptAll(self.df1).count(), 0)
        self.assertEqual(df.count(), self.df1.count())
        self.assertEqual(len(df.columns), len(self.df1.columns))

    def test_df1_merge_twice(self):
        merge(self.df1, self.test_table, pk=['id'], spark_session=self.spark)
        merge(self.df1, self.test_table, pk=['id'], spark_session=self.spark)

        df = self.spark.read.format('delta').table(self.test_table)
        h_df = self.spark.sql(f'DESCRIBE HISTORY {self.test_table}')

        self.assertEqual(df.exceptAll(self.df1).count(), 0)
        self.assertEqual(df.count(), self.df1.count())
        self.assertEqual(len(df.columns), len(self.df1.columns))
        self.assertEqual(h_df.count(), 2)

    def test_df1_merge_partition(self):
        merge(self.df1, self.test_table, pk=['id'], spark_session=self.spark, partition=['id'])
        merge(self.df2, self.test_table, pk=['id'], spark_session=self.spark, partition=['id'])

        ids = tuple(self.df2.select('id').rdd.flatMap(lambda x: x).collect())
        df = self.spark.read.format('delta').table(self.test_table).filter(f'id in {ids}')
        h_df = self.spark.sql(f'DESCRIBE HISTORY {self.test_table}')

        self.assertEqual(df.exceptAll(self.df2).count(), 0)
        self.assertEqual(df.count(), self.df2.count())
        self.assertEqual(len(df.columns), len(self.df1.columns))
        self.assertEqual(h_df.count(), 2)

        om = h_df.select('operationMetrics').rdd.flatMap(lambda x: x).collect()

        self.assertEqual(om[0]['numTargetFilesAdded'], '3')
        self.assertEqual(om[0]['numTargetFilesRemoved'], '3')
        self.assertEqual(om[1]['numFiles'], '5')

    def test_df1_merge_schema_evolution(self):
        merge(self.df1, self.test_table, pk=['id'], spark_session=self.spark)
        merge(self.df3, self.test_table, pk=['id'], spark_session=self.spark)

        for i in [1, 6]:
            df = self.spark.read.format('delta').table(self.test_table).filter(f'id = {i}')
            df3 = self.df3.filter(f'id = {i}')
            self.assertEqual(df.exceptAll(df3).count(), 0)
            self.assertEqual(df.count(), df3.count())
            self.assertEqual(len(df.columns), len(self.df3.columns))

        df = self.spark.read.format('delta').table(self.test_table)
        nulls = df.select(F.sum(F.when(df['quote'].isNull(), 1).otherwise(0))).rdd.flatMap(lambda x: x).collect()[0]
        self.assertEqual(nulls, 4)


class TableExists(BaseTest):

    def test_not_exists(self):

        self.assertFalse(table_exists('asdf.pqiwuerpqoiweurpqweuir', self.spark))

    def test_exists(self):

        self.addCleanup(self.cleanup_test_exists)

        self.spark.sql('CREATE DATABASE IF NOT EXISTS test_delta_utils')
        self.spark.sql("""
        CREATE OR REPLACE TABLE test_delta_utils.test_exists(
            col1 string, col2 bigint
        )
        USING DELTA
        """)

        self.assertTrue(table_exists('test_delta_utils.test_exists', self.spark))

    def cleanup_test_exists(self):
        self.spark.sql("DROP DATABASE test_delta_utils CASCADE")


class TestGenerateWhereClause(BaseTest):

    def test_empty1(self):
        self.assertEqual(generate_where_clause(self.df_empty1, []), [])

    def test_empty2(self):
        self.assertEqual(generate_where_clause(self.df_empty1, ['id']), [])

    def test_empty3(self):
        with self.assertRaises(AnalysisException) as e:
            generate_where_clause(self.df_empty1, ['fdsa'])

    def test_df1_id(self):

        c = generate_where_clause(self.df1, ['id'])
        self.assertEqual(c, ["id in (1, 2, 3, 4, 5)"])

    def test_df1_id_1(self):

        df = self.df1.filter('id = 1')
        c = generate_where_clause(df, ['id'])
        self.assertEqual(c, ["id = 1"])
    def test_df1_name(self):

        c = generate_where_clause(self.df1, ['name'])
        self.assertEqual(c, ["name in ('bob', 'alice', 'charlie', 'richard', 'qwer')"])

    def test_df1_name_1(self):

        df = self.df1.filter('id = 1')
        c = generate_where_clause(df, ['name'])
        self.assertEqual(c, ["name = 'bob'"])

    def test_df_dt(self):

        c = generate_where_clause(self.df_dt, ['id', 'dt'])
        self.assertEqual(c, ['id in (1, 2, 3)', "dt in ('2022-01-01', '2022-01-02', '2022-01-03')"])

    def test_df_dt_1(self):

        df = self.df_dt.filter('id = 2')
        c = generate_where_clause(df, ['id', 'dt'])
        self.assertEqual(c, ['id = 2', "dt = '2022-01-02'"])

    def test_df_ts(self):

        df = self.df_ts.filter('id in (1,2,3)')
        c = generate_where_clause(df, ['id', 'ts'])
        self.assertEqual(c, ['id in (1, 2, 3)', "ts in ('2022-03-01 12:23:49', '2022-03-02 13:01:32', '2022-03-03 14:00:00')"])

    def test_df_ts_1(self):

        df = self.df_ts.filter('id in (3)')
        c = generate_where_clause(df, ['id', 'ts'])
        self.assertEqual(c, ['id = 3', "ts = '2022-03-03 14:00:00'"])


if __name__ == '__main__':

    unittest.main()

    # bt = BaseTest()
    # bt.df1.show()
    # bt.df2.show()
    # bt.df3.show()