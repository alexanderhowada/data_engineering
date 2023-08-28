import sys
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip

def get_spark_builder():
    builder = SparkSession.builder \
        .master('local[1]') \
        .appName('first_script.main') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('spark.databricks.delta.schema.autoMerge.enabled', True)
    return builder

def get_spark_session(mode='prod'):
    builder = get_spark_builder()

    if mode == 'prod':
        builder = builder.config("hive.metastore.warehouse.dir", "s3://ahow-delta-lake/delta-lake")
        spark = builder.getOrCreate()
    else:
        builder = builder.config("spark.sql.warehouse.dir", mode)
        spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    return spark


def main(spark):

    spark.sql("CREATE DATABASE IF NOT EXISTS first_database")

    df = spark.createDataFrame(
        [
            [1, 3.14, ""],
            [2, 2.00, "two!"],
            [3, 2.71, "e?"]
        ],
        schema=['id', 'value', 'description']
    )
    df.write.format('delta').mode('overwrite').saveAsTable('first_database.first_table')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        spark = get_spark_session(mode=sys.argv[2])
    else:
        spark = get_spark_session(mode='prod')
    main(spark)

