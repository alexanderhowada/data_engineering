import sys
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip

def get_spark_builder():
    builder = SparkSession.builder \
        .master('local[1]') \
        .appName('first_script.main') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('spark.databricks.delta.schema.autoMerge.enabled', True) \
        .config(
            'spark.hadoop.hive.metastore.client.factory.class',
            'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
        )

    return builder

def get_spark_session(mode='prod'):
    builder = get_spark_builder()

    if mode == 'prod':
        spark = builder.getOrCreate()
    else:
        builder = builder.config("spark.sql.warehouse.dir", mode)
        spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    return spark


def main(spark):
    df = spark.createDataFrame(
        [
            [4, 3.14, ""],
            [5, 2.00, "two!"],
            [6, 2.71, "e?"]
        ],
        schema=['id', 'value', 'description']
    )
    df.write.format('delta').mode('append').save('s3://ahow-delta-lake/first_database/first_table')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        spark = get_spark_session(mode=sys.argv[2])
    else:
        spark = get_spark_session(mode='prod')
    main(spark)

