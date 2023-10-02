import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType, LongType, TimestampType
from delta.pip_utils import configure_spark_with_delta_pip


def get_spark_builder():
    builder = SparkSession.builder \
        .master('local[1]') \
        .appName('first_script.main') \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config('spark.databricks.delta.schema.autoMerge.enabled', True) \
        .config('spark.hadoop.hive.metastore.client.factory.class', 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory') \

    return builder

def get_spark_session(mode='prod'):
    builder = get_spark_builder()

    if mode == 'prod':
        spark = builder.getOrCreate()
    else:
        builder = builder.config("spark.sql.warehouse.dir", mode)
        spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    return spark


def process_batch(df, idx):
    df = df.withColumn(
        'filename', F.input_file_name()
    ).withColumn(
        'dt_extract',
        F.to_timestamp(
            F.regexp_extract(
                F.col('filename'),
                '(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})',
                1
            ),
            "yyyy-MM-dd_HH-mm-SS"
        )
    )

    assert df.count() == 1
    row = df.collect()[0]
    row_cols = ['country', 'id', 'state', 'filename', 'dt_extract']

    df1 = spark.createDataFrame(df.select('data').toPandas().loc[0, 'data'])
    df1 = df1.withColumn('humidity', df1['humidity']['humidity']) \
        .withColumn('pressure', df1['pressure']['pressure']) \
        .withColumn('temperature', df1['temperature']['temperature']) \
        .withColumn('precipitation', df1['rain']['precipitation']) \
        .withColumn('wind_direction', df1['wind']['direction']) \
        .withColumn('wind_directiondegrees', df1['wind']['directiondegrees']) \
        .withColumn('wind_gust', df1['wind']['gust']) \
        .withColumn('wind_velocity', df1['wind']['velocity'])
    for col in row_cols:
        df1 = df1.withColumn(col, F.lit(row[col]))
    df1 = df1.withColumn(
        'dt', F.date_trunc('dd', df1['date'])
    )

    df1.write.partitionBy('dt').format('delta').mode('append').save(TARGET_TB_PATH)

    return df1

def full_delete(checkpoint_location, target_tb_path):
    """Delete checkpoint and delta table. Only works locally with s3fs mounted."""
    import os
    import shutil

    print(f"Erasing {checkpoint_location}")
    shutil.rmtree(checkpoint_location, ignore_errors=True)
    print(f"Erasing {target_tb_path}")
    shutil.rmtree(target_tb_path, ignore_errors=True)

    os.makedirs(checkpoint_location)
    os.makedirs(target_tb_path)

def main(raw_path, checkpoin_location, schema):
    df_stream = spark.readStream \
        .option('maxFilesPertrigger', '1') \
        .option('trigger', 'availablenow') \
        .option('fileNameOnly', 'true') \
        .json(
            path=raw_path,
            multiLine=True, schema=schema,
        )

    sink = df_stream.writeStream \
        .trigger(availableNow=True) \
        .option("checkpointLocation", checkpoin_location) \
        .foreachBatch(process_batch).start()
    sink.awaitTermination()


SCHEMA = StructType([
    StructField('country', StringType(), True),
    StructField('data',
        ArrayType(
            StructType([
                StructField('date', TimestampType(), True),
                StructField('date_br', StringType(), True),
                StructField('humidity', StructType([StructField('humidity', DoubleType(), True)]), True),
                StructField('pressure', StructType([StructField('pressure', DoubleType(), True)]), True),
                StructField('rain', StructType([StructField('precipitation', DoubleType(), True)]), True),
                StructField('temperature', StructType([StructField('temperature', LongType(), True)]), True),
                StructField('wind', StructType([
                    StructField('direction', StringType(), True),
                    StructField('directiondegrees', DoubleType(), True),
                    StructField('gust', DoubleType(), True),
                    StructField('velocity', DoubleType(), True)]), True)
            ])
            , True
        ), True
    ),
    StructField('id', LongType(), True),
    StructField('name', StringType(), True),
    StructField('state', StringType(), True)]
)

if __name__ == '__main__':

    is_test = 'prod'

    # Path examples

    # Se to True to run test locally.
    if True:
        print("Running test")
        is_test = 'test'
        PREFIX = '/home/ahow/MyGitHub/local_bucket'
    else:
        PREFIX = "s3://ahow-delta-lake"

    # RAW_PATH = f'{PREFIX}/raw/clima_tempo/forecast_72/'
    # TARGET_TB_PATH = f'{PREFIX}/delta-lake/clima_tempo/forecast_72/'
    # CHECKPOINT_LOCATION = f'{PREFIX}/raw/clima_tempo/forecast_72/checkpoint'

    RAW_PATH = sys.argv[1]
    TARGET_TB_PATH = sys.argv[2]
    CHECKPOINT_LOCATION = sys.argv[3]

    # full_delete(CHECKPOINT_LOCATION, TARGET_TB_PATH)

    spark = get_spark_session(is_test)
    main(RAW_PATH, CHECKPOINT_LOCATION, SCHEMA)

