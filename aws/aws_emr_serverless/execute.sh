JOB_NAME="first_job8"
ENTRYPOINT=$1

aws emr-serverless start-job-run \
    --application-id ${AWS_EMR_SERV_APP_ID} \
    --execution-role-arn ${AWS_EMR_SERV_EXEC_ROLE} \
    --execution-timeout-minutes 15 \
    --name ${JOB_NAME} \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://ahow-delta-lake/repositories/'"${ENTRYPOINT}"'",
            "entryPointArguments": [],
            "sparkSubmitParameters": "--conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory --conf spark.jars=s3://ahow-delta-lake/spark-conf/delta-core_2.12-2.4.0.jar,s3://ahow-delta-lake/spark-conf/delta-storage-2.4.0.jar --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.driver.cores=1 --conf spark.driver.memory=2g --conf spark.executor.instances=1 --conf spark.archives=s3://ahow-delta-lake/spark-conf/emr_serverless_packages.tar.gz#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
        }
    }'
