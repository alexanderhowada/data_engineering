set -e

DOCKER_BUILDKIT=1 docker build -f aws/aws_emr_serverless/DockerfileDependencies --output . .
aws s3 cp pyspark_ge.tar.gz s3://ahow-delta-lake/spark-conf/emr_serverless_packages.tar.gz