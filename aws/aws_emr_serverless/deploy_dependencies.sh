set -e

DOCKER_BUILDKIT=1 docker build -f DockerfileDependencies --output . .
aws s3 cp pyspark_ge.tar.gz s3://ahow-delta-lake/emr_serverless_packages.tar.gz