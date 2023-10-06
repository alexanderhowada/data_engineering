#!/bin/bash

set -e

# Deploy EMR dependencies what will be copied in the next container.
DOCKER_BUILDKIT=1 docker build -f aws/aws_emr_serverless/DockerfileDependencies --output . .

docker build -f .github/workflows/aws_deploy/Dockerfile -t aws-deploy --no-cache .
docker run --privileged=true \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
    -e AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION}" \
    -e TF_VAR_AWS_LAMBDA_ROLE="${AWS_LAMBDA_ROLE}" \
    --dns 8.8.8.8 --dns 8.8.4.4 \
    aws-deploy