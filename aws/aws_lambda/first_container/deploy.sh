#!/bin/bash

REGION=${AWS_DEFAULT_REGION}
CONTAINER_NAME=first_container
IMAGE_URI="${AWS_ECR_URI}/${AWS_ECR_REPOSITORY}:${CONTAINER_NAME}"

# login into docker
aws ecr-public get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${AWS_ECR_URI}

docker build -t "${AWS_ECR_REPOSITORY}:${CONTAINER_NAME}" . --no-cache
docker tag "${AWS_ECR_REPOSITORY}:${CONTAINER_NAME}" ${IMAGE_URI}
docker push ${IMAGE_URI}

aws lambda create-function \
    --function-name ${CONTAINER_NAME} \
    --package-type Image \
    --code ImageUri=${IMAGE_URI} \
    --role ${AWS_LAMBDA_ROLE}

