#!/bin/bash

set -e

WORKFLOW_DIR=.github/workflows/aws_deploy

# Deploy lambda
bash ${WORKFLOW_DIR}/prepare_layer.sh
bash ${WORKFLOW_DIR}/terraform.sh

# Deploy EMR Serverless
bash ${WORKFLOW_DIR}/copy_repository.sh
bash aws/aws_emr_serverless/deploy_dependencies.sh