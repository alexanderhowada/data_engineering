# Data engineering projects and drafts.

Under construction. This repo implements several scripts for testing concepts.
Currently, this repository contains general purpose code
and a few deployments in AWS (see [AWS Project](#aws-project)).
The main projects are:

- airflow: Contains a few tests with Airflow, however I will not develop it favor of prefect.
- audio: tests with audio processing.
- aws: projects in aws (more code will be implemented in the near future).
  - lambda: contains implementations for lambda functions.
  - aws_emr_serverless: contains scripts and examples to run scripts in AWS EMR serverless.
  - tests: contains a few scripts used for manual testing.
  - CD: the <code>.github/workflows/aws_deploy</code> implements the deployment to AWS.
- clima_tempo: implementation of the clima tempo (weather) api.
- databricks: utilities for Databricks. Does not implements tests since it requires a databricks deploy.
- drafts: random drafts and code examples.
- postgres: utilities for postgres.
- prefect_deploy: examples and deployments using prefect.
  - aws: implements pipelines for AWS.
- random_person: implementation ETLs using the random person api.
- scraping: contains several scraping projects.
- supermetrics: implementation of ETLs using the supermetrics API.
- utils: contains several multipurpose scripts that can be used in several distinct projects.

# CI

The <code>.github/workflows</code> implements my concept of CI using GitHub actions.
See <code>.github/workflows/README.md</code> for more details.

# AWS Project

## Overview

This project contains an exemple deployment of an ETL using AWS lambda and EMR Serverless for processing,
and delta tables in S3 buckets for storage.
To orchestrate the pipeline, we use prefect cloud with and agent running locally.
The idea is to build a backend pipeline that will use the AWS API alongside prefect to orchestrate the pipeline.

Why I am using these technologies?
- lambda: its cheap and good for processing data.
- EMR Serverless: it runs Spark, hence it is excellent for processing large datasets, and it is not tied to AWS.
                  However, later on, I will be integrating this deployment with other solutions such as Glue and SageMaker.
- Delta tables: delta tables are cheap (stored in S3), and its data can be easily manipulated from any other place,
                since storage and processing is separated.
                This makes easy to use my own computer to prototype the pipeline.

## CI/CD

Alongside this pipeline, I also implemented a primitive CI/CD process.
The CI has two steps, and it is triggered on a PR:
1. Check if all .py scripts follow PEP 8
2. Run unittests

The CD process is not automated (yet), but there are scripts to deploy everything.
The CD process can be summarized in:
- lambda functions:
  1. Terraform deploy a layer with all the dependencies.
  2. Terraform creates the functions with this layer.
  3. Remember to taint the functions if redeploying them.
- EMR:
  1. Deploy AWS scripts to S3
  2. Create .zip file with all Python dependencies. 
- Prefect
  1. Every script deploys itself in the prefect cloud.
  2. Start the prefect agent and the deployments are good to go!

For more information, see the sections below.

Note: please, run every command (except terraform) from the root directory.

## Environment variables

Not all variables are required,
however these were the defined environment variables for the full execution of the project. 

- ACCESS_KEY
- SECRET_ACCESS_KEY
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_DEFAULT_REGION
- AWS_ECR_REPOSITORY
- AWS_ECR_URI
- AWS_LAMBDA_ROLE
- AWS_LAMBDA_BUCKET
- AWS_ROOT_USER
- AWS_EMR_SERV_APP_ID
- AWS_EMR_SERV_EXEC_ROLE
- AWS_EMR_BUCKET
- TF_VAR_AWS_LAMBDA_ROLE=${AWS_LAMBDA_ROLE}
- CLIMATEMPO_TOKEN
- PREFECT_KEY
- PREFECT_WORKSPACE
- CURRENT_WORKDIR=$(pwd)

## Infrastructure

### Bucket

Nothing to comment, it is just a bucket with a few files.

### Lambda

Every subfolder in aws/aws_lambda is an implementation of an AWS lambda function,
alongside its tests.
To make things easy and systematic, every entrypoint should be written in aws/aws_lambda/function_name/main.py,
and the entrypoint function should be named main.

To comply with the CI
- every Python code should follow PEP 8,
- every function should be tested,
- every folder should contain <code>__init__.py</code>,
- the tests scripts should be written using <code>unittest</code> and have <code>test_</code> as its prefix.

The CD process has two steps:
1. preparing the lambda layer
2. deploying the layer and functions.

To prepare the lambda layer,
update the aws/aws_lambda/requirements.txt file,
and run <code>.github/workflows/aws_deploy/prepare_layer.sh</code>.
This script will create a zip file with the dependencies in the requirements file
and will also deploy this repository (in its current branch).

To deploy, <code>cd</code> to <code>.github/workflows/aws_deploy</code> and:
```
# terraform taint if needed
terraform plan
terraform apply
```

### EMR Serverless

The EMR Serverless scripts are at <code>aws/aws_emr_serverless</code>.

The CI follows the same steps as in Lambda.
The CD is as follows:
1. Prepare dependencies of spark config:
        update the <code>aws/aws_emr_serverless/requirements.txt</code> file
        and run <code>aws/aws_emr_serverless/deploy_dependencies.sh</code>.
2. Copy the scripts to S3: run <code>.github/workflows/aws_deploy/copy_repository.sh</code>

## Prefect cloud deployment

Once the AWS infrastructure is deployed (Lambda and EMR + CD),
we can use Prefect to call and orchestrate the pipeline.

As it stands now, every flow implementation should deploy the flow if the script is executed.
Hence, to deploy a flow, run the deployment script such as <code>prefect_deploy/aws/pipelines/forecast_72.py</code>

To run the agent, run the following command from the root directory.
<code>docker compose -f prefect_deploy/docker-compose.yaml up --build</code>

Now you can execute the flow through Prefect.