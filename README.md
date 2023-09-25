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

1. Run every command (except terraform) from the root directory.

## Environment variables

## Infrastructure

### 1. Bucket

### 2. Lambda

### 3. EMR Server less

### 4. Postgres

## Prefect cloud deployment

<code>docker compose -f prefect_deploy/docker-compose.yaml up --build</code>