# General data engineering projects and drafts.

Under construction. This repo implements several scripts for testing concepts.
Currently, this repository only contains general purpose code,
however I plan to deploy a few tests in AWS.
The main projects are:

- airflow: ETLs with airflow. Will not be implemented in favor of prefect.
- audio: tests with audio processing.
- aws: projects in aws (more code will be implemented in the near future).
- clima_tempo: implementation of the clima tempo (weather) api.
- databricks: utilities for Databricks. Does not implements tests since it requires a databricks deploy.
- drafts: random drafts and code examples.
- postgres: utilities for postgres.
- prefect: examples using prefect.
- random_person: implementation ETLs using the random person api.
- scraping: contains several scraping projects.
- supermetrics: implementation of ETLs using the supermetrics API.
- utils: contains several multipurpose scripts that can be used in several distinct projects.

# CI

The <code>.github/workflows</code> implements my concept of CI using GitHub actions.
See <code>.github/workflows/README.md</code> for more details.
