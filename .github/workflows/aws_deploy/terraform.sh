#!/bin/bash

set -e

cd .github/workflows/aws_deploy

terraform init
terraform destroy -auto-approve
terraform plan
terraform apply -auto-approve