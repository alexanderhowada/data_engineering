#!/bin/bash

# This script must be executed from the root Git directory.
set -e

PACKAGE=.github/workflows/aws_deploy/package/python
mkdir -p ${PACKAGE}

bash create_wheel.sh

pip3 install \
    --platform manylinux2014_x86_64 \
    --target=${PACKAGE}/. \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all: --upgrade \
    dist/data_engineering-0.1-py3-none-any.whl

pip3 install \
    --platform manylinux2014_x86_64 \
    --target=${PACKAGE}/. \
    --implementation cp \
    --python-version 3.11 \
    --only-binary=:all: --upgrade \
    -r aws/aws_lambda/requirements.txt

cd ${PACKAGE}/..
zip -r lambda_layer.zip python
cd ..
mv package/lambda_layer.zip .
zip mock.zip mock.py

rm -rf package