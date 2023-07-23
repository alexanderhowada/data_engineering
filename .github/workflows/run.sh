#!/bin/bash

# Docker compose that will execute all tests.
# Each container is responsible for a single type of test.
# The test (container) must generate a .log file in the folder LOG_FOLDER.
# The log file must be empty if there is no error.
# Note that the environment variables from Github actions.

set -e

export BASE_PATH=$(pwd)
export LOG_FOLDER=".github/workflows/docker_logs"

mkdir $LOG_FOLDER

docker compose -f "${BASE_PATH}/.github/workflows/docker-compose.yml" up --build

# Read all logs
has_error=false
logs=$(find .github/workflows/ -name "*.log")
for l in "${logs[@]}"; do
    s=$(cat ${l})
    if [ "${#s}" -gt 0 ]; then
        has_error=true
    fi

    echo "--------------------------------------------------------------------------------"
    echo "--------------------------------------------------------------------------------"
    echo "Reading ${l}"
    echo "--------------------------------------------------------------------------------"
    echo "${s}"
    echo "--------------------------------------------------------------------------------"
done

# Erase LOG_FOLDER and logs
sudo chown -R $(whoami):$(whoami) ${LOG_FOLDER}
rm -rf ${LOG_FOLDER}/*.log
rmdir ${LOG_FOLDER}

if [ $has_error=true ]; then
    exit 1
fi