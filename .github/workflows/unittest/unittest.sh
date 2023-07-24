#!/bin/bash

# Example command.
# GITHUB_HEAD_REF=main LOG_FILE=.github/workflows/docker_logs/unittest.log bash .github/workflows/unittest/unittest.sh
git checkout origin/$GITHUB_HEAD_REF

python3 -m unittest discover -s . -v &> $LOG_FILE

if [ "$?" -eq 0 ]; then
    :> $LOG_FILE
fi

exit 1