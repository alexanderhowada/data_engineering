#!/bin/bash

# Example command.
# GITHUB_HEAD_REF=main LOG_FILE=.github/workflows/docker_logs/unittest.log bash .github/workflows/unittest/unittest.sh
git checkout $GITHUB_HEAD_REF

python3 -m unittest discover -s . -v &> $LOG_FILE

err=$(cat $LOG_FILE | grep -E "^FAILED \(errors=[0-9]+\)")
if [ ${#err} -gt 0 ]; then
    exit 1
fi

:> $LOG_FILE