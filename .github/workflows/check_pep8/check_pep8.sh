#!/bin/bash

# Get the added (A) or modified (M) files.
diff_files="$(
    git diff --name-status origin/main origin/${GITHUB_HEAD_REF} |
    grep -E "^[AM]\s+" |
    grep -E --only-matching "[a-zA-Z0-9_/ \t]+\.py$"
)"

# Returns if there is no modifications.
if [ ${#diff_files} -eq 0 ]; then
    echo "No modifications"
    exit 0
fi

# Assign the files to an array and run pycodestyle.
has_errors=false
for f in ${diff_files[@]}; do
    echo "-----------------------------------" >> $LOG_FILE
    echo "Checking style of ${f}" >> $LOG_FILE
    err=$(pycodestyle --max-line-length 120 --exclude=! --ignore=W,E226,E302,E402,E305 ${f})
    if [ "$?" -ne 0 ]; then
        has_errors=true
        echo "${err}" >> $LOG_FILE
    fi
    echo "-----------------------------------" >> $LOG_FILE
done

# Exits with status 1 in case of errors.
# Write an empty file if there is no errors.
if [ $has_errors = true ]; then
    exit 1
fi

:> $LOG_FILE