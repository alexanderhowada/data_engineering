#!/bin/bash

err_file=pycodestyle_failures

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
for f in "${diff_files[@]}"
do
    echo "-----------------------------------" >> $err_file
    echo "Checking style of ${f}" >> $err_file
    err=$(pycodestyle --max-line-length 120 --exclude=! --ignore=W,E226,E302,E402,E305 ${f})
    if [ ${#diff_files} -gt 0 ]; then
        has_errors=true
        echo "$err" >> $err_file
    fi
    echo "-----------------------------------" >> $err_file
done

# Exits with status 1 in case of errors.
if [ $has_errors=true ]; then
    cat $err_file
    rm -rf $err_file
    exit 1
fi

rm -rf $err_file
echo "OK!"