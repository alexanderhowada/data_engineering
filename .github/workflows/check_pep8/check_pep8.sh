#!/bin/bash

# Get the added or modified files.
diff_files=$(
  git diff --name-only origin/main origin/${GITHUB_HEAD_REF} |
  grep -E "^[AM]\s+" |
  grep -E --only-matching "[a-zA-Z_/ \t]+\.py$"
)

# Returns if there is no modifications.
if [ -n $diff_files ];
then
  echo "No modifications"
  exit 0
fi

# Assign the files to an array and run pycodestyle.
read -d "\n" -a diff_files_array <<< "$diff_files"
for f in "${diff_files_array[@]}"
do
  echo "------------------------------"
  echo "Checking style of ${f}"
  pycodestyle --max-line-length 120 --exclude=! --ignore=W,E226,E302,E402,E305 ${f}
  echo "------------------------------"
done

# Exits with status 1 in case of errors.
failures(){
  echo "PEP 8 problems."
  exit 1
}
trap "failures" ERR

echo "OK!"