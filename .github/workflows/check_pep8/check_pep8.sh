#!/bin/bash

git branch -a
git status

diff_files=$(
  git diff --name-status main |
  grep -E "^[AM]\s+" |
  grep -E --only-matching "[a-zA-Z_/ \t]+\.py$"
)
read -d "\n" -a diff_files_array <<< "$diff_files"

for f in "${diff_files_array[@]}"
do
  echo "------------------------------"
  echo "Checking style of ${f}"
  pycodestyle --max-line-length 120 --exclude=! --ignore=W,E226,E302,E402,E305 ${f}
  echo "------------------------------"
done