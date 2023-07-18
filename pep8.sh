#!/bin/bash

echo "\n--------------------------------------------\n"
for f in $@;
do
    echo "Checking PEP 8 against ${f}"
    pycodestyle --max-line-length 120 --ignore=W,E302,E226 $f
    # pycodestyle --max-line-length 120 --exclude=! --ignore=W,E226,E302,E402,E305 $(ls *.py)
    echo "\n--------------------------------------------\n"
done
