#!/bin/bash

echo "\n--------------------------------------------\n"
for f in $@;
do
    echo "Checking PEP 8 against ${f}"
    pycodestyle --max-line-length 120 --ignore=W,E302,E226 $f
    echo "\n--------------------------------------------\n"
done