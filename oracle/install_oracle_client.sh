#!/bin/bash

set -e

apt update -y && apt install libaio1 alien -y

# Downloads the latest oracle file and install it.
wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm
f_name=$(alien --to-deb oracle-instantclient-basic-linuxx64.rpm)
f_name=$(echo $f_name | sed -E 's/\sgenerated//')
apt install ./$f_name -y