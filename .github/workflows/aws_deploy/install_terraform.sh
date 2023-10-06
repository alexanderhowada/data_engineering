#!/bin/bash

# Intall terraform
# See https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli

set -e

sudo apt-get update -y && sudo apt-get install -y gnupg software-properties-common -y
wget -O- https://apt.releases.hashicorp.com/gpg | \
    gpg --dearmor | \

sudo tee /usr/share/keyrings/hashicorp-archive-keyring.gpg
echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
    https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
    sudo tee /etc/apt/sources.list.d/hashicorp.list

 sudo apt-get update -y && sudo apt-get install terraform -y