FROM ubuntu:latest

WORKDIR /app
COPY ./.github .github
COPY . .

# Update and install pip
RUN apt update && apt upgrade -y
RUN apt install python3-pip sudo wget zip -y

# Install docker
# RUN bash .github/workflows/install_docker.sh

# Install AWS cli
RUN pip3 install awscli

# Install terraform
RUN bash .github/workflows/aws_deploy/install_terraform.sh

ENTRYPOINT ["bash", ".github/workflows/aws_deploy/deploy.sh"]