FROM python:3-slim

WORKDIR /app
COPY . /app

RUN apt-get -qq update && apt-get install git default-jre -y -qq
RUN git config --global --add safe.directory /app
RUN pip3 install -r .github/workflows/unittest/requirements.txt
RUN pip3 install boto3

ENTRYPOINT ["bash", ".github/workflows/unittest/unittest.sh"]