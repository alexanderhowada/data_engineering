FROM python:3-slim

WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install git default-jre -y
RUN git config --global --add safe.directory /app
RUN pip3 install -r .github/workflows/unittest/requirements.txt

ENTRYPOINT ["bash", ".github/workflows/unittest/unittest.sh"]