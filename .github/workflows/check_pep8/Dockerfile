FROM python:3-slim

WORKDIR /app
COPY . /app

RUN apt-get update && apt-get install git -y
RUN git config --global --add safe.directory /app
RUN pip3 install -r .github/workflows/check_pep8/requirements.txt

ENTRYPOINT ["bash", ".github/workflows/check_pep8/check_pep8.sh"]