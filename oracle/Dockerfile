FROM python:3.10

RUN mkdir /app
WORKDIR /app
COPY requirements.txt \
    __init__.py \
    .env \
    long-operator-271320-63d431d61c79.json \
    run.sh \
    install_oracle_client.sh \
    oracle-instantclient-basic_21.11.0.0.0-2_amd64.deb \
    /app/

RUN bash install_oracle_client.sh
RUN pip3 install -r requirements.txt

ENTRYPOINT ["/bin/bash", "run.sh"]
