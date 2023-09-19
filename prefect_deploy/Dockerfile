FROM ubuntu:jammy

ARG CURRENT_WORKDIR
ARG PREFECT_KEY
ARG PREFECT_WORKSPACE

WORKDIR ${CURRENT_WORKDIR}
COPY aws ./aws
COPY clima_tempo ./clima_tempo
COPY prefect_deploy ./prefect_deploy
COPY utils ./utils

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install python3-pip -y
RUN DEBIAN_FRONTEND=noninteractive TZ=America/Sao_Paulo apt-get -y install tzdata
RUN pip3 install -r prefect_deploy/requirements.txt

RUN prefect cloud login --key ${PREFECT_KEY} --workspace=${PREFECT_WORKSPACE}

ENTRYPOINT ["prefect", "agent", "start", "--pool", "default-agent-pool"]