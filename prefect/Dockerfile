FROM ubuntu:jammy

WORKDIR /app
ARG PREFECT_KEY
ARG PREFECT_WORKSPACE

RUN apt-get update -y
RUN apt-get upgrade -y
RUN apt-get install python3-pip -y
RUN DEBIAN_FRONTEND=noninteractive TZ=America/Sao_Paulo apt-get -y install tzdata

RUN pip3 install prefect
RUN prefect cloud login --key ${PREFECT_KEY} --workspace=${PREFECT_WORKSPACE}

ENTRYPOINT ["prefect", "agent", "start", "--pool", "default-agent-pool"]