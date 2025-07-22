FROM python:3.10-slim-bookworm
RUN apt-get update
RUN apt-get install -y git
RUN apt-get install -y gcc
RUN apt-get install -y g++
WORKDIR /datadis
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
ADD . .

#  docker buildx build --platform linux/amd64,linux/arm64 --push -t 1l41bgc7.c1.gra9.container-registry.ovh.net/beegroup/datadis_ingestor .
