FROM docker.tech.beegroup-cimne.com/base_dockers/enma-jobs as cached
USER root
RUN apt-get update
RUN python3 -m pip install --upgrade pip
WORKDIR datadis
RUN chown -R ubuntu .
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt
USER ubuntu
COPY --chown=ubuntu . .
CMD ["python3"]
