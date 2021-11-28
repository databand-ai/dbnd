ARG DOCKER_IMAGE_BASE=us.gcr.io/dbnd-dev-260010/databand/dbnd-airflow.base:py36-airflow-1-10-10-latest
FROM ${DOCKER_IMAGE_BASE}

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8
ENV VIRTUALENV_NO_DOWNLOAD yes
ENV HOMEPATH /builds/databand/databand

# should be executed as root user:
USER root
RUN apt-get update -yqq && apt-get install -y vim
USER airflow

ARG AIRFLOW_FOLDER=/usr/local/airflow
ARG PLUGINS_FOLDER=${AIRFLOW_FOLDER}/plugins

# Create plugin folder and copy plugin file into it (airflow will search for this folder when starting)
RUN mkdir -p ${PLUGINS_FOLDER}
COPY ./dbnd-core/modules/dbnd-airflow/src/dbnd_airflow/export_plugin/dbnd_airflow_export_plugin.py ${PLUGINS_FOLDER}

COPY ./dbnd-core/dist-python/dbnd.requirements.txt /dist_req/
RUN pip install -r /dist_req/dbnd.requirements.txt --user
COPY ./dbnd-core/dist-python/databand-*.whl \
    ./dbnd-core/dist-python/dbnd-*.whl \
    /dist-python/

ARG DBND_VERSION=0.54.2

RUN pip install databand==$DBND_VERSION --user --find-links /dist-python/
RUN touch project.cfg

# Debug
RUN ls ${AIRFLOW_FOLDER}
RUN ls ${PLUGINS_FOLDER}
