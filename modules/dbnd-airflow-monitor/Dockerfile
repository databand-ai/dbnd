FROM python:3.6-slim


# MySQL dependencies:
# https://github.com/PyMySQL/mysqlclient-python#linux
RUN apt-get update -yqq && \
    apt-get install -y python3-dev default-libmysqlclient-dev build-essential vim wget && \
    pip install -U 'pip>=20,<20.3' mysqlclient

# install dbnd packages:
COPY ./dist/dbnd.requirements.txt \
    ./dist/dbnd-airflow-monitor.requirements.txt \
    ./dist/dbnd-airflow-monitor[[]composer].requirements.txt \
    /dist/
RUN pip install -r /dist/dbnd.requirements.txt \
    && pip install -r /dist/dbnd-airflow-monitor.requirements.txt \
    && pip install -r /dist/dbnd-airflow-monitor[composer].requirements.txt

COPY ./dist/databand-*.whl \
    ./dist/dbnd-*.whl \
    ./dist/dbnd_airflow_monitor-*.whl \
    /dist/

ENV DBND_VERSION=0.51.4

RUN pip install dbnd==$DBND_VERSION \
    dbnd_airflow_monitor==$DBND_VERSION \
    dbnd_airflow_monitor[composer]==$DBND_VERSION \
    --no-index \
    --find-links /dist/

ENV DBND_HOME=/dbnd
WORKDIR ${DBND_HOME}
RUN dbnd project-init

ARG SOURCE_VERSION
ENV DBND__RUN_INFO__SOURCE_VERSION ${SOURCE_VERSION:-""}
