FROM python:3.6-slim


# MySQL dependencies:
# https://github.com/PyMySQL/mysqlclient-python#linux
RUN apt-get update -yqq && \
    apt-get install -y python3-dev default-libmysqlclient-dev build-essential vim wget && \
    pip install -U 'pip>=20,<20.3' mysqlclient

# install dbnd packages:
COPY ./dbnd-core/dist-python/dbnd.requirements.txt \
    ./dbnd-core/dist-python/dbnd-airflow-monitor.requirements.txt \
    ./dbnd-core/dist-python/dbnd-airflow-monitor[[]composer].requirements.txt \
    /dist-python/
RUN pip install -r /dist-python/dbnd.requirements.txt \
    && pip install -r /dist-python/dbnd-airflow-monitor.requirements.txt \
    && pip install -r /dist-python/dbnd-airflow-monitor[composer].requirements.txt

COPY ./dbnd-core/dist-python/databand-*.whl \
    ./dbnd-core/dist-python/dbnd-*.whl \
    ./dbnd-core/dist-python/dbnd_airflow_monitor-*.whl \
    /dist-python/

ENV DBND_VERSION=0.57.4

RUN pip install dbnd==$DBND_VERSION \
    dbnd_airflow_monitor==$DBND_VERSION \
    dbnd_airflow_monitor[composer]==$DBND_VERSION \
    --no-index \
    --find-links /dist-python/

ENV DBND_HOME=/dbnd
WORKDIR ${DBND_HOME}
RUN dbnd project-init

ARG SOURCE_VERSION
ENV DBND__RUN_INFO__SOURCE_VERSION ${SOURCE_VERSION:-""}
