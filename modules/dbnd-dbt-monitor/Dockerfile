FROM us.gcr.io/dbnd-dev-260010/databand/dbnd-python.base:py39-slim-v5

# constants:
ENV DBND_HOME=/app
ARG DBND_USER=databand
ARG DBND_GROUP=databand
ARG DBND_UID=11000
ARG DBND_GID=11000

RUN groupadd -g ${DBND_GID} ${DBND_GROUP} && \
    useradd -u ${DBND_UID} -g ${DBND_GID} -d ${DBND_HOME} ${DBND_USER}
WORKDIR ${DBND_HOME}
# https://docs.openshift.com/container-platform/4.11/openshift_images/create-images.html
# see "Support arbitrary user ids"
RUN chown -R ${DBND_USER}:${DBND_GROUP} ${DBND_HOME} && \
    chgrp -R 0 ${DBND_HOME} && chmod -R g=u ${DBND_HOME}
CMD dbnd tracker wait && dbnd-dbt-monitor

# pre-install requirements:
COPY ./dbnd-core/modules/dbnd/dbnd.requirements.txt \
    ./dbnd-core/dist-python/dbnd-airflow-monitor.requirements.txt \
    ./dbnd-core/dist-python/dbnd-dbt-monitor.requirements.txt \
    /dist-databand/req/
RUN pip install -r /dist-databand/req/dbnd.requirements.txt && \
    pip install -r /dist-databand/req/dbnd-airflow-monitor.requirements.txt && \
    pip install -r /dist-databand/req/dbnd-dbt-monitor.requirements.txt

# install dbnd and dbnd-web from source code
COPY ./dbnd-core/modules/dbnd/ /dist-databand/dbnd/
COPY ./dbnd-core/modules/dbnd-airflow-monitor/ /dist-databand/dbnd-airflow-monitor/
COPY ./dbnd-core/modules/dbnd-dbt-monitor/ /dist-databand/dbnd-dbt-monitor/

# replace symlinks with original files
RUN rm /dist-databand/dbnd/LICENSE \
    /dist-databand/dbnd/setup.cfg \
    /dist-databand/dbnd-airflow-monitor/LICENSE \
    /dist-databand/dbnd-airflow-monitor/setup.cfg \
    /dist-databand/dbnd-dbt-monitor/LICENSE \
    /dist-databand/dbnd-dbt-monitor/setup.cfg
COPY ./dbnd-core/LICENSE ./dbnd-core/setup.cfg /dist-databand/dbnd/
COPY ./dbnd-core/LICENSE ./dbnd-core/setup.cfg /dist-databand/dbnd-airflow-monitor/
COPY ./dbnd-core/LICENSE ./dbnd-core/setup.cfg /dist-databand/dbnd-dbt-monitor/
RUN pip install /dist-databand/dbnd /dist-databand/dbnd-airflow-monitor /dist-databand/dbnd-dbt-monitor --no-index

# remove pip for security reasons
RUN pip uninstall -y pip

USER ${DBND_UID}

# versioning:
ARG SOURCE_VERSION
ENV DBND__RUN_INFO__SOURCE_VERSION ${SOURCE_VERSION:-""}
