FROM python:3.6-slim
MAINTAINER michael.gelfand@databand.ai

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

ARG DATABAND_USER_HOME=/usr/local/databand
ARG AIRFLOW_VERSION=1.10.3
# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

COPY etc/build_docker/generate_requirements.py /generate_requirements.py
COPY ./setup.py  /setup.py
RUN python ./generate_requirements.py /setup.py k8s,aws,gcp,mysql,examples /generated_requirements.txt

# Common packages
RUN set -ex \
    && buildDeps=' \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
        git \
    ' \
    && apt-get update -yqq \
    && apt-get upgrade -yqq \
    && apt-get install -yqq --no-install-recommends \
        $buildDeps \
        python-pip \
        python-requests \
        default-libmysqlclient-dev \
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${DATABAND_USER_HOME} databand \
    && pip install -U pip==18.1 setuptools wheel \
    && pip install Cython \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install -r /generated_requirements.txt \
    && apt-get purge --auto-remove -yqq $buildDeps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base



ADD . /databand
RUN pip install /databand[k8s,aws,gcp,mysql,examples] && rm -rf /databand

ADD tests/scenarios/projects/databand_example_subproject /databand_example_subproject

WORKDIR  /databand_example_subproject

#
#COPY etc/build_docker/entrypoint.sh /entrypoint.sh
#
#ADD ./tests/scenarios/projects/databand_example_subproject /app
#


