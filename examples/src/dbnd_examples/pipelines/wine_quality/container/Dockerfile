FROM python:2.7-slim

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

ARG WORK_DIR=/opt/databand
ARG DATABAND_USER_HOME=/usr/local/databand

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

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
        apt-utils \
        curl \
        rsync \
        netcat \
        locales \
    && sed -i 's/^# en_US.UTF-8 UTF-8$/en_US.UTF-8 UTF-8/g' /etc/locale.gen \
    && locale-gen \
    && update-locale LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8 \
    && useradd -ms /bin/bash -d ${DATABAND_USER_HOME} databand \
    && pip install -U pip setuptools wheel \
    && pip install Cython \
    && pip install pytz \
    && pip install pyOpenSSL \
    && pip install ndg-httpsclient \
    && pip install pyasn1 \
    && pip install celery[redis]==4.1.1

# Some optimizations
RUN pip install \
     "six" \
     "psycopg2-binary" \
     "hjson" \
     "colorlog" \
     "typing" \
     "colorama" \
     "termcolor" \
     "cookiecutter" \
     "more_itertools" \
     "cryptography>=0.9.3" \
     "numpy>=1.14,<1.15.0" \
     "flask-appbuilder<2.0.0,>=1.11.1" \
     "pandas" \
     "scikit-learn" \
     "dill"

# Set up the program in the image
WORKDIR ${WORK_DIR}
COPY . ${WORK_DIR}
RUN chown -R databand: ${DATABAND_USER_HOME} ${WORK_DIR}
EXPOSE 8080
# start model server
#ENTRYPOINT ["./entrypoint.sh"]
ENTRYPOINT ["python", "scoring_server/app.py", "8080"]
