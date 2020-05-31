#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

from configparser import ConfigParser

import setuptools


HERE = os.path.abspath(os.path.dirname(__file__))


def read_setup_cfg():
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    config_file = os.path.join(HERE, "setup.cfg")
    cp = ConfigParser()
    cp.read([config_file])
    return cp


setup_cfg = read_setup_cfg()
VERSION = setup_cfg.get("metadata", "version")
print(setup_cfg)


def dbnd_package(name):
    return "%s==%s" % (name, VERSION)


EXTRAS_REQUIRE = {
    "airflow": [
        dbnd_package("dbnd-airflow"),
        "psycopg2>=2.7.4,<2.8",
        "apache-airflow==1.10.7",
        "cattrs==1.0.0",  # airflow requires ~0.9 but it's py2 incompatible (bug)
    ],
    "airflow-1_10_7": [
        dbnd_package("dbnd-airflow"),
        "psycopg2>=2.7.4,<2.8",
        "apache-airflow==1.10.7",
        "cattrs==1.0.0",  # airflow requires ~0.9 but it's py2 incompatible (bug)
    ],
    "airflow-1_10_9": [
        dbnd_package("dbnd-airflow"),
        "psycopg2>=2.7.4,<2.8",
        "apache-airflow==1.10.9",
        "cattrs==1.0.0",  # airflow requires ~0.9 but it's py2 incompatible (bug)
    ],
    "airflow-export": [dbnd_package("dbnd-airflow-export")],
    "airflow-operator": [dbnd_package("dbnd-airflow-operator")],
    "airflow-versioned-dag": [dbnd_package("dbnd-airflow-versioned-dag")],
    "aws": [dbnd_package("dbnd-aws")],
    "azure": [dbnd_package("dbnd-azure")],
    "databricks": [dbnd_package("dbnd-databricks")],
    "docker": [dbnd_package("dbnd-docker")],
    "gcp": [dbnd_package("dbnd-gcp")],
    "hdfs": [dbnd_package("dbnd-hdfs")],
    "mlflow": [dbnd_package("dbnd-mlflow")],
    "qubole": [dbnd_package("dbnd-qubole")],
    "spark": [dbnd_package("dbnd-spark")],
    "tensorflow": [dbnd_package("dbnd-tensorflow")],
}

# Aliases:
EXTRAS_REQUIRE["k8s"] = EXTRAS_REQUIRE["docker"]

setuptools.setup(
    name="databand",
    install_requires=[dbnd_package("dbnd")],
    extras_require=EXTRAS_REQUIRE,
)
