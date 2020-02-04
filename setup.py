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
    "airflow": [dbnd_package("dbnd-airflow"), "psycopg2>=2.7.4,<2.8"],
    "airflow-versioned-dag": [dbnd_package("dbnd-airflow-versioned-dag")],
    "airflow-export": [dbnd_package("dbnd-airflow-export")],
    "aws": [dbnd_package("dbnd-aws")],
    "azure": [dbnd_package("dbnd-azure")],
    "databricks": [dbnd_package("dbnd-databricks")],
    "qubole": [dbnd_package("dbnd-qubole")],
    "gcp": [dbnd_package("dbnd-gcp")],
    "mlflow": [dbnd_package("dbnd-mlflow")],
    "docker": [dbnd_package("dbnd-docker")],
    "k8s": [dbnd_package("dbnd-docker")],
    "spark": [dbnd_package("dbnd-spark")],
    "hdfs": [dbnd_package("dbnd-hdfs")],
}

setuptools.setup(
    name="databand",
    install_requires=[dbnd_package("dbnd")],
    extras_require=EXTRAS_REQUIRE,
)
