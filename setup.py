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


def dbnd_package(name, extras=None):
    pkg = name
    if extras:
        pkg += "[%s]" % ",".join(extras)

    pkg += "==%s" % VERSION
    return pkg


EXTRAS_REQUIRE = {
    "airflow": [dbnd_package("dbnd-airflow")],
    "airflow_bundle": [dbnd_package("dbnd-airflow", ["airflow"])],
    "airflow_1_10_7": [dbnd_package("dbnd-airflow", ["airflow_1_10_7"])],
    "airflow_1_10_8": [dbnd_package("dbnd-airflow", ["airflow_1_10_8"])],
    "airflow_1_10_9": [dbnd_package("dbnd-airflow", ["airflow_1_10_9"])],
    "airflow_1_10_10": [dbnd_package("dbnd-airflow", ["airflow_1_10_10"])],
    "airflow_1_10_11": [dbnd_package("dbnd-airflow", ["airflow_1_10_11"])],
    "airflow_1_10_12": [dbnd_package("dbnd-airflow", ["airflow_1_10_12"])],
    "airflow_1_10_13": [dbnd_package("dbnd-airflow", ["airflow_1_10_13"])],
    "airflow_1_10_14": [dbnd_package("dbnd-airflow", ["airflow_1_10_14"])],
    "airflow_1_10_15": [dbnd_package("dbnd-airflow", ["airflow_1_10_15"])],
    "airflow-export": [dbnd_package("dbnd-airflow-export")],
    "airflow-auto-tracking": [dbnd_package("dbnd-airflow-auto-tracking")],
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
    "postgres": [dbnd_package("dbnd-postgres")],
    "redshift": [dbnd_package("dbnd-redshift")],
    "snowflake": [dbnd_package("dbnd-snowflake")],
    "tensorflow": [dbnd_package("dbnd-tensorflow")],
    "luigi": [dbnd_package("dbnd-luigi")],
}

# Aliases:
EXTRAS_REQUIRE["k8s"] = EXTRAS_REQUIRE["docker"]

setuptools.setup(
    name="databand",
    install_requires=[dbnd_package("dbnd")],
    extras_require=EXTRAS_REQUIRE,
)
