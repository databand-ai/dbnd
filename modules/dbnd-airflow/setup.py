# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-airflow",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "packaging"],
    # Only for orchestration, tracking users should install Airflow manually before DBND
    # The best way to install airflow is manually with constraints beforehand.
    # For example:
    # pip install apache-airflow  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt
    extras_require=dict(
        tests=[
            # airflow 2.3 has a problem with pluggy <1.0, that makes pytest 4 incompatible
            "pytest==6.2.5",
            "coverage==7.0.1",
            "pytest-cov==3.0.0",
            "boto3",
            "mock",
            "sh",
        ]
    ),
)
