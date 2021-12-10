from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

requirements_for_airflow = [
    "WTForms<2.3.0",  # fixing ImportError: cannot import name HTMLString at 2.3.0
    "Werkzeug<1.0.0,>=0.15.0",
    "psycopg2-binary>=2.7.4",
    "SQLAlchemy==1.3.18",  # Make sure Airflow uses SQLAlchemy 1.3.15, Airflow is incompatible with SQLAlchemy 1.4.x
    "marshmallow<3.0.0,>=2.18.0",
    "marshmallow-sqlalchemy<0.24.0,>=0.16.1;python_version>='3.0'",
    "itsdangerous<2.0,>=0.24",
    "tenacity>=4.12",
    "pyparsing<3,>=2.0.2",  # Fix requirement of packaging library
]

setuptools.setup(
    name="dbnd-airflow",
    package_dir={"": "src"},
    install_requires=[
        "dbnd==" + version,
        "packaging",
        "idna<3,>=2.5",  # fix compatibility with requests==2.23.0 from apache-airflow
    ],
    # Only for orchestration, tracking users should install Airflow manually before DBND
    # The best way to install airflow is manually with constraints beforehand.
    # For example:
    # pip install apache-airflow  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-{PYTHON_VERSION}.txt
    extras_require=dict(
        airflow_1_10_7=requirements_for_airflow + ["apache-airflow==1.10.7"],
        airflow_1_10_8=requirements_for_airflow + ["apache-airflow==1.10.8"],
        airflow_1_10_9=requirements_for_airflow + ["apache-airflow==1.10.9"],
        airflow_1_10_10=requirements_for_airflow + ["apache-airflow==1.10.10"],
        airflow_1_10_11=requirements_for_airflow + ["apache-airflow==1.10.11"],
        airflow_1_10_12=requirements_for_airflow + ["apache-airflow==1.10.12"],
        airflow_1_10_13=requirements_for_airflow + ["apache-airflow==1.10.13"],
        airflow_1_10_14=requirements_for_airflow + ["apache-airflow==1.10.14"],
        airflow_1_10_15=requirements_for_airflow + ["apache-airflow==1.10.15"],
        airflow_2_0_2=[
            "WTForms<2.3.0",
            "psycopg2-binary>=2.7.4",
            "apache-airflow==2.0.2",
            "apache-airflow-providers-apache-spark==1.0.3",
            # Airflow 2.0 installs versions 3.3.5 which has bad dependency to newer version of importlib-metadata
            "Markdown==3.3.4",
        ],
        airflow=requirements_for_airflow + ["apache-airflow==1.10.10"],
        tests=[
            # airflow support
            "pandas<2.0.0,>=0.17.1",
            # azure
            "azure-storage-blob",
            # aws
            "httplib2>=0.9.2",
            "boto3<=1.15.18",
            "s3fs",
            # gcs
            "httplib2>=0.9.2",
            "google-api-python-client>=1.6.0, <2.0.0dev",
            "google-auth>=1.0.0, <2.0.0dev",
            "google-auth-httplib2>=0.0.1",
            "google-cloud-container>=0.1.1",
            "PyOpenSSL",
            "pandas-gbq",
            # docker
            "docker~=3.0",
            "idna<=2.7",  # conflict with requests (require 2.8 <)
            # k8s
            "kubernetes==9.0.0",
            "cryptography>=2.0.0",
            "WTForms<2.3.0",  # fixing ImportError: cannot import name HTMLString at 2.3.0
            "dbnd_test_scenarios==" + version,
            "SQLAlchemy==1.3.18",
            "pytest==4.5.0",
            "mock",
            "sh",
        ],
    ),
    entry_points={"dbnd": ["dbnd-airflow = dbnd_airflow._plugin"],},
)
