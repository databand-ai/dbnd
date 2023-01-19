# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

# requirements_for_airflow = [
#     "WTForms<2.3.0",  # fixing ImportError: cannot import name HTMLString at 2.3.0
#     "Werkzeug<1.0.0,>=0.15.0",
#     "psycopg2-binary>=2.7.4",
#     "SQLAlchemy==1.3.18",  # Make sure Airflow uses SQLAlchemy 1.3.15, Airflow is incompatible with SQLAlchemy 1.4.x
#     "marshmallow<3.0.0,>=2.18.0",
#     "marshmallow-sqlalchemy<0.24.0,>=0.16.1;python_version>='3.0'",
#     "itsdangerous<2.0,>=0.24",
#     "tenacity>=4.12",
# ]

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
            # # airflow support
            "dbnd_test_scenarios==" + version,
            # airflow 2.3 has a problem with pluggy <1.0, that makes pytest 4 incompatible
            "pytest==6.2.4",
            "coverage==7.0.1",
            "boto3",
            "mock",
            "sh",
        ]
    ),
    entry_points={"dbnd": ["dbnd-airflow = dbnd_airflow._plugin"]},
)
