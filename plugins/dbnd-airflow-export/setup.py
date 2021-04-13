from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-airflow-export",
    package_dir={"": "src"},
    # we are not requiring airflow, as this plugin should be installed into existing airflow deployment
    install_requires=["dbnd==" + version, "setuptools", "six"],
    extras_require={
        "tests": [
            "pytest==4.5.0",
            "mock",
            "WTForms<2.3.0",
            "apache-airflow==1.10.9",
            "sh",
            "SQLAlchemy==1.3.15",
        ],
    },
    entry_points={
        "airflow.plugins": [
            "dbnd_airflow_export = dbnd_airflow_export.dbnd_airflow_export_plugin:DataExportAirflowPlugin"
        ]
    },
)
