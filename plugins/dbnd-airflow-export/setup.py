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
    install_requires=["apache-airflow>=1.10.3", "setuptools"],
    entry_points={
        "airflow.plugins": [
            "dbnd_airflow_export = dbnd_airflow_export.dbnd_airflow_export_plugin:DataExportAirflowPlugin"
        ]
    },
)
