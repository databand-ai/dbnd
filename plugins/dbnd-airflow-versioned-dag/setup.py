# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-airflow-versioned-dag",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "dbnd-airflow==" + version],
    entry_points={
        "airflow.plugins": [
            "dbnd_webserver_plugin = dbnd_airflow.plugins.dbnd_airflow_webserver_plugin:DatabandAirflowWebserverPlugin"
        ]
    },
)
