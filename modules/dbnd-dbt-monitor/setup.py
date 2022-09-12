# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-dbt-monitor",
    package_dir={"": "src"},
    install_requires=[
        "dbnd==" + version,
        "dbnd-airflow-monitor==" + version,
        "requests>=2.25.0",
    ],
    extras_require={"tests": ["pytest", "mock"]},
    entry_points={
        "dbnd": ["dbt-monitor = dbnd_dbt_monitor._plugin"],
        "console_scripts": [
            "dbnd-dbt-monitor = dbnd_dbt_monitor.multiserver.dbt_multiserver:dbt_monitor"
        ],
    },
)
