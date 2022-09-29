# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-datastage-monitor",
    package_dir={"": "src"},
    install_requires=[
        "dbnd==" + version,
        "dbnd-airflow-monitor==" + version,
        "requests>=2.25.0",
        "PyJWT==1.7.1",
    ],
    extras_require={"tests": ["pytest", "mock"]},
    entry_points={
        "dbnd": ["datastage-monitor = dbnd_datastage_monitor._plugin"],
        "console_scripts": [
            "dbnd-datastage-monitor = dbnd_datastage_monitor.multiserver.datastage_multiserver:datastage_monitor"
        ],
    },
)
