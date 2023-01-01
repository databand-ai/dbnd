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
        "python-json-logger",  # TODO: might need to remove when on-prem support needed
        "prometheus-client",
        "sentry-sdk",
    ],
    extras_require={"tests": ["pytest", "mock", "pytest-cov==3.0.0"]},
    entry_points={
        "console_scripts": [
            "dbnd-datastage-monitor = dbnd_datastage_monitor.multiserver.datastage_multiserver:datastage_monitor"
        ]
    },
)
