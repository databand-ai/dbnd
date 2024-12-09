# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-airflow-monitor",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "dbnd-monitor==" + version, "setuptools"],
    extras_require={"tests": ["pytest", "mock", "sh"]},
)
