# © Copyright Databand.ai, an IBM Company 2022

import os

import setuptools

from setuptools.config import read_configuration


BASE_PATH = os.path.dirname(__file__)
CFG_PATH = os.path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-snowflake",
    package_dir={"": "src"},
    install_requires=[
        "sqlparse",
        "snowflake-connector-python<2.6.0",
        "numpy",
        "dbnd==" + version,
        "certifi<2021.0.0",  # fix conflict with snowflake-connector-python
    ],
    entry_points={},
)
