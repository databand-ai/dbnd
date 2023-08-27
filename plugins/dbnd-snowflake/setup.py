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
    install_requires=["sqlparse", "dbnd==" + version],
    extras_require={
        "tests": [
            'pandas==1.3.5;python_version<"3.8"',  # M1 is supported
            'pandas==1.4.0;python_version>="3.8"',
            'numpy==1.21.6;python_version<"3.8"',  # M1 is supported
            'numpy==1.22.4;python_version>="3.8"',
            "snowflake-connector-python",
        ]
    },
)
