# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-spark",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version],
    extras_require={
        "tests": [
            "pandas<2.0.0,>=0.17.1",
            'pyspark==2.4.4;python_version<"3.8"',
            'pyspark==3.3.1;python_version>="3.8"',
            "pytest-spark==0.6.0",
        ]
    },
    entry_points={"dbnd": ["dbnd-spark = dbnd_spark._plugin"]},
)
