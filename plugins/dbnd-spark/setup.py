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
    extras_require={"tests": ["pyspark==2.4.4", "pytest-spark==0.6.0"]},
    entry_points={"dbnd": ["dbnd-spark = dbnd_spark._plugin"]},
)
