# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-databricks",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "dbnd-spark==" + version],
    extras_require=dict(tests=[]),
    entry_points={"dbnd": ["dbnd-databricks = dbnd_databricks._plugin"]},
)
