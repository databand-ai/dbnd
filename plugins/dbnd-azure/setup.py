# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-azure",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "azure-storage-blob>=2.1.0,<3.0.0"],
    extras_require=dict(tests=["dbnd_test_scenarios==" + version]),
    entry_points={"dbnd": ["dbnd-azure = dbnd_azure._plugin"]},
)
