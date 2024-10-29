# © Copyright Databand.ai, an IBM Company 2024

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-monitor",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "setuptools", "prometheus_client"],
    extras_require={"tests": ["pytest", "mock"]},
)
