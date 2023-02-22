# © Copyright Databand.ai, an IBM Company 2022

from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")
config = read_configuration(CFG_PATH)

setuptools.setup(
    name="dbnd-test-scenarios",
    package_dir={"": "src"},
    install_requires=[],
    extras_require=dict(tests=[]),
)
