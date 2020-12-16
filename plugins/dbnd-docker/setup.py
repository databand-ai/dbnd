from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-docker",
    package_dir={"": "src"},
    install_requires=[
        "dbnd==" + version,
        "dbnd-airflow==" + version,
        "docker~=3.0",
        # k8s
        "kubernetes==9.0.0",
        "cryptography>=2.0.0",
    ],
    entry_points={"dbnd": ["dbnd-docker = dbnd_docker._plugin"]},
)
