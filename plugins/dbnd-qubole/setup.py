from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-qubole",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "dbnd-spark==" + version, "qds-sdk==1.13.2"],
    extras_require=dict(tests=[]),
    entry_points={"dbnd": ["dbnd-qubole = dbnd_qubole._plugin"]},
)
