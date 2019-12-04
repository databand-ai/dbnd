from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-hdfs",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "pyox", "hdfs"],
    extras_require=dict(kerberos=["requests_kerberos", "hdfs"], tests=[]),
    entry_points={"dbnd": ["dbnd-hdfs = dbnd_hdfs._plugin"]},
)
