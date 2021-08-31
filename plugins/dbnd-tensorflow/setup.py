from os import path

import setuptools

from setuptools.config import read_configuration


BASE_PATH = path.dirname(__file__)
CFG_PATH = path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-tensorflow",
    package_dir={"": "src"},
    install_requires=["dbnd==" + version, "tensorflow>=2.5.0,<2.7"],
    extras_require=dict(tests=[]),
    entry_points={"dbnd": ["dbnd-tensorflow = dbnd_tensorflow._plugin"]},
)
