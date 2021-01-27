import os

import setuptools

from setuptools.config import read_configuration


BASE_PATH = os.path.dirname(__file__)
CFG_PATH = os.path.join(BASE_PATH, "setup.cfg")

config = read_configuration(CFG_PATH)
version = config["metadata"]["version"]

setuptools.setup(
    name="dbnd-snowflake",
    package_dir={"": "src"},
    install_requires=["snowflake-connector-python", "numpy", "dbnd==" + version],
    entry_points={},
)
