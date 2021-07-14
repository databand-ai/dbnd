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
    install_requires=[
        'sqlparse==0.3.1; python_version>="3.0"',
        "snowflake-connector-python",
        "numpy",
        "dbnd==" + version,
    ],
    entry_points={},
)
